package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/chinudotdev/pith/protocol"
)

// AgentLoop runs the main agent loop: stream LLM response → execute tools → repeat.
// It is stateless — takes a snapshot, returns events, owns no state between calls.
//
// The loop structure:
//
//	outer: while follow-up messages exist
//	  inner: while tool calls or steering messages exist
//	    1. Inject pending messages (steering)
//	    2. Stream assistant response (LLM call)
//	    3. Execute tool calls (sequential or parallel)
//	    4. Emit TurnEnd
//	    5. prepareNextTurn (swap model/context)
//	    6. shouldStopAfterTurn? -> break
//	    7. Poll steering messages
//	  Poll follow-up messages
//	Emit AgentEnd
func AgentLoop(
	ctx context.Context,
	prompts []protocol.Message,
	agentCtx AgentContext,
	config LoopConfig,
	sink EventSink,
) ([]protocol.Message, error) {
	return runLoop(ctx, prompts, agentCtx, config, sink, false)
}

// AgentLoopContinue continues the loop from the current context without adding new messages.
func AgentLoopContinue(
	ctx context.Context,
	agentCtx AgentContext,
	config LoopConfig,
	sink EventSink,
) ([]protocol.Message, error) {
	return runLoop(ctx, nil, agentCtx, config, sink, true)
}

// exitReason represents why the loop stopped.
// Per the Loop Exit Model contract (§5), every exit emits AgentEnd.
type exitReason int

const (
	exitStop         exitReason = iota // natural end: no tool calls, no steering, no follow-up
	exitAbort                          // context cancelled
	exitError                          // provider or tool error
	exitHookStop                       // shouldStopAfterTurn returned true
	exitToolTerminate                  // a tool result requested termination
)

func runLoop(
	ctx context.Context,
	prompts []protocol.Message,
	agentCtx AgentContext,
	config LoopConfig,
	sink EventSink,
	isContinue bool,
) ([]protocol.Message, error) {
	// Validate config
	if config.StreamFn == nil {
		return nil, &protocol.Error{Code: protocol.ErrInvalidArgument, Message: "LoopConfig.StreamFn is required"}
	}

	// Build tool lookup
	toolMap := make(map[string]*AgentTool)
	for i := range config.Tools {
		toolMap[config.Tools[i].Name] = &config.Tools[i]
	}

	// Default tool execution policy
	if config.ToolExecution == nil {
		config.ToolExecution = ParallelPolicy{}
	}

	// Working message list
	messages := make([]protocol.Message, len(agentCtx.Messages))
	copy(messages, agentCtx.Messages)

	// Add prompt messages
	if !isContinue && len(prompts) > 0 {
		for _, p := range prompts {
			messages = append(messages, p)
			sink(LoopEvent{
				Type:    LoopMessageStart,
				Message: p,
			})
			sink(LoopEvent{
				Type:    LoopMessageEnd,
				Message: p,
			})
		}
	}

	// Emit AgentStart
	sink(LoopEvent{Type: LoopAgentStart})

	var allNewMessages []protocol.Message
	allNewMessages = append(allNewMessages, prompts...)

	// Main loop (outer: follow-up, inner: steering + tool calls)
	var loopErr error
outer:
	for {
		// Inner loop: stream response, execute tools, inject steering messages
	inner:
		for {
			// Inject any steering messages before the next LLM call
			if config.SteeringDrain != nil {
				if steered := config.SteeringDrain(); len(steered) > 0 {
					for _, sm := range steered {
						messages = append(messages, sm)
						allNewMessages = append(allNewMessages, sm)
						sink(LoopEvent{Type: LoopMessageStart, Message: sm})
						sink(LoopEvent{Type: LoopMessageEnd, Message: sm})
					}
				}
			}

			// Stream LLM response
			assistantMsg, toolCalls, err := streamResponse(ctx, messages, agentCtx, config, sink)
			if err != nil {
				if config.ErrorChannel != nil {
					config.ErrorChannel.OnError(&ProviderError{
						Code:      protocol.ErrUnknown,
						Message:   err.Error(),
						Retryable: false,
						Cause:     err,
					})
				}
				loopErr = err
				break outer // exitError
			}

			messages = append(messages, *assistantMsg)
			allNewMessages = append(allNewMessages, *assistantMsg)

			// Emit MessageStart + MessageEnd for the assistant message
			sink(LoopEvent{Type: LoopMessageStart, Message: *assistantMsg})
			sink(LoopEvent{Type: LoopMessageEnd, Message: *assistantMsg})

			// No tool calls → done with inner loop
			if len(toolCalls) == 0 {
				sink(LoopEvent{Type: LoopTurnEnd, Assistant: assistantMsg, ToolResults: nil})

				if config.Hooks.ShouldStopAfterTurn != nil {
					stopCtx := ShouldStopAfterTurnContext{Message: *assistantMsg, NewMessages: allNewMessages, Context: agentCtx}
					if config.Hooks.ShouldStopAfterTurn(stopCtx) {
						break outer // exitHookStop
					}
				}
				break inner // exitStop (check follow-up)
			}

			// Execute tool calls
			toolResults := executeToolCalls(ctx, toolCalls, *assistantMsg, toolMap, config, sink, agentCtx)

			for _, tr := range toolResults {
				messages = append(messages, tr)
				allNewMessages = append(allNewMessages, tr)
			}

			sink(LoopEvent{Type: LoopTurnEnd, Assistant: assistantMsg, ToolResults: toolResults})

			if config.Hooks.ShouldStopAfterTurn != nil {
				stopCtx := ShouldStopAfterTurnContext{Message: *assistantMsg, ToolResults: toolResults, Context: agentCtx, NewMessages: allNewMessages}
				if config.Hooks.ShouldStopAfterTurn(stopCtx) {
					break outer // exitHookStop
				}
			}

			// Check if any tool result requested termination
			for _, tr := range toolResults {
				if details, ok := tr.Details.(ToolResultDetails); ok && details.Terminate {
					break outer // exitToolTerminate
				}
			}

			// prepareNextTurn
			if config.Hooks.PrepareNextTurn != nil {
				prepCtx := PrepareNextTurnContext{Message: *assistantMsg, ToolResults: toolResults, Context: agentCtx, NewMessages: allNewMessages}
				if update := config.Hooks.PrepareNextTurn(prepCtx); update != nil {
					if update.Model != nil {
						config.Model = *update.Model
					}
					if update.Context != nil {
						agentCtx = *update.Context
					}
				}
			}

			// Check context cancellation (abort takes priority)
			select {
			case <-ctx.Done():
				if config.ErrorChannel != nil {
					config.ErrorChannel.OnError(&AbortError{Reason: ctx.Err().Error()})
				}
				loopErr = ctx.Err()
				break outer // exitAbort
			default:
			}
		} // end inner loop

		// Check for follow-up messages
		if config.FollowUpDrain != nil {
			if followUps := config.FollowUpDrain(); len(followUps) > 0 {
				for _, fm := range followUps {
					messages = append(messages, fm)
					allNewMessages = append(allNewMessages, fm)
					sink(LoopEvent{Type: LoopMessageStart, Message: fm})
					sink(LoopEvent{Type: LoopMessageEnd, Message: fm})
				}
				continue // restart outer loop with follow-up messages
			}
		}

		break // no follow-ups, exit
	} // end outer loop

	// Emit AgentEnd (per §5: every exit emits AgentEnd, no exceptions)
	sink(LoopEvent{
		Type:     LoopAgentEnd,
		Messages: allNewMessages,
	})

	return allNewMessages, loopErr
}

// ToolResultDetails is a helper type that can be embedded in tool result Details
// to signal loop termination.
type ToolResultDetails struct {
	Terminate bool `json:"terminate,omitempty"`
}

// streamResponse calls the LLM and accumulates the assistant message.
func streamResponse(
	ctx context.Context,
	messages []protocol.Message,
	agentCtx AgentContext,
	config LoopConfig,
	sink EventSink,
) (*protocol.AssistantMessage, []protocol.ToolCall, error) {
	// Convert messages using the pipeline
	llmMessages := messages
	if config.ConvertToLlm != nil {
		llmMessages = config.ConvertToLlm(messages)
	}

	// Build protocol context
	pctx := protocol.Context{
		SystemPrompt: agentCtx.SystemPrompt,
		Messages:     llmMessages,
		Tools:        toolsToProtocolTools(config.Tools),
	}

	// Build stream options
	opts := protocol.StreamOptions{
		Reasoning:       config.ThinkingLevel,
		ThinkingBudgets: config.ThinkingBudgets,
		Transport:       config.Transport,
		SessionID:       config.SessionID,
		MaxRetryDelayMs: config.MaxRetryDelayMs,
	}

	// Call the stream function
	ch, err := config.StreamFn(ctx, config.Model, pctx, opts)
	if err != nil {
		return nil, nil, err
	}

	// Accumulate the response
	var assistant protocol.AssistantMessage
	var toolCalls []protocol.ToolCall
	var textContent []protocol.ContentBlock

	sink(LoopEvent{Type: LoopTurnStart})

	for event := range ch {
		switch event.Type {
		case protocol.EventStart:
			if event.Partial != nil {
				assistant = *event.Partial
			}

		case protocol.EventTextStart, protocol.EventTextDelta, protocol.EventTextEnd:
			sink(LoopEvent{
				Type:        LoopMessageUpdate,
				StreamEvent: &event,
			})
			if event.Type == protocol.EventTextEnd && event.Content != "" {
				textContent = append(textContent, protocol.TextContent{
					Type: "text",
					Text: event.Content,
				})
			}

		case protocol.EventThinkingStart, protocol.EventThinkingDelta, protocol.EventThinkingEnd:
			sink(LoopEvent{
				Type:        LoopMessageUpdate,
				StreamEvent: &event,
			})

		case protocol.EventToolCallStart, protocol.EventToolCallDelta, protocol.EventToolCallEnd:
			sink(LoopEvent{
				Type:        LoopMessageUpdate,
				StreamEvent: &event,
			})
			if event.Type == protocol.EventToolCallEnd && event.ToolCall != nil {
				toolCalls = append(toolCalls, *event.ToolCall)
			}

		case protocol.EventDone:
			if event.Message != nil {
				assistant = *event.Message
			}

			case protocol.EventError:
			if event.Message != nil {
				assistant = *event.Message
			}
			if event.Reason == protocol.StopAborted {
				return nil, nil, ctx.Err()
			}
		}
	}

	// Build the final assistant message if not fully populated
	if assistant.Role == "" {
		assistant = protocol.AssistantMessage{
			Role:      "assistant",
			Content:   textContent,
			API:       config.Model.API,
			Provider:  config.Model.Provider,
			Model:     config.Model.ID,
			Timestamp: protocol.Now(),
		}
		// No complete message from provider — manually append tool calls
		for _, tc := range toolCalls {
			assistant.Content = append(assistant.Content, tc)
		}
	} else {
		// Ensure tool calls are present in content even if provider
		// didn't include them in the final message
		hasToolCalls := false
		for _, block := range assistant.Content {
			if _, ok := block.(protocol.ToolCall); ok {
				hasToolCalls = true
				break
			}
		}
		if !hasToolCalls {
			for _, tc := range toolCalls {
				assistant.Content = append(assistant.Content, tc)
			}
		}
	}

	return &assistant, toolCalls, nil
}

// executeToolCalls runs tool calls either sequentially or in parallel.
func executeToolCalls(
	ctx context.Context,
	toolCalls []protocol.ToolCall,
	assistantMsg protocol.AssistantMessage,
	toolMap map[string]*AgentTool,
	config LoopConfig,
	sink EventSink,
	agentCtx AgentContext,
) []protocol.ToolResultMessage {
	isParallel := config.ToolExecution.IsParallel

	// Use an indexed result slice to preserve original tool call order (INV-1).
	// Unknown tools produce immediate error results; known tools are scheduled
	// for sequential or parallel execution.
	results := make([]protocol.ToolResultMessage, len(toolCalls))

	// Track which positions need sequential vs parallel execution.
	type indexedCall struct {
		index int
		call  protocol.ToolCall
	}
	var sequentialCalls, parallelCalls []indexedCall

	for i, tc := range toolCalls {
		tool, ok := toolMap[tc.Name]
		if !ok {
			// Unknown tool — return error result at the correct position
			results[i] = protocol.ToolResultMessage{
				Role:       "toolResult",
				ToolCallID: tc.ID,
				ToolName:   tc.Name,
				Content:    []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("unknown tool: %s", tc.Name)}},
				IsError:    true,
				Timestamp:  protocol.Now(),
			}
			continue
		}

		// Determine execution mode
		parallel := isParallel(tc.Name)
		if tool.Mode == ModeSequential {
			parallel = false
		} else if tool.Mode == ModeParallel {
			parallel = true
		}

		ic := indexedCall{index: i, call: tc}
		if parallel {
			parallelCalls = append(parallelCalls, ic)
		} else {
			sequentialCalls = append(sequentialCalls, ic)
		}
	}

	// Start parallel calls without waiting for sequential calls to finish.
	if len(parallelCalls) > 0 {
		var mu sync.Mutex
		var wg sync.WaitGroup

		for _, ic := range parallelCalls {
			wg.Add(1)
			go func(idx int, call protocol.ToolCall) {
				defer wg.Done()
				result := executeOneToolCall(ctx, call, assistantMsg, toolMap, config, sink, agentCtx)
				mu.Lock()
				results[idx] = result
				mu.Unlock()
			}(ic.index, ic.call)
		}

		// Execute sequential calls concurrently with parallel calls.
		for _, ic := range sequentialCalls {
			results[ic.index] = executeOneToolCall(ctx, ic.call, assistantMsg, toolMap, config, sink, agentCtx)
		}

		wg.Wait()
	} else {
		for _, ic := range sequentialCalls {
			results[ic.index] = executeOneToolCall(ctx, ic.call, assistantMsg, toolMap, config, sink, agentCtx)
		}
	}

	return results
}

// executeOneToolCall executes a single tool call with hooks.
func executeOneToolCall(
	ctx context.Context,
	tc protocol.ToolCall,
	assistantMsg protocol.AssistantMessage,
	toolMap map[string]*AgentTool,
	config LoopConfig,
	sink EventSink,
	agentCtx AgentContext,
) protocol.ToolResultMessage {
	tool, ok := toolMap[tc.Name]
	if !ok {
		return protocol.ToolResultMessage{
			Role:       "toolResult",
			ToolCallID: tc.ID,
			ToolName:   tc.Name,
			Content:    []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("unknown tool: %s", tc.Name)}},
			IsError:    true,
			Timestamp:  protocol.Now(),
		}
	}

	// Parse arguments
	var args map[string]any
	if tc.Arguments != "" {
		if err := json.Unmarshal([]byte(tc.Arguments), &args); err != nil {
			return protocol.ToolResultMessage{
				Role:       "toolResult",
				ToolCallID: tc.ID,
				ToolName:   tc.Name,
				Content:    []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("invalid arguments: %s", err)}},
				IsError:    true,
				Timestamp:  protocol.Now(),
			}
		}
	}

	// Prepare arguments
	if tool.PrepareArgs != nil {
		prepared, err := tool.PrepareArgs(args)
		if err != nil {
			return protocol.ToolResultMessage{
				Role:       "toolResult",
				ToolCallID: tc.ID,
				ToolName:   tc.Name,
				Content:    []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("argument preparation failed: %s", err)}},
				IsError:    true,
				Timestamp:  protocol.Now(),
			}
		}
		args = prepared
	}

	// Emit ToolExecutionStart
	sink(LoopEvent{
		Type:       LoopToolExecutionStart,
		ToolCallID: tc.ID,
		ToolName:   tc.Name,
		Args:       args,
	})

	// beforeToolCall hook
	if config.Hooks.BeforeToolCall != nil {
		beforeCtx := BeforeToolCallContext{
			AssistantMessage: assistantMsg,
			ToolCall:         tc,
			Args:             args,
			Context:          agentCtx,
		}
		if result := config.Hooks.BeforeToolCall(beforeCtx, ctx.Done()); result != nil && result.Block {
			result := protocol.ToolResultMessage{
				Role:       "toolResult",
				ToolCallID: tc.ID,
				ToolName:   tc.Name,
				Content:    []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("tool call blocked: %s", result.Reason)}},
				IsError:    true,
				Timestamp:  protocol.Now(),
			}
			sink(LoopEvent{
				Type:       LoopToolExecutionEnd,
				ToolCallID: tc.ID,
				ToolName:   tc.Name,
				Result:     result,
				IsError:    true,
			})
			return result
		}
	}

	// Execute the tool
	var toolResult ToolResult
	if tool.Execute != nil {
		signal := ctx.Done()
		onUpdate := func(partial any) {
			sink(LoopEvent{
				Type:          LoopToolExecutionUpdate,
				ToolCallID:    tc.ID,
				ToolName:      tc.Name,
				Args:          args,
				PartialResult: partial,
			})
		}
		toolResult = tool.Execute(tc.ID, args, signal, onUpdate)
	} else {
		toolResult = ToolResult{
			Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "tool has no execute function"}},
		}
	}

	isError := false

	// afterToolCall hook
	if config.Hooks.AfterToolCall != nil {
		afterCtx := AfterToolCallContext{
			AssistantMessage: assistantMsg,
			ToolCall:         tc,
			Args:             args,
			Result:           toolResult,
			IsError:          isError,
			Context:          agentCtx,
		}
		if result := config.Hooks.AfterToolCall(afterCtx, ctx.Done()); result != nil {
			if result.Content != nil {
				toolResult.Content = result.Content
			}
			if result.Details != nil {
				toolResult.Details = result.Details
			}
			isError = result.IsError
			if result.Terminate {
				toolResult.Terminate = true
			}
		}
	}

	// Emit ToolExecutionEnd
	sink(LoopEvent{
		Type:       LoopToolExecutionEnd,
		ToolCallID: tc.ID,
		ToolName:   tc.Name,
		Result:     toolResult,
		IsError:    isError,
	})

	// Build ToolResultMessage
	resultMsg := protocol.ToolResultMessage{
		Role:       "toolResult",
		ToolCallID: tc.ID,
		ToolName:   tc.Name,
		Content:    toolResult.Content,
		Details:    toolResult.Details,
		IsError:    isError,
		Timestamp:  protocol.Now(),
	}

	// Emit MessageStart + MessageEnd for the tool result
	sink(LoopEvent{
		Type:    LoopMessageStart,
		Message: resultMsg,
	})
	sink(LoopEvent{
		Type:    LoopMessageEnd,
		Message: resultMsg,
	})

	return resultMsg
}

// toolsToProtocolTools converts AgentTools to protocol Tools.
func toolsToProtocolTools(tools []AgentTool) []protocol.Tool {
	result := make([]protocol.Tool, len(tools))
	for i, t := range tools {
		result[i] = protocol.Tool{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.Parameters,
		}
	}
	return result
}
