package loop_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// TestStreamFnReceivesContext verifies that the loop passes context.Context
// to StreamFn, enabling abort propagation (Issue #4 regression test).
func TestStreamFnReceivesContext(t *testing.T) {
	model := testModel()

	var streamFnCtx context.Context
	baseStream := fakeStream(streamFakeConfig{Text: "Hello!", StreamDelay: 50 * time.Millisecond})
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtx = ctx
			return baseStream(ctx, m, pctx, opts)
		},
	}

	agentCtx := loop.AgentContext{
		SystemPrompt: "You are helpful.",
		Messages:     nil,
	}

	prompts := []protocol.Message{
		protocol.UserMessage{
			Role:      "user",
			Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}},
			Timestamp: protocol.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if streamFnCtx == nil {
		t.Fatal("StreamFn should receive a non-nil context.Context")
	}

	if streamFnCtx != ctx {
		select {
		case <-streamFnCtx.Done():
			t.Error("streamFn context should not be done yet")
		default:
		}
	}
}

// TestLoopAbortReturnsError verifies that cancelling the loop context
// causes AgentLoop to return a non-nil error (not silently succeed).
// Regression test for Issue #4: abort was returning nil error because
// streamResponse swallowed EventError with StopAborted.
func TestLoopAbortReturnsError(t *testing.T) {
	model := testModel()

	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: fakeStream(streamFakeConfig{
			Text:        "Hello world, this is a long response that should be interrupted!",
			StreamDelay: 100 * time.Millisecond,
		}),
	}

	agentCtx := loop.AgentContext{
		SystemPrompt: "You are helpful.",
		Messages:     nil,
	}

	prompts := []protocol.Message{
		protocol.UserMessage{
			Role:      "user",
			Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}},
			Timestamp: protocol.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	loopDone := make(chan error, 1)
	go func() {
		_, err := loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
		loopDone <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-loopDone:
		if err == nil {
			t.Error("AgentLoop should return a non-nil error when context is cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for AgentLoop to finish")
	}
}

// TestLoopAbortCancelsStreamFn verifies that cancelling the loop context
// cancels the context passed to StreamFn.
func TestLoopAbortCancelsStreamFn(t *testing.T) {
	model := testModel()

	streamFnCtxCh := make(chan context.Context, 1)
	baseStream := fakeStream(streamFakeConfig{Text: "Hello!", StreamDelay: 100 * time.Millisecond})
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtxCh <- ctx
			return baseStream(ctx, m, pctx, opts)
		},
	}

	agentCtx := loop.AgentContext{
		SystemPrompt: "You are helpful.",
		Messages:     nil,
	}

	prompts := []protocol.Message{
		protocol.UserMessage{
			Role:      "user",
			Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}},
			Timestamp: protocol.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
	}()

	var streamFnCtx context.Context
	select {
	case streamFnCtx = <-streamFnCtxCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamFn to be called")
	}

	cancel()

	select {
	case <-streamFnCtx.Done():
	case <-time.After(2 * time.Second):
		t.Error("StreamFn context should be cancelled after loop context is cancelled")
	}

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Error("loop should finish after context cancellation")
	}
}

func TestProviderAbortWithoutContextCancellationReturnsError(t *testing.T) {
	model := testModel()
	streamFn := func(ctx context.Context, m protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		ch := make(chan protocol.StreamEvent, 2)
		go func() {
			defer close(ch)
			ch <- protocol.StreamEvent{Type: protocol.EventStart}
			ch <- protocol.StreamEvent{
				Type:   protocol.EventError,
				Reason: protocol.StopAborted,
				Message: &protocol.AssistantMessage{
					Role:         "assistant",
					ErrorMessage: "provider aborted",
				},
			}
		}()
		return ch, nil
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, loop.LoopConfig{Model: model, StreamFn: streamFn}, func(event loop.LoopEvent) {})
	if err == nil {
		t.Fatal("expected non-nil error when provider emits StopAborted")
	}
}

func TestNonAbortProviderErrorReturnsError(t *testing.T) {
	model := testModel()
	streamFn := func(ctx context.Context, m protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		ch := make(chan protocol.StreamEvent, 2)
		go func() {
			defer close(ch)
			ch <- protocol.StreamEvent{Type: protocol.EventStart}
			ch <- protocol.StreamEvent{
				Type:   protocol.EventError,
				Reason: protocol.StopError,
				Message: &protocol.AssistantMessage{
					Role:         "assistant",
					ErrorMessage: "provider failed",
				},
			}
		}()
		return ch, nil
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, loop.LoopConfig{Model: model, StreamFn: streamFn}, func(event loop.LoopEvent) {})
	if err == nil {
		t.Fatal("expected non-nil error when provider emits EventError")
	}
}

func TestSteeringDuringTextTurnContinuesInnerLoop(t *testing.T) {
	model := testModel()
	callCount := 0
	streamFn := func(ctx context.Context, m protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		callCount++
		ch := make(chan protocol.StreamEvent, 4)
		go func() {
			defer close(ch)
			ch <- protocol.StreamEvent{Type: protocol.EventStart}
			ch <- protocol.StreamEvent{Type: protocol.EventTextStart, ContentIndex: 0}
			ch <- protocol.StreamEvent{Type: protocol.EventTextDelta, ContentIndex: 0, Delta: "ok"}
			ch <- protocol.StreamEvent{
				Type:   protocol.EventDone,
				Reason: protocol.StopEnd,
				Message: &protocol.AssistantMessage{
					Role:       "assistant",
					Content:    []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "ok"}},
					StopReason: protocol.StopEnd,
					Timestamp:  protocol.Now(),
				},
			}
		}()
		return ch, nil
	}

	steeringProvided := false
	cfg := loop.LoopConfig{
		Model:    model,
		StreamFn: streamFn,
		SteeringDrain: func() []protocol.Message {
			if steeringProvided || callCount == 0 {
				return nil
			}
			steeringProvided = true
			return []protocol.Message{
				protocol.UserMessage{
					Role:      "user",
					Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "steer"}},
					Timestamp: protocol.Now(),
				},
			}
		},
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "start"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, cfg, func(event loop.LoopEvent) {})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount < 2 {
		t.Fatalf("expected inner loop to continue after steering; stream called %d times", callCount)
	}
}

func toolCallStreamFn(model protocol.ModelDescriptor, toolName string) loop.StreamFn {
	return func(ctx context.Context, m protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		ch := make(chan protocol.StreamEvent, 8)
		go func() {
			defer close(ch)
			partial := &protocol.AssistantMessage{
				Role: "assistant", API: model.API, Provider: model.Provider, Model: model.ID, Timestamp: protocol.Now(),
			}
			tc := protocol.ToolCall{Type: "toolCall", ID: "tc1", Name: toolName, Arguments: `{}`}
			ch <- protocol.StreamEvent{Type: protocol.EventStart, Partial: partial}
			ch <- protocol.StreamEvent{Type: protocol.EventToolCallStart, ContentIndex: 0, Partial: partial}
			ch <- protocol.StreamEvent{Type: protocol.EventToolCallEnd, ContentIndex: 0, ToolCall: &tc, Partial: partial}
			ch <- protocol.StreamEvent{
				Type: protocol.EventDone, Reason: protocol.StopTool,
				Message: &protocol.AssistantMessage{
					Role: "assistant", API: model.API, Provider: model.Provider, Model: model.ID,
					Content: []protocol.ContentBlock{tc}, StopReason: protocol.StopTool, Timestamp: protocol.Now(),
				},
			}
		}()
		return ch, nil
	}
}

func TestToolTerminateFromExecuteStopsLoop(t *testing.T) {
	model := testModel()
	streamCalls := 0
	cfg := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamCalls++
			return toolCallStreamFn(model, "stop_tool")(ctx, m, pctx, opts)
		},
		Tools: []loop.AgentTool{{
			Name: "stop_tool",
			Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
				return loop.ToolResult{
					Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "done"}},
					Terminate: true,
				}
			},
		}},
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "go"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, cfg, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if streamCalls != 1 {
		t.Fatalf("expected loop to stop after terminating tool; stream called %d times", streamCalls)
	}
}

func TestAfterToolCallTerminateStopsLoop(t *testing.T) {
	model := testModel()
	streamCalls := 0
	cfg := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamCalls++
			return toolCallStreamFn(model, "hook_stop")(ctx, m, pctx, opts)
		},
		Tools: []loop.AgentTool{{
			Name: "hook_stop",
			Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
				return loop.ToolResult{Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "ok"}}}
			},
		}},
		Hooks: loop.LoopHooks{
			AfterToolCall: func(ctx loop.AfterToolCallContext, signal <-chan struct{}) *loop.AfterToolCallResult {
				return &loop.AfterToolCallResult{Terminate: true}
			},
		},
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "go"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, cfg, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if streamCalls != 1 {
		t.Fatalf("expected loop to stop after AfterToolCall terminate; stream called %d times", streamCalls)
	}
}

func TestPrepareNextTurnThinkingLevelForwarded(t *testing.T) {
	model := testModel()
	streamCalls := 0
	var seenLevels []protocol.ThinkingLevel
	cfg := loop.LoopConfig{
		Model:         model,
		ThinkingLevel: protocol.ThinkingOff,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamCalls++
			seenLevels = append(seenLevels, opts.Reasoning)
			if streamCalls == 1 {
				return toolCallStreamFn(model, "noop")(ctx, m, pctx, opts)
			}
			return fakeStream(streamFakeConfig{Text: "second turn"})(ctx, m, pctx, opts)
		},
		Tools: []loop.AgentTool{{
			Name: "noop",
			Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
				return loop.ToolResult{Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "ok"}}}
			},
		}},
		Hooks: loop.LoopHooks{
			PrepareNextTurn: func(ctx loop.PrepareNextTurnContext) *loop.TurnUpdate {
				return &loop.TurnUpdate{ThinkingLevel: protocol.ThinkingHigh}
			},
		},
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "go"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, cfg, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if streamCalls != 2 {
		t.Fatalf("expected two stream calls, got %d", streamCalls)
	}
	if len(seenLevels) != 2 || seenLevels[0] != protocol.ThinkingOff || seenLevels[1] != protocol.ThinkingHigh {
		t.Fatalf("unexpected reasoning levels: %#v", seenLevels)
	}
}

func TestStreamOptionsTemperatureAndMaxTokensForwarded(t *testing.T) {
	model := testModel()
	temp := 0.5
	maxTokens := 128
	var seenOpts []protocol.StreamOptions
	cfg := loop.LoopConfig{
		Model:       model,
		Temperature: &temp,
		MaxTokens:   &maxTokens,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			seenOpts = append(seenOpts, opts)
			return fakeStream(streamFakeConfig{Text: "ok"})(ctx, m, pctx, opts)
		},
	}

	_, err := loop.AgentLoop(context.Background(), []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}}, Timestamp: protocol.Now()},
	}, loop.AgentContext{}, cfg, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seenOpts) != 1 {
		t.Fatalf("expected one stream call, got %d", len(seenOpts))
	}
	if seenOpts[0].Temperature == nil || *seenOpts[0].Temperature != temp {
		t.Fatalf("expected temperature %v, got %#v", temp, seenOpts[0].Temperature)
	}
	if seenOpts[0].MaxTokens == nil || *seenOpts[0].MaxTokens != maxTokens {
		t.Fatalf("expected max tokens %v, got %#v", maxTokens, seenOpts[0].MaxTokens)
	}
}
