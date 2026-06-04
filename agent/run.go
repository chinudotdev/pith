package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// --- Agent Config ---

// QueueConfig controls how the steering and follow-up queues drain.
type QueueConfig struct {
	SteeringMode DrainMode
	FollowUpMode DrainMode
}

// AgentConfig configures the creation of an Agent.
type AgentConfig struct {
	InitialState   *AgentState
	StreamFn       loop.StreamFn
	ConvertToLlm   loop.MessageTransformer
	QueueConfig    *QueueConfig
	SessionID      string
	ThinkingBudgets *protocol.ThinkingBudgets
	Transport      protocol.Transport
	MaxRetryDelayMs *int
	ToolExecution  loop.ToolExecutionPolicy
	BeforeToolCall func(ctx loop.BeforeToolCallContext, signal <-chan struct{}) *loop.BeforeToolCallResult
	AfterToolCall  func(ctx loop.AfterToolCallContext, signal <-chan struct{}) *loop.AfterToolCallResult
	PrepareNextTurn func(ctx loop.PrepareNextTurnContext) *loop.TurnUpdate
}

// --- Agent ---

// Agent is a stateful, in-memory session manager. It owns the transcript,
// model, tools, and queues. It delegates execution to L2.
type Agent struct {
	state    AgentState
	eventBus *EventBus
	registry *MessageRegistry

	steeringQueue *MessageQueue
	followUpQueue *MessageQueue

	// Config
	streamFn      loop.StreamFn
	convertToLlm  loop.MessageTransformer
	toolExecution loop.ToolExecutionPolicy
	hooks         loop.LoopHooks
	queueConfig   QueueConfig

	// Run state
	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewAgent creates a new Agent from the given config.
func NewAgent(config AgentConfig) *Agent {
	state := AgentState{}
	if config.InitialState != nil {
		state = *config.InitialState
	}

	queueConfig := QueueConfig{
		SteeringMode:  DrainOne,
		FollowUpMode:  DrainOne,
	}
	if config.QueueConfig != nil {
		queueConfig = *config.QueueConfig
	}

	registry := NewMessageRegistry()

	var convertToLlm loop.MessageTransformer
	if config.ConvertToLlm != nil {
		convertToLlm = config.ConvertToLlm
	} else {
		convertToLlm = registry.ConvertAll
	}

	hooks := loop.LoopHooks{
		BeforeToolCall:  config.BeforeToolCall,
		AfterToolCall:   config.AfterToolCall,
		PrepareNextTurn: config.PrepareNextTurn,
	}

	return &Agent{
		state:         state,
		eventBus:      NewEventBus(),
		registry:      registry,
		steeringQueue: NewMessageQueue(queueConfig.SteeringMode),
		followUpQueue: NewMessageQueue(queueConfig.FollowUpMode),
		streamFn:      config.StreamFn,
		convertToLlm:  convertToLlm,
		toolExecution: config.ToolExecution,
		hooks:         hooks,
		queueConfig:   queueConfig,
	}
}

// State returns a copy of the current agent state.
func (a *Agent) State() AgentState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state.Copy()
}

// EventBus returns the agent's event bus for subscribing to events.
func (a *Agent) EventBus() *EventBus {
	return a.eventBus
}

// Registry returns the agent's message registry for registering custom message types.
func (a *Agent) Registry() *MessageRegistry {
	return a.registry
}

// Prompt sends a prompt to the agent and runs the loop until completion.
func (a *Agent) Prompt(ctx context.Context, input string, images ...protocol.ImageContent) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return &protocol.Error{Code: protocol.ErrBusy, Message: "agent is already processing"}
	}
	a.running = true
	a.state.IsStreaming = true
	runCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.state.IsStreaming = false
		a.cancel = nil
		close(a.done)
		a.mu.Unlock()
	}()

	// Build user message
	content := []protocol.Content{protocol.TextContent{Type: "text", Text: input}}
	for _, img := range images {
		content = append(content, img)
	}
	userMsg := protocol.UserMessage{
		Role:      "user",
		Content:   content,
		Timestamp: protocol.Now(),
	}

	// Run the loop
	prompts := []protocol.Message{userMsg}
	_, err := a.runLoop(runCtx, prompts)
	return err
}

// Continue continues the loop from the current transcript.
func (a *Agent) Continue(ctx context.Context) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return &protocol.Error{Code: protocol.ErrBusy, Message: "agent is already processing"}
	}
	a.running = true
	a.state.IsStreaming = true
	runCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.state.IsStreaming = false
		a.cancel = nil
		close(a.done)
		a.mu.Unlock()
	}()

	_, err := a.runLoopContinue(runCtx)
	return err
}

// Abort cancels the current run. Callers should call WaitForIdle() after
// Abort to ensure the run has fully stopped before starting a new one.
func (a *Agent) Abort() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancel != nil {
		a.cancel()
	}
}

// WaitForIdle blocks until the current run completes.
func (a *Agent) WaitForIdle() {
	a.mu.Lock()
	done := a.done
	a.mu.Unlock()
	if done != nil {
		<-done
	}
}

// Reset clears the transcript, runtime state, and queued messages.
func (a *Agent) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.Messages = nil
	a.state.StreamingMessage = nil
	a.state.PendingToolCalls = nil
	a.state.LastError = nil
	a.steeringQueue.Clear()
	a.followUpQueue.Clear()
}

// Compact applies compaction to the agent's message list.
// Emits CompactEvent on the event bus.
// Returns an error if firstKeptIndex is out of bounds.
func (a *Agent) Compact(summary string, firstKeptIndex int, tokensBefore int, fileOps *protocol.FileOperations) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	tokensAfter := EstimateTokens(a.state.Messages[firstKeptIndex:]) + len(summary)/4
	compacted, err := CompactMessages(a.state.Messages, summary, firstKeptIndex, tokensBefore, tokensAfter, fileOps)
	if err != nil {
		return err
	}
	a.state.Messages = compacted

	a.eventBus.Emit(AgentEvent{
		CompactEvent: &CompactEvent{
			Summary:      summary,
			FirstKeptID:  fmt.Sprintf("%d", firstKeptIndex),
			TokensBefore: tokensBefore,
			TokensAfter:  tokensAfter,
			FileOps:      fileOps,
		},
	})
	return nil
}

// Steer enqueues a message to be injected after the current assistant turn.
func (a *Agent) Steer(msg protocol.Message) {
	a.steeringQueue.Enqueue(msg)
}

// FollowUp enqueues a message to be processed after the agent would otherwise stop.
func (a *Agent) FollowUp(msg protocol.Message) {
	a.followUpQueue.Enqueue(msg)
}

// ClearSteeringQueue removes all steering messages.
func (a *Agent) ClearSteeringQueue() { a.steeringQueue.Clear() }

// ClearFollowUpQueue removes all follow-up messages.
func (a *Agent) ClearFollowUpQueue() { a.followUpQueue.Clear() }

// ClearAllQueues removes all queued messages.
func (a *Agent) ClearAllQueues() {
	a.steeringQueue.Clear()
	a.followUpQueue.Clear()
}

// HasQueuedMessages returns true if any messages are queued.
func (a *Agent) HasQueuedMessages() bool {
	return a.steeringQueue.HasItems() || a.followUpQueue.HasItems()
}

// SetModel changes the model for subsequent turns.
func (a *Agent) SetModel(model protocol.ModelDescriptor) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.Model = model
}

// SetThinkingLevel changes the thinking level for subsequent turns.
// Pass nil to use the model's DefaultThinkingLevel; pass a non-nil value
// (including &protocol.ThinkingOff) to explicitly set it.
func (a *Agent) SetThinkingLevel(level *protocol.ThinkingLevel) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.ThinkingLevel = level
}

// SetTools changes the available tools for subsequent turns.
func (a *Agent) SetTools(tools []loop.AgentTool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.Tools = tools
}

// SetSystemPrompt changes the system prompt for subsequent turns.
func (a *Agent) SetSystemPrompt(prompt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.SystemPrompt = prompt
}

// --- Internal: Loop Execution ---

func (a *Agent) runLoop(ctx context.Context, prompts []protocol.Message) ([]protocol.Message, error) {
	agentCtx := loop.AgentContext{
		SystemPrompt: a.state.SystemPrompt,
		Messages:     a.state.Messages,
		Tools:        a.state.Tools,
	}

	config := a.buildLoopConfig()

	// Event sink: forward loop events to agent event bus
	sink := func(event loop.LoopEvent) {
		a.eventBus.Emit(AgentEvent{LoopEvent: &event})

		// Update agent state from loop events
		switch event.Type {
		case loop.LoopMessageEnd:
			if event.Message != nil {
				a.mu.Lock()
				a.state.Messages = append(a.state.Messages, event.Message)
				a.mu.Unlock()
			}
		case loop.LoopAgentEnd:
			// Loop finished
		}
	}

	newMessages, err := loop.AgentLoop(ctx, prompts, agentCtx, config, sink)
	if err != nil {
		a.mu.Lock()
		a.state.LastError = err
		a.mu.Unlock()
	}
	return newMessages, err
}

func (a *Agent) runLoopContinue(ctx context.Context) ([]protocol.Message, error) {
	agentCtx := loop.AgentContext{
		SystemPrompt: a.state.SystemPrompt,
		Messages:     a.state.Messages,
		Tools:        a.state.Tools,
	}

	config := a.buildLoopConfig()

	sink := func(event loop.LoopEvent) {
		a.eventBus.Emit(AgentEvent{LoopEvent: &event})

		switch event.Type {
		case loop.LoopMessageEnd:
			if event.Message != nil {
				a.mu.Lock()
				a.state.Messages = append(a.state.Messages, event.Message)
				a.mu.Unlock()
			}
		}
	}

	newMessages, err := loop.AgentLoopContinue(ctx, agentCtx, config, sink)
	if err != nil {
		a.mu.Lock()
		a.state.LastError = err
		a.mu.Unlock()
	}
	return newMessages, err
}

func (a *Agent) buildLoopConfig() loop.LoopConfig {
	// Resolve effective thinking level:
	//   1. nil (unset) → use model's DefaultThinkingLevel
	//   2. nil + no model default → off
	//   3. non-nil (including "off") → use it
	effectiveThinking := protocol.ThinkingOff
	if a.state.ThinkingLevel != nil {
		effectiveThinking = *a.state.ThinkingLevel
	} else if a.state.Model.Capabilities.DefaultThinkingLevel != "" {
		effectiveThinking = a.state.Model.Capabilities.DefaultThinkingLevel
	}

	return loop.LoopConfig{
		Model:          a.state.Model,
		ThinkingLevel:  effectiveThinking,
		Tools:          a.state.Tools,
		StreamFn:       a.streamFn,
		ConvertToLlm:   a.convertToLlm,
		ToolExecution:  a.toolExecution,
		Hooks:          a.hooks,
		SteeringDrain:  a.steeringQueue.Drain,
		FollowUpDrain:  a.followUpQueue.Drain,
	}
}
