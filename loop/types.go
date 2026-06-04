package loop

import (
	"context"

	"github.com/chinudotdev/pith/protocol"
)

// --- Tool Types ---

// AgentTool extends the protocol Tool with execution behavior.
type AgentTool struct {
	Name        string         `json:"name"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	Parameters  any            `json:"parameters"` // JSON Schema
	Execute     ToolExecutor   `json:"-"`
	PrepareArgs func(raw map[string]any) (map[string]any, error) `json:"-"`
	Mode        ExecutionMode  `json:"mode,omitempty"` // overrides default policy
}

// ExecutionMode controls whether a tool runs sequentially or in parallel.
type ExecutionMode int

const (
	ModeDefault   ExecutionMode = iota // use the LoopConfig policy
	ModeSequential                     // always sequential
	ModeParallel                       // always parallel
)

// ToolExecutor is the function signature for tool execution.
type ToolExecutor func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) ToolResult

// ToolResult is the output of a tool execution.
type ToolResult struct {
	Content   []protocol.Content `json:"content"`
	Details   any                `json:"details,omitempty"`
	Terminate bool               `json:"terminate,omitempty"` // if true, stop the loop
}

// --- Tool Execution Policy ---

// ToolExecutionPolicy controls how tool calls are executed.
type ToolExecutionPolicy interface {
	IsParallel(toolName string) bool
}

// SequentialPolicy executes all tool calls one at a time.
type SequentialPolicy struct{}

func (SequentialPolicy) IsParallel(_ string) bool { return false }

// ParallelPolicy executes all tool calls concurrently.
type ParallelPolicy struct{}

func (ParallelPolicy) IsParallel(_ string) bool { return true }

// PerToolPolicy allows per-tool overrides.
type PerToolPolicy struct {
	Default   bool                // true = parallel, false = sequential
	Overrides map[string]bool     // tool name -> isParallel
}

func (p *PerToolPolicy) IsParallel(toolName string) bool {
	if override, ok := p.Overrides[toolName]; ok {
		return override
	}
	return p.Default
}

// --- Loop Hooks ---

// BeforeToolCallContext provides context for the beforeToolCall hook.
type BeforeToolCallContext struct {
	AssistantMessage protocol.AssistantMessage
	ToolCall         protocol.ToolCall
	Args             any
	Context          AgentContext
}

// BeforeToolCallResult allows blocking a tool call.
type BeforeToolCallResult struct {
	Block  bool   `json:"block,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// AfterToolCallContext provides context for the afterToolCall hook.
type AfterToolCallContext struct {
	AssistantMessage protocol.AssistantMessage
	ToolCall         protocol.ToolCall
	Args             any
	Result           ToolResult
	IsError          bool
	Context          AgentContext
}

// AfterToolCallResult allows overriding a tool result.
type AfterToolCallResult struct {
	Content   []protocol.Content `json:"content,omitempty"`
	Details   any                `json:"details,omitempty"`
	IsError   bool               `json:"isError,omitempty"`
	Terminate bool               `json:"terminate,omitempty"`
}

// ShouldStopAfterTurnContext provides context for the shouldStopAfterTurn hook.
type ShouldStopAfterTurnContext struct {
	Message     protocol.AssistantMessage
	ToolResults []protocol.ToolResultMessage
	Context     AgentContext
	NewMessages []protocol.Message
}

// PrepareNextTurnContext provides context for the prepareNextTurn hook.
type PrepareNextTurnContext = ShouldStopAfterTurnContext

// TurnUpdate allows modifying the context for the next turn.
type TurnUpdate struct {
	Context      *AgentContext    `json:"context,omitempty"`
	Model        *protocol.ModelDescriptor `json:"model,omitempty"`
	ThinkingLevel protocol.ThinkingLevel `json:"thinkingLevel,omitempty"`
}

// LoopHooks are callbacks that the loop calls at specific points.
type LoopHooks struct {
	BeforeToolCall     func(ctx BeforeToolCallContext, signal <-chan struct{}) *BeforeToolCallResult
	AfterToolCall      func(ctx AfterToolCallContext, signal <-chan struct{}) *AfterToolCallResult
	ShouldStopAfterTurn func(ctx ShouldStopAfterTurnContext) bool
	PrepareNextTurn    func(ctx PrepareNextTurnContext) *TurnUpdate
}

// --- Agent Context ---

// AgentContext is the input to the agent loop for a single run.
type AgentContext struct {
	SystemPrompt string
	Messages     []protocol.Message
	Tools        []AgentTool
}

// --- Loop Config ---

// LoopConfig configures a single run of the agent loop.
type LoopConfig struct {
	Model         protocol.ModelDescriptor
	ThinkingLevel protocol.ThinkingLevel
	Tools         []AgentTool
	StreamFn      StreamFn
	ConvertToLlm  MessageTransformer
	ToolExecution ToolExecutionPolicy
	Hooks         LoopHooks
	SessionID     string
	ThinkingBudgets *protocol.ThinkingBudgets
	Transport     protocol.Transport
	MaxRetryDelayMs *int
	ErrorChannel  ErrorChannel

	// SteeringDrain returns messages to inject after the current assistant turn.
	// Called between turns. Return nil/empty to continue normally.
	SteeringDrain func() []protocol.Message

	// FollowUpDrain returns messages to process after the loop would otherwise stop.
	// Called after the inner loop exits. Return nil/empty to stop.
	FollowUpDrain func() []protocol.Message
}

// StreamFn is the function signature for streaming LLM completions.
// This is the L1 gateway's stream function.
// The context.Context supports cancellation (abort) and timeouts.
type StreamFn func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error)

// --- Message Pipeline ---

// MessageTransformer converts AgentMessages to LLM Messages.
type MessageTransformer func(messages []protocol.Message) []protocol.Message

// MessageStage is a single stage in a composable message pipeline.
type MessageStage interface {
	Apply(messages []protocol.Message) []protocol.Message
}

// FilterStage filters messages based on a predicate.
type FilterStage struct {
	Predicate func(protocol.Message) bool
}

func (f FilterStage) Apply(messages []protocol.Message) []protocol.Message {
	var result []protocol.Message
	for _, m := range messages {
		if f.Predicate(m) {
			result = append(result, m)
		}
	}
	return result
}

// MapStage transforms each message, potentially producing multiple messages.
type MapStage struct {
	Transform func(protocol.Message) []protocol.Message
}

func (m MapStage) Apply(messages []protocol.Message) []protocol.Message {
	var result []protocol.Message
	for _, msg := range messages {
		result = append(result, m.Transform(msg)...)
	}
	return result
}

// ConvertStage converts each message using a converter function.
type ConvertStage struct {
	ToLlm func(protocol.Message) *protocol.Message
}

func (c ConvertStage) Apply(messages []protocol.Message) []protocol.Message {
	var result []protocol.Message
	for _, msg := range messages {
		if converted := c.ToLlm(msg); converted != nil {
			result = append(result, *converted)
		}
	}
	return result
}

// MessagePipeline composes multiple stages into a single transformer.
type MessagePipeline struct {
	Stages []MessageStage
}

// Run applies all stages in order.
func (p *MessagePipeline) Run(messages []protocol.Message) []protocol.Message {
	result := messages
	for _, stage := range p.Stages {
		result = stage.Apply(result)
	}
	return result
}

// Transformer returns the pipeline as a MessageTransformer function.
func (p *MessagePipeline) Transformer() MessageTransformer {
	return func(messages []protocol.Message) []protocol.Message {
		return p.Run(messages)
	}
}

// --- Error Channel ---

// ErrorChannel receives structured errors from the loop.
type ErrorChannel interface {
	OnError(err LoopError)
}

// LoopError is a structured error from the agent loop.
type LoopError interface {
	LoopErrorCode() protocol.ErrorCode
	LoopErrorMessage() string
}

// ProviderError is an error from the LLM provider.
type ProviderError struct {
	Code      protocol.ErrorCode
	Message   string
	Retryable bool
	Cause     error
}

func (e *ProviderError) LoopErrorCode() protocol.ErrorCode { return e.Code }
func (e *ProviderError) LoopErrorMessage() string          { return e.Message }
func (e *ProviderError) Error() string                     { return e.Message }
func (e *ProviderError) Unwrap() error                     { return e.Cause }

// ToolError is an error from tool execution.
type ToolError struct {
	ToolCallID string
	ToolName   string
	Code       protocol.ErrorCode
	Message    string
}

func (e *ToolError) LoopErrorCode() protocol.ErrorCode { return e.Code }
func (e *ToolError) LoopErrorMessage() string          { return e.Message }
func (e *ToolError) Error() string                     { return e.Message }

// AbortError is returned when the loop is aborted.
type AbortError struct {
	Reason string
}

func (e *AbortError) LoopErrorCode() protocol.ErrorCode { return protocol.ErrAborted }
func (e *AbortError) LoopErrorMessage() string          { return e.Reason }
func (e *AbortError) Error() string                     { return e.Reason }

// --- Loop Events ---

// LoopEventType identifies the type of a loop event.
type LoopEventType string

const (
	LoopAgentStart          LoopEventType = "agentStart"
	LoopAgentEnd            LoopEventType = "agentEnd"
	LoopTurnStart           LoopEventType = "turnStart"
	LoopTurnEnd             LoopEventType = "turnEnd"
	LoopMessageStart        LoopEventType = "messageStart"
	LoopMessageUpdate       LoopEventType = "messageUpdate"
	LoopMessageEnd          LoopEventType = "messageEnd"
	LoopToolExecutionStart  LoopEventType = "toolExecutionStart"
	LoopToolExecutionUpdate LoopEventType = "toolExecutionUpdate"
	LoopToolExecutionEnd    LoopEventType = "toolExecutionEnd"
)

// LoopEvent represents a single event emitted by the agent loop.
type LoopEvent struct {
	Type         LoopEventType
	Message      protocol.Message   // for MessageStart, MessageEnd
	StreamEvent  *protocol.StreamEvent // for MessageUpdate
	ToolCallID   string             // for ToolExecution*
	ToolName     string             // for ToolExecution*
	Args         any                // for ToolExecutionStart, ToolExecutionUpdate
	Result       any                // for ToolExecutionEnd
	IsError      bool               // for ToolExecutionEnd
	PartialResult any              // for ToolExecutionUpdate
	Messages     []protocol.Message // for AgentEnd
	Assistant    *protocol.AssistantMessage // for TurnEnd
	ToolResults  []protocol.ToolResultMessage // for TurnEnd
}

// EventSink receives loop events.
type EventSink func(event LoopEvent)
