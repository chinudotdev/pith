package protocol

// --- Streaming Events ---

// StreamEventType identifies the type of a streaming event.
type StreamEventType string

const (
	EventStart         StreamEventType = "start"
	EventTextStart     StreamEventType = "textStart"
	EventTextDelta     StreamEventType = "textDelta"
	EventTextEnd       StreamEventType = "textEnd"
	EventThinkingStart StreamEventType = "thinkingStart"
	EventThinkingDelta StreamEventType = "thinkingDelta"
	EventThinkingEnd   StreamEventType = "thinkingEnd"
	EventToolCallStart StreamEventType = "toolCallStart"
	EventToolCallDelta StreamEventType = "toolCallDelta"
	EventToolCallEnd   StreamEventType = "toolCallEnd"
	EventDone          StreamEventType = "done"
	EventError         StreamEventType = "error"
)

// StreamEvent represents a single event during streaming.
// The event type is discriminated by the Type field.
// Partial always contains the accumulated AssistantMessage so far.
type StreamEvent struct {
	Type         StreamEventType `json:"type"`
	ContentIndex int             `json:"contentIndex,omitempty"`
	Delta        string          `json:"delta,omitempty"`     // for TextDelta, ThinkingDelta, ToolCallDelta
	Content      string          `json:"content,omitempty"`   // for TextEnd, ThinkingEnd
	ToolCall     *ToolCall       `json:"toolCall,omitempty"`  // for ToolCallEnd
	Reason       StopReason      `json:"reason,omitempty"`    // for Done, Error
	Message      *AssistantMessage `json:"message,omitempty"` // for Done, Error; final message
	Partial      *AssistantMessage `json:"partial,omitempty"` // always present: accumulated so far
}
