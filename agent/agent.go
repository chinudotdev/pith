package agent

import (
	"sync"

	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// --- Agent State ---

// AgentState holds the current state of the agent.
type AgentState struct {
	SystemPrompt     string
	Model            protocol.ModelDescriptor
	ThinkingLevel    protocol.ThinkingLevel
	Tools            []loop.AgentTool
	Messages         []protocol.Message
	IsStreaming       bool
	StreamingMessage  *protocol.AssistantMessage
	PendingToolCalls map[string]bool
	LastError        error
}

// Copy returns a shallow copy of the state to prevent external mutation.
// Note: this is a shallow copy. The Messages and Tools slices are copied,
// but individual Message and AgentTool values may contain shared mutable
// data (e.g., maps, slices, or pointers within Tool.Parameters). Callers
// must not modify individual message or tool values from the copy.
func (s *AgentState) Copy() AgentState {
	cp := *s
	cp.Messages = make([]protocol.Message, len(s.Messages))
	copy(cp.Messages, s.Messages)
	cp.Tools = make([]loop.AgentTool, len(s.Tools))
	copy(cp.Tools, s.Tools)
	if s.PendingToolCalls != nil {
		cp.PendingToolCalls = make(map[string]bool, len(s.PendingToolCalls))
		for k, v := range s.PendingToolCalls {
			cp.PendingToolCalls[k] = v
		}
	}
	return cp
}

// --- Event Bus ---

// AgentEvent is the union of loop events + agent-specific events.
type AgentEvent struct {
	LoopEvent    *loop.LoopEvent
	CompactEvent *CompactEvent
}

// CompactEvent is emitted when compaction is applied to the agent's messages.
type CompactEvent struct {
	Summary      string
	FirstKeptID  string
	TokensBefore int
	TokensAfter  int
	FileOps      *protocol.FileOperations
}

// Listener is a function that receives agent events.
type Listener func(event AgentEvent)

// EventBus is a typed, ordered event bus for the agent.
type EventBus struct {
	mu        sync.Mutex
	listeners []Listener
}

// NewEventBus creates a new event bus.
func NewEventBus() *EventBus {
	return &EventBus{}
}

// Subscribe registers a listener and returns an unsubscribe function.
func (b *EventBus) Subscribe(listener Listener) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners = append(b.listeners, listener)
	index := len(b.listeners) - 1
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.listeners[index] = nil // nil out to preserve order
		b.compactIfNeeded()
	}
}

// compactIfNeeded compacts the listener slice when the nil-hole ratio
// exceeds 50%, preventing unbounded memory growth from subscribe/unsubscribe churn.
func (b *EventBus) compactIfNeeded() {
	if len(b.listeners) < 16 {
		return // too small to bother
	}
	nilCount := 0
	for _, l := range b.listeners {
		if l == nil {
			nilCount++
		}
	}
	if nilCount > len(b.listeners)/2 {
		compacted := b.listeners[:0]
		for _, l := range b.listeners {
			if l != nil {
				compacted = append(compacted, l)
			}
		}
		b.listeners = compacted
	}
}

// Emit sends an event to all listeners in order.
func (b *EventBus) Emit(event AgentEvent) {
	b.mu.Lock()
	listeners := make([]Listener, len(b.listeners))
	copy(listeners, b.listeners)
	b.mu.Unlock()

	for _, l := range listeners {
		if l != nil {
			l(event)
		}
	}
}

// --- Message Queue ---

// DrainMode controls how messages are drained from a queue.
type DrainMode int

const (
	DrainAll DrainMode = iota // drain every queued message at once
	DrainOne                  // drain only the oldest message
)

// MessageQueue is a thread-safe queue of messages.
type MessageQueue struct {
	mu       sync.Mutex
	messages []protocol.Message
	mode     DrainMode
}

// NewMessageQueue creates a new message queue.
func NewMessageQueue(mode DrainMode) *MessageQueue {
	return &MessageQueue{mode: mode}
}

// Enqueue adds a message to the queue.
func (q *MessageQueue) Enqueue(msg protocol.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, msg)
}

// Drain removes and returns messages from the queue.
func (q *MessageQueue) Drain() []protocol.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	var result []protocol.Message
	if q.mode == DrainOne {
		result = q.messages[:1]
		q.messages = q.messages[1:]
	} else {
		result = q.messages
		q.messages = nil
	}
	return result
}

// HasItems returns true if the queue has messages.
func (q *MessageQueue) HasItems() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) > 0
}

// Clear removes all messages from the queue.
func (q *MessageQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = nil
}
