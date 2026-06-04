package agent

import (
	"sync"

	"github.com/chinudotdev/pith/protocol"
)

// --- Message Registry ---

// ToLlmConverter converts an AgentMessage to an LLM Message.
// Return nil to filter the message out.
type ToLlmConverter func(msg protocol.Message) *protocol.Message

// MessageRegistry replaces declaration merging with an explicit, runtime-validated registry.
type MessageRegistry struct {
	mu         sync.RWMutex
	converters map[string]ToLlmConverter
}

// NewMessageRegistry creates a new registry with built-in types pre-registered.
func NewMessageRegistry() *MessageRegistry {
	r := &MessageRegistry{
		converters: make(map[string]ToLlmConverter),
	}

	// Built-in types: pass through
	r.converters["user"] = func(m protocol.Message) *protocol.Message { return &m }
	r.converters["assistant"] = func(m protocol.Message) *protocol.Message { return &m }
	r.converters["toolResult"] = func(m protocol.Message) *protocol.Message { return &m }

	// CompactSummary: convert to UserMessage with summary text
	r.converters["compactSummary"] = func(m protocol.Message) *protocol.Message {
		if csm, ok := m.(protocol.CompactSummaryMessage); ok {
			userMsg := protocol.UserMessage{
				Role:      "user",
				Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: csm.Summary}},
				Timestamp: csm.Timestamp,
			}
			msg := protocol.Message(userMsg)
			return &msg
		}
		return nil
	}

	return r
}

// Register adds a custom message type with its conversion logic.
func (r *MessageRegistry) Register(role string, converter ToLlmConverter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.converters[role] = converter
}

// Convert converts a single message to an LLM message (or filters it out).
func (r *MessageRegistry) Convert(msg protocol.Message) *protocol.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()

	role := protocol.MessageRole(msg)
	if converter, ok := r.converters[role]; ok {
		return converter(msg)
	}
	// Unknown role: filter out
	return nil
}

// ConvertAll converts all messages to LLM messages.
func (r *MessageRegistry) ConvertAll(messages []protocol.Message) []protocol.Message {
	result := make([]protocol.Message, 0, len(messages))
	for _, msg := range messages {
		if converted := r.Convert(msg); converted != nil {
			result = append(result, *converted)
		}
	}
	return result
}

// Roles returns all registered message role names.
func (r *MessageRegistry) Roles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	roles := make([]string, 0, len(r.converters))
	for role := range r.converters {
		roles = append(roles, role)
	}
	return roles
}
