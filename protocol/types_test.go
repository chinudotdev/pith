package protocol_test

import (
	"testing"

	"github.com/chinudotdev/pith/protocol"
)

// TestStopReasons verifies all stop reasons are defined.
func TestStopReasons(t *testing.T) {
	reasons := []protocol.StopReason{
		protocol.StopEnd,
		protocol.StopLength,
		protocol.StopTool,
		protocol.StopError,
		protocol.StopAborted,
	}
	for _, r := range reasons {
		if r == "" {
			t.Error("stop reason should not be empty")
		}
	}
}

// TestMessageRole verifies the MessageRole helper.
func TestMessageRole(t *testing.T) {
	tests := []struct {
		msg      protocol.Message
		expected string
	}{
		{protocol.UserMessage{Role: "user"}, "user"},
		{protocol.AssistantMessage{Role: "assistant"}, "assistant"},
		{protocol.ToolResultMessage{Role: "toolResult"}, "toolResult"},
		{protocol.CompactSummaryMessage{Role: "compactSummary"}, "compactSummary"},
	}
	for _, tt := range tests {
		if got := protocol.MessageRole(tt.msg); got != tt.expected {
			t.Errorf("MessageRole(%+v) = %q, want %q", tt.msg, got, tt.expected)
		}
	}
}

// TestUsageFields verifies Usage struct fields.
func TestUsageFields(t *testing.T) {
	u := protocol.Usage{
		Input:       100,
		Output:      50,
		TotalTokens: 150,
		CacheRead:   10,
		CacheWrite:  20,
	}
	if u.Input != 100 || u.Output != 50 || u.TotalTokens != 150 {
		t.Errorf("unexpected usage: %+v", u)
	}
}

// TestStreamEventTypes verifies all event types are defined.
func TestStreamEventTypes(t *testing.T) {
	types := []protocol.StreamEventType{
		protocol.EventStart,
		protocol.EventTextStart,
		protocol.EventTextDelta,
		protocol.EventTextEnd,
		protocol.EventThinkingStart,
		protocol.EventThinkingDelta,
		protocol.EventThinkingEnd,
		protocol.EventToolCallStart,
		protocol.EventToolCallDelta,
		protocol.EventToolCallEnd,
		protocol.EventDone,
		protocol.EventError,
	}
	for _, typ := range types {
		if typ == "" {
			t.Error("stream event type should not be empty")
		}
	}
}

// TestContentBlockTypeAssertions verifies that content blocks can be
// type-asserted from protocol.ContentBlock interface.
// This is a regression test for the pointer-vs-value bug (§9).
func TestContentBlockTypeAssertions(t *testing.T) {
	blocks := []protocol.ContentBlock{
		protocol.TextContent{Type: "text", Text: "hello"},
		protocol.ThinkingContent{Type: "thinking", Thinking: "hmm"},
		protocol.ToolCall{Type: "toolCall", ID: "tc1", Name: "test", Arguments: "{}"},
	}

	if _, ok := blocks[0].(protocol.TextContent); !ok {
		t.Error("TextContent type assertion should succeed")
	}
	if _, ok := blocks[1].(protocol.ThinkingContent); !ok {
		t.Error("ThinkingContent type assertion should succeed")
	}
	if _, ok := blocks[2].(protocol.ToolCall); !ok {
		t.Error("ToolCall type assertion should succeed")
	}
}

// TestMessageValueTypeAssertions verifies that messages stored as
// protocol.Message interface values can be type-asserted.
// This is a regression test for the pointer-vs-value bug (§9).
func TestMessageValueTypeAssertions(t *testing.T) {
	msgs := []protocol.Message{
		protocol.UserMessage{Role: "user"},
		protocol.AssistantMessage{Role: "assistant"},
		protocol.ToolResultMessage{Role: "toolResult"},
		protocol.CompactSummaryMessage{Role: "compactSummary"},
	}

	if _, ok := msgs[0].(protocol.UserMessage); !ok {
		t.Error("UserMessage type assertion should succeed (value semantics)")
	}
	if _, ok := msgs[1].(protocol.AssistantMessage); !ok {
		t.Error("AssistantMessage type assertion should succeed (value semantics)")
	}
	if _, ok := msgs[2].(protocol.ToolResultMessage); !ok {
		t.Error("ToolResultMessage type assertion should succeed (value semantics)")
	}
	if _, ok := msgs[3].(protocol.CompactSummaryMessage); !ok {
		t.Error("CompactSummaryMessage type assertion should succeed (value semantics)")
	}
}
