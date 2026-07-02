package agent_test

import (
	"testing"
	"time"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/protocol"
)

func TestDefaultConvertToLlmDropsUnknownTypes(t *testing.T) {
	messages := []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hello"}}, Timestamp: time.Now()},
		protocol.FilterTestMessage{Role: "filterTest"},
		protocol.CompactSummaryMessage{Role: "compactSummary", Summary: "prior context", Timestamp: time.Now()},
	}

	result := agent.DefaultConvertToLlm(messages)
	if len(result) != 2 {
		t.Fatalf("expected 2 messages after filtering, got %d", len(result))
	}
	if _, ok := result[0].(protocol.UserMessage); !ok {
		t.Fatalf("expected first message to be UserMessage, got %T", result[0])
	}
	userMsg, ok := result[1].(protocol.UserMessage)
	if !ok {
		t.Fatalf("expected compact summary to convert to UserMessage, got %T", result[1])
	}
	if userMsg.Content[0].(protocol.TextContent).Text != "prior context" {
		t.Fatalf("unexpected converted summary text: %q", userMsg.Content[0].(protocol.TextContent).Text)
	}
}
