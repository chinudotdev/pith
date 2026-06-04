// Example 08: Custom Messages & MessageRegistry — sealed interface, converters, pipeline.
//
// This example demonstrates message types locally (no API call needed).
//
//	mkdir my-agent && cd my-agent && go mod init my-agent
//	cp main.go . && go mod tidy
//	go run main.go
package main

import (
	"fmt"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// SystemNotification is a custom App-layer type. It does NOT implement
// protocol.Message (can't — sealed interface with unexported method).
// Convert it to a UserMessage before passing to the SDK.
type SystemNotification struct {
	Level string
	Text  string
}

func (sn SystemNotification) ToUserMessage() protocol.UserMessage {
	return protocol.UserMessage{
		Role: "user",
		Content: []protocol.Content{
			protocol.TextContent{Type: "text", Text: fmt.Sprintf("[System %s] %s", sn.Level, sn.Text)},
		},
		Timestamp: protocol.Now(),
	}
}

func main() {
	// --- Custom type conversion ---
	fmt.Println("--- Custom Type Conversion ---")
	notification := SystemNotification{Level: "warning", Text: "Disk space is running low"}
	userMsg := notification.ToUserMessage()
	fmt.Printf("Custom type → UserMessage: %q\n", func() string {
		for _, c := range userMsg.Content {
			if tc, ok := c.(protocol.TextContent); ok {
				return tc.Text
			}
		}
		return ""
	}())

	// --- MessageRegistry ---
	fmt.Println("\n--- MessageRegistry ---")
	registry := agent.NewMessageRegistry()
	fmt.Printf("Built-in roles: %v\n", registry.Roles())

	// CompactSummaryMessage is auto-converted to UserMessage.
	compactMsg := protocol.CompactSummaryMessage{
		Role:         "compactSummary",
		Summary:      "Previous conversation about capitals.",
		TokensBefore: 500,
		TokensAfter:  50,
		Timestamp:    protocol.Now(),
	}
	converted := registry.Convert(compactMsg)
	if converted != nil {
		fmt.Printf("CompactSummary → %s\n", protocol.MessageRole(*converted))
	}

	// --- MessagePipeline ---
	fmt.Println("\n--- MessagePipeline ---")

	turnCounter := 0
	pipeline := loop.MessagePipeline{
		Stages: []loop.MessageStage{
			loop.FilterStage{Predicate: func(m protocol.Message) bool {
				role := protocol.MessageRole(m)
				return role == "user" || role == "assistant"
			}},
			loop.MapStage{Transform: func(m protocol.Message) []protocol.Message {
				if protocol.MessageRole(m) == "user" {
					turnCounter++
					if um, ok := m.(protocol.UserMessage); ok {
						um.Content = append([]protocol.Content{
							protocol.TextContent{Type: "text", Text: fmt.Sprintf("[Turn %d] ", turnCounter)},
						}, um.Content...)
						return []protocol.Message{um}
					}
				}
				return []protocol.Message{m}
			}},
		},
	}

	allMessages := []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "Hello"}}, Timestamp: protocol.Now()},
		protocol.AssistantMessage{Role: "assistant", Content: []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "Hi!"}}, Timestamp: protocol.Now()},
		protocol.CompactSummaryMessage{Role: "compactSummary", Summary: "Previous conversation", Timestamp: protocol.Now()},
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "How are you?"}}, Timestamp: protocol.Now()},
	}

	result := pipeline.Run(allMessages)
	fmt.Printf("Before: %d messages, After pipeline: %d messages\n", len(allMessages), len(result))
	for i, msg := range result {
		fmt.Printf("  [%d] %s", i, protocol.MessageRole(msg))
		if um, ok := msg.(protocol.UserMessage); ok {
			for _, c := range um.Content {
				if tc, ok := c.(protocol.TextContent); ok {
					fmt.Printf(": %q", tc.Text)
				}
			}
		}
		fmt.Println()
	}

	// --- Design Note ---
	fmt.Println("\n--- Design Note ---")
	fmt.Println("protocol.Message uses an unexported method (sealed interface).")
	fmt.Println("External packages CANNOT implement it. Convert custom types to built-in types at the App layer.")
}
