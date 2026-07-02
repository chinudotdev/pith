package agent

import "github.com/chinudotdev/pith/protocol"

// DefaultConvertToLlm provides the built-in message conversion behavior
// for loop runs. Applications can override this via AgentConfig.ConvertToLlm.
func DefaultConvertToLlm(messages []protocol.Message) []protocol.Message {
	result := make([]protocol.Message, 0, len(messages))
	for _, msg := range messages {
		switch m := msg.(type) {
		case protocol.UserMessage, protocol.AssistantMessage, protocol.ToolResultMessage:
			result = append(result, msg)
		case protocol.CompactSummaryMessage:
			result = append(result, protocol.UserMessage{
				Role:      "user",
				Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: m.Summary}},
				Timestamp: m.Timestamp,
			})
		}
	}
	return result
}
