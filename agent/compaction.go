package agent

import (
	"fmt"

	"github.com/chinudotdev/pith/protocol"
)

// --- Compaction Primitives ---
// The SDK provides three compaction primitives. The App layer (PiCompaction)
// decides *when* to call them and *what to do* with the results.

// CompactionSettings controls when compaction should be triggered.
type CompactionSettings struct {
	Enabled       bool
	ReserveTokens int // tokens to keep free after compaction
}

// EstimateTokens estimates the token count for a message list.
// This is a rough heuristic: ~4 characters per token.
func EstimateTokens(messages []protocol.Message) int {
	totalChars := 0
	for _, msg := range messages {
		switch m := msg.(type) {
		case protocol.UserMessage:
			for _, c := range m.Content {
				if tc, ok := c.(protocol.TextContent); ok {
					totalChars += len(tc.Text)
				}
			}
		case protocol.AssistantMessage:
			for _, c := range m.Content {
				switch block := c.(type) {
				case protocol.TextContent:
					totalChars += len(block.Text)
				case protocol.ThinkingContent:
					totalChars += len(block.Thinking)
				case protocol.ToolCall:
					totalChars += len(block.Arguments)
				}
			}
		case protocol.ToolResultMessage:
			for _, c := range m.Content {
				if tc, ok := c.(protocol.TextContent); ok {
					totalChars += len(tc.Text)
				}
			}
		case protocol.CompactSummaryMessage:
			totalChars += len(m.Summary)
		}
	}
	return totalChars / 4
}

// ShouldCompact checks whether compaction is needed based on token count and settings.
func ShouldCompact(messages []protocol.Message, settings CompactionSettings, model protocol.ModelDescriptor) bool {
	if !settings.Enabled {
		return false
	}
	tokens := EstimateTokens(messages)
	threshold := model.ContextWindow - settings.ReserveTokens
	return tokens > threshold
}

// CompactMessages takes messages, a summary, and a cut point. Returns the compacted
// message list. No side effects. No LLM call. This is the mechanical part of compaction.
// Returns an error if firstKeptIndex is out of bounds.
func CompactMessages(messages []protocol.Message, summary string, firstKeptIndex int, tokensBefore int, tokensAfter int, fileOps *protocol.FileOperations) ([]protocol.Message, error) {
	if firstKeptIndex < 0 {
		return nil, protocol.NewErrorWithCause(protocol.ErrInvalidArgument, "firstKeptIndex must be >= 0", nil)
	}
	if firstKeptIndex > len(messages) {
		return nil, protocol.NewErrorWithCause(protocol.ErrInvalidArgument, fmt.Sprintf("firstKeptIndex %d exceeds message count %d", firstKeptIndex, len(messages)), nil)
	}

	compactMsg := protocol.CompactSummaryMessage{
		Role:         "compactSummary",
		Summary:      summary,
		TokensBefore: tokensBefore,
		TokensAfter:  tokensAfter,
		FileOps:      fileOps,
		Timestamp:    protocol.Now(),
	}

	result := make([]protocol.Message, 0, 1+len(messages)-firstKeptIndex)
	result = append(result, compactMsg)
	result = append(result, messages[firstKeptIndex:]...)
	return result, nil
}
