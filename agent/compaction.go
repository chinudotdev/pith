package agent

import (
	"fmt"

	"github.com/chinudotdev/pith/protocol"
)

// CompactMessages takes messages, a summary, and a cut point. Returns the compacted
// message list. No side effects. No LLM call. This is the mechanical part of compaction.
// Returns an error if firstKeptIndex is out of bounds.
func CompactMessages(messages []protocol.Message, summary string, firstKeptIndex int, tokensBefore int, tokensAfter int) ([]protocol.Message, error) {
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
		Timestamp:    protocol.Now(),
	}

	result := make([]protocol.Message, 0, 1+len(messages)-firstKeptIndex)
	result = append(result, compactMsg)
	result = append(result, messages[firstKeptIndex:]...)
	return result, nil
}
