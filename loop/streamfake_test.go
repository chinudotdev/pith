package loop_test

import (
	"context"
	"time"

	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// streamFakeConfig configures a test StreamFn that simulates streaming text.
type streamFakeConfig struct {
	Text        string
	StreamDelay time.Duration
}

func testModel() protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ID:       "test-model",
		Name:     "Test Model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "test",
		BaseURL:  "https://test.example.com",
		Capabilities: protocol.ModelCapabilities{
			Input:          map[protocol.MediaType]bool{protocol.MediaText: true},
			ThinkingLevels: map[protocol.ThinkingLevel]bool{protocol.ThinkingOff: true},
			Caching:        map[protocol.CacheMode]bool{protocol.CacheNone: true},
			Transport:      map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
		ContextWindow: 128000,
		MaxTokens:     4096,
	}
}

// fakeStream returns a StreamFn that streams text in chunks and respects ctx cancellation.
func fakeStream(cfg streamFakeConfig) loop.StreamFn {
	return func(ctx context.Context, model protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		ch := make(chan protocol.StreamEvent, 64)

		go func() {
			defer close(ch)

			now := protocol.Now()
			partial := &protocol.AssistantMessage{
				Role:      "assistant",
				API:       model.API,
				Provider:  model.Provider,
				Model:     model.ID,
				Timestamp: now,
			}

			snapshot := func() *protocol.AssistantMessage {
				snap := *partial
				snap.Content = make([]protocol.ContentBlock, len(partial.Content))
				copy(snap.Content, partial.Content)
				return &snap
			}

			ch <- protocol.StreamEvent{Type: protocol.EventStart, Partial: snapshot()}

			if cfg.Text != "" {
				contentIndex := len(partial.Content)
				ch <- protocol.StreamEvent{Type: protocol.EventTextStart, ContentIndex: contentIndex, Partial: snapshot()}

				text := cfg.Text
				for i := 0; i < len(text); i += 5 {
					end := i + 5
					if end > len(text) {
						end = len(text)
					}
					delta := text[i:end]

					if cfg.StreamDelay > 0 {
						select {
						case <-ctx.Done():
							ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopAborted, Message: snapshot()}
							return
						case <-time.After(cfg.StreamDelay):
						}
					}

					ch <- protocol.StreamEvent{
						Type:         protocol.EventTextDelta,
						ContentIndex: contentIndex,
						Delta:        delta,
						Partial:      snapshot(),
					}
				}

				partial.Content = append(partial.Content, protocol.TextContent{Type: "text", Text: text})
				ch <- protocol.StreamEvent{
					Type:         protocol.EventTextEnd,
					ContentIndex: contentIndex,
					Content:      text,
					Partial:      snapshot(),
				}
			}

			partial.StopReason = protocol.StopEnd
			partial.Usage = protocol.Usage{Input: 10, Output: len(cfg.Text), TotalTokens: len(cfg.Text) + 10}
			ch <- protocol.StreamEvent{
				Type:    protocol.EventDone,
				Reason:  protocol.StopEnd,
				Message: snapshot(),
				Partial: snapshot(),
			}
		}()

		return ch, nil
	}
}
