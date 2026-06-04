package gateway

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chinudotdev/pith/protocol"
)

// FauxProvider is a test double that returns pre-configured responses
// without making any real API calls. Useful for testing L2 and L3.
type FauxProvider struct {
	api          protocol.ApiId
	name         string
	capabilities ProviderCapabilities
	responses    []FauxResponse
	index        atomic.Int32
	initialized  bool
}

// FauxResponse configures what the FauxProvider returns for a call.
type FauxResponse struct {
	// Text is the text content to stream.
	Text string

	// ToolCalls are tool calls to include in the response.
	ToolCalls []protocol.ToolCall

	// StopReason overrides the default stop reason.
	StopReason protocol.StopReason

	// Error optionally causes the provider to return an error.
	Error error

	// StreamDelay is the delay between each text delta.
	StreamDelay time.Duration
}

// NewFauxProvider creates a FauxProvider with the given responses.
// Responses are cycled through: after the last response, it wraps around.
func NewFauxProvider(api protocol.ApiId, name string, responses ...FauxResponse) *FauxProvider {
	return &FauxProvider{
		api:       api,
		name:      name,
		responses: responses,
	}
}

// WithCapabilities sets the provider capabilities.
func (f *FauxProvider) WithCapabilities(caps ProviderCapabilities) *FauxProvider {
	f.capabilities = caps
	return f
}

func (f *FauxProvider) API() protocol.ApiId    { return f.api }
func (f *FauxProvider) Name() string           { return f.name }
func (f *FauxProvider) Capabilities() ProviderCapabilities { return f.capabilities }

func (f *FauxProvider) Initialize() error {
	f.initialized = true
	return nil
}

func (f *FauxProvider) Cleanup() {
	f.initialized = false
}

// Stream returns a channel of events that simulate the configured response.
func (f *FauxProvider) Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	if len(f.responses) == 0 {
		return nil, &protocol.Error{Code: protocol.ErrInvalid, Message: "faux provider has no responses configured"}
	}

	idx := f.index.Add(1) - 1
	resp := f.responses[idx%int32(len(f.responses))]

	if resp.Error != nil {
		return nil, resp.Error
	}

	ch := make(chan protocol.StreamEvent, 64)

	go func() {
		defer close(ch)

		stopReason := resp.StopReason
		if stopReason == "" {
			stopReason = protocol.StopEnd
		}

		now := protocol.Now()
		partial := &protocol.AssistantMessage{
			Role:      "assistant",
			API:       model.API,
			Provider:  model.Provider,
			Model:     model.ID,
			Timestamp: now,
		}

		// snapshotPartial returns a copy of the current partial message.
		// This prevents data races: the goroutine mutates partial between
		// events, but each event carries an independent snapshot.
		snapshotPartial := func() *protocol.AssistantMessage {
			snap := *partial
			snap.Content = make([]protocol.ContentBlock, len(partial.Content))
			copy(snap.Content, partial.Content)
			return &snap
		}

		// Start event
		ch <- protocol.StreamEvent{
			Type:    protocol.EventStart,
			Partial: snapshotPartial(),
		}

		// Stream text content
		if resp.Text != "" {
			contentIndex := len(partial.Content)

			ch <- protocol.StreamEvent{
				Type:         protocol.EventTextStart,
				ContentIndex: contentIndex,
				Partial:      snapshotPartial(),
			}

			// Stream in chunks of ~5 chars
			text := resp.Text
			for i := 0; i < len(text); i += 5 {
				end := i + 5
				if end > len(text) {
					end = len(text)
				}
				delta := text[i:end]

				if resp.StreamDelay > 0 {
					select {
					case <-ctx.Done():
						ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopAborted, Message: snapshotPartial()}
						return
					case <-time.After(resp.StreamDelay):
					}
				}

				ch <- protocol.StreamEvent{
					Type:         protocol.EventTextDelta,
					ContentIndex: contentIndex,
					Delta:        delta,
					Partial:      snapshotPartial(),
				}
			}

			textContent := protocol.TextContent{Type: "text", Text: text}
			partial.Content = append(partial.Content, textContent)

			ch <- protocol.StreamEvent{
				Type:         protocol.EventTextEnd,
				ContentIndex: contentIndex,
				Content:      text,
				Partial:      snapshotPartial(),
			}
		}

		// Add tool calls
		for i, tc := range resp.ToolCalls {
			contentIndex := len(partial.Content)

			ch <- protocol.StreamEvent{
				Type:         protocol.EventToolCallStart,
				ContentIndex: contentIndex,
				Partial:      snapshotPartial(),
			}

			ch <- protocol.StreamEvent{
				Type:         protocol.EventToolCallDelta,
				ContentIndex: contentIndex,
				Delta:        tc.Arguments,
				Partial:      snapshotPartial(),
			}

			partial.Content = append(partial.Content, tc)

			ch <- protocol.StreamEvent{
				Type:         protocol.EventToolCallEnd,
				ContentIndex: contentIndex,
				ToolCall:     &resp.ToolCalls[i],
				Partial:      snapshotPartial(),
			}

			if stopReason == protocol.StopEnd && len(resp.ToolCalls) > 0 {
				stopReason = protocol.StopTool
			}
		}

		// Finalize
		partial.StopReason = stopReason
		partial.Usage = protocol.Usage{
			Input:       10,
			Output:      len(resp.Text) + len(resp.ToolCalls)*50,
			TotalTokens: len(resp.Text) + len(resp.ToolCalls)*50 + 10,
		}

		if stopReason == protocol.StopEnd || stopReason == protocol.StopTool {
			ch <- protocol.StreamEvent{
				Type:    protocol.EventDone,
				Reason:  stopReason,
				Message: snapshotPartial(),
				Partial: snapshotPartial(),
			}
		} else {
			ch <- protocol.StreamEvent{
				Type:    protocol.EventError,
				Reason:  stopReason,
				Message: snapshotPartial(),
				Partial: snapshotPartial(),
			}
		}
	}()

	return ch, nil
}

// --- Faux Credential Provider ---

// FauxCredentialProvider returns a fixed API key for any provider.
// For testing only.
type FauxCredentialProvider struct {
	Key string
}

func (f *FauxCredentialProvider) Resolve(_ protocol.ProviderId) (protocol.Credential, error) {
	if f.Key == "" {
		return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "no faux key configured"}
	}
	return protocol.ApiKey{Key: f.Key}, nil
}

// --- Faux Helper: Quick Setup ---

// NewFauxGateway creates a fully-wired LLMGateway with a FauxProvider
// for quick testing of L2/L3 layers.
func NewFauxGateway(responses ...FauxResponse) *LLMGateway {
	gw := NewLLMGateway()
	faux := NewFauxProvider(protocol.ApiOpenAICompletions, "faux", responses...)
	gw.Providers.Register(faux)
	gw.Credentials = &FauxCredentialProvider{Key: "test-key"}

	gw.Catalog.Register("faux", protocol.ModelDescriptor{
		ID:       "faux-model",
		Name:     "Faux Model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "faux",
		BaseURL:  "https://faux.example.com",
		Capabilities: protocol.ModelCapabilities{
			Input:          map[protocol.MediaType]bool{protocol.MediaText: true},
			Thinking:       false,
			ThinkingLevels: map[protocol.ThinkingLevel]bool{protocol.ThinkingOff: true},
			Caching:        map[protocol.CacheMode]bool{protocol.CacheNone: true},
			Transport:      map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
		Cost:          protocol.CostRates{Input: 0, Output: 0, CacheRead: 0, CacheWrite: 0},
		ContextWindow: 128000,
		MaxTokens:     4096,
	})

	return gw
}

// FauxModel returns the default faux model descriptor.
func FauxModel() protocol.ModelDescriptor {
	return protocol.ModelDescriptor{
		ID:       "faux-model",
		Name:     "Faux Model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "faux",
		BaseURL:  "https://faux.example.com",
		Capabilities: protocol.ModelCapabilities{
			Input:          map[protocol.MediaType]bool{protocol.MediaText: true},
			Thinking:       false,
			ThinkingLevels: map[protocol.ThinkingLevel]bool{protocol.ThinkingOff: true},
			Caching:        map[protocol.CacheMode]bool{protocol.CacheNone: true},
			Transport:      map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
		Cost:          protocol.CostRates{Input: 0, Output: 0, CacheRead: 0, CacheWrite: 0},
		ContextWindow: 128000,
		MaxTokens:     4096,
	}
}

// Verify FauxProvider implements ProviderPort at compile time.
var _ ProviderPort = (*FauxProvider)(nil)

// Verify FauxCredentialProvider implements CredentialProvider at compile time.
var _ CredentialProvider = (*FauxCredentialProvider)(nil)

// Compile-time check placeholder.
var _ context.Context
