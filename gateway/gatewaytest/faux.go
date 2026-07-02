package gatewaytest

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/protocol"
)

// FauxProvider is a test double that returns pre-configured responses
// without making any real API calls.
type FauxProvider struct {
	api          protocol.ApiId
	name         string
	capabilities gateway.ProviderCapabilities
	responses    []FauxResponse
	index        atomic.Int32
	initialized  bool
}

type FauxResponse struct {
	Text        string
	ToolCalls   []protocol.ToolCall
	StopReason  protocol.StopReason
	Error       error
	StreamDelay time.Duration
}

func NewFauxProvider(api protocol.ApiId, name string, responses ...FauxResponse) *FauxProvider {
	return &FauxProvider{
		api:       api,
		name:      name,
		responses: responses,
	}
}

func (f *FauxProvider) WithCapabilities(caps gateway.ProviderCapabilities) *FauxProvider {
	f.capabilities = caps
	return f
}

func (f *FauxProvider) API() protocol.ApiId                        { return f.api }
func (f *FauxProvider) Name() string                               { return f.name }
func (f *FauxProvider) Capabilities() gateway.ProviderCapabilities { return f.capabilities }
func (f *FauxProvider) Initialize() error                          { f.initialized = true; return nil }
func (f *FauxProvider) Cleanup()                                   { f.initialized = false }

func (f *FauxProvider) Stream(ctx context.Context, model protocol.ModelDescriptor, _ protocol.Context, _ protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
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

		partial := &protocol.AssistantMessage{
			Role:      "assistant",
			API:       model.API,
			Provider:  model.Provider,
			Model:     model.ID,
			Timestamp: protocol.Now(),
		}

		snapshotPartial := func() *protocol.AssistantMessage {
			snap := *partial
			snap.Content = make([]protocol.ContentBlock, len(partial.Content))
			copy(snap.Content, partial.Content)
			return &snap
		}

		ch <- protocol.StreamEvent{Type: protocol.EventStart, Partial: snapshotPartial()}

		if resp.Text != "" {
			contentIndex := len(partial.Content)
			ch <- protocol.StreamEvent{Type: protocol.EventTextStart, ContentIndex: contentIndex, Partial: snapshotPartial()}
			for i := 0; i < len(resp.Text); i += 5 {
				end := i + 5
				if end > len(resp.Text) {
					end = len(resp.Text)
				}
				if resp.StreamDelay > 0 {
					select {
					case <-ctx.Done():
						ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: protocol.StopAborted, Message: snapshotPartial()}
						return
					case <-time.After(resp.StreamDelay):
					}
				}
				ch <- protocol.StreamEvent{Type: protocol.EventTextDelta, ContentIndex: contentIndex, Delta: resp.Text[i:end], Partial: snapshotPartial()}
			}
			partial.Content = append(partial.Content, protocol.TextContent{Type: "text", Text: resp.Text})
			ch <- protocol.StreamEvent{Type: protocol.EventTextEnd, ContentIndex: contentIndex, Content: resp.Text, Partial: snapshotPartial()}
		}

		for i, tc := range resp.ToolCalls {
			contentIndex := len(partial.Content)
			ch <- protocol.StreamEvent{Type: protocol.EventToolCallStart, ContentIndex: contentIndex, Partial: snapshotPartial()}
			ch <- protocol.StreamEvent{Type: protocol.EventToolCallDelta, ContentIndex: contentIndex, Delta: tc.Arguments, Partial: snapshotPartial()}
			partial.Content = append(partial.Content, tc)
			ch <- protocol.StreamEvent{Type: protocol.EventToolCallEnd, ContentIndex: contentIndex, ToolCall: &resp.ToolCalls[i], Partial: snapshotPartial()}
			if stopReason == protocol.StopEnd && len(resp.ToolCalls) > 0 {
				stopReason = protocol.StopTool
			}
		}

		partial.StopReason = stopReason
		partial.Usage = protocol.Usage{
			Input:       10,
			Output:      len(resp.Text) + len(resp.ToolCalls)*50,
			TotalTokens: len(resp.Text) + len(resp.ToolCalls)*50 + 10,
		}

		if stopReason == protocol.StopEnd || stopReason == protocol.StopTool {
			ch <- protocol.StreamEvent{Type: protocol.EventDone, Reason: stopReason, Message: snapshotPartial(), Partial: snapshotPartial()}
			return
		}
		ch <- protocol.StreamEvent{Type: protocol.EventError, Reason: stopReason, Message: snapshotPartial(), Partial: snapshotPartial()}
	}()

	return ch, nil
}

type FauxCredentialProvider struct {
	Key string
}

func (f *FauxCredentialProvider) Resolve(_ protocol.ProviderId) (protocol.Credential, error) {
	if f.Key == "" {
		return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "no faux key configured"}
	}
	return protocol.ApiKey{Key: f.Key}, nil
}

func NewFauxGateway(responses ...FauxResponse) *gateway.LLMGateway {
	gw := gateway.NewLLMGateway()
	faux := NewFauxProvider(protocol.ApiOpenAICompletions, "faux", responses...)
	gw.Providers.Register(faux)
	gw.Credentials = &FauxCredentialProvider{Key: "test-key"}
	return gw
}

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

var _ gateway.ProviderPort = (*FauxProvider)(nil)
var _ gateway.CredentialProvider = (*FauxCredentialProvider)(nil)
