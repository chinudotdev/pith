package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/gatewaytest"
	"github.com/chinudotdev/pith/protocol"
)

type middlewareRecorder struct {
	name string
	log  *[]string
}

func (m *middlewareRecorder) Name() string { return m.name }
func (m *middlewareRecorder) Wrap(inner gateway.StreamFunction) gateway.StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		*m.log = append(*m.log, "before:"+m.name)
		ch, err := inner(ctx, model, pctx, opts)
		if err != nil {
			*m.log = append(*m.log, "after:"+m.name)
			return nil, err
		}
		*m.log = append(*m.log, "after:"+m.name)
		return ch, nil
	}
}

type credentialProbeProvider struct {
	seenCredential bool
}

func (p *credentialProbeProvider) API() protocol.ApiId { return protocol.ApiOpenAICompletions }
func (p *credentialProbeProvider) Name() string        { return "credential-probe" }
func (p *credentialProbeProvider) Capabilities() gateway.ProviderCapabilities {
	return gateway.ProviderCapabilities{}
}
func (p *credentialProbeProvider) Initialize() error { return nil }
func (p *credentialProbeProvider) Cleanup()          {}
func (p *credentialProbeProvider) Stream(_ context.Context, model protocol.ModelDescriptor, _ protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	p.seenCredential = opts.Credential != nil
	ch := make(chan protocol.StreamEvent, 1)
	ch <- protocol.StreamEvent{
		Type: protocol.EventDone,
		Message: &protocol.AssistantMessage{
			Role:       "assistant",
			Content:    []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "ok"}},
			StopReason: protocol.StopEnd,
			Model:      model.ID,
			API:        model.API,
			Provider:   model.Provider,
			Timestamp:  protocol.Now(),
		},
	}
	close(ch)
	return ch, nil
}

func TestProviderRegistryRegisterAndGet(t *testing.T) {
	reg := gateway.NewProviderRegistry()
	provider := gatewaytest.NewFauxProvider(protocol.ApiOpenAICompletions, "faux", gatewaytest.FauxResponse{Text: "hello"})
	reg.Register(provider)

	got := reg.Get(protocol.ApiOpenAICompletions)
	if got == nil || got.Name() != "faux" {
		t.Fatal("expected provider to be registered and retrievable")
	}
}

func TestMiddlewareOrdering(t *testing.T) {
	gw := gatewaytest.NewFauxGateway(gatewaytest.FauxResponse{Text: "hello"})
	model := gatewaytest.FauxModel()
	callLog := []string{}
	gw.Middleware = []gateway.Middleware{
		&middlewareRecorder{name: "outer", log: &callLog},
		&middlewareRecorder{name: "inner", log: &callLog},
	}

	ch, err := gw.Stream(context.Background(), model, protocol.Context{}, protocol.StreamOptions{})
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	for range ch {
	}

	want := []string{"before:outer", "before:inner", "after:inner", "after:outer"}
	if len(callLog) != len(want) {
		t.Fatalf("unexpected middleware call count: got %d want %d", len(callLog), len(want))
	}
	for i := range want {
		if callLog[i] != want[i] {
			t.Fatalf("middleware order mismatch at %d: got %q want %q", i, callLog[i], want[i])
		}
	}
}

func TestCredentialInjection(t *testing.T) {
	gw := gateway.NewLLMGateway()
	probe := &credentialProbeProvider{}
	gw.Providers.Register(probe)
	gw.Credentials = gateway.CredentialProviderFunc(func(providerID protocol.ProviderId) (protocol.Credential, error) {
		return protocol.ApiKey{Key: "test"}, nil
	})
	model := protocol.ModelDescriptor{
		ID:       "probe-model",
		API:      protocol.ApiOpenAICompletions,
		Provider: "probe-provider",
	}

	ch, err := gw.Stream(context.Background(), model, protocol.Context{}, protocol.StreamOptions{})
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	for range ch {
	}
	if !probe.seenCredential {
		t.Fatal("expected credential to be injected into provider stream options")
	}
}

func TestCompleteErrorPath(t *testing.T) {
	gw := gatewaytest.NewFauxGateway(gatewaytest.FauxResponse{StopReason: protocol.StopError})
	model := gatewaytest.FauxModel()
	_, err := gw.Complete(context.Background(), model, protocol.Context{}, protocol.StreamOptions{})
	if err == nil {
		t.Fatal("expected complete to return error on EventError")
	}
	var perr *protocol.Error
	if !errors.As(err, &perr) {
		t.Fatalf("expected protocol.Error, got %T", err)
	}
}
