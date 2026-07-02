package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/chinudotdev/pith/protocol"
)

// --- Credential Provider ---

// CredentialProvider resolves credentials for a given provider.
// The SDK defines only the interface. The App provides implementations
// (env vars, files, ADC, vault, etc.).
type CredentialProvider interface {
	// Resolve returns a credential for the given provider, or an error.
	Resolve(providerID protocol.ProviderId) (protocol.Credential, error)
}

// CredentialProviderFunc is a function adapter for CredentialProvider.
type CredentialProviderFunc func(providerID protocol.ProviderId) (protocol.Credential, error)

func (f CredentialProviderFunc) Resolve(providerID protocol.ProviderId) (protocol.Credential, error) {
	return f(providerID)
}

// --- Provider Port ---

// ProviderPort is the adapter interface for an LLM API provider.
// Each provider implements this interface (Anthropic, OpenAI, etc.).
type ProviderPort interface {
	// API returns the API identifier this provider handles (e.g. "anthropic-messages").
	API() protocol.ApiId

	// Name returns a human-readable name for this provider.
	Name() string

	// Capabilities returns the declared capabilities of this provider.
	Capabilities() ProviderCapabilities

	// Initialize is called on first use. Providers may do lazy setup here.
	Initialize() error

	// Cleanup releases any resources held by the provider.
	Cleanup()

	// Stream performs a streaming LLM call and returns a channel of events.
	// The context.Context supports cancellation (abort) and timeouts.
	Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error)
}

// --- Provider Capabilities ---

// MaxTokensField specifies which JSON field to use for max tokens.
type MaxTokensField string

const (
	MaxTokensCompletion MaxTokensField = "max_completion_tokens"
	MaxTokensLegacy     MaxTokensField = "max_tokens"
)

// ProviderCapabilities declares what a provider supports.
// Capabilities belong to the provider, not the model.
// A model says "I support thinking"; the provider says "I format thinking as reasoning_effort".
type ProviderCapabilities struct {
	SupportsDeveloperRole    bool
	SupportsReasoningEffort  bool
	SupportsUsageInStreaming bool
	MaxTokensField           MaxTokensField
	RequiresToolResultName   bool
	RequiresThinkingAsText   bool
}

// --- Provider Registry ---

// ProviderRegistry is an instance-based registry of LLM providers.
// Not a module-level singleton. Each LLMGateway has its own registry.
type ProviderRegistry struct {
	mu          sync.Mutex
	providers   map[protocol.ApiId]ProviderPort
	initialized map[protocol.ApiId]bool
}

// NewProviderRegistry creates a new empty provider registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers:   make(map[protocol.ApiId]ProviderPort),
		initialized: make(map[protocol.ApiId]bool),
	}
}

// Register adds a provider to the registry.
func (r *ProviderRegistry) Register(p ProviderPort) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.API()] = p
	r.initialized[p.API()] = false // reset on re-register
}

// Unregister removes a provider from the registry.
func (r *ProviderRegistry) Unregister(api protocol.ApiId) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, api)
	delete(r.initialized, api)
}

// Get returns a provider by API identifier, or nil if not found.
func (r *ProviderRegistry) Get(api protocol.ApiId) ProviderPort {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.providers[api]
}

// List returns all registered providers.
func (r *ProviderRegistry) List() []ProviderPort {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]ProviderPort, 0, len(r.providers))
	for _, p := range r.providers {
		result = append(result, p)
	}
	return result
}

// Clear removes all providers from the registry.
func (r *ProviderRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers = make(map[protocol.ApiId]ProviderPort)
	r.initialized = make(map[protocol.ApiId]bool)
}

// EnsureInitialized calls Initialize on the provider for api once and returns
// any initialization error.
func (r *ProviderRegistry) EnsureInitialized(api protocol.ApiId) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.initialized[api] {
		return nil
	}
	provider := r.providers[api]
	if provider == nil {
		return &protocol.Error{
			Code:    protocol.ErrNotSupported,
			Message: fmt.Sprintf("no provider registered for API %q", api),
		}
	}
	if err := provider.Initialize(); err != nil {
		return err
	}
	r.initialized[api] = true
	return nil
}

// LLMGateway is the top-level L1 composer. It owns the provider registry,
// credential provider, and middleware pipeline.
// Instance-based: no module-level singletons. Create with NewLLMGateway().
type LLMGateway struct {
	Providers   *ProviderRegistry
	Credentials CredentialProvider
	Middleware  []Middleware
}

// NewLLMGateway creates a new LLMGateway with empty registries.
func NewLLMGateway() *LLMGateway {
	return &LLMGateway{
		Providers: NewProviderRegistry(),
		Credentials: CredentialProviderFunc(func(protocol.ProviderId) (protocol.Credential, error) {
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "no credential provider configured"}
		}),
	}
}

// Stream performs a streaming LLM call through the full pipeline:
// resolve credentials → apply middleware → call provider.
func (g *LLMGateway) Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	provider := g.Providers.Get(model.API)
	if provider == nil {
		return nil, &protocol.Error{
			Code:    protocol.ErrNotSupported,
			Message: fmt.Sprintf("no provider registered for API %q", model.API),
		}
	}

	// Deep-copy opts so middleware and providers can freely mutate it.
	optsCopy := opts
	opts = optsCopy
	if opts.Headers != nil {
		opts.Headers = copyMap(opts.Headers)
	}
	if opts.Metadata != nil {
		opts.Metadata = copyMapAny(opts.Metadata)
	}
	if opts.Temperature != nil {
		v := *opts.Temperature
		opts.Temperature = &v
	}
	if opts.MaxTokens != nil {
		v := *opts.MaxTokens
		opts.MaxTokens = &v
	}
	if opts.MaxRetryDelayMs != nil {
		v := *opts.MaxRetryDelayMs
		opts.MaxRetryDelayMs = &v
	}

	// Resolve credentials
	if g.Credentials != nil {
		cred, err := g.Credentials.Resolve(model.Provider)
		if err != nil {
			return nil, &protocol.Error{
				Code:    protocol.ErrAuth,
				Message: fmt.Sprintf("failed to resolve credentials for provider %q: %s", model.Provider, err.Error()),
				Cause:   err,
			}
		}
		opts.Credential = cred
	}

	if err := g.Providers.EnsureInitialized(model.API); err != nil {
		return nil, &protocol.Error{
			Code:    protocol.ErrUnknown,
			Message: fmt.Sprintf("failed to initialize provider %q: %s", provider.Name(), err.Error()),
			Cause:   err,
		}
	}

	streamFn := func(ctx context.Context, m protocol.ModelDescriptor, p protocol.Context, o protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		return provider.Stream(ctx, m, p, o)
	}

	for i := len(g.Middleware) - 1; i >= 0; i-- {
		streamFn = g.Middleware[i].Wrap(streamFn)
	}

	return streamFn(ctx, model, pctx, opts)
}

// Complete performs a non-streaming LLM call. It streams internally
// and returns the final accumulated message.
func (g *LLMGateway) Complete(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (*protocol.AssistantMessage, error) {
	ch, err := g.Stream(ctx, model, pctx, opts)
	if err != nil {
		return nil, err
	}

	var final *protocol.AssistantMessage
	for event := range ch {
		if event.Type == protocol.EventDone && event.Message != nil {
			final = event.Message
		}
		if event.Type == protocol.EventError {
			if event.Message != nil {
				return nil, &protocol.Error{
					Code:    protocol.ErrUnknown,
					Message: event.Message.ErrorMessage,
				}
			}
			return nil, &protocol.Error{
				Code:    protocol.ErrUnknown,
				Message: "streaming error with no message",
			}
		}
	}

	if final == nil {
		return nil, &protocol.Error{
			Code:    protocol.ErrUnknown,
			Message: "stream completed without a Done event",
		}
	}
	return final, nil
}

func copyMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func copyMapAny(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
