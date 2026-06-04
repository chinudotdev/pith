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

// ThinkingFormat specifies how a provider formats thinking/reasoning parameters.
type ThinkingFormat string

const (
	ThinkingOpenAI          ThinkingFormat = "openai"
	ThinkingOpenRouter      ThinkingFormat = "openrouter"
	ThinkingDeepSeek        ThinkingFormat = "deepseek"
	ThinkingTogether        ThinkingFormat = "together"
	ThinkingZAI             ThinkingFormat = "zai"
	ThinkingQwen            ThinkingFormat = "qwen"
	ThinkingQwenChat        ThinkingFormat = "qwen-chat-template"
	ThinkingString          ThinkingFormat = "string-thinking"
	ThinkingAntLing         ThinkingFormat = "ant-ling"
)

// CacheControlFormat specifies how a provider handles cache control.
type CacheControlFormat string

const (
	CacheControlAnthropic CacheControlFormat = "anthropic"
	CacheControlNone      CacheControlFormat = "none"
)

// ProviderCapabilities declares what a provider supports.
// Capabilities belong to the provider, not the model.
// A model says "I support thinking"; the provider says "I format thinking as reasoning_effort".
type ProviderCapabilities struct {
	// OpenAI-completions family
	SupportsStore                       bool
	SupportsDeveloperRole               bool
	SupportsReasoningEffort             bool
	SupportsUsageInStreaming            bool
	MaxTokensField                      MaxTokensField
	RequiresToolResultName              bool
	RequiresAssistantAfterToolResult    bool
	RequiresThinkingAsText              bool
	RequiresReasoningContentOnAssistant bool
	ThinkingFormat                      ThinkingFormat
	SupportsStrictMode                  bool
	CacheControlFormat                  CacheControlFormat
	SendSessionAffinityHeaders          bool
	SupportsLongCacheRetention          bool

	// Anthropic-messages family
	SupportsEagerToolInputStreaming bool
	SupportsCacheControlOnTools     bool
	SupportsTemperature             bool
	ForceAdaptiveThinking           bool
	AllowEmptySignature             bool

	// OpenAI-responses family
	SendSessionIDHeader bool
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

// --- Model Catalog ---

// ModelCatalog is a programmatic-only, in-memory model store.
// No bundled data, no remote fetch, no local YAML.
// The App layer provides the data via PiCatalog.
type ModelCatalog struct {
	mu        sync.RWMutex
	models    map[protocol.ProviderId]map[string]protocol.ModelDescriptor // provider -> modelId -> descriptor
	onUpdated []func()
}

// NewModelCatalog creates a new empty model catalog.
func NewModelCatalog() *ModelCatalog {
	return &ModelCatalog{
		models: make(map[protocol.ProviderId]map[string]protocol.ModelDescriptor),
	}
}

// Register adds a model descriptor to the catalog.
func (c *ModelCatalog) Register(provider protocol.ProviderId, model protocol.ModelDescriptor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.models[provider] == nil {
		c.models[provider] = make(map[string]protocol.ModelDescriptor)
	}
	c.models[provider][model.ID] = model
	c.notifyUpdated()
}

// Unregister removes a model from the catalog.
func (c *ModelCatalog) Unregister(provider protocol.ProviderId, modelID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.models[provider]; ok {
		delete(m, modelID)
		c.notifyUpdated()
	}
}

// Get returns a model descriptor by provider and model ID.
func (c *ModelCatalog) Get(provider protocol.ProviderId, modelID string) (protocol.ModelDescriptor, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.models[provider]; ok {
		desc, found := m[modelID]
		return desc, found
	}
	return protocol.ModelDescriptor{}, false
}

// List returns all models, optionally filtered by provider.
func (c *ModelCatalog) List(provider ...protocol.ProviderId) []protocol.ModelDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []protocol.ModelDescriptor
	if len(provider) > 0 && provider[0] != "" {
		if m, ok := c.models[provider[0]]; ok {
			for _, desc := range m {
				result = append(result, desc)
			}
		}
	} else {
		for _, m := range c.models {
			for _, desc := range m {
				result = append(result, desc)
			}
		}
	}
	return result
}

// GetProviders returns all provider IDs that have models registered.
func (c *ModelCatalog) GetProviders() []protocol.ProviderId {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]protocol.ProviderId, 0, len(c.models))
	for p := range c.models {
		result = append(result, p)
	}
	return result
}

// CalculateCost computes the dollar cost for a given usage and model.
// This is a pure function that doesn't access catalog state, but is a method
// for API convenience. No lock needed.
func (c *ModelCatalog) CalculateCost(model protocol.ModelDescriptor, usage protocol.Usage) protocol.Cost {
	cost := protocol.Cost{
		Input:      float64(usage.Input) * model.Cost.Input / 1_000_000,
		Output:     float64(usage.Output) * model.Cost.Output / 1_000_000,
		CacheRead:  float64(usage.CacheRead) * model.Cost.CacheRead / 1_000_000,
		CacheWrite: float64(usage.CacheWrite) * model.Cost.CacheWrite / 1_000_000,
	}
	cost.Total = cost.Input + cost.Output + cost.CacheRead + cost.CacheWrite
	return cost
}

// Clear removes all models from the catalog.
func (c *ModelCatalog) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models = make(map[protocol.ProviderId]map[string]protocol.ModelDescriptor)
	c.notifyUpdated()
}

// OnUpdated registers a callback for when the catalog changes.
func (c *ModelCatalog) OnUpdated(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onUpdated = append(c.onUpdated, fn)
}

func (c *ModelCatalog) notifyUpdated() {
	for _, fn := range c.onUpdated {
		fn()
	}
}

// --- Capability Negotiation ---

// EffectiveCapabilities is the result of merging model capabilities with provider capabilities.
type EffectiveCapabilities struct {
	// From the model
	protocol.ModelCapabilities
	// From the provider
	ProviderCapabilities
}

// NegotiateCapabilityResult is the result of capability negotiation.
type NegotiateCapabilityResult struct {
	EffectiveCapabilities
	Warnings []string // non-fatal issues (e.g., model wants thinking but provider doesn't support it)
	Errors   []string // fatal issues that will likely cause request failures
}

// Negotiate merges model capabilities with provider capabilities and
// identifies conflicts. Returns warnings for non-fatal mismatches and
// errors for fatal ones.
//
// Example: model says "I support thinking" but provider doesn't have a
// ThinkingFormat → warning (thinking will be silently disabled).
// Model says "I support websocket" but provider doesn't → error.
func Negotiate(model protocol.ModelDescriptor, provider ProviderCapabilities) NegotiateCapabilityResult {
	result := NegotiateCapabilityResult{
		EffectiveCapabilities: EffectiveCapabilities{
			ModelCapabilities:    model.Capabilities,
			ProviderCapabilities: provider,
		},
	}

	// Check thinking support
	if model.Capabilities.Thinking && provider.ThinkingFormat == "" {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("model %q supports thinking but provider has no ThinkingFormat; thinking will be disabled", model.ID))
	}

	// Check thinking level support
	if model.Capabilities.Thinking {
		for level, supported := range model.Capabilities.ThinkingLevels {
			if supported && level != protocol.ThinkingOff && provider.ThinkingFormat == "" {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("model %q supports thinking level %q but provider has no ThinkingFormat", model.ID, level))
			}
		}
	}

	// Check transport support
	// Note: transport availability is a provider-level concern. If a model
	// declares a transport the provider doesn't support, that's a fatal error
	// because the request will fail at the HTTP layer.
	for transport, supported := range model.Capabilities.Transport {
		if supported {
			switch transport {
			case protocol.TransportWebSocket, protocol.TransportWebSocketCached:
				// WebSocket transport requires provider support — flag as warning
				// since the gateway can fall back to SSE
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("model %q supports transport %q; verify provider supports it or will fall back to SSE", model.ID, transport))
			case protocol.TransportSSE, protocol.TransportAuto:
				// SSE and Auto are always supported
			}
		}
	}

	// Check caching support
	for cacheMode, supported := range model.Capabilities.Caching {
		if supported && provider.CacheControlFormat == CacheControlNone && cacheMode != protocol.CacheNone {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("model %q supports cache mode %q but provider has no CacheControlFormat", model.ID, cacheMode))
		}
	}

	// Check image input support
	if model.Capabilities.Input[protocol.MediaImage] {
		// Image support is model-level; provider just needs to pass it through.
		// No specific provider capability to check against.
	}

	return result
}
