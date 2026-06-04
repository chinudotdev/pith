package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/chinudotdev/pith/protocol"
)

// --- Middleware ---

// Middleware is a composable pipeline stage that can intercept and modify
// LLM requests and responses. Each middleware can short-circuit the pipeline.
type Middleware interface {
	// Name returns a unique name for this middleware.
	Name() string

	// Wrap wraps a StreamFunction with this middleware's behavior.
	// The inner function is the next stage in the pipeline (or the actual provider).
	Wrap(inner StreamFunction) StreamFunction
}

// StreamFunction is the signature for a function that streams LLM completions.
type StreamFunction func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error)

// --- Built-in Middleware Types ---

// RetryPolicy retries failed requests with configurable backoff.
//
// Note: RetryPolicy only retries on errors returned by the inner StreamFunction
// (i.e., errors that occur before the stream channel is created). Mid-stream
// errors (EventError on the channel) are NOT retried, as partial events have
// already been delivered to the consumer. For mid-stream resilience, use
// application-level retry logic.
type RetryPolicy struct {
	MaxAttempts  int
	Backoff      BackoffStrategy
	RetryableOn  map[protocol.ErrorCode]bool
}

func (r *RetryPolicy) Name() string { return "retry" }

func (r *RetryPolicy) Wrap(inner StreamFunction) StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		var lastErr error
		for attempt := 0; attempt < r.MaxAttempts; attempt++ {
			if attempt > 0 {
				delay := r.Backoff.Delay(attempt)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
			}
			ch, err := inner(ctx, model, pctx, opts)
			if err == nil {
				return ch, nil
			}
			lastErr = err
			if perr, ok := err.(*protocol.Error); ok {
				if !r.RetryableOn[perr.Code] {
					return nil, err
				}
			}
		}
		return nil, lastErr
	}
}

// BackoffStrategy computes delay between retry attempts.
type BackoffStrategy interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay on each attempt.
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func (e *ExponentialBackoff) Delay(attempt int) time.Duration {
	delay := e.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * e.Multiplier)
		if delay > e.MaxDelay {
			delay = e.MaxDelay
		}
	}
	return delay
}

// FixedBackoff returns a constant delay.
type FixedBackoff struct {
	Wait time.Duration
}

func (f *FixedBackoff) Delay(_ int) time.Duration { return f.Wait }

// RateLimiter limits requests per minute and tokens per minute.
//
// NOTE: This is a stub that panics on use. Implement token bucket rate limiting
// before using in production, or replace with a middleware that integrates with
// your infrastructure's rate limiting service.
type RateLimiter struct {
	MaxRPM int
	MaxTPM int
}

func (r *RateLimiter) Name() string { return "rateLimiter" }

func (r *RateLimiter) Wrap(inner StreamFunction) StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		panic("RateLimiter is a stub and must not be used in production. Implement token bucket rate limiting or remove this middleware.")
	}
}

// HeaderInjector adds custom headers to requests.
type HeaderInjector struct {
	Headers map[string]string
}

func (h *HeaderInjector) Name() string { return "headerInjector" }

func (h *HeaderInjector) Wrap(inner StreamFunction) StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		for k, v := range h.Headers {
			opts.Headers[k] = v
		}
		return inner(ctx, model, pctx, opts)
	}
}

// --- LLM Gateway ---

// LLMGateway is the top-level L1 composer. It owns the provider registry,
// model catalog, credential provider, and middleware pipeline.
// Instance-based: no module-level singletons. Create with NewLLMGateway().
type LLMGateway struct {
	Providers   *ProviderRegistry
	Catalog     *ModelCatalog
	Credentials CredentialProvider
	Middleware  []Middleware
}

// NewLLMGateway creates a new LLMGateway with empty registries.
func NewLLMGateway() *LLMGateway {
	return &LLMGateway{
		Providers:   NewProviderRegistry(),
		Catalog:     NewModelCatalog(),
		Credentials: CredentialProviderFunc(func(protocol.ProviderId) (protocol.Credential, error) {
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "no credential provider configured"}
		}),
	}
}

// Stream performs a streaming LLM call through the full pipeline:
// resolve credentials → negotiate capabilities → apply middleware → call provider.
func (g *LLMGateway) Stream(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	provider := g.Providers.Get(model.API)
	if provider == nil {
		return nil, &protocol.Error{
			Code:    protocol.ErrNotSupported,
			Message: fmt.Sprintf("no provider registered for API %q", model.API),
		}
	}

	// Deep-copy opts so middleware and providers can freely mutate it.
	// The struct is shallow-copied, then mutable reference types (maps, pointers)
	// are deep-copied per the Middleware Ownership Contract (§4).
	optsCopy := opts
	opts = optsCopy
	if opts.Headers != nil {
		opts.Headers = copyMap(opts.Headers)
	}
	if opts.Metadata != nil {
		opts.Metadata = copyMapAny(opts.Metadata)
	}
	if opts.ThinkingBudgets != nil {
		tb := *opts.ThinkingBudgets
		opts.ThinkingBudgets = &tb
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
		// Inject credential into the typed field for providers to read.
		opts.Credential = cred
	}

	// Initialize provider on first use only
	g.Providers.mu.Lock()
	if !g.Providers.initialized[model.API] {
		if err := provider.Initialize(); err != nil {
			g.Providers.mu.Unlock()
			return nil, &protocol.Error{
				Code:    protocol.ErrUnknown,
				Message: fmt.Sprintf("failed to initialize provider %q: %s", provider.Name(), err.Error()),
				Cause:   err,
			}
		}
		g.Providers.initialized[model.API] = true
	}
	g.Providers.mu.Unlock()

	// Build the stream function with middleware pipeline
	streamFn := func(ctx context.Context, m protocol.ModelDescriptor, p protocol.Context, o protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		return provider.Stream(ctx, m, p, o)
	}

	// Apply middleware in reverse order so the first middleware wraps the outermost
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
				return event.Message, &protocol.Error{
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

// StreamSimple is a convenience that maps a ThinkingLevel to provider-specific
// reasoning parameters before calling Stream.
func (g *LLMGateway) StreamSimple(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
	if opts.Reasoning == "" {
		opts.Reasoning = protocol.ThinkingOff
	}
	return g.Stream(ctx, model, pctx, opts)
}

// CompleteSimple is a convenience that maps a ThinkingLevel before calling Complete.
func (g *LLMGateway) CompleteSimple(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (*protocol.AssistantMessage, error) {
	if opts.Reasoning == "" {
		opts.Reasoning = protocol.ThinkingOff
	}
	return g.Complete(ctx, model, pctx, opts)
}

// copyMap creates a shallow copy of a map[string]string.
func copyMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// copyMapAny creates a shallow copy of a map[string]any.
func copyMapAny(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
