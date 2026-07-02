package gateway

import (
	"context"

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
