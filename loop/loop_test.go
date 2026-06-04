package loop_test

import (
	"context"
	"testing"
	"time"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// TestStreamFnReceivesContext verifies that the loop passes context.Context
// to StreamFn, enabling abort propagation (Issue #4 regression test).
func TestStreamFnReceivesContext(t *testing.T) {
	gw := gateway.NewFauxGateway(
		gateway.FauxResponse{Text: "Hello!", StreamDelay: 50 * time.Millisecond},
	)

	model, _ := gw.Catalog.Get("faux", "faux-model")

	var streamFnCtx context.Context
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtx = ctx
			return gw.Stream(ctx, m, pctx, opts)
		},
	}

	agentCtx := loop.AgentContext{
		SystemPrompt: "You are helpful.",
		Messages:     nil,
	}

	prompts := []protocol.Message{
		protocol.UserMessage{
			Role:      "user",
			Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}},
			Timestamp: protocol.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if streamFnCtx == nil {
		t.Fatal("StreamFn should receive a non-nil context.Context")
	}

	// The context should be the same one passed to AgentLoop (or a child)
	if streamFnCtx != ctx {
		// It may be a derived context, but it should have the same deadline/cancel
		select {
		case <-streamFnCtx.Done():
			t.Error("streamFn context should not be done yet")
		default:
		}
	}
}

// TestLoopAbortCancelsStreamFn verifies that cancelling the loop context
// cancels the context passed to StreamFn.
func TestLoopAbortCancelsStreamFn(t *testing.T) {
	gw := gateway.NewFauxGateway(
		gateway.FauxResponse{Text: "Hello!", StreamDelay: 100 * time.Millisecond},
	)

	model, _ := gw.Catalog.Get("faux", "faux-model")

	streamFnCtxCh := make(chan context.Context, 1)
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtxCh <- ctx
			return gw.Stream(ctx, m, pctx, opts)
		},
	}

	agentCtx := loop.AgentContext{
		SystemPrompt: "You are helpful.",
		Messages:     nil,
	}

	prompts := []protocol.Message{
		protocol.UserMessage{
			Role:      "user",
			Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "hi"}},
			Timestamp: protocol.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
	}()

	// Wait for StreamFn to receive the context
	var streamFnCtx context.Context
	select {
	case streamFnCtx = <-streamFnCtxCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamFn to be called")
	}

	// Cancel the loop context
	cancel()

	// The StreamFn's context should be cancelled
	select {
	case <-streamFnCtx.Done():
		// Good — context was cancelled
	case <-time.After(2 * time.Second):
		t.Error("StreamFn context should be cancelled after loop context is cancelled")
	}

	// Loop should finish
	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Error("loop should finish after context cancellation")
	}
}
