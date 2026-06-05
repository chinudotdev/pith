package loop_test

import (
	"context"
	"testing"
	"time"

	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

// TestStreamFnReceivesContext verifies that the loop passes context.Context
// to StreamFn, enabling abort propagation (Issue #4 regression test).
func TestStreamFnReceivesContext(t *testing.T) {
	model := testModel()

	var streamFnCtx context.Context
	baseStream := fakeStream(streamFakeConfig{Text: "Hello!", StreamDelay: 50 * time.Millisecond})
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtx = ctx
			return baseStream(ctx, m, pctx, opts)
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

	if streamFnCtx != ctx {
		select {
		case <-streamFnCtx.Done():
			t.Error("streamFn context should not be done yet")
		default:
		}
	}
}

// TestLoopAbortReturnsError verifies that cancelling the loop context
// causes AgentLoop to return a non-nil error (not silently succeed).
// Regression test for Issue #4: abort was returning nil error because
// streamResponse swallowed EventError with StopAborted.
func TestLoopAbortReturnsError(t *testing.T) {
	model := testModel()

	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: fakeStream(streamFakeConfig{
			Text:        "Hello world, this is a long response that should be interrupted!",
			StreamDelay: 100 * time.Millisecond,
		}),
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

	loopDone := make(chan error, 1)
	go func() {
		_, err := loop.AgentLoop(ctx, prompts, agentCtx, agConfig, func(event loop.LoopEvent) {})
		loopDone <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-loopDone:
		if err == nil {
			t.Error("AgentLoop should return a non-nil error when context is cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for AgentLoop to finish")
	}
}

// TestLoopAbortCancelsStreamFn verifies that cancelling the loop context
// cancels the context passed to StreamFn.
func TestLoopAbortCancelsStreamFn(t *testing.T) {
	model := testModel()

	streamFnCtxCh := make(chan context.Context, 1)
	baseStream := fakeStream(streamFakeConfig{Text: "Hello!", StreamDelay: 100 * time.Millisecond})
	agConfig := loop.LoopConfig{
		Model: model,
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			streamFnCtxCh <- ctx
			return baseStream(ctx, m, pctx, opts)
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

	var streamFnCtx context.Context
	select {
	case streamFnCtx = <-streamFnCtxCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StreamFn to be called")
	}

	cancel()

	select {
	case <-streamFnCtx.Done():
	case <-time.After(2 * time.Second):
		t.Error("StreamFn context should be cancelled after loop context is cancelled")
	}

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Error("loop should finish after context cancellation")
	}
}
