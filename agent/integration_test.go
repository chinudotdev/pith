package agent_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/gatewaytest"
	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

func thinkingPtr(level protocol.ThinkingLevel) *protocol.ThinkingLevel {
	return &level
}

// TestL0ProtocolTypes verifies all protocol types compile and satisfy interfaces.
func TestL0ProtocolTypes(t *testing.T) {
	var _ protocol.Message = protocol.UserMessage{Role: "user"}
	var _ protocol.Message = protocol.AssistantMessage{Role: "assistant"}
	var _ protocol.Message = protocol.ToolResultMessage{Role: "toolResult"}
	var _ protocol.Message = protocol.CompactSummaryMessage{Role: "compactSummary"}

	var _ protocol.Content = protocol.TextContent{Type: "text"}
	var _ protocol.Content = protocol.ImageContent{Type: "image"}

	var _ protocol.ContentBlock = protocol.TextContent{Type: "text"}
	var _ protocol.ContentBlock = protocol.ThinkingContent{Type: "thinking"}
	var _ protocol.ContentBlock = protocol.ToolCall{Type: "toolCall"}

	var _ protocol.Credential = protocol.ApiKey{Key: "test"}
	var _ protocol.Credential = &protocol.BearerToken{Token: "test"}
	var _ protocol.Credential = &protocol.AwsCredentials{Region: "us-east-1"}
	var _ protocol.Credential = &protocol.GcpCredentials{Project: "test", Location: "us"}
	var _ protocol.Credential = &protocol.OAuthToken{Token: "test"}

	err := protocol.NewError(protocol.ErrNotFound, "test error")
	if err.Code != protocol.ErrNotFound {
		t.Errorf("expected code %s, got %s", protocol.ErrNotFound, err.Code)
	}
}

// TestL1GatewayInstanceBased verifies the gateway is instance-based, not global.
func TestL1GatewayInstanceBased(t *testing.T) {
	gw1 := gateway.NewLLMGateway()
	gw2 := gateway.NewLLMGateway()

	faux1 := gatewaytest.NewFauxProvider(protocol.ApiOpenAICompletions, "faux-1",
		gatewaytest.FauxResponse{Text: "hello from gw1"},
	)
	faux2 := gatewaytest.NewFauxProvider(protocol.ApiOpenAICompletions, "faux-2",
		gatewaytest.FauxResponse{Text: "hello from gw2"},
	)

	gw1.Providers.Register(faux1)
	gw2.Providers.Register(faux2)

	if gw1.Providers.Get(protocol.ApiOpenAICompletions).Name() != "faux-1" {
		t.Error("gw1 should have faux-1")
	}
	if gw2.Providers.Get(protocol.ApiOpenAICompletions).Name() != "faux-2" {
		t.Error("gw2 should have faux-2")
	}

}

// TestL1CredentialProvider verifies the credential provider interface.
func TestL1CredentialProvider(t *testing.T) {
	gw := gateway.NewLLMGateway()

	_, err := gw.Credentials.Resolve("anthropic")
	if err == nil {
		t.Error("expected error from default credential provider")
	}

	gw.Credentials = gateway.CredentialProviderFunc(func(providerID protocol.ProviderId) (protocol.Credential, error) {
		if providerID == "anthropic" {
			return protocol.ApiKey{Key: "sk-ant-test"}, nil
		}
		return nil, fmt.Errorf("no credentials for %s", providerID)
	})

	cred, err := gw.Credentials.Resolve("anthropic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if apiKey, ok := cred.(protocol.ApiKey); !ok || apiKey.Key != "sk-ant-test" {
		t.Error("expected API key credential")
	}

	_, err = gw.Credentials.Resolve("unknown")
	if err == nil {
		t.Error("expected error for unknown provider")
	}
}

// TestL1FauxProviderStreaming verifies the FauxProvider streams correctly.
func TestL1FauxProviderStreaming(t *testing.T) {
	gw := gatewaytest.NewFauxGateway(
		gatewaytest.FauxResponse{Text: "Hello, world!"},
	)

	model := gatewaytest.FauxModel()
	ctx := context.Background()
	pctx := protocol.Context{
		SystemPrompt: "You are helpful.",
		Messages:     []protocol.Message{},
	}

	ch, err := gw.Stream(ctx, model, pctx, protocol.StreamOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var textDeltas int
	var gotDone bool
	for event := range ch {
		switch event.Type {
		case protocol.EventTextDelta:
			textDeltas++
		case protocol.EventDone:
			gotDone = true
			if event.Message == nil {
				t.Error("Done event should have a message")
			}
		}
	}

	if textDeltas == 0 {
		t.Error("expected text delta events")
	}
	if !gotDone {
		t.Error("expected Done event")
	}
}

// TestL2MessagePipeline verifies the composable message pipeline.
func TestL2MessagePipeline(t *testing.T) {
	messages := []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hello"}}, Timestamp: time.Now()},
		protocol.AssistantMessage{Role: "assistant", Content: []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "hi"}}, Timestamp: time.Now()},
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "world"}}, Timestamp: time.Now()},
	}

	pipeline := loop.MessagePipeline{
		Stages: []loop.MessageStage{
			loop.FilterStage{Predicate: func(m protocol.Message) bool {
				return protocol.MessageRole(m) == "user"
			}},
		},
	}

	result := pipeline.Run(messages)
	if len(result) != 2 {
		t.Errorf("expected 2 user messages after filter, got %d", len(result))
	}
}

// TestL2ToolExecutionPolicy verifies tool execution policies.
func TestL2ToolExecutionPolicy(t *testing.T) {
	seq := loop.SequentialPolicy{}
	if seq.IsParallel("any") {
		t.Error("sequential policy should not be parallel")
	}

	par := loop.ParallelPolicy{}
	if !par.IsParallel("any") {
		t.Error("parallel policy should be parallel")
	}

	perTool := &loop.PerToolPolicy{
		Default:   true,
		Overrides: map[string]bool{"read_file": false},
	}
	if !perTool.IsParallel("bash") {
		t.Error("bash should be parallel (default)")
	}
	if perTool.IsParallel("read_file") {
		t.Error("read_file should be sequential (override)")
	}
}

// TestL3AgentBasic verifies basic agent lifecycle.
func TestL3AgentBasic(t *testing.T) {
	gw := gatewaytest.NewFauxGateway(
		gatewaytest.FauxResponse{Text: "Hello! I am a helpful assistant."},
	)

	model := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	var events []agent.AgentEvent
	unsub := ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		events = append(events, e)
	})
	defer unsub()

	err := ag.Prompt(context.Background(), "Hi there!")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state := ag.State()
	if len(state.Messages) == 0 {
		t.Error("expected messages after prompt")
	}

	if len(events) == 0 {
		t.Error("expected events from agent")
	}
}

// TestL3AgentWithTools verifies agent with tool execution.
func TestL3AgentWithTools(t *testing.T) {
	readTool := loop.AgentTool{
		Name:        "read_file",
		Label:       "Read File",
		Description: "Read a file from disk",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string"},
			},
		},
		Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
			return loop.ToolResult{
				Content: []protocol.Content{
					protocol.TextContent{Type: "text", Text: "file contents here"},
				},
			}
		},
	}

	gw := gatewaytest.NewFauxGateway(
		gatewaytest.FauxResponse{
			ToolCalls: []protocol.ToolCall{
				{Type: "toolCall", ID: "tc1", Name: "read_file", Arguments: `{"path":"/tmp/test.txt"}`},
			},
		},
		gatewaytest.FauxResponse{Text: "I read the file. It contains: file contents here"},
	)

	model := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
			Tools:         []loop.AgentTool{readTool},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	err := ag.Prompt(context.Background(), "Read /tmp/test.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state := ag.State()
	if len(state.Messages) == 0 {
		t.Error("expected messages after prompt with tools")
	}
}

// TestL3CompactionPrimitives verifies compaction primitives.
func TestL3CompactionPrimitives(t *testing.T) {
	messages := []protocol.Message{
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "message 1"}}, Timestamp: time.Now()},
		protocol.AssistantMessage{Role: "assistant", Content: []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "response 1"}}, Timestamp: time.Now()},
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "message 2"}}, Timestamp: time.Now()},
		protocol.AssistantMessage{Role: "assistant", Content: []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "response 2"}}, Timestamp: time.Now()},
		protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "message 3"}}, Timestamp: time.Now()},
	}

	tokensBefore := 100
	tokensAfter := 40
	compacted, err := agent.CompactMessages(messages, "Summary of messages 1-2", 2, tokensBefore, tokensAfter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compacted) != 4 {
		t.Errorf("expected 4 compacted messages, got %d", len(compacted))
	}

	if csm, ok := compacted[0].(protocol.CompactSummaryMessage); !ok {
		t.Error("first compacted message should be CompactSummaryMessage")
	} else if csm.Summary != "Summary of messages 1-2" {
		t.Errorf("unexpected summary: %s", csm.Summary)
	}
}

// TestL3AgentCompact verifies Agent.Compact() emits events.
func TestL3AgentCompact(t *testing.T) {
	gw := gatewaytest.NewFauxGateway()
	model := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
			Messages: []protocol.Message{
				protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "old message"}}, Timestamp: time.Now()},
				protocol.AssistantMessage{Role: "assistant", Content: []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: "old response"}}, Timestamp: time.Now()},
				protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "recent message"}}, Timestamp: time.Now()},
			},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	var compactEvents []*agent.CompactEvent
	unsub := ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.CompactEvent != nil {
			compactEvents = append(compactEvents, e.CompactEvent)
		}
	})
	defer unsub()

	err := ag.Compact("Summary of old conversation", 2, 100, 40)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(compactEvents) != 1 {
		t.Fatalf("expected 1 compact event, got %d", len(compactEvents))
	}
	if compactEvents[0].Summary != "Summary of old conversation" {
		t.Errorf("unexpected summary: %s", compactEvents[0].Summary)
	}

	state := ag.State()
	if len(state.Messages) != 2 {
		t.Errorf("expected 2 messages after compaction (summary + recent), got %d", len(state.Messages))
	}
}

// TestL3AgentAbort verifies agent abort.
func TestL3AgentAbort(t *testing.T) {
	gw := gatewaytest.NewFauxGateway(
		gatewaytest.FauxResponse{Text: "Hello!", StreamDelay: 100 * time.Millisecond},
	)

	model := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Start prompt in goroutine
	done := make(chan error, 1)
	go func() {
		done <- ag.Prompt(ctx, "Hi!")
	}()

	// Abort
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Should not panic
	_ = ag.State()
}

// TestL3AgentQueues verifies steering and follow-up queues.
func TestL3AgentQueues(t *testing.T) {
	gw := gatewaytest.NewFauxGateway()
	model := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	ag.Steer(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "steer"}}, Timestamp: time.Now()})
	ag.FollowUp(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "followup"}}, Timestamp: time.Now()})

	if !ag.HasQueuedMessages() {
		t.Error("expected queued messages")
	}

	ag.ClearAllQueues()
	if ag.HasQueuedMessages() {
		t.Error("expected no queued messages after clear")
	}
}

// TestL3AgentSetState verifies runtime state mutations.
func TestL3AgentSetState(t *testing.T) {
	gw := gatewaytest.NewFauxGateway()
	model1 := gatewaytest.FauxModel()

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:         model1,
			SystemPrompt:  "You are helpful.",
			ThinkingLevel: thinkingPtr(protocol.ThinkingOff),
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	model2 := model1
	model2.ID = "faux-model-v2"
	ag.SetModel(model2)
	if ag.State().Model.ID != "faux-model-v2" {
		t.Error("model should be updated")
	}

	ag.SetThinkingLevel(thinkingPtr(protocol.ThinkingHigh))
	if *ag.State().ThinkingLevel != protocol.ThinkingHigh {
		t.Error("thinking level should be updated")
	}

	ag.SetSystemPrompt("New prompt")
	if ag.State().SystemPrompt != "New prompt" {
		t.Error("system prompt should be updated")
	}

	newTools := []loop.AgentTool{{Name: "custom_tool", Label: "Custom", Description: "A custom tool"}}
	ag.SetTools(newTools)
	if len(ag.State().Tools) != 1 || ag.State().Tools[0].Name != "custom_tool" {
		t.Error("tools should be updated")
	}

	ag.Reset()
	if len(ag.State().Messages) != 0 {
		t.Error("messages should be cleared after reset")
	}
}

func TestL3AgentCompactOutOfRangeReturnsError(t *testing.T) {
	gw := gatewaytest.NewFauxGateway()
	model := gatewaytest.FauxModel()
	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model: model,
			Messages: []protocol.Message{
				protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "hello"}}, Timestamp: time.Now()},
			},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	err := ag.Compact("summary", 3, 10, 5)
	if err == nil {
		t.Fatal("expected error for out-of-range firstKeptIndex")
	}
}
