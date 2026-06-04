// Example 10: Capability Negotiation & Tool Execution Policies.
//
//	mkdir my-agent && cd my-agent && go mod init my-agent
//	cp main.go . && go mod tidy
//	OPENAI_API_KEY="sk-..." go run main.go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/providers"
	"github.com/chinudotdev/pith/loop"
	"github.com/chinudotdev/pith/protocol"
)

func main() {
	ctx := context.Background()

	gw := gateway.NewLLMGateway()
	gw.Providers.Register(providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
	}))
	gw.Credentials = gateway.CredentialProviderFunc(func(pid protocol.ProviderId) (protocol.Credential, error) {
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "OPENAI_API_KEY not set"}
		}
		return protocol.ApiKey{Key: key}, nil
	})
	gw.Catalog.Register("openai", protocol.ModelDescriptor{
		ID:            "gpt-4o-mini",
		Name:          "GPT-4o Mini",
		API:           protocol.ApiOpenAICompletions,
		Provider:      "openai",
		BaseURL:       "https://api.openai.com",
		ContextWindow: 128000,
		MaxTokens:     4096,
		Capabilities: protocol.ModelCapabilities{
			Input:     map[protocol.MediaType]bool{protocol.MediaText: true, protocol.MediaImage: true},
			Transport: map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
	})

	model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

	// --- Capability Negotiation ---
	fmt.Println("--- Capability Negotiation ---")

	thinkingModel := protocol.ModelDescriptor{
		ID:       "claude-sonnet-4",
		Name:     "Claude Sonnet 4",
		API:      protocol.ApiAnthropicMessages,
		Provider: protocol.ProviderAnthropic,
		Capabilities: protocol.ModelCapabilities{
			Input:          map[protocol.MediaType]bool{protocol.MediaText: true, protocol.MediaImage: true},
			Thinking:       true,
			ThinkingLevels: map[protocol.ThinkingLevel]bool{protocol.ThinkingOff: true, protocol.ThinkingMedium: true, protocol.ThinkingHigh: true},
			Caching:        map[protocol.CacheMode]bool{protocol.CacheNone: true, protocol.CacheShort: true},
			Transport:      map[protocol.Transport]bool{protocol.TransportSSE: true, protocol.TransportWebSocket: true},
		},
		ContextWindow: 200000,
		MaxTokens:     64000,
	}

	// Provider that supports thinking
	anthropicCaps := gateway.ProviderCapabilities{
		ThinkingFormat:        gateway.ThinkingString,
		CacheControlFormat:    gateway.CacheControlAnthropic,
		SupportsTemperature:   true,
		ForceAdaptiveThinking: true,
	}
	result := gateway.Negotiate(thinkingModel, anthropicCaps)
	fmt.Printf("Model: %s vs Anthropic provider: %d warnings, %d errors\n",
		thinkingModel.ID, len(result.Warnings), len(result.Errors))
	for _, w := range result.Warnings {
		fmt.Printf("  ⚠ %s\n", w)
	}

	// Provider that does NOT support thinking
	noThinkingProvider := gateway.ProviderCapabilities{CacheControlFormat: gateway.CacheControlNone}
	result2 := gateway.Negotiate(thinkingModel, noThinkingProvider)
	fmt.Printf("Model with thinking vs provider without: %d warnings, %d errors\n",
		len(result2.Warnings), len(result2.Errors))
	for _, w := range result2.Warnings {
		fmt.Printf("  ⚠ %s\n", w)
	}

	// --- Tool Execution Policies ---
	fmt.Println("\n--- Tool Execution Policies ---")

	readTool := loop.AgentTool{
		Name:        "read_file",
		Label:       "Read File",
		Description: "Read a file",
		Parameters:  map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		Mode:        loop.ModeSequential,
		Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
			fmt.Println("    [read_file started]")
			time.Sleep(50 * time.Millisecond)
			return loop.ToolResult{Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "file contents"}}}
		},
	}

	bashTool := loop.AgentTool{
		Name:        "bash",
		Label:       "Bash",
		Description: "Run a command",
		Parameters:  map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		Mode:        loop.ModeParallel,
		Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
			fmt.Println("    [bash started]")
			time.Sleep(30 * time.Millisecond)
			return loop.ToolResult{Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "command output"}}}
		},
	}

	perToolPolicy := &loop.PerToolPolicy{
		Default:   true, // parallel by default
		Overrides: map[string]bool{"read_file": false},
	}

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a coding assistant. Use tools when asked.",
			Tools:        []loop.AgentTool{readTool, bashTool},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
		ToolExecution: perToolPolicy,
	})

	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent == nil {
			return
		}
		le := e.LoopEvent
		switch le.Type {
		case loop.LoopToolExecutionStart:
			fmt.Printf("  [tool started: %s]\n", le.ToolName)
		case loop.LoopToolExecutionEnd:
			fmt.Printf("  [tool ended: %s]\n", le.ToolName)
		case "messageUpdate":
			if le.StreamEvent != nil && le.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(le.StreamEvent.Delta)
			}
		case "messageEnd":
			fmt.Println()
		}
	})

	fmt.Println("Prompting agent with tools...")
	err := ag.Prompt(ctx, "Read the file /tmp/test.txt and run ls")
	if err != nil {
		fmt.Fprintf(os.Stderr, "prompt failed: %v\n", err)
		os.Exit(1)
	}

	// --- BeforeToolCall / AfterToolCall hooks ---
	fmt.Println("\n--- Tool Hooks ---")

	safeBashTool := loop.AgentTool{
		Name:        "bash",
		Label:       "Bash",
		Description: "Run a command",
		Parameters:  map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		Execute: func(callID string, params map[string]any, signal <-chan struct{}, onUpdate func(partial any)) loop.ToolResult {
			cmd, _ := params["command"].(string)
			return loop.ToolResult{Content: []protocol.Content{protocol.TextContent{Type: "text", Text: fmt.Sprintf("output of: %s", cmd)}}}
		},
	}

	ag2 := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a coding assistant.",
			Tools:        []loop.AgentTool{safeBashTool},
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
		BeforeToolCall: func(ctx loop.BeforeToolCallContext, signal <-chan struct{}) *loop.BeforeToolCallResult {
			fmt.Printf("  [hook:beforeToolCall] %s(%s)\n", ctx.ToolCall.Name, ctx.ToolCall.Arguments)
			return nil // allow
		},
		AfterToolCall: func(ctx loop.AfterToolCallContext, signal <-chan struct{}) *loop.AfterToolCallResult {
			fmt.Printf("  [hook:afterToolCall] %s completed, isError=%v\n", ctx.ToolCall.Name, ctx.IsError)
			return nil
		},
	})

	fmt.Println("Prompting agent with tool hooks...")
	ag2.Prompt(ctx, "Run the command: echo hello")
	fmt.Println("Done.")
}
