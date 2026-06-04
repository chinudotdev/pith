// Example 02: Basic Agent — create an agent, subscribe to events, send a prompt.
//
//	mkdir my-agent && cd my-agent && go mod init my-agent
//	cp main.go . && go mod tidy
//	OPENAI_API_KEY="sk-..." go run main.go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/chinudotdev/pith/agent"
	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/providers"
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
			Input:     map[protocol.MediaType]bool{protocol.MediaText: true},
			Transport: map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
	})

	model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a helpful assistant. Be concise.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	// Subscribe to events.
	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent == nil {
			return
		}
		le := e.LoopEvent
		switch le.Type {
		case "agentStart":
			fmt.Println("[agent started]")
		case "messageUpdate":
			if le.StreamEvent != nil && le.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(le.StreamEvent.Delta)
			}
		case "messageEnd":
			fmt.Println()
		case "agentEnd":
			fmt.Println("[agent ended]")
		}
	})

	err := ag.Prompt(ctx, "Hello, who are you?")
	if err != nil {
		fmt.Fprintf(os.Stderr, "prompt failed: %v\n", err)
		os.Exit(1)
	}

	// Inspect the transcript.
	state := ag.State()
	fmt.Printf("\nTranscript: %d messages\n", len(state.Messages))
	for i, msg := range state.Messages {
		fmt.Printf("  [%d] %s\n", i, protocol.MessageRole(msg))
	}
}
