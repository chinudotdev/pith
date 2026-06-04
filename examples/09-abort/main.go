// Example 09: Agent Abort — context cancellation, Agent.Abort(), WaitForIdle().
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

	// --- Abort via context cancellation ---
	fmt.Println("--- Abort via Context ---")

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	// Use a short timeout to abort.
	timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fmt.Println("Starting prompt with 2s timeout...")
	err := ag.Prompt(timeoutCtx, "Tell me a very long story about a dragon.")
	if err != nil {
		fmt.Printf("Prompt returned error (expected): %v\n", err)
	} else {
		fmt.Println("Prompt completed before timeout.")
	}

	// --- Abort via Agent.Abort() ---
	fmt.Println("\n--- Abort via Agent.Abort() ---")

	ag2 := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- ag2.Prompt(ctx, "Tell me a very long story about a wizard.")
	}()

	time.Sleep(1 * time.Second)
	fmt.Println("Calling Abort()...")
	ag2.Abort()

	err = <-done
	if err != nil {
		fmt.Printf("Prompt returned error after abort: %v\n", err)
	} else {
		fmt.Println("Prompt completed before abort took effect.")
	}

	// --- WaitForIdle ---
	fmt.Println("\n--- WaitForIdle ---")

	ag3 := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful. Be concise.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	go func() { ag3.Prompt(ctx, "Hello") }()
	fmt.Println("Waiting for agent to become idle...")
	ag3.WaitForIdle()
	fmt.Println("Agent is idle. Safe to prompt again.")

	err = ag3.Prompt(ctx, "Another question")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Second prompt succeeded.")
	}
}
