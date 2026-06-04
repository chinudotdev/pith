// Example 04: Multi-Turn — conversation across multiple Prompt() calls.
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
			SystemPrompt: "You are a conversational assistant. Be concise.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	// Print each response as it streams.
	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent != nil && e.LoopEvent.Type == "messageUpdate" {
			if e.LoopEvent.StreamEvent != nil && e.LoopEvent.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(e.LoopEvent.StreamEvent.Delta)
			}
		}
	})

	turns := []string{
		"Hi, what's your name?",
		"Please remember the word 'apples' for later.",
		"What word did I ask you to remember?",
		"Goodbye!",
	}

	for i, input := range turns {
		fmt.Printf("--- Turn %d ---\nUser: %s\nAssistant: ", i+1, input)

		err := ag.Prompt(ctx, input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError on turn %d: %v\n", i+1, err)
			continue
		}
		fmt.Println()
	}

	fmt.Printf("\nTotal messages in transcript: %d\n", len(ag.State().Messages))
}
