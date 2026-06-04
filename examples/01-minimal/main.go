// Example 01: Minimal — one-shot prompt with an OpenAI-compatible provider.
//
// Copy this file into a new Go project:
//
//	mkdir my-agent && cd my-agent
//	go mod init my-agent
//	cp main.go .
//	go mod tidy
//	OPENAI_API_KEY="sk-..." go run main.go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/providers"
	"github.com/chinudotdev/pith/protocol"
)

func main() {
	ctx := context.Background()

	gw := gateway.NewLLMGateway()

	// Register an OpenAI-compatible provider.
	// Change BaseURL for Groq, Together, DeepSeek, OpenRouter, etc.
	gw.Providers.Register(providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
		BaseURL: "https://api.openai.com",
	}))

	// Resolve API keys from environment variables.
	gw.Credentials = gateway.CredentialProviderFunc(func(pid protocol.ProviderId) (protocol.Credential, error) {
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, &protocol.Error{Code: protocol.ErrAuth, Message: "OPENAI_API_KEY not set"}
		}
		return protocol.ApiKey{Key: key}, nil
	})

	// Register the model you want to use.
	gw.Catalog.Register("openai", protocol.ModelDescriptor{
		ID:           "gpt-4o-mini",
		Name:         "GPT-4o Mini",
		API:          protocol.ApiOpenAICompletions,
		Provider:     "openai",
		BaseURL:      "https://api.openai.com",
		ContextWindow: 128000,
		MaxTokens:     4096,
		Capabilities: protocol.ModelCapabilities{
			Input:     map[protocol.MediaType]bool{protocol.MediaText: true, protocol.MediaImage: true},
			Transport: map[protocol.Transport]bool{protocol.TransportSSE: true},
		},
	})

	model, ok := gw.Catalog.Get("openai", "gpt-4o-mini")
	if !ok {
		fmt.Fprintln(os.Stderr, "model not found")
		os.Exit(1)
	}

	// Stream a response.
	pctx := protocol.Context{
		SystemPrompt: "You are a helpful assistant. Be concise.",
		Messages: []protocol.Message{
			protocol.UserMessage{
				Role:      "user",
				Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "What is Go? Answer in one sentence."}},
				Timestamp: protocol.Now(),
			},
		},
	}

	ch, err := gw.Stream(ctx, model, pctx, protocol.StreamOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream failed: %v\n", err)
		os.Exit(1)
	}

	for event := range ch {
		switch event.Type {
		case protocol.EventTextDelta:
			fmt.Print(event.Delta)
		case protocol.EventDone:
			fmt.Println()
			if event.Message != nil {
				fmt.Printf("Tokens: input=%d output=%d\n", event.Message.Usage.Input, event.Message.Usage.Output)
			}
		}
	}
}
