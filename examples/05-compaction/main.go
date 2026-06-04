// Example 05: Compaction — EstimateTokens, ShouldCompact, CompactMessages, Agent.Compact().
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

	// Build a long conversation to compact.
	messages := []protocol.Message{
		userMsg("What is the capital of France?"),
		assistantMsg("The capital of France is Paris."),
		userMsg("What about Germany?"),
		assistantMsg("The capital of Germany is Berlin."),
		userMsg("And Japan?"),
		assistantMsg("The capital of Japan is Tokyo."),
		userMsg("Tell me about Italy."),
		assistantMsg("The capital of Italy is Rome."),
	}

	// --- Compaction primitives ---
	tokens := agent.EstimateTokens(messages)
	fmt.Printf("Estimated tokens for %d messages: %d\n", len(messages), tokens)

	settings := agent.CompactionSettings{Enabled: true, ReserveTokens: 20}
	shouldCompact := agent.ShouldCompact(messages, settings, protocol.ModelDescriptor{ContextWindow: 100})
	fmt.Printf("Should compact (context=100, reserve=20): %v\n", shouldCompact)

	// Compact: keep only the last 2 messages.
	summary := "User asked about capitals of France (Paris), Germany (Berlin), and Japan (Tokyo)."
	firstKeptIndex := 6
	tokensAfter := agent.EstimateTokens(messages[firstKeptIndex:]) + len(summary)/4
	compacted, err := agent.CompactMessages(messages, summary, firstKeptIndex, tokens, tokensAfter, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compact failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("After compaction: %d messages (was %d)\n", len(compacted), len(messages))

	// --- Agent.Compact() with EventBus ---
	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are a helpful assistant.",
			Messages:     messages,
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.CompactEvent != nil {
			fmt.Printf("[compact event] summary=%q, tokensBefore=%d, tokensAfter=%d\n",
				e.CompactEvent.Summary, e.CompactEvent.TokensBefore, e.CompactEvent.TokensAfter)
		}
	})

	fmt.Printf("\nBefore compact: %d messages\n", len(ag.State().Messages))
	err = ag.Compact("Discussed capitals of France, Germany, Japan", 6, tokens, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent compact failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("After compact: %d messages\n", len(ag.State().Messages))

	if csm, ok := ag.State().Messages[0].(protocol.CompactSummaryMessage); ok {
		fmt.Printf("First message is CompactSummary: %q\n", csm.Summary)
	}
}

func userMsg(text string) protocol.Message {
	return protocol.UserMessage{
		Role:      "user",
		Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: text}},
		Timestamp: protocol.Now(),
	}
}

func assistantMsg(text string) protocol.Message {
	return protocol.AssistantMessage{
		Role:      "assistant",
		Content:   []protocol.ContentBlock{protocol.TextContent{Type: "text", Text: text}},
		Timestamp: protocol.Now(),
	}
}
