// Example 06: Steering & Follow-Up Queues — inject messages mid-loop and after a turn.
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

	// --- Follow-Up Queue ---
	fmt.Println("--- Follow-Up Queue ---")

	ag := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful. Be concise.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
		QueueConfig: &agent.QueueConfig{
			FollowUpMode: agent.DrainAll,
		},
	})

	ag.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent != nil && e.LoopEvent.Type == "messageUpdate" {
			if e.LoopEvent.StreamEvent != nil && e.LoopEvent.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(e.LoopEvent.StreamEvent.Delta)
			}
		}
	})

	// Queue a follow-up before prompting — processed after the first turn ends.
	ag.FollowUp(protocol.UserMessage{
		Role:      "user",
		Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "Now tell me a joke."}},
		Timestamp: protocol.Now(),
	})

	fmt.Printf("User: Answer my first question.\nAssistant: ")
	ag.Prompt(ctx, "Answer my first question.")
	fmt.Println()

	// --- Steering Queue ---
	fmt.Println("\n--- Steering Queue ---")

	ag2 := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful. Be concise.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
		QueueConfig: &agent.QueueConfig{
			SteeringMode: agent.DrainAll,
		},
	})

	ag2.EventBus().Subscribe(func(e agent.AgentEvent) {
		if e.LoopEvent != nil && e.LoopEvent.Type == "messageUpdate" {
			if e.LoopEvent.StreamEvent != nil && e.LoopEvent.StreamEvent.Type == protocol.EventTextDelta {
				fmt.Print(e.LoopEvent.StreamEvent.Delta)
			}
		}
	})

	// Steer a message in — injected before the next LLM call.
	ag2.Steer(protocol.UserMessage{
		Role:      "user",
		Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "[STEER] Be extra brief."}},
		Timestamp: protocol.Now(),
	})

	fmt.Printf("User: Explain quantum computing.\nAssistant: ")
	ag2.Prompt(ctx, "Explain quantum computing.")
	fmt.Println()

	// --- Queue Management ---
	fmt.Println("\n--- Queue Management ---")
	ag3 := agent.NewAgent(agent.AgentConfig{
		InitialState: &agent.AgentState{
			Model:        model,
			SystemPrompt: "You are helpful.",
		},
		StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
			return gw.Stream(ctx, m, pctx, opts)
		},
	})

	ag3.Steer(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "steer1"}}, Timestamp: protocol.Now()})
	ag3.FollowUp(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "followup1"}}, Timestamp: protocol.Now()})
	fmt.Printf("Has queued messages: %v\n", ag3.HasQueuedMessages())

	ag3.ClearSteeringQueue()
	fmt.Printf("After clearing steering: %v\n", ag3.HasQueuedMessages())

	ag3.ClearAllQueues()
	fmt.Printf("After clearing all: %v\n", ag3.HasQueuedMessages())
}
