// Example 07: Middleware Pipeline — logging, timing, RetryPolicy, HeaderInjector.
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

	"github.com/chinudotdev/pith/gateway"
	"github.com/chinudotdev/pith/gateway/providers"
	"github.com/chinudotdev/pith/protocol"
)

// LoggingMiddleware logs each request before and after.
type LoggingMiddleware struct{}

func (l *LoggingMiddleware) Name() string { return "logging" }
func (l *LoggingMiddleware) Wrap(inner gateway.StreamFunction) gateway.StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		fmt.Printf("[middleware:logging] → Request starting: model=%s\n", model.ID)
		start := time.Now()
		ch, err := inner(ctx, model, pctx, opts)
		if err != nil {
			fmt.Printf("[middleware:logging] ✗ Failed: %v (%v)\n", err, time.Since(start))
			return nil, err
		}
		fmt.Printf("[middleware:logging] ✓ Stream started (%v)\n", time.Since(start))
		return ch, nil
	}
}

// TimingMiddleware measures total stream duration.
type TimingMiddleware struct{}

func (t *TimingMiddleware) Name() string { return "timing" }
func (t *TimingMiddleware) Wrap(inner gateway.StreamFunction) gateway.StreamFunction {
	return func(ctx context.Context, model protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
		start := time.Now()
		ch, err := inner(ctx, model, pctx, opts)
		if err != nil {
			return nil, err
		}
		wrappedCh := make(chan protocol.StreamEvent, 64)
		go func() {
			defer close(wrappedCh)
			for event := range ch {
				wrappedCh <- event
				if event.Type == protocol.EventDone || event.Type == protocol.EventError {
					fmt.Printf("[middleware:timing] Total stream time: %v\n", time.Since(start))
				}
			}
		}()
		return wrappedCh, nil
	}
}

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

	// Add middleware (first added = outermost).
	gw.Middleware = []gateway.Middleware{
		&LoggingMiddleware{},
		&TimingMiddleware{},
		&gateway.HeaderInjector{
			Headers: map[string]string{"X-Custom-Header": "my-value"},
		},
		&gateway.RetryPolicy{
			MaxAttempts: 3,
			Backoff: &gateway.ExponentialBackoff{
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     5 * time.Second,
				Multiplier:   2.0,
			},
			RetryableOn: map[protocol.ErrorCode]bool{
				protocol.ErrRateLimited: true,
				protocol.ErrTimeout:    true,
			},
		},
	}

	model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

	pctx := protocol.Context{
		SystemPrompt: "You are helpful. Be concise.",
		Messages: []protocol.Message{
			protocol.UserMessage{
				Role:      "user",
				Content:   []protocol.Content{protocol.TextContent{Type: "text", Text: "What is Go? One sentence."}},
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
		if event.Type == protocol.EventTextDelta {
			fmt.Print(event.Delta)
		}
		if event.Type == protocol.EventDone {
			fmt.Println("\n[done]")
		}
	}
}
