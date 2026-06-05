# SDK Examples

Standalone Go programs demonstrating the Pith SDK. Copy any example into a new project and run it — no need to clone this repo.

## Prerequisites

1. **Go 1.24+**
2. **An API key** — set as an environment variable:
   ```bash
   export OPENAI_API_KEY="sk-..."
   ```
   The examples use an OpenAI-compatible provider. Change the `BaseURL` in the provider config for Groq, Together, DeepSeek, OpenRouter, etc.

## Quick Start

```bash
# Create a new project
mkdir my-agent && cd my-agent
go mod init my-agent

# Copy an example
cp /path/to/examples/01-minimal/main.go .

# Add the dependency
go mod tidy

# Run
OPENAI_API_KEY="sk-..." go run main.go
```

## Examples

| # | Directory | Description |
|---|-----------|-------------|
| 01 | `01-minimal/` | Simplest usage — stream a response via the gateway |
| 02 | `02-basic-agent/` | Agent lifecycle, event bus, transcript inspection |
| 03 | `03-agent-with-tools/` | Tool definition, tool execution, transcript with tool calls |
| 04 | `04-multi-turn/` | Multi-turn conversation across Prompt() calls |
| 05 | `05-compaction/` | EstimateTokens, ShouldCompact, CompactMessages, Agent.Compact() |
| 06 | `06-steering-followup/` | Steering queue (mid-loop injection) & follow-up queue |
| 07 | `07-middleware/` | Custom middleware, RetryPolicy, HeaderInjector, logging |
| 08 | `08-custom-messages/` | MessageRegistry, MessagePipeline, sealed interface limitation |
| 09 | `09-abort/` | Context cancellation, Agent.Abort(), WaitForIdle() |
| 10 | `10-capability-negotiation/` | Capability negotiation, tool execution policies, tool hooks |
| 11 | `11-custom-provider/` | Build a custom ProviderPort (Anthropic Messages API) and wire it through the gateway and agent |

## Quick Reference

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/chinudotdev/pith/agent"
    "github.com/chinudotdev/pith/gateway"
    "github.com/chinudotdev/pith/gateway/providers"
    "github.com/chinudotdev/pith/loop"
    "github.com/chinudotdev/pith/protocol"
)

func main() {
    ctx := context.Background()

    // --- Gateway setup (do once) ---
    gw := gateway.NewLLMGateway()
    gw.Providers.Register(providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
        BaseURL: "https://api.openai.com",
    }))
    gw.Credentials = gateway.CredentialProviderFunc(func(pid protocol.ProviderId) (protocol.Credential, error) {
        return protocol.ApiKey{Key: os.Getenv("OPENAI_API_KEY")}, nil
    })
    gw.Catalog.Register("openai", protocol.ModelDescriptor{
        ID: "gpt-4o-mini", API: protocol.ApiOpenAICompletions, Provider: "openai",
        BaseURL: "https://api.openai.com", ContextWindow: 128000, MaxTokens: 4096,
        Capabilities: protocol.ModelCapabilities{
            Input: map[protocol.MediaType]bool{protocol.MediaText: true},
        },
    })
    model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

    // --- Minimal: stream directly ---
    ch, _ := gw.Stream(ctx, model, protocol.Context{
        SystemPrompt: "You are helpful.",
        Messages: []protocol.Message{
            protocol.UserMessage{Role: "user", Content: []protocol.Content{
                protocol.TextContent{Type: "text", Text: "Hello!"},
            }, Timestamp: protocol.Now()},
        },
    }, protocol.StreamOptions{})
    for event := range ch {
        if event.Type == protocol.EventTextDelta {
            fmt.Print(event.Delta)
        }
    }

    // --- Agent: stateful, with events ---
    ag := agent.NewAgent(agent.AgentConfig{
        InitialState: &agent.AgentState{
            Model: model, SystemPrompt: "You are helpful.",
            Tools: []loop.AgentTool{myTool},
        },
        StreamFn: func(ctx context.Context, m protocol.ModelDescriptor, pctx protocol.Context, opts protocol.StreamOptions) (<-chan protocol.StreamEvent, error) {
            return gw.Stream(ctx, m, pctx, opts)
        },
    })
    ag.EventBus().Subscribe(func(e agent.AgentEvent) {
        if e.LoopEvent != nil && e.LoopEvent.Type == "messageUpdate" {
            if e.LoopEvent.StreamEvent != nil && e.LoopEvent.StreamEvent.Type == protocol.EventTextDelta {
                fmt.Print(e.LoopEvent.StreamEvent.Delta)
            }
        }
    })
    ag.Prompt(ctx, "Hello!")

    // --- Compaction ---
    if agent.ShouldCompact(ag.State().Messages, agent.CompactionSettings{Enabled: true}, model) {
        tokens := agent.EstimateTokens(ag.State().Messages)
        ag.Compact("Summary of conversation", 2, tokens, nil)
    }

    // --- Steering & Follow-Up ---
    ag.FollowUp(protocol.UserMessage{Role: "user", Content: []protocol.Content{
        protocol.TextContent{Type: "text", Text: "Follow-up question"},
    }, Timestamp: protocol.Now()})
    ag.Steer(protocol.UserMessage{Role: "user", Content: []protocol.Content{
        protocol.TextContent{Type: "text", Text: "Be concise"},
    }, Timestamp: protocol.Now()})

    // --- Abort ---
    go ag.Prompt(ctx, "Long task...")
    ag.Abort()
    ag.WaitForIdle()
}
```

## Using Other Providers

Change the `BaseURL` in `OpenAICompatConfig`:

```go
// Groq
providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
    BaseURL: "https://api.groq.com",
})

// Together AI
providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
    BaseURL: "https://api.together.xyz",
})

// DeepSeek
providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
    BaseURL: "https://api.deepseek.com",
})

// OpenRouter
providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
    BaseURL: "https://openrouter.ai/api",
})

// Local (Ollama, llama.cpp, etc.)
providers.NewOpenAICompatProvider(providers.OpenAICompatConfig{
    BaseURL: "http://localhost:11434",
})
```
