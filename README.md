# Pith

Go SDK for building LLM agent applications. Four layers — protocol types, LLM gateway, agent loop, stateful agent — each with a strict dependency boundary and no runtime assumptions.

The built-in `OpenAICompatProvider` covers any `/v1/chat/completions` API (OpenAI, Groq, Together, DeepSeek, OpenRouter, Ollama, etc.). Custom providers (e.g., Anthropic Messages API) are built using the `ProviderPort` interface — see example 11.

**Build agents quickly → [pith-sdk](https://github.com/chinudotdev/pith-sdk)**

> **Multi-module repo.** Import a layer directly:
> [agent](https://pkg.go.dev/github.com/chinudotdev/pith/agent) ·
> [gateway](https://pkg.go.dev/github.com/chinudotdev/pith/gateway) ·
> [loop](https://pkg.go.dev/github.com/chinudotdev/pith/loop) ·
> [protocol](https://pkg.go.dev/github.com/chinudotdev/pith/protocol)

## Installation

Import the layer you need. The agent module pulls in gateway, loop, and protocol:

```bash
go get github.com/chinudotdev/pith/agent@v0.1.4
go get github.com/chinudotdev/pith/gateway@v0.1.4
go get github.com/chinudotdev/pith/loop@v0.1.4
go get github.com/chinudotdev/pith/protocol@v0.1.4
```

Requires Go 1.24+.

## Examples

| # | File | Description |
|---|------|-------------|
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

Examples are standalone programs (not listed in `go.work`). Copy one into a new module and run it — see [examples/README.md](examples/README.md):

```bash
mkdir my-agent && cd my-agent
go mod init my-agent
cp /path/to/pith/examples/01-minimal/main.go .
go get github.com/chinudotdev/pith/gateway@v0.1.4
OPENAI_API_KEY="sk-..." go run main.go
```

## Quick Reference

```go
import (
    "context"
    "os"

    "github.com/chinudotdev/pith/agent"
    "github.com/chinudotdev/pith/gateway"
    "github.com/chinudotdev/pith/gateway/providers"
    "github.com/chinudotdev/pith/protocol"
)

// --- L1: Gateway ---

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
})

model, _ := gw.Catalog.Get("openai", "gpt-4o-mini")

stream, _ := gw.Stream(ctx, model, protocol.Context{
    SystemPrompt: "You are helpful.",
}, opts)
for event := range stream {
    if event.Type == protocol.EventTextDelta {
        fmt.Print(event.Delta)
    }
}

// --- L3: Agent ---

ag := agent.NewAgent(agent.AgentConfig{
    InitialState: &agent.AgentState{
        Model:        model,
        SystemPrompt: "You are helpful.",
        Tools:        myTools,
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

ag.FollowUp(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "Now explain that."}}, Timestamp: protocol.Now()})
ag.Prompt(ctx, "What is Go?")

ag.Steer(protocol.UserMessage{Role: "user", Content: []protocol.Content{protocol.TextContent{Type: "text", Text: "Be more concise."}}, Timestamp: protocol.Now()})

// --- Abort ---

go ag.Prompt(ctx, "Long running task...")
time.Sleep(100 * time.Millisecond)
ag.Abort()
ag.WaitForIdle()
```

## API Primitives

| Primitive | Layer | Purpose |
|-----------|-------|---------|
| `protocol.*` | L0 | Pure types: messages, content, stream events, model descriptors, errors |
| `LLMGateway.Stream / Complete` | L1 | Compose providers + catalog + credentials + middleware |
| `OpenAICompatProvider` | L1 | Provider for any /v1/chat/completions API (OpenAI, Groq, Together, etc.) |
| `ProviderRegistry` | L1 | Instance-based provider registration |
| `ModelCatalog` | L1 | Programmatic model registration and cost calculation |
| `CredentialProvider` | L1 | Interface for API key resolution (no default) |
| `Middleware` | L1 | Composable pipeline: RetryPolicy, HeaderInjector |
| `AgentLoop` | L2 | Stateless turn executor (takes StreamFn, not L1 directly) |
| `Agent.Prompt / Continue / Abort` | L3 | Stateful in-memory agent |
| `EventBus` | L3 | Typed subscribe/unsubscribe event dispatch |
| `MessageQueue` | L3 | Steering + follow-up queues |
| `MessageRegistry` | L3 | Runtime message type conversion |
| `EstimateTokens / ShouldCompact / CompactMessages` | L3 | Compaction primitives (no policy) |
