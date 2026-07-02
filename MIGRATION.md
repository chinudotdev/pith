# Migrating to v0.2.0

Pith `v0.2.0` is a breaking release that trims core APIs to an agents-only surface.

## Major Breaking Changes

- Removed `LLMGateway.Catalog` and `ModelCatalog` from core.
  - Define and pass `protocol.ModelDescriptor` directly in applications.
- Removed `gateway.Negotiate` and related negotiation result types.
  - Capability preflight moves to `pith-sdk`.
- Removed built-in middleware implementations:
  - `RetryPolicy`, `HeaderInjector`, `RateLimiter`, `BackoffStrategy`, `ExponentialBackoff`, `FixedBackoff`.
  - Keep using `gateway.Middleware` with app-defined middleware.
- Removed `StreamSimple` and `CompleteSimple` from `LLMGateway`.
  - Use `Stream` and `Complete`.
- Removed agent compaction heuristics:
  - `EstimateTokens`, `ShouldCompact`, `CompactionSettings`.
  - Keep using `CompactMessages` and `Agent.Compact`.
- Changed `Agent.Compact` signature:
  - from: `Compact(summary, firstKeptIndex, tokensBefore, fileOps)`
  - to: `Compact(summary, firstKeptIndex, tokensBefore, tokensAfter)`
- Removed `FileOperations` and `CompactSummaryMessage.FileOps`.
- Removed `MessageRegistry` and `Agent.Registry()`.
  - Use `agent.DefaultConvertToLlm` or supply `AgentConfig.ConvertToLlm`.
- Removed unused `StreamOptions` fields:
  - `Signal`, `TimeoutMs`, `MaxRetries`, `CacheRetention`, `WebsocketConnectTimeoutMs`, `OnPayload`, `OnResponse`, `ThinkingBudgets`.
- Removed unimplemented enum values:
  - `protocol.TransportWebSocket`, `protocol.TransportWebSocketCached`
  - `protocol.CacheShort`, `protocol.CacheLong`
- Moved faux testing doubles out of core:
  - `gateway.NewFauxProvider`, `gateway.NewFauxGateway`, `gateway.FauxCredentialProvider` moved to `gateway/gatewaytest`.

## New/Updated Core Behavior

- `loop.LoopConfig` now supports `Temperature` and `MaxTokens` and forwards them to providers.
- `TurnUpdate.ThinkingLevel` is now applied by the loop.
- OpenAI-compatible SSE parsing is hardened:
  - larger scanner buffer, scanner error propagation, thinking accumulation, and tool-result image stripping.

## Example Rewrite Pattern

```go
model := protocol.ModelDescriptor{
    ID: "gpt-4o-mini",
    API: protocol.ApiOpenAICompletions,
    Provider: "openai",
}

gw := gateway.NewLLMGateway()
ch, err := gw.Stream(ctx, model, pctx, opts)
```

## Notes

- APIs moved out of core will be reintroduced through `pith-sdk`.
