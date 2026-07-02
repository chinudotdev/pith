# Migration Guide

## Migrating to v0.2.0

Pith `v0.2.0` is a breaking release that trims core APIs to an agents-only surface.

### Major Breaking Changes

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

### New/Updated Core Behavior

- `loop.LoopConfig` now supports `Temperature` and `MaxTokens` and forwards them to providers.
- `TurnUpdate.ThinkingLevel` is now applied by the loop.
- OpenAI-compatible SSE parsing is hardened:
  - larger scanner buffer, scanner error propagation, thinking accumulation, and tool-result image stripping.

### Example Rewrite Pattern

```go
model := protocol.ModelDescriptor{
    ID: "gpt-4o-mini",
    API: protocol.ApiOpenAICompletions,
    Provider: "openai",
}

gw := gateway.NewLLMGateway()
ch, err := gw.Stream(ctx, model, pctx, opts)
```

### Notes

- APIs moved out of core will be reintroduced through `pith-sdk`.

## Migrating to v0.2.1

`v0.2.1` is a follow-up patch release on top of `v0.2.0` with review fixes and small API corrections.

### Breaking Changes

- Renamed `CompactEvent.FirstKeptID` to `CompactEvent.FirstKeptIndex`.
  - In `v0.2.0`, `FirstKeptID` was a `string` that held a decimal index (`fmt.Sprintf("%d", firstKeptIndex)`), not a message ID. `v0.2.1` replaces it with an `int` field aligned with the `Agent.Compact(..., firstKeptIndex, ...)` parameter.
  - Update compaction event handlers: read `event.CompactEvent.FirstKeptIndex` instead of `event.CompactEvent.FirstKeptID`.
- Removed unused gateway types from `gateway`:
  - `ThinkingFormat` and its constants (`ThinkingOpenAI`, `ThinkingOpenRouter`, `ThinkingDeepSeek`, `ThinkingTogether`, `ThinkingZAI`, `ThinkingQwen`, `ThinkingQwenChat`, `ThinkingString`, `ThinkingAntLing`).
  - `CacheControlFormat` and its constants (`CacheControlAnthropic`, `CacheControlNone`).
  - These were dead code in core; provider capabilities are declared via `ProviderCapabilities` instead.
- Changed `LLMGateway.Complete` error return on stream failure.
  - When the internal stream emits `EventError`, `Complete` now returns `(nil, err)` instead of `(partialMessage, err)`.
  - Do not rely on a non-nil assistant message when `err != nil`; check the error and treat the message as absent.

### New/Updated Behavior

- `AgentConfig.ShouldStopAfterTurn` wires the existing `loop.LoopHooks.ShouldStopAfterTurn` hook through the agent.
  - When the hook returns `true` after a turn, the agent stops without draining follow-up or steering queues.
- `ToolResult.Terminate` is now honored by the agent loop.
  - Setting `Terminate: true` on a `loop.ToolResult` (or via `AfterToolCallResult.Terminate`) stops the loop as intended. Previously only `ToolResultDetails.Terminate` inside `Details` was detected; the top-level field was ignored.
- OpenAI-compatible SSE parsing: malformed JSON chunks now emit `EventError` and abort the stream instead of being silently skipped.
- `LLMGateway.Stream` defensively copies pointer fields in `StreamOptions` (`Temperature`, `MaxTokens`, `MaxRetryDelayMs`) so middleware cannot mutate caller-owned values.
