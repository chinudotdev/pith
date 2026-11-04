# Pith — Review Findings & Restructuring Plan

Source: full code review of all four modules at v0.1.4 (commit `8a574d8`).
Tests pass (`go test`, `-race` clean) — everything below was verified against the source.

---

## Part 1: Bugs Found

### Crashes

1. **Nil-pointer panic when a provider self-aborts** — `loop/loop.go` (`streamResponse`)
   On `EventError` with `StopAborted`, the function returns `nil, nil, ctx.Err()`. If the
   provider aborted on its own (internal timeout) while the loop's context is still alive,
   `ctx.Err()` is `nil` → caller sees `err == nil` and dereferences the nil assistant message
   at `messages = append(messages, *assistantMsg)`.

2. **`Agent.Compact` panics before its own validation runs** — `agent/run.go`
   `EstimateTokens(a.state.Messages[firstKeptIndex:])` executes before `CompactMessages`
   bounds-checks `firstKeptIndex`. Any out-of-range index panics, despite the doc comment
   promising an error return.

### Correctness

3. **Mid-stream provider errors are silently swallowed** — `loop/loop.go` (`streamResponse`)
   A non-abort `EventError` falls through: the error message becomes a normal assistant
   message, `loopErr` stays nil, and the loop continues as if the turn succeeded.

4. **SSE parser breaks on lines > 64KB and never checks `scanner.Err()`** —
   `gateway/providers/openai_compat.go` (`parseSSEStream`)
   Default `bufio.Scanner` buffer with no `scanner.Buffer(...)` call; large chunks (big
   tool-call arguments) kill the scanner silently and the code emits a normal `EventDone`
   with a truncated message.

5. **Thinking/reasoning content is dropped** — `openai_compat.go` + `loop/loop.go`
   `EventThinkingDelta` is emitted from `reasoning_content` but never accumulated into the
   partial message; no `ThinkingStart`/`ThinkingEnd` events; the loop's fallback message
   collects only text and tool calls. Reasoning traces vanish from the transcript.

6. **Goroutine leak per request** — `openai_compat.go` (`parseSSEStream`)
   `go func() { <-ctx.Done(); doClose() }()` only exits when the context is cancelled.
   Long-lived contexts accumulate one blocked goroutine per request.

7. **Steering contract mismatch** — `loop/loop.go`
   The header comment says the inner loop runs "while tool calls **or steering messages**
   exist," but the code exits the inner loop whenever there are no tool calls. A message
   steered during a text-only turn is stranded until the next `Prompt()`.

8. **`mapHTTPStatus` maps all 5xx (and connection failures) to `ErrTimeout`** —
   `openai_compat.go`. A 500 is not a timeout; this poisons any retry policy keyed on error
   codes (exactly what `RetryPolicy.RetryableOn` does). No 4xx → `ErrInvalid` mapping either.

9. **`ModelCatalog.notifyUpdated` runs callbacks while holding the write lock** —
   `gateway/gateway.go`. A callback that calls back into the catalog (e.g. `List()`)
   deadlocks.

10. **`EventSink` invoked concurrently during parallel tool execution** — `loop/loop.go`
    Safe for the Agent's mutex-protected sink, but undocumented for direct `loop` consumers;
    parallel tool-result messages land in the agent transcript in completion order, not
    call order.

11. **Tool-role messages with images produce invalid OpenAI payloads** —
    `openai_compat.go` (`contentToOpenAI`) emits `image_url` parts inside `role: "tool"`
    messages, which OpenAI rejects.

### Dead / misleading API surface

12. `protocol.StreamOptions` — `Signal`, `TimeoutMs`, `MaxRetries`, `CacheRetention`,
    `WebsocketConnectTimeoutMs`, `OnPayload`, `OnResponse` are never read anywhere.
13. `agent.AgentState.StreamingMessage` and `PendingToolCalls` — declared and copied,
    never populated.
14. `loop.TurnUpdate.ThinkingLevel` — accepted from `PrepareNextTurn` hooks, ignored
    (only `Model` and `Context` are applied).
15. `gateway.Negotiate` — implemented and tested but never called by `LLMGateway.Stream`,
    despite `Stream`'s doc comment claiming negotiation is part of the pipeline.
16. `gateway.RateLimiter` — a middleware stub that panics on use, shipped in a tagged
    release.
17. Dead code: unused `args` unmarshal in `buildRequest`'s `ToolCall` case;
    `var _ context.Context` at the bottom of `gateway/faux.go`; the no-op
    `optsCopy := opts; opts = optsCopy` in `gateway.Stream`.

### Propagation gaps (violate `architecture/contracts.md` §1)

18. `Temperature` and `MaxTokens` — supported by the provider via `StreamOptions`, but
    `LoopConfig` has no fields for them, so they are unreachable through the Agent.
19. `ThinkingBudgets` — threaded L3 → L2 → L1, then ignored by the built-in provider.

### Hygiene / tooling

20. **16 files fail `gofmt -l`** (misaligned struct fields, mis-indented
    `case protocol.EventError:` in `loop.go`).
21. **CI runs only `go vet` + `go test`** — no gofmt/lint gate, no `-race`.
22. **Thin test coverage for a tagged release** — 5 test files total; the `gateway`
    package itself has zero tests; the SSE parser (the most failure-prone code in the
    repo) has no tests for malformed chunks, oversized lines, or mid-stream errors.
23. CI sets `GOPRIVATE: github.com/chinudotdev/pith` for a public repo — leftover.
24. `protocol.ErrorCode = string` is a type alias, not a defined type — any string
    satisfies it, losing the type safety the codes suggest.

### Doc-vs-code drift

25. `architecture/README.md` layer map lists `ImagesGateway` — does not exist.
26. `architecture/README.md` names `TurnExecutor` / `ToolDispatcher` as L2 components —
    conceptual only; the actual API is `AgentLoop()` + `LoopConfig`.

---

## Part 2: Recommendations — Core vs. Application SDK

Guiding rule: **core keeps only what every agent needs and what must be correct in one
place** (types, wire protocol, loop invariants, provider transport). Anything that is
*policy*, *heuristic*, or *convenience* moves to `pith-sdk` or the app.

### 2.1 Delete outright (re-add only when implemented)

- Dead `StreamOptions` fields: `Signal`, `TimeoutMs`, `MaxRetries`, `CacheRetention`,
  `WebsocketConnectTimeoutMs`, `OnPayload`, `OnResponse`.
- `RateLimiter` (panicking stub; rate limiting is infrastructure policy anyway).
- Declared-only enums with zero implementation: `TransportWebSocket`,
  `TransportWebSocketCached`, `CacheShort`, `CacheLong`, `ThinkingBudgets`.
- `AgentState.StreamingMessage`, `AgentState.PendingToolCalls`.
- ~14 of ~20 `ProviderCapabilities` flags — keep only the 6 the built-in provider reads:
  `SupportsDeveloperRole`, `RequiresThinkingAsText`, `RequiresToolResultName`,
  `MaxTokensField`, `SupportsUsageInStreaming`, `SupportsReasoningEffort`.
  Custom providers can define their own capability structs.
- App-specific leftovers in `protocol`: `FileOperations`,
  `CompactSummaryMessage.FileOps`, and error codes `ErrNotDirectory`, `ErrIsDirectory`,
  `ErrSpawnError`, `ErrStorage`, `ErrInvalidSession`. A protocol layer claiming
  "zero runtime assumptions" should not know files exist.
- `StreamSimple` / `CompleteSimple` — they only default `Reasoning` to `"off"`, which is
  already the effective zero-value behavior.
- `loop.TurnUpdate.ThinkingLevel` (or implement it — do not keep it ignored).

### 2.2 Move to `pith-sdk` (application SDK level)

- **`ModelCatalog`** — `gateway.Stream` already takes a `ModelDescriptor` directly; the
  catalog is application state management. Move model data, `OnUpdated` callbacks, and
  `CalculateCost` (cost accounting is a product feature). Also removes the
  callbacks-under-lock deadlock from core.
- **Compaction heuristics** — `EstimateTokens` (4-chars/token guess) and `ShouldCompact`
  are policy; apps should use real tokenizers. Keep only the mechanical
  `CompactMessages` in core (if `Agent.Compact` stays at all).
- **`MessageRegistry`** — the agent already accepts a `ConvertToLlm` function; that is
  the core contract. The registry framework (runtime role → converter map) moves to the
  SDK. Core keeps the built-in conversion (compact summary → user message) as a default
  function.
- **Concrete middleware** — keep the `Middleware` interface in core; move `RetryPolicy`,
  `HeaderInjector`, `ExponentialBackoff`, `FixedBackoff` implementations to the SDK.
  Retry policy (which codes, what backoff) is app policy.
- **`Negotiate` / capability negotiation** — core never calls it. Either wire it into
  `Stream` or move it to the SDK as a preflight validation helper.
- **Faux test doubles** — `FauxProvider`, `FauxGateway`, `FauxCredentialProvider` live in
  the production `gateway` package and pollute its API. Move to a `gatewaytest` package
  (Go convention, like `httptest`) or the SDK's testing kit.

### 2.3 Keep in core

- `protocol`: messages, content blocks, stream events, `ModelDescriptor`, `Tool`,
  `Error`, `StopReason`, `ThinkingLevel` — trimmed of the app leftovers above.
- `gateway`: `ProviderPort`, `ProviderRegistry`, `CredentialProvider`, the `Middleware`
  interface, `Stream`/`Complete`, and the one built-in `OpenAICompatProvider`.
- `loop`: everything — the loop, tool execution policies, hooks, message pipeline
  stages. This is the heart of "agents only" and it is correctly stateless.
- `agent`: lifecycle (`Prompt`/`Continue`/`Abort`/`WaitForIdle`/`Reset`), `EventBus`,
  steering/follow-up queues, state snapshot. The minimal stateful shell that a CLI,
  GUI, or server all need; `EventBus` is the boundary a UI subscribes to.

### 2.4 Suggested order of work

1. Fix the two crash paths (bugs 1, 2) and stop swallowing mid-stream errors (bug 3).
2. Harden the SSE parser: `scanner.Buffer`, check `scanner.Err()`, accumulate thinking
   content (bugs 4, 5); fix the goroutine leak (bug 6).
3. Do the core/SDK split (§2.1, §2.2) **before v0.2.0**, while adoption is near zero —
   every release these fields survive makes them harder to delete.
4. Close the propagation gaps: add `Temperature` and `MaxTokens` to `LoopConfig`
   (bug 18); fix `mapHTTPStatus` (bug 8).
5. Hygiene: gofmt all files, add gofmt + `-race` gates to CI, backfill tests for the
   gateway package and SSE parser (bugs 20–23).
6. Sync the architecture docs with reality (bugs 7, 25, 26 and `contracts.md` §1 table).
