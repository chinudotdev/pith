// Package loop provides the L2 Agent Loop: a stateless turn executor that
// takes a snapshot, runs turns, emits events, and owns no state between calls.
//
// The loop does NOT import L1 (gateway). Instead, it takes a StreamFn function
// parameter whose signature matches L1's stream function. L3 wires the gateway
// into the loop at configuration time — dependency inversion.
//
// The loop:
//  1. Converts AgentMessages to LLM Messages (via MessagePipeline)
//  2. Calls the LLM (via StreamFn from L1, wired by L3)
//  3. Executes tool calls (sequential or parallel, per ToolExecutionPolicy)
//  4. Checks steering/follow-up queues
//  5. Repeats until no more tool calls or stop condition
//  6. Emits LoopEvents for every state change
//
// Key types:
//   - AgentLoop / AgentLoopContinue: entry points
//   - LoopConfig: model, tools, StreamFn, hooks, tool execution policy
//   - LoopEvent: typed events (AgentStart, TurnEnd, MessageUpdate, ToolExecutionEnd, ...)
//   - LoopHooks: BeforeToolCall, AfterToolCall, ShouldStopAfterTurn, PrepareNextTurn
//   - ErrorChannel: structured errors (ProviderError, ToolError, AbortError)
//
// Layer map:
//
//	L0: Protocol       — pure types
//	L1: LLM Gateway    — depends on protocol
//	L2: Agent Loop     (this package) — depends on protocol only
//	L3: Agent          — depends on protocol, loop
package loop
