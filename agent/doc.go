// Package agent provides the L3 Agent: a stateful, in-memory session manager
// that owns the transcript, model, tools, and queues. It delegates execution
// to L2 (AgentLoop) and provides compaction primitives.
//
// The Agent does NOT:
//   - Decide when to compact (that's PiCompaction in the App)
//   - Call an LLM to generate summaries (that's PiCompaction)
//   - Persist the compaction result (that's PiSession)
//   - Fire hooks before/after compaction (that's PiHooks)
//
// It only applies the mechanical transformation and emits an event.
//
// Key types:
//   - Agent: Prompt, Continue, Abort, Steer, FollowUp, Compact
//   - AgentState: model, system prompt, thinking level, tools, messages
//   - EventBus: typed subscribe/unsubscribe event dispatch
//   - MessageQueue: steering + follow-up queues with DrainAll/DrainOne modes
//   - MessageRegistry: runtime message type conversion
//   - Compaction: EstimateTokens, ShouldCompact, CompactMessages, Agent.Compact
//
// Layer map:
//
//	L0: Protocol       — pure types
//	L1: LLM Gateway    — depends on protocol
//	L2: Agent Loop     — depends on protocol only (L1 via StreamFn parameter)
//	L3: Agent          (this package) — depends on protocol, loop
package agent
