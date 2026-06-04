// Package protocol defines the pure types that form the shared vocabulary
// for every layer in the SDK. Zero behavior, zero imports, zero side effects.
//
// Key types:
//
//   - Messages: UserMessage, AssistantMessage, ToolResultMessage, CompactSummaryMessage
//   - Content:  TextContent, ImageContent, ThinkingContent, ToolCall
//   - Streaming: StreamEvent (Start, TextDelta, ThinkingDelta, Done, Error, ...)
//   - Model:    ModelDescriptor, ModelCapabilities, CostRates
//   - Errors:   Error with ErrorCode constants
//
// The Message, Content, and ContentBlock interfaces are sealed — they use
// unexported methods so only this package can implement them. Custom App-layer
// types must be converted to built-in types before passing to the SDK.
//
// Layer map:
//
//	L0: Protocol       (this package) — pure types
//	L1: LLM Gateway    — depends on protocol
//	L2: Agent Loop     — depends on protocol only (L1 via StreamFn parameter)
//	L3: Agent          — depends on protocol, loop
package protocol
