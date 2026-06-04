// Package gateway provides the L1 LLM Gateway: an instance-based, zero-global-state
// gateway for streaming LLM completions.
//
// It composes:
//   - ProviderRegistry: instance-based, not a module-level singleton
//   - ModelCatalog: programmatic-only (register/get/list/calculateCost)
//   - CredentialProvider: interface only, no default implementation
//   - Middleware: composable pipeline (RetryPolicy, HeaderInjector)
//   - LLMGateway: top-level composer (Stream, Complete, StreamSimple)
//   - FauxProvider: test double for L2/L3 testing
//
// No env vars. No bundled data. No filesystem paths. The consumer provides everything.
//
// Layer map:
//
//	L0: Protocol       — pure types
//	L1: LLM Gateway    (this package) — depends on protocol
//	L2: Agent Loop     — depends on protocol only (L1 via StreamFn parameter)
//	L3: Agent          — depends on protocol, loop
package gateway
