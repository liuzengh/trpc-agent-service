// Package agent hosts tenant-specific agents built on tRPC-Agent-Go
// (llmagent, graph, chain/parallel/cycle) and runner.Runner.
//
// The first implementation in this repository is intentionally small: it
// exposes one LLMAgent backed by an in-memory session service and a deterministic
// model. This gives new contributors a runnable baseline before Redis,
// multi-tenancy, channels, and distributed coordination are introduced.
package agent
