# 01: Pin Framework Runtime And Build Minimal Agent

**What to build:** Make the service use the published `trpc-agent-go v1.11.2` runtime and build a minimal deterministic or injectable-model Agent from a server-owned, published Deployment Version. Requests cannot override Agent, model, Prompt, Tool, or Runtime configuration. EchoRunner remains available as a deterministic test double.

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] `go.mod` and `go.sum` resolve the official `trpc.group/trpc-go/trpc-agent-go v1.11.2` module without a local `replace` dependency.
- [x] AgentFactory accepts only a validated, published Deployment Version and rejects missing, invalid, or inactive configuration.
- [x] Framework-backed execution works without external model credentials through a deterministic or injectable-model fixture.
- [x] EchoRunner remains usable for existing compatibility and unit-test coverage.
- [x] Tests prove request fields cannot override Agent, model, Prompt, Tool, or Runtime configuration.

## Answer

Pinned the official v1.11.2 module, added the framework-backed deterministic Agent and AgentFactory boundary, switched production composition to the framework Runtime, and retained EchoRunner for compatibility tests. Go unit, race, vet, and build checks pass.
