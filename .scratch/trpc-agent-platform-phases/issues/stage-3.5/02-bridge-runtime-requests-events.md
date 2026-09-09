# 02: Bridge Runtime Requests And Events

**What to build:** Make Chat API requests execute through the upstream Agent runtime while preserving the Stage 3 platform contracts. Convert platform input into the framework message and run context, then expose framework progress through the stable platform runtime event model and SSE envelope without leaking upstream event types.

**Blocked by:** 01: Pin Framework Runtime And Build Minimal Agent

**Status:** resolved

- [x] Tenant, App, Deployment, Version, User, Session, and `request_id` identity reaches framework execution and returned events.
- [x] Normal completion, failure, and streaming delta events map to the existing platform event types.
- [x] SSE ordering, event identity, deduplication, and resume behavior remain compatible with Stage 3.
- [x] Upstream framework event and model types do not cross the public API boundary.
- [x] EchoRunner and the framework runtime execute through the same platform boundary.

## Answer

Added the streaming RuntimeEvent boundary, mapped platform requests to upstream model messages and request options, translated upstream events into platform event types, and routed Chat execution through Runtime.Stream. Existing Stage 3 SSE and Chat behavior remains green.
