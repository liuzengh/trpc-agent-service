# 01: Operations Health And Graceful Drain Baseline

**What to build:** Operators can inspect Gateway, Worker, storage, and provider health, request a graceful drain, and watch in-flight work finish safely while new work is rejected during shutdown.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Health state distinguishes healthy, degraded, unavailable, draining, and closing components without exposing Tenant business data.
- [x] An authorized operator can start a drain; the request requires confirmation and records an Audit Event with the required identity, trace, and decision fields.
- [x] Active executions continue to their terminal Session Event and all runtime, storage, provider, and event-channel resources are released before shutdown completes.
- [x] New executions are rejected with a stable `service_closing` error while existing executions are still draining.
- [x] The Management Console shows component health, drain status, active work, and shutdown progress for operators, while lower-privilege roles cannot start a drain.
- [x] Backend, frontend, and race tests cover successful drain, blocked shutdown cancellation, authorization, confirmation, audit recording, and health-state isolation.
