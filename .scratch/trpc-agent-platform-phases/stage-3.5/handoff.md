# Stage 3.5 Handoff

Status: ready

Stage 4 and Stage 5 may consume these frozen boundaries:

- `AgentFactory` accepts a server-owned `DeploymentVersion`; callers cannot supply Agent, model, prompt, Tool, or runtime configuration through `RunnerRequest` or Chat input.
- `FrameworkRunnerAdapter` is the production streaming adapter around upstream `runner.Runner`; `EchoRunner` remains a deterministic compatibility test double.
- `RunnerRequest` carries trusted Tenant, App, Session, User, Deployment Version, and request identities. The adapter passes user/session/message/request options upstream and preserves the complete identity in context and platform RuntimeEvent data.
- `RuntimeEvent` is the platform event boundary. Upstream `event.Event` and `model` values are translated before Chat persistence and SSE serialization.
- One Runner is cached per active Deployment Version. Inactive versions are rejected before framework execution; pausing retires the version Runner; service shutdown cancels registered runs and closes all cached Runners.
- Stage 2 `DataStore` remains the Session/Memory boundary. Framework Chat inputs, outputs, failures, cancellations, and terminal events are persisted through the tenant-selected adapter with idempotent terminal keys.
- Stage 4 must preserve these identity, persistence, event, cancellation, and lifecycle contracts for Enterprise WeChat and Telegram. Stage 5 must add governance and authorization around this AgentFactory/RunnerAdapter boundary instead of replacing it.
