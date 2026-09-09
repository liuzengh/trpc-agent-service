# Stage 2 Handoff

Status: ready

Stage 3 may consume the following frozen boundaries:

- `DataStore` is the platform Storage Adapter for Session Events, replayed Session state/Summary, and Memory.
- Event identity is scoped to Tenant, Session, and idempotency key; conflicting replays fail and sequence is monotonic per Tenant/Session.
- Trusted Tenant Context chooses the server-configured backend; request input and browser state cannot supply credentials, addresses, paths, or tenant authority.
- Backend health uses `healthy` and `unavailable` without exposing connection details.
- Routed runs persist `message.input`, `message.output`, and `run.failed` events.
- Redis-to-SQL migration reports status, total/processed Sessions, source/destination counts, a full-content checksum, resume state, and a sanitized error message.
- The Management Console data page owns backend health/selection, Session/Event inspection, Memory/Summary status, migration creation, progress, result, authorization, empty/error, tenant-switch remounting, and stale-response handling.

Stage 3 must preserve these contracts while adding Chat Session APIs and SSE. It must not expose backend configuration secrets or treat browser storage as conversation history.
