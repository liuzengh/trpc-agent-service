# Phase 7 Fault Matrix

The fault harness uses only the documented HTTP controls of `mock-model` and
`mock-telegram`, plus Docker service lifecycle commands. Every case records a
baseline, injects one dependency failure, asserts the public HTTP state and
error code, restores the dependency, and verifies recovery with a new message
ID. Evidence is written to the local, ignored `phase7-evidence/` directory.

| Case | Injection | Public assertion | Recovery |
| --- | --- | --- | --- |
| F1 duplicate IM | repeat one mock update | one task and one successful send | next update accepted |
| F2 worker crash | kill worker-1 during delayed model call | worker-2 claims after lease expiry, attempt 2 | task succeeds |
| F3 reconciler | start `ha`, kill leader | standby reports ready leadership | assignment converges |
| F4 control Redis | stop control-redis | `/readyz=503`, new request `not_ready`, Admin `control_plane_unavailable` | ready returns 200 |
| F5 messaging Redis | stop messaging-redis after enqueue | new request `not_ready`, queued task retained | original task completes |
| F6/F7 SQL | stop only target PG/MySQL | target reports `sql_unavailable`, Redis tenant remains usable | same process handles new SQL task |
| F8 model timeout | set mock delay beyond request timeout | terminal `model_timeout`, bounded attempts | new task succeeds |
| F9 tool governance | enable deterministic dangerous tool | initial `tool_rejected`, confirmation is single-use | confirmed call succeeds |
| F10 outbound | mock send status 500 | outbound `retry_wait/send_failed` | one eventual ack after restore |
| F11 shutdown | stop worker while task is in flight | no new claims; in-flight becomes `worker_shutdown` retry | restart or peer completes |

The Admin task endpoint returns only `task_id`, state, attempt, error code,
node, assignment state and persistence attempt. It never exposes payloads,
Redis keys, credentials or message text. Admin `/metrics` remains a discrete
Redis event query; Prometheus time series are served by the local `obs`
overlay.
