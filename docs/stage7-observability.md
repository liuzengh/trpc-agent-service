# Phase 7 Observability

Business spans cover ingress, routing, scheduling, worker claim/admit,
execution, persistence and outbound. The framework's native model/tool spans
remain authoritative for provider calls; platform spans add boundaries without
double-counting. W3C context is extracted from HTTP and carried in task
`trace_parent` when digest-v2 is enabled.

Counters and histograms cover requests, latency, model tokens, tools, IM
outbound, storage errors, persistence, assignments, heartbeats, degraded state
and telemetry drops. A process-local observable gauge reports in-flight tasks.
Each process exports `service.instance.id` from its explicit environment value
or hostname, so Gateway and Worker series do not collide. Labels are limited to
stable dimensions such as tenant, channel, backend, operation, status, token
type and reason. Collector failures are logged and dropped after bounded
queues; they never change `/readyz`.

The Redis metric sink stores bounded request events for Admin queries. Those
events contain tenant, channel, status, operation, duration and count only;
request, task, session and message IDs are never labels. The sink and OTel
provider are independent fail-open paths.
