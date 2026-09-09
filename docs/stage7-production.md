# Production Deployment Design

The shipped production example is a real Compose closure only. Kubernetes,
Prometheus, production Collector, backups and cross-region disaster recovery
are design artifacts and examples, not deployed dependencies. The local
`obs` overlay is an acceptance topology and must not be interpreted as a
production SLO or retention policy.

Production should run Gateway and Worker Deployments with separate readiness
checks, a Collector DaemonSet or gateway, and Prometheus recording rules. Use
Secret objects for model, Redis, SQL and IM credentials. Backups must cover SQL
data and Redis AOF/snapshots; restores are tested with tenant fingerprint and
session-head validation. Cross-region recovery requires fencing the old region,
replaying durable inbox tasks and verifying outbound idempotency before traffic
shift. Capacity is sized from peak tasks per second times model latency plus
lease and retry overhead.
