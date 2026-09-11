# Production services

Each directory below is one independently built production workload. Service
implementations are private to their own `internal/` tree. Cross-workload
business protocols live under `/api` and generated code under `/gen`. The
explicitly permitted public technical library `/platform/im/wecom` contains
provider protocol behavior only; it is imported into its caller, not deployed
as a service. It must not import service internals or own platform business state.

The first implemented workload is [`control-api`](control-api/README.md), covering
Identity, Admin, Tenant, Agent, and Runtime Profile V1 including private credentials.
The first Channel Gateway slice includes [`channel-gateway`](channel-gateway/README.md):
Routing projection, Telegram durable admission, PostgreSQL Outbox, and NATS transport.
Its complete capabilities and current verification evidence are tracked in the
[implementation status](../docs/architecture-next/channel-gateway/implementation-status.md).
The current worktree also includes the persisted continuous apply-lag gate (0004),
Connection leases/Supervisor (0005), and durable WeCom ingress using the public P0
library in-process. Delivery Final, original-reply provenance, and Sender reservation now have module
implementations. The current Runtime slice adds bounded Maintenance/Runner, real
LocalOwner snapshots, migration 0007, and Decode error classification. The default
App starts maintenance only, including replicas without accounts; it does not start
a sending Runner, Provider, ReplyIntent consumer, or permissive verifier. Final
acceptance for this slice is pending in implementation status section 11. Production
Control/Worker, reply transport, credentials and sending assembly remain pending.
Prior runtime evidence stays in its dated historical sections. Other workloads are added
as verifiable vertical slices, not empty service placeholders.
Channel Gateway design keeps routing, admission, connection, and delivery in one Go
workload; WeCom integration imports the public library in-process without
adding a deployment unit. Helm remains FINAL-INTEGRATION after all production workloads.
