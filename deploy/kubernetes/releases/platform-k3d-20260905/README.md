# Enterprise Multi-Tenant Agent Platform K3d validation release

Generated from the checked-in Kubernetes baselines by `scripts/render_k3d_release.ps1`.

- Scope: local three-node K3d evidence only; this is not a production certification.
- Schema class: `bootstrap`.
- Images: immutable digests in the in-cluster registry; exact values are recorded in `SHA256SUMS.txt`.
- Transport: Linkerd mesh mode; the evidence identifier is an immutable historical lab label, not a project version.
- Storage: isolated in-namespace PostgreSQL and Redis with explicit local-only insecure transport flags.
- Secrets: generated at runtime by `scripts/run_k3d_validation.ps1`; no credential values are stored here.

The eight release inputs are `migration.yaml`, `profiles.yaml`,
`availability-policies.yaml`, `worker.yaml`, `summary.yaml`, `pipeline.yaml`,
`admin.yaml`, and `gateway.yaml`. `network-policies.yaml` is the reviewed policy
input. `k3d-validation-prerequisites.yaml` is local test infrastructure and is
not passed to `releaseverify`.
