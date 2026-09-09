# 02: Shared Provider Route Registry

Type: task
Status: ready-for-human

## Goal

Make real-IM Tenant/App routing authoritative and immediately consistent across
all Gateways in the competition topology.

## Work

- Add Provider Routes to the Control Plane snapshot and package-private store
  interface using revision/CAS persistence.
- Make Provider Runtime and HTTP route management use the same context-aware
  registry interface.
- Fail closed on Control Plane read failure; do not use a stale local route.
- Remove per-Gateway Bot Route JSON paths from Stage 7 Compose.
- Keep any legacy JSON support as explicit, idempotent migration input only.

## Tests

- Create/update/disable/delete through Gateway B and resolve callbacks through
  Gateway A after every mutation.
- Restart either Gateway and prove the shared state remains authoritative.
- Inject a Control Plane outage and assert a stable, non-leaking failure.
- Add the shared-route path to `stage7-compose-acceptance.sh`.

## Done

The Compose topology contains one shared Provider Route source of truth and no
Gateway-local runtime route file.

## Evidence

- `./scripts/stage7-compose-acceptance.sh` passed route create/read/update/
  disable/delete checks across Gateway A and B.
- `./scripts/verify-docs.sh` passed the shared route documentation checks.
