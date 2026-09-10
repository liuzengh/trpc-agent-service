# Deterministic fault-injection E2E

This example exercises the complete in-process boundary used by issue #99:

- HTTP authentication and strict request decoding reject forged tenant routes.
- Runner/provider failures are returned as stable, redacted errors.
- Outbox retry, lease fencing, worker races, and exactly-once delivery are
  tested with controllable fakes.
- Reply materialization failures do not expose storage/provider details or
  leave partial rows behind.
- Two tenant-scoped execution plans are constructed concurrently and never
  share a cache key.

The suite is fully deterministic and does not require PostgreSQL, network
access, or production credentials.

```bash
go test ./examples/fault-injection-e2e -count=1 -v
go test -race ./examples/fault-injection-e2e -count=1 -v
```

The same commands run in `.github/workflows/fault-injection-e2e.yml` for pushes
and pull requests.
