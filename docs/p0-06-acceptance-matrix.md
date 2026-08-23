# P0-06 Acceptance Matrix

Baseline captured before P0-06A work:

- `git status --short`: dirty worktree with pre-existing P0-06 drafts and unrelated documentation changes.
- `git diff --check`: no whitespace errors.
- Existing `go test ./...` and `go test ./... -race` results are historical evidence only; they do not close any matrix item.

## P0-06A: Claim Contract and Single Backend

| Item | Scope | Unit/contract | Real integration | Race | Status |
| --- | --- | ---: | ---: | ---: | --- |
| Public DedupKey/Claim/error semantics | A | passed | n/a | passed | verified |
| Fake Claim contract | A | passed | n/a | passed | verified |
| Redis Claim acquire/duplicate/takeover | A | passed | passed | passed | verified |
| Redis Complete/Fail fencing | A | passed | passed | passed | verified |
| PostgreSQL Claim acquire/duplicate/takeover | A | passed | passed | passed | verified |
| PostgreSQL Complete/Fail fencing | A | passed | passed | passed | verified |
| Cross-backend equivalent outcomes | A | passed | passed | passed | verified |
| P0-06A full `go test` | A | passed | n/a | n/a | verified |
| P0-06A full `go test -race` | A | passed | n/a | passed | verified |
| P0-06A `go vet` | A | passed | n/a | n/a | verified |

## P0-06B: Session Lease Baseline

| Item | Status |
| --- | --- |
| Redis lease Acquire | verified |
| Redis lease Renew | verified |
| Redis lease Release | verified |
| Redis lease Validate | verified |
| PostgreSQL lease Acquire | verified |
| PostgreSQL lease Renew | verified |
| PostgreSQL lease Release | verified |
| PostgreSQL lease Validate | verified |
| Cross-backend lease outcomes | verified |
| Shared epoch authority | verified |
| PostgreSQL Claim epoch integration | verified |
| Redis Claim epoch integration | verified |
| Cross-backend Claim epoch outcomes | verified |
| Lease renewal runner | verified |
| P0-06B overall | verified |

## P0-06C: Failover Core

| Item | Status |
| --- | --- |
| FailoverStore public contract | verified |
| Normal active-primary state | verified |
| Quarantine transition/error classification | verified |
| Epoch bump on switch | verified |
| Old epoch fencing after switch | verified |
| Recovered backend cannot activate directly | verified (deterministic only) |
| Concurrent state transitions | verified |
| Real Redis/PostgreSQL FailoverStore contract | verified |
| Circuit breaker state model | verified |
| Failure classification | verified |
| Open/quarantine transition | verified |
| Half-open/probing transition | verified |
| Probe success/recovery integration | verified |
| Probe failure behavior | verified |
| Real Redis recovery/probe | verified |
| Real PostgreSQL recovery/probe | verified |
| Automatic switch state/trigger contract | verified |
| Automatic switch exactly-once epoch bump | verified |
| Non-availability errors do not switch | verified |
| Switch failure fail-closed | verified |
| Real Redis/PostgreSQL automatic switch | verified |
| Cross-backend no-double-owner acceptance | verified |
| Three-dimensional rate limiting contract | verified |
| Redis three-dimensional rate limiting integration | verified |
| PostgreSQL rate-limit data plane | not applicable (not in plan) |
| Fault injection/network partition simulation | verified |
| Cross-backend double-owner acceptance | verified |
| Migration catalog/checksum fail-closed | verified |
| Migration missing/unknown version fail-closed | verified |
| Migration apply rollback/readiness blocking | verified |
| Migration down dependency order | verified |
| Migration fail-closed catalog validation | verified |
| P0-06C isolated final acceptance | verified |
| P0-06C overall | verified |
