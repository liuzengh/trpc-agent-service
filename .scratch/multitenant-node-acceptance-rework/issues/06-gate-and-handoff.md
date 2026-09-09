# 06: Acceptance Gate And Handoff

Type: task
Status: ready-for-human
Blocked by: 01, 02, 03, 04, 05

## Goal

Turn every fixed review finding into reproducible acceptance evidence and leave
the repository ready for final review.

## Work

- Extend Stage 7 Compose with shared Provider Route, same-request A/B, and
  remote-cancel scenarios.
- Update architecture, data model, storage strategy, acceptance checklist, and
  known limitations to match the implementation exactly.
- Run every command in the root spec's Required Gates from a clean checkout.
- Record the resulting commit/schema evidence using the existing Stage 7
  evidence convention.

## Done

Every row in the root spec Acceptance Matrix has an automated assertion, every
required gate passes, and no known limitation contradicts README multi-tenant or
multi-node behavior.

## Evidence

- `./format.sh`, `go test ./...`, `go test -race ./...`, `./lint.sh`, and
  `./build.sh` passed.
- `npm --prefix frontend run typecheck`, `npm --prefix frontend test`, and
  `npm --prefix frontend run build` passed (22 frontend tests).
- `./scripts/verify-docs.sh` passed (2,167 Han characters in architecture
  documentation).
- `DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=1 ./scripts/stage7-compose-acceptance.sh`
  passed all two-Gateway assertions. A local `golang:1.23-alpine` tag was used
  after Docker Hub returned a transient 500 during the first build attempts.
