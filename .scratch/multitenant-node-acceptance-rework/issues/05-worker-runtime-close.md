# 05: Worker Runtime Close

Type: task
Status: ready-for-human

## Goal

Close and drain the standalone Worker's cached upstream Runners during bounded
shutdown.

## Work

- Add an idempotent `WorkerServer.Close()` that delegates to the framework
  Runner adapter.
- Call it after HTTP admission shutdown in the standalone Worker process.
- Preserve bounded error reporting and process exit.

## Tests

- A blocking run is cancelled and its event channel closes during shutdown.
- cached Runner `Close` executes exactly once across repeated closes.
- `go test -race` reports no lifecycle race or goroutine leak.

## Done

HTTP admission, active execution, event streams, and cached Runners all reach a
bounded terminal state on Worker shutdown.

## Evidence

- `go test -race ./...` passed worker shutdown and blocking-run lifecycle
  coverage.
- Stage 7 Compose passed Worker stop/restart and no-replay-after-owner-loss
  checks.
