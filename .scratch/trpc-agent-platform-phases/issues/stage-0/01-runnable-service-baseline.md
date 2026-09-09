# 01: Runnable Service Baseline

**What to build:** A reproducible service baseline that can be built and started, exposes health and version checks, and can be stopped through the repository's standard scripts without requiring real model, IM, or external storage integrations.

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] The minimal service builds and starts through the repository's documented commands.
- [x] Health and version endpoints return stable, machine-checkable responses.
- [x] Build, start, and stop flows are reproducible from a clean checkout.
- [x] The baseline does not connect to real model, IM, authentication, or external storage services.

## Comments

- Added a standard-library HTTP baseline with `GET /healthz` (`{"status":"ok"}`) and `GET /version` (`{"version":"0.1.0"}`).
- The binary accepts `-addr` and `TRPC_SERVICE_ADDR`; the default is `:8080`. SIGINT/SIGTERM trigger a bounded graceful shutdown.
- Verification: `./build.sh`, `go test ./...`, `go vet ./...`, and a live `start.sh`/`curl`/`stop.sh` smoke test passed.
