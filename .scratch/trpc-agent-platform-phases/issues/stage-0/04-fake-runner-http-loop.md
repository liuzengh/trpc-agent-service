# 04: Fake Runner And Minimal HTTP Platform Loop

**What to build:** A minimal HTTP JSON request path that invokes the platform application port and a replaceable fake Runner, returning deterministic success and error responses without implementing a real model or IM integration.

**Blocked by:** 01: Runnable Service Baseline; 02: Platform Domain And Port Contracts; 03: Cancellation And Graceful Shutdown

**Status:** resolved

- [x] HTTP parsing, context propagation, and error mapping are adapters around the application port rather than domain logic.
- [x] A valid request reaches the injected fake Runner and returns a deterministic response.
- [x] The Runner implementation can be replaced through dependency injection.
- [x] Invalid requests, request cancellation, and Runner failures produce stable JSON error responses.
- [x] Tests cover the adapter, port invocation, fake Runner replacement, and cancellation behavior.

**Verification:** `platform.RunHandler` and `web.NewHandlerWithRunner` implement the JSON adapter and trusted tenant injection. `EchoRunner` is replaceable via `RunnerAdapter`; tests cover success, validation, missing tenant, runner error, and cancellation mapping. `go test ./...` and `go vet ./...` pass.
