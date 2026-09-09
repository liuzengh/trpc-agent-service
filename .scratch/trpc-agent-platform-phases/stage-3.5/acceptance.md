# Stage 3.5 Acceptance

Status: passed

## Scope

Stage 3.5 binds the Stage 3 Chat and SSE contracts to the published `trpc.group/trpc-go/trpc-agent-go v1.11.2` module through `AgentFactory` and `FrameworkRunnerAdapter`. `EchoRunner` remains available for Stage 3 compatibility and deterministic test-double coverage.

## Automated Gate

```bash
./format.sh
go test ./...
go test -race ./...
./lint.sh
go vet ./...
go build ./...
git diff --check
```

The framework-backed tests run without external model credentials. They cover deterministic streaming, request and context identity, stable RuntimeEvent mapping, HTTP Chat/SSE persistence and replay, framework failure, framework cancellation, Runner reuse, inactive-version rejection, version retirement, service shutdown, and Redis/SQLite Session Event persistence.

The existing frontend and Stage 3 regression suites remain unchanged by this phase and are covered by the previously passed Stage 3 gate. Stage 3.5 adds no real IM provider and requires no provider or model credential.

## Proven Behavior

- The official module is pinned at `v1.11.2` with no local `replace` directive.
- Agent construction receives the server-owned Deployment Version; request input contains no Agent, model, prompt, Tool, or runtime override fields.
- Only the active Deployment Version can create or reuse a Runner. Pausing a Deployment retires its cached Runner and cancels active framework work.
- Framework input maps to upstream user ID, session ID, `model.Message`, and request ID. Full Tenant/App/Deployment/Version/Session/request identity is carried in the framework context and RuntimeEvent data.
- Upstream events are translated to platform `message.delta`, `message.completed`, `run.failed`, `run.cancelled`, and `run.completed` events. Upstream event and model types do not cross the platform boundary.
- Chat persistence uses the selected Stage 2 DataStore and includes tenant, app, deployment, version, session, user, and request identity in execution payloads.

## Known Limitations

- The deterministic Agent is a fixture; production model selection, Tool/MCP, Plugin/Guardrail, governance, and production identity remain later-phase work.
- Deployment and Agent Version control state remains process-local in the current platform skeleton; Stage 2 storage owns Session/Memory data and remains the persistence boundary.
- Real Enterprise WeChat and Telegram protocols and credential smoke tests remain Stage 4 scope.
