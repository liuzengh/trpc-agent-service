# Stage 5 Acceptance

Status: passed

## Delivered Scope

Stage 5 adds explicit development/production identity modes, server-owned
Tenant/role assignments, governance policy enforcement around the Stage 3.5
framework runtime, dangerous Tool confirmations, external IM authorization,
budgets, Tenant rate limits, persistent Audit Events, metrics/cost accounting,
platform trace propagation, redaction, and the corresponding Management Console
workflows.

## Reproduction

```bash
gofmt -w cmd trpcservice
go test ./...
go test -race ./...
go vet ./...
cd frontend
npm run typecheck
npm test
npm run build
npm run test:e2e
cd ..
./build.sh
```

Automated production-auth tests generate deterministic local HS256 fixtures and
do not require a hosted identity provider. Provider tests use deterministic
Telegram and Enterprise WeChat protocol fixtures. Real credential smoke is not
run automatically because it can consume live messages or replace an existing
WeCom Bot connection.

## Acceptance Matrix

| Requirement | Evidence |
| --- | --- |
| Production identity and role enforcement | `identity_test.go`, HTTP authorization suites, console login/session tests |
| Tool/MCP and Guardrail decisions | `governance_test.go`, `governance_http_test.go`, framework runtime tests |
| Confirmation, IM authorization, budgets, limits | governance HTTP/unit tests and provider replay tests |
| Audit, metrics, cost, traces | governance persistence/API tests and Stage 5 console/Playwright tests |
| Redaction and secret non-disclosure | log, policy API, runtime, component, and DOM canary assertions |
| Desktop/mobile workflows | `frontend/e2e/stage5.spec.ts` in Chromium desktop/mobile projects |

## Exclusions And Known Limitations

- `non-blocking-quality`: production JWT acceptance uses deterministic HS256;
  hosted OIDC discovery, JWKS/RS256, key rotation, and revocation are adapter work.
- `non-blocking-quality`: governance state and aggregate metrics use an atomic
  local JSON file, not a shared multi-node control-plane or telemetry backend.
- `non-blocking-quality`: Platform Trace is an equivalent standard bounded
  model and does not export OTLP.
- `non-blocking-quality`: token usage is estimated when the deterministic
  runtime supplies no usage metadata.
- `non-blocking-quality`: dangerous Tool confirmation uses retry-after-approval
  with the same request ID rather than suspending a Tool goroutine.
- `non-blocking-quality`: an existing write-only redaction list can be replaced,
  but explicit clearing through an empty/placeholder-only update is not modeled.

No known limitation is classified as `blocking-acceptance`.

Final validation on 2026-09-03 passed all commands above. Playwright reported
eight passing desktop/mobile scenarios across Stages 1, 3, 4, and 5; direct
visual inspection at 1440x900 and 390x844 found no horizontal overflow,
overlapping controls, or inaccessible Governance navigation.
