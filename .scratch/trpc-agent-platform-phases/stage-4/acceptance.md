# Stage 4 Acceptance

Status: complete

Stage 4 adds a WeCom API-mode Smart Bot WebSocket adapter and a Telegram long-polling Bot adapter behind the Stage 3 ChannelAdapter contract. Neither path uses a traditional WeCom self-built application. Both providers support deterministic local protocol replay without external credentials.

## Automated Gate

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `cd frontend && npm run typecheck && npm test && npm run build`
- `cd frontend && npm run test:e2e`

The provider tests cover WeCom subscription authentication and ACK correlation, Telegram update parsing and bounded retry, tenant-scoped allowlist selection, duplicate suppression through Session idempotency, bounded text limits, explicit unsupported-media outcomes, context cancellation, and callback-to-Runner-to-provider-reply routing. Provider credentials are process-only and never enter Channel Bindings. Desktop and mobile Playwright flows cover route creation, local protocol replay, disable, latest delivery status, and deletion. Credential smoke tests are optional and must be reported as unavailable or not run, never passed, without current evidence. The provider status API and console expose this non-secret state as `credential_smoke_status`; this implementation records `not_run` because live credentials are deliberately not exercised by CI.

## Known Limitations

Text is the required provider format. Telegram non-text updates and WeCom media callbacks are acknowledged or ignored without Agent execution; media download, conversion, OCR, and storage remain outside this phase. The allowlist is an atomic local file, so multi-node distribution requires the Stage 5 control-plane configuration mechanism. Latest delivery status is bounded in process memory while durable per-Session reply and failure events remain in the selected DataStore. Live credential smoke was not run automatically because Telegram polling can consume pending updates and a WeCom connection can replace an existing bot connection.
