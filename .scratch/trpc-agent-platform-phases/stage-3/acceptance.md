# Stage 3 Acceptance

Status: passed

## Scope

Stage 3 delivers the standard tenant-scoped Channel Adapter and Channel Binding contracts, signed Mock IM callback conversion, deterministic duplicate/out-of-order handling, Mock delivery fault semantics, chat Session APIs, ordered persisted history, request-scoped retry, cancellation, and SSE streaming. The existing management console now includes a full-height Chat workspace with history restoration, streaming, cancellation, retry, tenant switching, and authorized Mock fault controls.

## Automated Gate

```bash
./format.sh
go test ./...
go test -race ./...
./lint.sh
go vet ./...
./build.sh

cd frontend
npm run typecheck
npm test -- --run
npm run build
npm run test:e2e
```

All commands passed on 2026-08-30. The frontend gate covered 7 unit/component files and 16 tests. The complete Playwright run covered Stage 1 and Stage 3 on desktop and mobile (4 workflows).

## Proven Behavior

- Channel conversion maps signed Mock callbacks through a tenant-scoped binding to the bound Agent App, Session, external user, conversation type, and conversation ID, then returns the reply through the same binding.
- HMAC-SHA256 signatures are required; invalid signatures, unknown bindings, and apparent identifiers from another Tenant are rejected without executing the Runner or exposing another Tenant's binding.
- Callback message IDs suppress duplicates, and provider sequence is used to reject out-of-order messages. Accepted events retain monotonic Tenant/Session ordering.
- Mock IM models timeout, bounded retry exhaustion, inbound/outbound rate limits, message length, and attachment failures with stable public codes and no provider secret disclosure.
- Chat Sessions are created for an authorized Agent App, are scoped by trusted Tenant Context, and automatically receive a Mock binding. Viewer writes and cross-Tenant Session guesses are rejected.
- `request_id` is generated or accepted per logical browser/channel request, propagated through Gateway and Runner, and used as the idempotency scope for input, terminal, and replay decisions.
- Replaying the same Tenant/Session/request/input resumes the original logical run. Reusing the request ID with different input conflicts, and concurrent retries share one active run.
- History is persisted in Session Events and restored after refresh. Browser storage contains only the most recently opened Session ID per Tenant, never conversation history, secrets, or Tenant authority.
- SSE emits the stable envelope (`event_id`, `request_id`, `session_id`, monotonic `sequence`, `type`, `data`) with `run.started`, `message.delta`, `message.completed`, `run.failed`, `run.cancelled`, and `run.completed`. Clients deduplicate by event ID, resume by sequence or `Last-Event-ID`, and recover history from the backend.
- Cancellation propagates through the active run, persists a terminal cancelled event, closes the stream, and does not hang service shutdown.
- The Chat workspace is available from the main navigation, Agent App, and Deployment detail entry points. It supports desktop/mobile layouts, authorization states, empty/error states, tenant-switch remounting, streaming, cancellation, retry, refresh recovery, and Mock fault injection.

## Public Interfaces

- `GET/POST /api/v1/chat/bindings`
- `POST /api/v1/chat/channels/mock/callback`
- `GET/POST /api/v1/chat/mock/faults`
- `POST /api/v1/chat/sessions`
- `GET /api/v1/chat/sessions/{session_id}/events`
- `POST /api/v1/chat/sessions/{session_id}/messages`
- `POST /api/v1/chat/sessions/{session_id}/cancel`
- `GET /api/v1/chat/sessions/{session_id}/stream?after=&request_id=`

Session mutation, message execution, cancellation, and fault configuration require an operator-capable role. History and streams are scoped by trusted Tenant Context. Session IDs use `[a-z][a-z0-9-]{2,62}`; request IDs use 1–128 printable ASCII characters.

## Data Contracts

- `ChannelBinding` is tenant-scoped and includes App, channel, conversation type/ID, external user ID, and Session ID. List responses omit secrets.
- `ChannelMessage`, `ChannelReply`, `ChannelCallback`, `ChannelCredential`, and `ChannelDelivery` form the adapter boundary. Delivery failures expose stable codes rather than provider diagnostics.
- `SessionEvent` remains the immutable ordered history record. In JSON history responses, `payload` is the base64 encoding of its JSON byte payload; SSE `data` is the decoded JSON object.
- Chat run responses expose `session_id`, `request_id`, and one of `running`, `pending`, `completed`, `failed`, or `cancelled`.
- Mock fault scenarios are `none`, `timeout`, `retry`, `rate_limit`, `message_length`, and `attachment`.

## Exclusions And Known Limitations

- Enterprise WeChat and Telegram protocols, production credentials, provider configuration UI, credential storage, and real-provider webhook authentication remain Stage 4 scope.
- The Web UI and Mock IM are local validation surfaces only and do not count as one of the two required real IM providers.
- Mock bindings, provider message-ID/sequence watermarks, fault selections, and fault counters are process-local; persisted Session Events continue to use the Stage 2 tenant backend contract.
- The local Mock callback route is protected by the development trusted identity boundary and a binding-specific HMAC signature. A provider-facing public callback boundary is deferred to Stage 4.
- Attachment fault semantics are exercised at the adapter contract; a full browser attachment upload workflow is not part of Stage 3.
- Production identity, broader authorization policy, audit, and trace integration remain Stage 5 scope.
