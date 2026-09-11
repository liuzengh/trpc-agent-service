# Execution events v1: RunRequested and ReplyIntent

This directory owns the Gateway-to-Worker `execution.run-requested.v1` wire
contract. Gateway's Admission transaction saves the immutable event in its
Outbox; a JetStream publish acknowledgement is not Worker Run materialization.
Worker must validate the wire event and persist its consumer receipt and Run
before acknowledging consumption. This directory also owns the text-Final
`execution.reply-intent.v1` contract described below. It does not implement Worker,
a production committed-Final verifier, or a NATS reply consumer.

## Source and generated transport

- `run-requested.schema.json`: closed JSON Schema Draft 2020-12 for RunRequested.
- `reply-intent.schema.json` / `reply_intent.go`: closed text-Final schema and
  strict codec/digest functions; generated DTO is `gen/events/execution/v1/reply_intent.go`.
- `schema.go`: embedded schema, strict decoder, deterministic encoder and the
  narrow normalized-reply-context decoder.
- `generate/main.go`: schema-driven, dependency-free DTO generator.
- `gen/events/execution/v1/run_requested.go`: generated transport values only.
- `fixtures/valid` and `fixtures/invalid`: executable wire-contract cases;
  invalid fixtures include relational errors not expressible in JSON Schema.

Run `go generate ./api/events/execution/v1` from the repository root. Tests run
`go run ./generate -check` from this directory to reject stale generated output.
No generated DTO imports a service, SDK, database, or provider client.

## Closed RunRequested contract

The envelope requires `schema_version=1`, stable `event_id`, `admission_id`,
`run_id`, `route`, and `input`. `input.kind` is exactly `text`: ignored updates
and interactions do not become RunRequested events. Text is preserved without
trimming, but an entirely whitespace input is rejected. The complete event is
at most 1 MiB, and UTF-8 text is at most 65,536 bytes.

The route requires provider/account/tenant/binding, positive generation, a
fixed Deployment revision, immutable manifest reference, and a lowercase
`sha256:<64 hex characters>` digest. Route fields share the Control route
projection's identifier/ref bounds. Generation is at most 9,007,199,254,740,991;
schema validation uses exact `json.Number` before numeric normalization. IDs
remain strings, including Telegram values above the floating-point integer
precision boundary. Platform IDs are 1–128 ASCII identifier characters;
external IDs are nonempty, at most 256 Unicode code points, and contain no
ASCII whitespace or control characters.

The codec enforces additional relationships:

- `route.provider/account_id` equals `input.key.provider/account_id`.
- Telegram reply `chat_id` and optional `message_thread_id` match the immutable
  input conversation/thread; decimal provider IDs have canonical signed-int64
  spelling, with negative chat IDs permitted but zero chat/sender/message IDs
  rejected. An update ID of zero is allowed.
- WeCom `chatid_or_userid` matches the input conversation; its reply timestamp
  equals `input.received_at` as an instant. `chat_type` is normalized to
  `single|group`. The Gateway WeCom inbound adapter now produces this normalized
  context; protocol authentication and Admission are separate from wire validation.
- `received_at` is a nonzero RFC3339Nano timestamp; `source_digest` is the
  Admission's lowercase 64-hex source digest, not a digest recomputed from the
  narrower execution payload.

`reply_context` is a required typed address, never an arbitrary SDK payload:

| Provider | Required | Optional |
| --- | --- | --- |
| Telegram | `chat_id` | `message_thread_id`, `source_message_id` |
| WeCom | `chat_type`, `chatid_or_userid`, `received_at` | `callback_req_id` |

Unknown fields, nulls (including null optional fields), duplicate or escaped
alias keys, invalid UTF-8, unpaired UTF-16 escapes, trailing JSON, fractional or
out-of-range integers, provider mismatches and credential-bearing extras are
rejected. Optional fields are absent rather than null/empty; the generated
DTO's `omitempty` tags implement this rule on encoding. The Encoder validates
Go strings before `encoding/json` can replace invalid UTF-8.

## Adapter API

```go
import (
    wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
    dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
)

// Adapters map service-owned values into generated values field by field.
reply, err := wire.DecodeReplyContext(provider, normalizedReplyJSON)
// Handle err before constructing the full event.
event := dto.RunRequested{ /* fixed envelope, route, and input fields */ }
event.Input.ReplyContext = reply
payload, err := wire.EncodeRunRequested(event)
// Persist payload with the Admission transaction, never after commit.
received, err := wire.DecodeRunRequested(payload)
```

`DecodeReplyContext` is only for normalized address JSON; it does not accept a
Telegram `Update` or a WeCom callback object. Sender authentication, route
authorization, source-payload digest computation and semantic Admission are
owned by Gateway. Execution authorization and immutable ReplyIntent/Final
production are owned by Worker and are not granted by schema validation.

### Gateway-only reply origin

Migration `0006_delivery.sql` adds a nullable `gateway_admissions.reply_origin`
column. The WeCom inbound adapter binds the original callback's socket generation
to the acquired owner instance/epoch/configuration revision, and Admission saves
that snapshot on its first commit. Replays preserve the first reply context and
origin. Old rows remain SQL NULL; Gateway does not infer origin from the current
socket or rewrite it after ownership changes.

This is Gateway-local reply authority metadata, not a new execution DTO field:
`Inbound.ReplyOrigin` is `json:"-"`; it is excluded from SourceDigest, stored input
JSON, RunRequested and ReplyIntent. Delivery resolves it through Admission's
immutable reply reader. A raw `callback_req_id` in the wire is not permission to
send on a new owner/socket. The wire keeps `callback_req_id` optional for WeCom;
the current Gateway callback adapter provides it, and the reply path requires a
known original context rather than weakening the versioned wire schema.

## ReplyIntent text Final v1

`reply-intent.schema.json` owns `execution.reply-intent.v1`. The strict
`DecodeReplyIntent`, `EncodeReplyIntent`, and `ReplyIntentDigest` functions use
its generated `gen/events/execution/v1/reply_intent.go` DTO. `go generate` emits
both event DTOs; tests independently check each generated file.

One immutable logical Final contains `intent_id` (also its stable transport
message ID), `admission_id`, `run_id`, `execution` (`attempt_id`, positive
`generation`, `completion_id`), positive `sequence`, `kind=final`, complete
`content` (`type=text`, `text`), and `deadline`. The deadline must have the exact
UTC RFC3339Nano spelling. Text preserves whitespace and Unicode, rejects blank
or NUL-containing input, and is limited to 65,536 UTF-8 bytes; the event is at
most 1 MiB. Progress, markup, caller-provided destinations, credential values,
trace metadata and owner/socket fences are not accepted as business fields.

The SHA-256 digest covers the entire RFC 8785 canonical business document and
uses `sha256:<lowercase hex>`. It is not itself authorization: Delivery must
resolve the immutable Admission reply target and consult the Execution owner's
committed, immutable Final record for that exact intent/digest and execution
identity. A current active lease check would incorrectly reject a valid delayed
Final after execution ended. Workload/NATS permissions alone do not authorize
an individual Run. Trace carriers belong in bounded transport headers, not the
business digest; complete trace transport wiring is a separate integration.

The Gateway owns the durable part plan and each part's delivery certainty. The
Worker sends complete logical text, not provider-specific chunks. The current
Gateway policy splits Telegram at Unicode code-point boundaries of 4,096 per
part, with stable order and no truncation; WeCom P0 is one Final per callback,
so text above its 20,480-byte SDK bound is explicitly unsupported rather than
silently segmented. These contracts do not implement Worker or prove real IM
delivery. Fixtures under `fixtures/reply-intent` and the valid Final fixture
exercise the contract separately from RunRequested fixtures.

## Implementation and deployment status

The current source includes the Gateway Delivery Acceptor, PostgreSQL ledger,
Dispatcher, strict event adapter, direct Telegram SDK Sender, and reserved WeCom
Sender obtained through Connection. Local vertical tests exercise real PostgreSQL
plus httptest HTTP/WebSocket: A1/A2, provider ACK/uncertainty, durable observations,
Finish, and stale WeCom origin rejection. Their `committedFinalFixture` represents
an immutable Execution authorization record; it is explicitly not a real Worker.

The default `bootstrap.App` still does not construct a ReplyIntent NATS consumer
or start a production Delivery dispatcher loop. A real Execution-owned committed
Final verifier and Telegram outbound credential ownership/resolution/rotation are
still required. Creating the 0006 schema, compiling this codec, or receiving a
RunRequested PubAck therefore does not enable automatic Agent replies.

Historical SDK/Connection acceptance evidence remains separate. Delivery Final
worktree/image acceptance is recorded in implementation status section 10;
Runtime and the seven-migration image acceptance is recorded in section 11.
The default App adds Maintenance only, not a production sending Runner or Reply
consumer. These local acceptance records do not prove real Execution or external
IM integration, and this documentation-only update does not rerun those tests. See the
[Gateway service status](../../../../services/channel-gateway/README.md) and
[implementation record](../../../../docs/architecture-next/channel-gateway/implementation-status.md).
## Tracing 传输元数据（M2 增量）

`execution.run-requested.v1` 的 W3C `traceparent`/`tracestate` 位于 NATS Header，
不进入本目录 JSON Schema、DTO、Event/Run digest 或 `Nats-Msg-Id`。Gateway Outbox
保存不可变 creation context；每次实际发布的 Span 不替换消息 Header 中的 creation。
Worker 在原接纳事务保存 process context；ACK、重投和首次接纳语义不变。

仅两个 W3C 字段，各最多 512 字节，tracestate 最多 32 项；非法元数据归一化为空，
不以元数据格式错误拒绝业务事件。没有 Baggage 或原始错误/用户正文。有效未采样 context
仍传播，数据库首次值（包括 NULL）不被重放替换。ReplyIntent 的同类接线由 M4 实施。
