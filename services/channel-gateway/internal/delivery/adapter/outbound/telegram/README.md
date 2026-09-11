# Telegram Final Sender Adapter

This adapter directly imports the pinned `github.com/go-telegram/bot v1.25.0`.
It implements Delivery's `SenderProvider` / `ReservedSender`; it is not a
credential owner, another workload, a Worker, or a replacement for A2.

```go
provider, err := telegramadapter.NewProvider(configuredAccountBots)
// Check err, then inject provider into application.NewDispatcher.
```

The constructor accepts a preconfigured `map[string]*bot.Bot`, copies the map,
and performs no HTTP. Empty configuration is permitted; an unconfigured account
is unavailable. The composition owner must establish each bot's authenticated
account identity, configure bounded HTTP transport and sanitized SDK handlers,
and never call `SetToken` or mutate SDK options concurrently with use. This
adapter does not invent credential lookup, rotation or authentication from a
non-nil client. Its tests skip `getMe` only for synthetic local HTTP fixtures.

`Reserve` validates the fixed original Admission target and exact persisted
part/digest without contacting Telegram. It captures a one-shot handle. Only a
matching A2 Attempt may enter `SendFinal`: claim token, intent/part, instance,
request ID/digest, positive attempt number and nonempty attempt ID/evidence
capability are checked. A local handle cannot verify that a remote database
transaction committed; the Application/real-PG integration must enforce the
A2-only calling path. Telegram uses no Connection owner lease.

Each handle admits at most one concurrent SDK call. `Release` is idempotent,
prevents later sends and cancels an in-flight call. The call's context is bounded
by the caller, the immutable Final deadline, `CallingUntil`, and a one-minute
adapter ceiling. Calling after cancellation returns `NOT_SENT/deadline`; after
entering `SendMessage`, cancellation, transport failures and malformed responses
return `UNKNOWN`, never proof of non-transmission.

The request fixes `chat_id`, optional `message_thread_id`, original reply message
and plaintext part. It does not set parse mode or allow sending without the
original reply target. A migration response does not change the chat. Positive
results require a valid message ID, the original chat ID and, when requested,
the original topic. Typed SDK 400/401/403/404/409 and migration failures become
`REJECTED/permanent`; typed 429 becomes `REJECTED/rate_limited`. No raw SDK error,
HTTP URL, response description or body enters a Domain Result. An untyped failure
remains `UNKNOWN`; Delivery's retry policy never retries an UNKNOWN Final.

`PlanText` owns Unicode-preserving Telegram chunks of at most 4,096 code points.
The adapter sends only the exact precommitted part; the ledger owns ordering,
per-part certainty, Final barrier and recovery. The SDK's `raw_request.go` uses
`io.ReadAll`, so the supplied HTTP transport's response-body budget is a real
composition requirement, not a guarantee supplied by the SDK itself.

The local `httptest` suite exercises exact multipart parameters, no reserve
side effect, one-shot concurrency, fixed-target/attempt fencing, cancellation,
release, typed rejection/migration, malformed and wrong-target responses,
connection loss and persisted multipart text. These tests do not contact the
real Telegram API or establish production credential/IM acceptance.

## Accepted Artifact attachments

ReplyIntent v1 keeps its required text and optionally adds
`content.attachments: [{name, version, mime_type, size_bytes, sha256}]`.
`version: 0` is a fixed version, not latest. Names are basenames; SHA-256 is
64 lowercase hexadecimal characters. URLs, paths and inline file content are
not accepted. MIME parameters are valid when the media type parses correctly.
Absent attachments preserve existing text-only canonical bytes/digests.

Gateway verifies the committed Final before persisting an immutable plan:
text parts first, then one document part per attachment. Existing JSON intent
storage and ordered part rows preserve these descriptors without new DDL.
Attachment bodies contain descriptor JSON; part index distinguishes documents
from text, so user text resembling JSON never becomes an upload instruction.
Every document uses the existing claim/MarkCalling/result boundary. Rejected or
unknown document delivery is not converted into acceptance, and later parts
cannot bypass an unaccepted preceding part. An accepted handoff receipt means
planned, not that every provider part has been delivered.

For a document part, the configured Worker mTLS client POSTs
`/internal/v1/reply-artifacts` with exactly
`{intent_id, run_id, completion_id, name, version}`. Worker owns authorization
against its durable accepted intent, scope and fixed Manifest credentials.
Gateway requires matching Content-Type, Content-Length and X-Content-SHA256,
then checks actual bytes/hash before calling the Telegram SDK `SendDocument`
with `InputFileUpload`. It never obtains object-store credentials. Original
chat, thread and reply source are retained. An accepted document response must
include a valid message and document file ID.

The public Telegram multipart file limit is 50 MB; larger documents are marked
not-sent/permanent without fetching bytes. See
[Telegram sendDocument](https://core.telegram.org/bots/api#senddocument).
The pinned SDK sends upload parts as application/octet-stream; Telegram may
infer the document MIME type. This does not alter the Worker content integrity
checks or put raw file content into the execution event.

WeCom attachment plans are explicitly unsupported in this slice rather than
silently delivering text while claiming that files were delivered.
