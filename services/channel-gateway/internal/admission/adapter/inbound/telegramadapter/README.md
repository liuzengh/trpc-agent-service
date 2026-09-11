# Telegram webhook inbound Adapter

This package imports `github.com/go-telegram/bot/models` at the repository's
pinned v1.25.0 version. It does not start the SDK webhook queue: HTTP 200 follows
`Acceptor.AcceptInbound` returning a committed receipt, including durable ignore
and interaction receipts. Transaction failures return HTTP 503.

`NewHandler(accountID, webhookSecret, acceptor)` binds one trusted account to a
registered route. Neither body fields nor path/query values choose the account,
Tenant, Binding, or Deployment. Authentication compares SHA-256 digests of the
secret header with `crypto/subtle`; duplicate authentication headers are rejected.
The caller supplies server read/write deadlines and secret rotation wiring.

## Implemented capability boundary

- Private nonempty text from an explicitly identified non-bot user becomes `text`.
- Callback queries from a non-bot user become `interaction`, never prompt text.
  ReplyContext retains query/address IDs, not callback data or message content.
- Group/supergroup inputs, edits, bots, anonymous senders, service messages, media,
  and unknown message capabilities become durable `ignore` receipts with no text.
- Group commands/replies require verified own-bot identity and trigger policy;
  they are deliberately not enabled by this initial private-chat capability.
- Interaction command execution and `answerCallbackQuery` are separate application
  and delivery work. This Adapter only authenticates and normalizes the event.

An exact text-envelope member allowlist prevents a service/media message with a
`text` member from implicitly entering the model path. New message capabilities
need an explicit normalization/trigger update before they become executable.

## Identity and parsing

- Event key: `telegram + configured stable account ID + update_id`.
- SDK `Update.ID`, Chat ID and User ID are `int64`; string conversion preserves
  values beyond JavaScript's exact-integer range. Negative update IDs are invalid.
- Body limit: 1 MiB, including chunked bodies. Exactly one UTF-8 JSON object is
  accepted; whitespace/newlines after that object are allowed, another value is
  rejected. Missing/null IDs, numeric overflow, duplicate keys and over-64-level
  nesting are rejected before admission.
- SourceDigest: SHA-256 of recursively key-sorted JSON, with `json.Number`
  preserving number precision. Whitespace, object order and escaped string
  presentation do not change the digest. Unknown provider fields remain in the
  digest. Number spellings are not rewritten beyond the original JSON number.
- Raw JSON stays request-local; only normalized input, minimal ReplyContext and
  digest cross the admission boundary. Replays invoke admission with the same
  event key; the durable store owns deduplication and payload conflicts.

Tests use `httptest` request/response recorders and a delayed acceptor to prove
there is no early HTTP acknowledgement. They cover auth, trust boundaries,
lossless identity, canonical replay, callbacks, ignored capabilities, parser
ambiguity, size limits, request cancellation and commit failure. These tests do
not claim a PostgreSQL, Telegram network or complete Gateway end-to-end run.
