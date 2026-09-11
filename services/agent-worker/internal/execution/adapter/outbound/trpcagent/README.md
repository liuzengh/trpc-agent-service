# Worker V1 SDK / Session adapter contract

This package adapts exactly one validated `llm` node. The application owns the
Attempt Grant and checks it before/after SDK execution and before accepting any
external candidate. `Execute` has no database dependency and cannot commit a Run.

## Reproducible SDK dependency

- Module: `trpc.group/trpc-go/trpc-agent-go` **v1.11.2**.
- Upstream tag commit: `5a0030b628a5bd93c8c5a30b1451f6fdd9d6740e`.
- Module sum: `h1:qBgrX48JXvgf+k6cq1BSWR4oPfFXxp8/boYydAs4PNM=`.
- No local `replace`, no unpublished checkout requirement.
- `go list -m -json trpc.group/trpc-go/trpc-agent-go` verifies the pin.

The public module source was re-read after downloading the exact version. Tests
exercise the real SDK Runner, LLMAgent, OpenAI-compatible model, HTTP stream,
Session callbacks and completion event; they do not substitute a fake SDK.

## Pinned generation / context behavior

| Manifest / Worker input | SDK / provider mapping |
| --- | --- |
| Single `llm` node ID and instruction | `llmagent.New`, `WithInstruction` |
| Fixed model/base URL | OpenAI-compatible model, explicitly `VariantOpenAI` |
| Explicit `generation.max_output_tokens` | `GenerationConfig.MaxTokens` |
| Omitted per-node limit | Published `execution.max_output_tokens` |
| Validated effective output limit | Public typed SDK request callback sets HTTP `max_completion_tokens` |
| Omitted temperature | No temperature property sent |
| Explicit temperature `0` | HTTP temperature `0`, not replaced by a default |
| Streaming | Final comes from last complete assistant message after stream close |
| Actual usage | Observation only, no pre-reservation or cumulative quota |

The existing node schema caps `generation.max_output_tokens` at 262144. The
separate published `execution.max_output_tokens` contract only requires a positive
integer; the node cap is not a Worker policy on that field. Full Manifest decoding
validates the node schema before this adapter. The adapter checks the effective
value against the published execution limit and the SDK native-int representation,
without a private ceiling. The unadapted SDK calls `ClampMaxTokensForModel` even
with tailoring disabled (`gpt-4o`: 18000 becomes 16384). Recording that default as
Worker behavior did not satisfy the no-silent-clipping contract. Worker now uses
the SDK's public `WithChatRequestCallback` after conversion to set the typed
`MaxCompletionTokens` to the already validated effective value. Model identity,
history, temperature and usage remain unchanged. No SDK fork, model-name alias,
custom serializer or HTTP payload rewrite is involved.

The callback is one synchronous assignment with attempt-local immutable input;
the SDK applies it on both channel and iterator paths, before streaming or
non-streaming requests. Worker currently exposes no model/request ExtraFields or
JSON override options that could subsequently replace this typed field. Recheck
this ordering if such capabilities are later introduced or the SDK is upgraded.
Real SDK/HTTP regressions cover known-model fallback, node override, a lower
explicit limit, concurrent attempts, and provider rejection without a lower-limit
retry. Direct unadapted-SDK characterization remains separate from Worker's target
behavior. This guarantees outgoing configuration, not provider acceptance or a
promise to generate the maximum token count.

Provider retries are explicitly **zero**. The application owns finite Attempt
retry/recovery policy. This adapter does not add a total-output-token cap or a
new model-call allowance. Token tailoring, history-window truncation, code
response execution, Memory and summaries are disabled. Same-branch assistant
roles are preserved. No tools, Skills, Knowledge or Memory services are wired.
Environment API keys, organization IDs and project IDs are explicitly overridden;
an absent declared API key does not inherit an ambient process credential.

## Attempt-local session and errors

- SDK identity is derived from trusted TenantID and internal SessionID.
- Snapshot version is `trpc-agent-go-session/v1` and includes the entire SDK
  session: history events, session state and timestamps.
- Accepted JSON is deep-decoded into a fresh attempt. Different attempts never
  share an SDK session pointer.
- Every Session Service mutation either updates the overlay or records a sticky
  error. App/user-wide state writes are rejected rather than leaking state
  across sessions. Summary job methods are explicit no-ops; no task is queued.
- The pinned Runner may log, rather than return, AppendEvent failures. The adapter
  tests sticky failures both on assistant events and runner-completion events.
  Done/channel-close alone is never a successful result. Final text is validated
  by the existing shared Reply codec before returning a Snapshot: the existing
  65,536-byte boundary and Unicode/NUL rules reject rather than truncate.
- Cancellation is not detached. Provider request cancellation and a bounded
  stream drain are tested. Attempt-owned HTTP transport idle connections close
  on all returns; Runner receives a borrowed overlay and no external services.
- Snapshot capacity and cancellation drain timeout are required explicit
  configuration. Capacity failure rejects the attempt; it never crops history.

## Immutable PostgreSQL candidate store

The sibling `sessionstore` package holds one complete candidate per
`(TenantID, SessionID, RunID, AttemptID)` in `runtime_session.session_candidates`.
Its ref is `sc1_<identity sha256>` and digest is `sha256:<content sha256>`; the
version, fixed identity, parent head and complete snapshot are all digest-bound.
Runtime is `session_runtime`, with SELECT/INSERT only. Replays compare persisted
content; a conflicting key never overwrites an older candidate. Reads require
the caller's accepted ref/digest, not a latest-row scan. Thus durable orphan
candidates remain invisible to subsequent accepted-head reads.

`sessionmigrations.Apply` is an explicit provisioning operation using
`session_migrator`. `Open`, `Load`, `Put` and `Execute` never run migrations. The
session DSN must match the immutable Manifest host/port/database/username/SSL
policy; schema is fixed to `runtime_session`. Credentials cannot add driver
options, change identity, or select another schema. A missing prepared store
returns `ErrPreparation`.

## Side-effect approval callback

A selected MCP resource with exact capability `test.ticket.status.update` is wrapped by the
pinned SDK BeforeTool/AfterTool callbacks. BeforeTool canonicalizes the closed ticket-status
arguments, persists the tenant/run/attempt/invocation/tool-call identity and blocks only that
active SDK invocation. A database CAS from APPROVED to EXECUTING is required before the MCP
call. AfterTool persists bounded JSON results; provider or persistence uncertainty becomes
UNKNOWN and is not mapped to the ordinary dependency retry. Cancellation expires an undecided
request. Startup reconciliation is scoped to the configured worker identity, and Scheduled
excludes UNKNOWN approval runs, so recovery cannot replay the whole Agent turn. Control owns
member/OWNER authorization; callback text or model output never grants approval.

## Gates

```sh
go test -race -count=1 ./services/agent-worker/internal/execution/adapter/outbound/trpcagent
```

The real PostgreSQL contract additionally requires the following explicit test
fixture settings (the last permits resetting only the disposable session tables):

- `WORKER_SESSION_TEST_ADMIN_URL`
- `WORKER_SESSION_TEST_MIGRATION_URL`
- `WORKER_SESSION_TEST_RUNTIME_URL`
- `WORKER_SESSION_TEST_ALLOW_RESET=1`

Run `TestSessionCandidatePostgresContract` separately from other tests using the
same session namespace. It tests eight concurrent migrations, twelve concurrent
candidate replays, same-key/parent conflicts, cross-scope rejection, corruption,
append-only runtime privileges, no implicit DDL and orphan invisibility. A skipped
environment-dependent test is not real database acceptance evidence.

## Execution bridge

The sibling `runtimeadapter` implements the application-owned `RuntimeFactory`
and `AttemptRuntime` ports. It uses one context-bound HTTPS credential resolution
request per Attempt and validates the entire batch before any store constructor:
Tenant, Profile ID/revision, Run, Attempt, Worker, Lease epoch, Manifest ID/digest,
exact credential-use set and positive credential revisions. Redirects are disabled.
The HTTP client is borrowed; bootstrap owns its mTLS certificate/trust configuration.

A bounded, explicitly configured Attempt-initialization map retains tombstones
until the fixed execution deadline. Concurrent calls and repeats after response
loss return `ErrAlreadyPrepared`; they do not resolve the original Attempt again.
The caller must recover through a fresh fenced Attempt. Invalid batches are stable
denials, while transport failures, 429 and 5xx are typed dependency errors. Error
mapping drops raw provider/driver text so credential values do not escape through
application errors. An absent prepared Session store has the distinct persisted
`SESSION_PREPARATION_REQUIRED` reason.

The bridge binds execution input to exactly the loaded accepted snapshot and binds
staging to exactly the successful SDK result. Stage writes a deterministic
candidate under a current Grant; an uncertain write is reconciled by reading that
same candidate key/digest, never by resolving credentials again. Completion stays
in Execution, outside both the SDK and the external store.
