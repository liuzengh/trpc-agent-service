# Stage 3 Handoff

Status: ready

Stage 4 may consume the following frozen boundaries:

- `ChannelAdapter` converts an inbound `ChannelCallback` into a `ChannelMessage` and sends a `ChannelReply` as a tenant-scoped `ChannelDelivery`.
- `ChannelBinding` identifies the Tenant, Agent App, provider channel, conversation type/ID, external user, and Session. Bindings are selected only by trusted server-side Tenant Context.
- `ChannelCredential` is server-owned. Signature verification is represented by `ChannelSignatureVerifier`; request bodies and browser storage cannot provide Tenant authority or credentials.
- Provider message IDs drive duplicate suppression, provider sequence drives out-of-order rejection, and Session Events retain monotonic Tenant/Session ordering.
- Stable delivery outcomes are `delivered`, `failed`, timeout, retry exhaustion, rate limit, message length, attachment rejection, signature invalid, and unavailable. Public errors expose stable codes without secrets or provider diagnostics.
- Mock fault scenarios remain `none`, `timeout`, `retry`, `rate_limit`, `message_length`, and `attachment`, with Session-scoped configuration and Tenant fallback.
- Chat Session identity is Tenant + Session ID. Session IDs use `[a-z][a-z0-9-]{2,62}`; missing browser Session IDs may be server generated.
- The chat API boundary is Session creation, history, message send, cancel, and SSE stream. Mutations require operator authorization and all resources are selected through trusted Tenant Context.
- Logical execution identity is Tenant + Session ID + `request_id`. Replaying the same input resumes the original run; changing input under the same ID conflicts. Terminal states are completed, failed, and cancelled.
- `request_id` is 1–128 printable ASCII characters, originates in the browser or provider callback, and must propagate through Gateway, Worker, Runner, Session Events, and SSE.
- The SSE envelope is `event_id`, `request_id`, `session_id`, monotonic `sequence`, `type`, and `data`. The minimum event types are `run.started`, `message.delta`, `message.completed`, `run.failed`, `run.cancelled`, and `run.completed`; channel events may extend this set.
- SSE clients deduplicate by `event_id`, resume from sequence or `Last-Event-ID`, and recover from persisted Session Events. The runtime-facing event model must remain hidden behind this stable envelope.
- The Chat workspace stores only the most recently opened Session ID in browser storage. Backend Session Events are the sole conversation history.

Stage 4 must implement Enterprise WeChat and Telegram behind these contracts without weakening Stage 2 storage guarantees or Stage 3 Session/API/SSE boundaries. Provider configuration, write-only credential replacement, public webhook authentication, replay fixtures, and credential smoke status are Stage 4 additions.
