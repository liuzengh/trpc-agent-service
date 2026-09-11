# AgentSpec Schema v1

This directory owns the immutable public AgentSpec V1 protocol:

- [`agent-spec.schema.json`](agent-spec.schema.json) is the Draft 2020-12 JSON
  Schema.
- `embed.go` exposes the same schema to Go verification without copying it.
- `examples/valid/` and `examples/invalid/` are executable compatibility
  fixtures shared by schema and domain-validator tests.

The accepted ownership, publication boundary, validation levels, diagnostic
codes, and canonicalization rules are documented in the
[Agent subdomain design](../../../../docs/architecture-next/control-api/agent.md)
and
[AgentSpec V1 design](../../../../docs/architecture-next/control-api/agent-spec.md).

V1 accepts only `llm`, `sequence`, `parallel`, and bounded `loop` nodes. It does
not expose credentials, concrete Runtime Profile bindings, editor state, Graph
expressions, or tRPC-Agent-Go runtime options.

## Runtime data declarations (P0a source contract)

These are optional AgentSpec V1 source fields. This batch does not enable Worker
execution; Deployment rejects their presence with `DEPLOYMENT_ENTRYPOINT_UNSUPPORTED`
until matching Manifest compilation is delivered. No source field is silently lost.
Existing documents without these fields retain exactly their canonical bytes and digest.
Explicit disabled objects remain explicit in canonical source; they are not rewritten
into absent objects. Resource closure treats no-behavior Memory as disabled.

| Field | Source contract |
| --- | --- |
| `nodes.<llm>.memory.tools` | Required when `memory` is present; unique array drawn from `memory_add`, `memory_update`, `memory_delete`, `memory_clear`, `memory_search`, `memory_load`; empty allowed |
| `nodes.<llm>.memory.preload_limit` | Optional safe integer: `-1` loads all; `0` disables preload; positive value is the SDK adaptive entry count, not a token budget; omission means no preload |
| `nodes.<llm>.artifact.enabled` | Required boolean when `artifact` is present; enables SDK service availability only, never injects file tools |
| `runtime.summary.enabled` | Required boolean when `summary` is present; omission of runtime/summary disables generation |
| `runtime.summary.model_slot` | Required only when enabled; must reference a declared chat-capable model requirement, counted as a used model |
| `runtime.summary.event_threshold` | Required only when enabled; positive safe integer, explicit event-count trigger; no invented implicit budget |
| `nodes.<llm>.add_session_summary` | Optional boolean; only true consumes summaries, requiring enabled session-wide summary generation |

Disabled summary forbids model/threshold fields. Null, unknown fields, duplicate tool
names, unknown tools, preload below -1 and unsafe integers are rejected. Data fields
on sequence/parallel/loop nodes are rejected. No new Storage Slot is introduced.
Memory is used only if its tools array is nonempty or preload is nonzero (-1 included).
An empty tools array with absent/zero preload has no behavior and needs no Memory resource.

Subject is not supplied by AgentSpec or Profile. Worker derives it from authenticated
SocialIdentity (Tenant/Provider/Account/Sender), combines stable `sources.agent.agent_id`,
and isolates Memory across Agents/Bots while reusing the logical scope across Sessions
and Agent revisions. This source-only batch does not implement that runtime mapping.

Worker owns Summary SDK mapping, Artifact file-tool references, runtime gates and
Memory staged-write/accepted-application semantics. Control owns this source contract.
Summary storage follows Session; failed/unaccepted Attempts do not write formal Memory.
Profile and Manifest changes, including precise resource closure, are the next P0b/P1
batches. This batch has no new APIs, Worker adapters, SDK dependencies or Web changes.
