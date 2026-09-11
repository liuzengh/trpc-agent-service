# Scoped SDK Knowledge on fixed Qdrant REST

The root SDK v1.11.2 owns text parsing, default FixedSizeChunking, synchronous
BuiltinKnowledge.AddSource, OpenAI-compatible embedding, and
BuiltinKnowledge.Search/retrieval/reranking. This package implements only fixed
binding, the Qdrant named-vector REST storage boundary and failure handling.
It adds no SDK upgrade, RAG algorithm, background ingestion or shared platform.

Open accepts a fixed validated backend Snapshot, EmbedderConfig, resolved Qdrant
API key and Scope (TenantID/ProfileID/ResourceID). It checks that the existing
collection's named vector matches fixed dimensions and distance. It does not
create/recreate collections or infer a gRPC port from the HTTP endpoint.
Credential resolution/authorization belongs to the caller.

Scope fingerprint includes tenant, profile, resource, full backend digest and
embedding Model/BaseURL/Dimensions. Changing a model at the same dimension does
not expose old vectors; rotating API keys leaves scope unchanged. Every upsert
uses deterministic scoped UUID IDs. Every search applies the required scope
filter server-side and validates returned payload scope again.

ImportText rejects empty/oversized text and uses the SDK text reader's default
chunking. The importer explicitly disables source sync/recreate and uses one
source/document execution lane, returning only after completion. Progress errors
are also checked. Identical name+body imports reuse point IDs. A changed body
creates distinct points; this is additive ingestion, not document replacement.
A multi-chunk failure may leave prior chunks visible and always returns an error.
No atomic batch, garbage collector or rollback protocol is claimed.

Search is dense-only: SDK default mode 0 and explicit vector mode are accepted.
Keyword/hybrid-specific features, nonempty custom SearchFilter, collection management and
wider VectorStore mutation methods are explicitly unsupported. No unsupported
filter is silently discarded. The SDK tool default non-nil but semantically empty
SearchFilter is normalized to nil without modifying the caller request. SDK UserID/SessionID do not override fixed scope.
An omitted/nonpositive result count uses the official SDK Qdrant default of 10;
explicit positive counts are forwarded unchanged rather than capped.
No matches return ErrNoResults rather than a backend-failure classification.

The SDK embedder receives the exact fixed model/base URL/dimensions, zero SDK and
HTTP retries, and the bounded operation context. Empty, wrong-dimension and
non-finite vectors are rejected. Non-2xx embedding response diagnostics are
redacted before the SDK can log provider error text. Successful responses are
not rewritten. The transport rejects redirects, disables ambient proxies and
verifies TLS normally. Concurrency, timeout and input byte budget use the fixed
backend limits. Close closes only owned idle connections; inflight operations
remain bounded by their caller context.

test-integration.sh runs an actual independent Qdrant container and the real SDK
HTTP embedding adapter against a deterministic provider fixture. It proves
named-vector persistence, retrieval, scope/model/credential boundaries, repeated
imports and explicit partial-failure behavior. It does NOT prove real external
embedding quality/provider access or Control/IM publication. Those require the
separate joint/live acceptance.
