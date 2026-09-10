# Runtime Capabilities (Issue #75)

The runtime storage contract is intentionally platform-owned. Every operation
accepts an explicit tenant_id; IDs and namespace prefixes are not a security
boundary. PostgreSQL uses composite tenant keys and foreign keys, while the
in-memory implementation uses the same key shape and error semantics for local
development and conformance tests.

## Contract Surface

trpcservice/runtime/storage defines tenant-scoped contracts for Session,
Memory, Summary, Knowledge, Artifact, Audit, Vector, and Object storage. Each
record is defensively copied at the boundary. Writes use stable IDs and
idempotent upserts where a retry can be safely replayed. Versioned summaries
reject an older event sequence, and memory writes enqueue their durable record
for asynchronous vector indexing (the explicit EnqueueMemoryIndex operation is
also available for retries).

Session state and immutable event history are published through the neutral
`trpcservice/storage/session` contract. The `agent/sessionstore` adapter and
runtime storage implementations depend on that contract; runtime-specific
message/reply capabilities remain separate.

Backend profiles select session, memory, summary, knowledge, artifact, and
audit bindings. runtime/storage/factory.CapabilitySet exposes typed accessors
for those capabilities; providers are resolved only under the requested tenant
and are closed by the owning set. Vector and object adapters travel with their
source knowledge and artifact capabilities so a provider cannot accidentally
cross a tenant namespace.

## Commit Ordering

The durable execution barrier is:

~~~text
event/state -> durable memory -> reply outbox -> asynchronous summary/vector index
~~~

An index or summary failure does not roll back the source event or memory. A
retry uses the stable memory/document ID and version, so an older queued index
cannot overwrite a newer record. PostgreSQL readers observe committed rows from
any worker process; NewBackend in the in-memory adapter provides the equivalent
shared-state view for multi-node tests. Closing one NewWithBackend view does not
stop the shared index worker while another view remains active; the backend
owner and all views must be closed before the worker exits.

## PostgreSQL Tables

Migration 0012_runtime_capabilities.up.sql adds tenant-keyed Memory, Summary,
Knowledge, Artifact, Audit, Vector Index, and Object rows. Every table has a
composite primary key beginning with tenant_id, explicit tenant foreign keys,
bounded fields, JSON shape checks, and grants limited to the runtime role.

The vector index is an eventually consistent projection. Its composite key is
tenant_id, source, document_id, so memory and knowledge records may safely use
the same document identifier. Knowledge documents remain the source of truth;
migration or reindexing can rebuild the projection by source, document ID, and
digest without changing tenant authorization. Object content is returned
through an io.ReadCloser and is never mixed into control-plane configuration or
execution snapshots.
