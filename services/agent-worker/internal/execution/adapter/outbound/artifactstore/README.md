# SDK Artifact: fixed S3 content + Worker PostgreSQL metadata

This package implements the root SDK v1.11.2 `artifact.Service`. It does not
implement artifact tools, change Runner identity, or open a production gate.

## Assembly

`Open(ctx, workerPool, backendSnapshot, Credentials{AccessKeyID, SecretAccessKey},
tenantID, trustedSDKSessionInfo)` borrows the existing Worker PostgreSQL pool.
The caller resolves both credential uses against the fixed backend digest and
supplies the trusted SDK AppName/UserID/SessionID. Every SDK method rejects other
session information. Close closes owned idle HTTP connections, never the pool;
inflight calls retain their bounded caller context.

Worker migration `0020_artifact_metadata.sql` implements
`worker-artifact-metadata-v1` in the existing Worker database. Startup owns
migration and grants; Open neither runs DDL nor creates buckets. The S3 bucket
must already exist. Scope hashes include tenant, full backend snapshot digest,
AppName, UserID and SessionID; filenames are separately hashed for keys.
A changed backend snapshot is a separate namespace, not an automatic migration.

The MinIO Go client performs standard S3 signing. Endpoint, bucket, region,
path-style choice, timeout, maximum concurrent operations and byte limit come
from the validated snapshot. Static keys remain internal. TLS verifies the
actual request hostname (including virtual-host-style bucket names); redirects
are not followed by the client. The transport disables ambient HTTP proxies.
MaxRetries=1 means one SDK request attempt; no retry mechanism is added here.
Multipart upload uses one thread and avoids a new fixed single-object-size cap.

## Durability and deletion

Save clones input bytes, holds a PostgreSQL file row lock, allocates the next
version (initially 0), uploads under a unique object key, then inserts metadata
and advances the sequence in one PostgreSQL transaction. A failure returns no
success. A failed/uncertain PostgreSQL commit can leave an S3 orphan; no
cross-store transaction, garbage collection queue, or recovery platform exists.
A caller retry after an uncertain successful commit is a new version: this SDK
API has no request idempotency key. Concurrent successful versions are serialized.

Load(nil) selects the highest visible version. Missing metadata returns nil as
the SDK specifies. Missing/unavailable S3 bytes for existing metadata are an
error, not an empty artifact. Each load reads at most declared length plus one
and validates length and SHA-256 before exposing bytes. Name, MIME and URL are
metadata; URL is not dereferenced or transformed into a public S3 URL.

Delete is **logical deletion of all visible versions in a PG transaction**.
It does not physically erase S3 bytes. The file sequence remains, so a later
Save gets the next version and old explicit versions remain absent. Deleting
an unknown filename succeeds. This is not a claim of physical erasure.

These are immediate SDK save semantics. The adapter does not claim Artifact
writes roll back with a later failed Runner attempt or depend on Session/Memory
acceptance. Any future accepted-only policy belongs above this adapter.

## Verification

Run `test-integration.sh` from this worktree. It creates separate ephemeral
PostgreSQL and MinIO containers, applies actual Worker migrations, uses a limited
metadata runtime role, and cleans up both containers. Only this package runs
with the race detector. Tests cover binary metadata round-trip, versions,
parallel writes, scoped/backend isolation, logical deletion, corrupted objects,
S3/PG failures, capacity, cancellation, timeout, TLS rejection and redacted errors.
This is store integration, not Control publication or IM/Runner acceptance.
