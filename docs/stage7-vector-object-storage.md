# Vector and Object Storage Design

Knowledge and embedding data should use Qdrant or Milvus behind a tenant-aware
profile. Each tenant receives an isolated collection namespace and credential
reference. Artifact data should use S3 or MinIO with bucket/prefix isolation,
server-side encryption and credentials resolved at runtime. Neither backend is
implemented in Phase 7.

Writes are idempotent by tenant, document ID and content digest. Reads and
indexing failures are isolated to the affected tenant and reported as degraded
without blocking unrelated Redis or SQL tenants. Migrations run as a shadow
collection/bucket copy with count, digest and sample verification before a
profile switch. Rollback points the profile back to the prior version; garbage
collection is delayed until the retention window expires. Production secrets
use Secret references, never catalog literals.
