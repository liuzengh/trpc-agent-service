# Managed backend descriptor fixtures

These examples describe PostgreSQL, Redis, Qdrant and S3 connection targets;
they do not provision services, grant database privileges or enable Worker roles.
PostgreSQL and Redis entries may both offer Session and Memory on one physical
backend. Catalogue role authorization is checked before deriving a fixed snapshot.
Session uses `tenant-session-v1`; Memory uses `tenant-subject-agent-v1`. Consumers
must call `ValidateForRole` and actually enforce SQL/schema/account or Redis
namespace/ACL isolation. Merely changing a descriptor string grants no database
isolation. Separate scoped credentials may be required by the deployed backend.

The source target's isolation field documents its base shape; role-bound snapshots
are derived deterministically. Their authorization digests differ by tenant/role,
while physical target ID/revision stay fixed. Existing Session snapshots are byte
identical. A consumer must not compare whole role-bound snapshots to decide whether
two roles selected the same physical target.

Control now wires the directory, Profile eligibility Port, and Deployment resolver
from release-pinned catalog/target files. The compiler handles declared capabilities
under an explicit static contract; production runtime registration remains gated.
No backend is silently enabled, no runtime client is created by Profile publication,
and absent BackendAccess rejects managed selection rather than trusting syntax.


Configure `CONTROL_PLATFORM_BACKEND_CATALOG_FILE` and its SHA256 together with
`CONTROL_PLATFORM_BACKEND_TARGETS_FILE` and its SHA256. Keep private targets mode
600. Refresh the release-pinned expected Deployment contract digest using the
existing digest tooling when these hashes change. Startup rejects mismatches
before opening the database. Empty configuration yields an empty directory.
The authenticated `GET /v1/tenants/{tenant_id}/runtime-backends` endpoint returns
logical metadata only; available selections do not automatically enable execution.
