# Data-backend deployment helpers

`backendctl.py` uses Python 3.11+ standard libraries only (HTTP/TLS, SigV4 and
RESP2). No pip package, SDK upgrade, curl, or Docker socket is required. A pinned
Python Alpine tools image can mount this directory read-only; the Compose owner
selects its exact image tag/digest. These tools never restart services themselves.

## Fixed interfaces

All credential arguments are **file paths**, never secret values. Files contain
one value, with an optional final LF. No ambient HTTP proxy or HTTP redirect is
followed. TLS certificate/hostname verification remains enabled. Provider error
bodies and arbitrary exception text never appear in command output.

```sh
python3 /tools/backendctl.py redis-acl \
  --admin-password-file /run/secrets/redis_admin \
  --memory-password-file /run/secrets/redis_memory \
  --session-password-file /run/secrets/redis_session \
  --output /generated/users.acl
```

This creates/replaces one ACL file atomically, mode `0600`. It uses SHA-256
password rules, with `default off` and three distinct credentials:

- `deployment_admin`: administration and persistence probes.
- `memory_runtime`: `~runtime_memory:*`, existing Worker Memory command set.
- `session_runtime`: `~runtime_session:*`, existing Worker Session command set.

**Ownership matters:** if the tools job runs as root but Redis drops privileges,
pass `--owner-uid UID --owner-gid GID` matching the selected Redis image's actual
runtime account, or run the generation job as that account. Do not guess an image
UID or make the hashed credential file world-readable. Re-running updates the
file; a running Redis process still needs its explicit restart or `ACL LOAD`.

Compose must launch Redis with persistent `/data`, `--aclfile /generated/users.acl`,
`--appendonly yes --appendfsync always --maxmemory-policy noeviction`. The smoke
command independently checks these three persistence settings.

```sh
python3 /tools/backendctl.py minio-init \
  --endpoint http://minio:9000 --bucket worker-artifacts --region us-east-1 \
  --access-key-file /run/secrets/minio_user \
  --secret-key-file /run/secrets/minio_password

python3 /tools/backendctl.py qdrant-init \
  --endpoint http://qdrant:6333 --api-key-file /run/secrets/qdrant_api_key \
  --collection worker_knowledge --vector-name published_dense \
  --dimensions "$EXPLICIT_EMBEDDING_DIMENSIONS" --distance Cosine
```

MinIO initialization waits on authenticated S3 readiness, creates only a missing
bucket, and reads back its presence and disabled versioning. It never clears or
rewrites an existing bucket/versioning configuration. Conflict fails explicitly.

Qdrant creates only a missing collection and validates the selected named vector's
size and metric on existing collections; it never deletes/recreates a conflicting
collection. Omit `--dimensions` entirely when unconfigured: the command reports
`QDRANT_COLLECTION=SKIPPED`, with **no business collection creation and no claim of
working embedding/RAG semantics**. There is no default 1536-dimensional vector.

All network operations use `--timeout` (default 5 seconds). Readiness waits use
`--wait-seconds` (default 60). A final in-flight request can add at most its request
timeout to that wait. Initialization performs no blind retry of successful writes.

## Readiness without in-image curl

```sh
python3 /tools/backendctl.py health --service redis --host redis \
  --password-file /run/secrets/redis_admin
python3 /tools/backendctl.py health --service qdrant --endpoint http://qdrant:6333 \
  --api-key-file /run/secrets/qdrant_api_key
python3 /tools/backendctl.py health --service minio --endpoint http://minio:9000
```

Use external tools jobs after `service_started`; consumers can depend on their
`service_completed_successfully`. Do not label a service healthy based on an
unavailable curl binary. MinIO's health endpoint alone does not prove authenticated
S3 initialization; `minio-init` does the stronger check. Qdrant v1.15.4's official
Dockerfile does not install curl by default. Its normal command is
`/qdrant/entrypoint.sh` from `/qdrant`; preserve that entrypoint if adding a shell
wrapper to load `QDRANT__SERVICE__API_KEY` from a secret file. Do not assume a
Qdrant `_FILE` setting exists.

## Persistence smoke across an operator-controlled restart

Invoke `smoke --action write`, restart the **same service with the same named
volume**, invoke `smoke --action read`, then `smoke --action delete` with exactly
the same explicit `--probe-id`. The orchestration owner records restart outcomes.

```sh
python3 /tools/backendctl.py smoke --service redis --action write \
  --probe-id release-check --host redis --password-file /run/secrets/redis_admin
# After restart: repeat with --action read; after evidence: --action delete.
```

For MinIO use the same endpoint/bucket/region/key-file arguments as `minio-init`.
For Qdrant use endpoint/api-key-file arguments. Each operation prints only a fixed
PASS/FAIL marker, never credentials or payloads.

- Redis uses `deployment_smoke:ID` with no TTL, and requires exact marker bytes.
- S3 uses `_deployment_smoke/ID` and independently reads exact object bytes.
- Qdrant uses a dedicated `deployment_smoke_ID` collection with one explicitly
  one-dimensional `probe` vector. This is infrastructure durability testing,
  **not the application's embedding schema**. It verifies an exact payload;
  deletion requires that it is the only point. It never touches the configured
  business Knowledge collection.

Use a fresh unique probe ID per deployment check. Existing mismatching marker
content fails rather than being overwritten/deleted. The command is not a lease
system: concurrent operators must not reuse one probe ID.

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s deploy/compose/data-backends/tests -v
```

Unit tests validate initialization conflicts, idempotence, ACL namespace/hash,
readiness, redacted errors and probe collision handling. They do not substitute
for the Compose owner's real backend/restart smoke.

Primary references checked for these interfaces:
- Redis ACL and SHA-256 rules: https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/
- Redis AOF policies: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Qdrant collection API: https://api.qdrant.tech/api-reference/collections/create-collection
- Qdrant v1.15.4 image: https://github.com/qdrant/qdrant/blob/v1.15.4/Dockerfile

## Startup wrappers and dedicated Artifact account

Compose can select `entrypoint: ["/bin/sh", "/tools/redis-start.sh"]` (and the
corresponding MinIO/Qdrant scripts). Redis copies the mounted hash-only ACL into
`/data/users.acl`, owns it as `redis` when started as root, and then invokes the
image's original privilege-dropping entrypoint. It does not chown the host secret
or generated ACL mount. MinIO uses secret files `/run/secrets/minio_user` and
`/run/secrets/minio_password`; Qdrant uses `/run/secrets/qdrant_api_key`.
Startup wrappers never enable shell tracing.

After root-credential `minio-init` has completed, run:

```sh
python3 /tools/backendctl.py minio-account-init \
  --endpoint http://minio:9000 --bucket worker-artifacts \
  --access-key-file /run/secrets/minio_user \
  --secret-key-file /run/secrets/minio_password \
  --app-access-key-file /run/secrets/minio_app_user \
  --app-secret-key-file /run/secrets/minio_app_password
```

This command requires the official `mc` binary alongside Python. It uses a private
short-lived mc config directory, discards mc diagnostic output, disables inherited
proxy settings, and bounds each subprocess by `--timeout`. The admin protocol is
implemented by mc, not a new signing implementation. Root credentials are used
only in the initialization job; Profile credentials must be the app pair. This
job reconciles its deployment-owned app user/password and bucket-derived policy,
then authenticates as that user and inspects the bucket. The policy permits only
bucket location/list plus object get/put/delete under that bucket. Do not supply
an unrelated pre-existing user: the job does not remove other externally attached
policies. Existing bucket data is not changed by account initialization.

The exact shared secret filenames are `redis_admin`, `redis_memory`,
`redis_session`, `minio_user`, `minio_password`, `minio_app_user`,
`minio_app_password`, and `qdrant_api_key`.

Compose aggregate entries are `health-all.sh` (Redis, MinIO, Qdrant in order) and
`init-all.sh` (bucket, dedicated app account, optional business collection). They
use `/tooling/backendctl.py`. `MANAGED_S3_BUCKET` defaults to `agent-artifacts`,
`MANAGED_QDRANT_COLLECTION` to `agent-knowledge`, and `MANAGED_QDRANT_VECTOR` to
`embedding`. Empty `MANAGED_QDRANT_DIMENSIONS` omits the CLI argument, retaining
SKIPPED semantics. Redis startup's default ACL input is
`/run/redis-private/users.acl`, overridable by `REDIS_ACL_FILE`.

`MANAGED_QDRANT_DISTANCE` is forwarded explicitly (default `Cosine`); existing
collection metric mismatches fail instead of silently selecting Cosine.
