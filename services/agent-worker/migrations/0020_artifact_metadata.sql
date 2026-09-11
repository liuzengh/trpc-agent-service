-- worker-artifact-metadata-v1: bytes live in the fixed S3 backend; this is the
-- existing Worker database, not another deployed database.
CREATE TABLE worker_artifact_files (
 file_id text PRIMARY KEY,
 scope_id text NOT NULL,
 filename text NOT NULL,
 next_version bigint NOT NULL DEFAULT 0 CHECK(next_version >= 0)
);
CREATE INDEX worker_artifact_scope ON worker_artifact_files(scope_id, filename);
CREATE TABLE worker_artifact_versions (
 file_id text NOT NULL REFERENCES worker_artifact_files(file_id),
 version bigint NOT NULL CHECK(version >= 0),
 object_key text NOT NULL,
 content_sha256 text NOT NULL,
 content_length bigint NOT NULL CHECK(content_length >= 0),
 mime_type text NOT NULL,
 display_name text NOT NULL,
 artifact_url text NOT NULL,
 PRIMARY KEY(file_id,version)
);
