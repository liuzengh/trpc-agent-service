CREATE TABLE runtime_memory.memory_heads (
 tenant_id text NOT NULL,
 scope_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision >= 0),
 content bytea NOT NULL,
 digest text NOT NULL,
 PRIMARY KEY(tenant_id,scope_id)
);
CREATE TABLE runtime_memory.memory_receipts (
 tenant_id text NOT NULL,
 completion_id text NOT NULL,
 run_id text NOT NULL,
 attempt_id text NOT NULL,
 scope_id text NOT NULL,
 candidate_digest text NOT NULL,
 applied_revision bigint NOT NULL CHECK(applied_revision > 0),
 content bytea NOT NULL,
 UNIQUE(tenant_id,run_id,attempt_id),
 PRIMARY KEY(tenant_id,completion_id)
);
REVOKE ALL ON runtime_memory.memory_heads,runtime_memory.memory_receipts FROM PUBLIC,memory_runtime;
GRANT SELECT,INSERT,UPDATE ON runtime_memory.memory_heads TO memory_runtime;
GRANT SELECT,INSERT ON runtime_memory.memory_receipts TO memory_runtime;
