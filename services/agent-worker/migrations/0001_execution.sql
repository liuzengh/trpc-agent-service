CREATE TABLE execution_sessions (
 tenant_id text NOT NULL,
 session_id text NOT NULL,
 scope_json jsonb NOT NULL,
 next_sequence bigint NOT NULL DEFAULT 0 CHECK(next_sequence >= 0),
 settled_sequence bigint NOT NULL DEFAULT 0 CHECK(settled_sequence >= 0 AND settled_sequence <= next_sequence),
 accepted_ref text NOT NULL DEFAULT '',
 accepted_digest text NOT NULL DEFAULT '',
 PRIMARY KEY(tenant_id,session_id),
 CHECK((accepted_ref='' AND accepted_digest='') OR (accepted_ref<>'' AND accepted_digest ~ '^sha256:[0-9a-f]{64}$'))
);
CREATE TABLE execution_runs (
 tenant_id text NOT NULL,
 run_id text NOT NULL UNIQUE,
 admission_id text NOT NULL UNIQUE,
 request_digest text NOT NULL,
 request_json jsonb NOT NULL,
 session_id text NOT NULL,
 session_sequence bigint NOT NULL CHECK(session_sequence>0),
 status text NOT NULL CHECK(status IN ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED')),
 wait_reason text NOT NULL DEFAULT '',
 policy_json jsonb NOT NULL,
 accepted_at timestamptz NOT NULL,
 run_deadline timestamptz NOT NULL,
 reply_deadline timestamptz NOT NULL,
 execution_deadline timestamptz,
 retry_at timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0),
 lease_epoch bigint NOT NULL DEFAULT 0 CHECK(lease_epoch>=0),
 current_attempt_id text NOT NULL DEFAULT '',
 PRIMARY KEY(tenant_id,run_id),
 FOREIGN KEY(tenant_id,session_id) REFERENCES execution_sessions(tenant_id,session_id),
 UNIQUE(tenant_id,session_id,session_sequence)
);
CREATE INDEX execution_runs_ready ON execution_runs(status,accepted_at) WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT');
CREATE TABLE execution_receipts (
 event_id text PRIMARY KEY,
 event_digest text NOT NULL,
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 outcome text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id)
);
CREATE TABLE execution_rejections (
 rejection_id text PRIMARY KEY,
 source_identity text NOT NULL,
 digest text NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE execution_attempts (
 tenant_id text NOT NULL,
 attempt_id text NOT NULL,
 run_id text NOT NULL,
 worker_id text NOT NULL,
 generation bigint NOT NULL CHECK(generation>0),
 lease_epoch bigint NOT NULL CHECK(lease_epoch>0),
 token_hash text NOT NULL UNIQUE,
 lease_until timestamptz NOT NULL,
 status text NOT NULL CHECK(status IN ('PREPARING','EXECUTING','SUCCEEDED','FAILED','ABORTED')),
 parent_ref text NOT NULL,
 parent_digest text NOT NULL,
 created_at timestamptz NOT NULL,
 agent_started_at timestamptz,
 ended_at timestamptz,
 reason text NOT NULL DEFAULT '',
 PRIMARY KEY(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id),
 UNIQUE(tenant_id,run_id,generation),
 UNIQUE(tenant_id,run_id,lease_epoch)
);
CREATE INDEX execution_attempts_live ON execution_attempts(lease_until) WHERE status IN ('PREPARING','EXECUTING');
CREATE TABLE execution_completions (
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 completion_id text NOT NULL UNIQUE,
 attempt_id text,
 kind text NOT NULL CHECK(kind IN ('ATTEMPT','SYSTEM_TERMINATION')),
 status text NOT NULL CHECK(status IN ('SUCCEEDED','FAILED')),
 candidate_ref text NOT NULL DEFAULT '',
 candidate_digest text NOT NULL DEFAULT '',
 final_intent_id text NOT NULL DEFAULT '',
 reply_disposition text NOT NULL,
 reason text NOT NULL DEFAULT '',
 result_digest text NOT NULL,
 completed_at timestamptz NOT NULL,
 PRIMARY KEY(tenant_id,run_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES execution_attempts(tenant_id,attempt_id),
 CHECK((status='SUCCEEDED' AND candidate_ref<>'' AND candidate_digest ~ '^sha256:[0-9a-f]{64}$') OR (status='FAILED' AND candidate_ref='' AND candidate_digest='')),
 CHECK(kind<>'SYSTEM_TERMINATION' OR (status='FAILED' AND final_intent_id='' AND reply_disposition='NONE'))
);
CREATE TABLE execution_session_commits (
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 session_id text NOT NULL,
 candidate_ref text NOT NULL,
 candidate_digest text NOT NULL,
 PRIMARY KEY(tenant_id,run_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_completions(tenant_id,run_id),
 FOREIGN KEY(tenant_id,session_id) REFERENCES execution_sessions(tenant_id,session_id)
);
CREATE TABLE execution_reply_outbox (
 intent_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 run_id text NOT NULL UNIQUE,
 digest text NOT NULL,
 payload bytea NOT NULL,
 created_at timestamptz NOT NULL,
 published_at timestamptz,
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_completions(tenant_id,run_id)
);
