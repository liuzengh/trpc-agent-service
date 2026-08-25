CREATE TABLE IF NOT EXISTS tenants (
  id text PRIMARY KEY,
  name text NOT NULL,
  enabled boolean NOT NULL,
  profile jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_apps (
  tenant_id text NOT NULL REFERENCES tenants(id),
  id text NOT NULL,
  name text NOT NULL,
  published_version text,
	revision bigint NOT NULL DEFAULT 0,
	updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

ALTER TABLE agent_apps ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 0;
ALTER TABLE agent_apps ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS agent_versions (
  tenant_id text NOT NULL,
  agent_id text NOT NULL,
  version text NOT NULL,
  profile jsonb NOT NULL,
  status text NOT NULL DEFAULT 'draft',
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id, version)
);

CREATE TABLE IF NOT EXISTS channel_bindings (
  tenant_id text NOT NULL REFERENCES tenants(id),
  id text NOT NULL,
  channel text NOT NULL,
  credential_ref text NOT NULL,
  enabled boolean NOT NULL DEFAULT false,
  profile jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS backend_profiles (
  tenant_id text NOT NULL REFERENCES tenants(id),
  id text NOT NULL,
  profile jsonb NOT NULL,
  PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS sessions (
  tenant_id text NOT NULL,
  session_id text NOT NULL,
  version bigint NOT NULL DEFAULT 0,
  state jsonb NOT NULL DEFAULT '{}',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE IF NOT EXISTS session_events (
  tenant_id text NOT NULL,
  session_id text NOT NULL,
  sequence_no bigint NOT NULL,
  event jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS memories (
  tenant_id text NOT NULL,
  id text NOT NULL,
  session_id text,
  value jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS summaries (
  tenant_id text NOT NULL,
  session_id text NOT NULL,
  version bigint NOT NULL,
  summary text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id, version)
);

CREATE TABLE IF NOT EXISTS inbound_messages (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  binding_id text NOT NULL,
  channel text NOT NULL,
  external_message_id text NOT NULL,
  payload jsonb NOT NULL,
  received_at timestamptz NOT NULL,
  UNIQUE (tenant_id, binding_id, channel, external_message_id)
);

CREATE TABLE IF NOT EXISTS dispatch_outbox (
  id text PRIMARY KEY,
  payload jsonb NOT NULL,
  status text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reply_outbox (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  binding_id text NOT NULL,
  channel text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id bigserial PRIMARY KEY,
  tenant_id text NOT NULL,
  channel text NOT NULL,
  user_id text NOT NULL,
  session_id text NOT NULL,
  agent_name text NOT NULL,
  tool_name text,
  decision text NOT NULL,
  latency_ms bigint NOT NULL,
  error_type text,
  cost double precision NOT NULL DEFAULT 0,
  trace_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dispatch_outbox_ready ON dispatch_outbox(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS reply_outbox_ready ON reply_outbox(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS audit_tenant_time ON audit_logs(tenant_id, created_at DESC);
