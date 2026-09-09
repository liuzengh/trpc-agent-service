CREATE TABLE IF NOT EXISTS tenants (
  tenant_id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE,
  current_config_version BIGINT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS config_versions (
  tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id), version BIGINT NOT NULL,
  config_yaml BYTEA NOT NULL, config_sha256 TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'published', rolled_back_from BIGINT,
  created_by TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (tenant_id, version)
);
CREATE TABLE IF NOT EXISTS agent_apps (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, name TEXT NOT NULL, enabled BOOLEAN NOT NULL,
  config_version BIGINT NOT NULL, PRIMARY KEY (tenant_id, app_id),
  FOREIGN KEY (tenant_id, config_version) REFERENCES config_versions(tenant_id, version)
);
CREATE TABLE IF NOT EXISTS channel_bindings (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, binding_id TEXT NOT NULL, channel_type TEXT NOT NULL,
  provider_account_id TEXT NOT NULL, enabled BOOLEAN NOT NULL, config_version BIGINT NOT NULL,
  PRIMARY KEY (tenant_id, binding_id), UNIQUE (tenant_id, channel_type, provider_account_id),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id),
  FOREIGN KEY (tenant_id, config_version) REFERENCES config_versions(tenant_id, version)
);
CREATE TABLE IF NOT EXISTS identity_mappings (
  tenant_id TEXT NOT NULL, binding_id TEXT NOT NULL, external_user_id TEXT NOT NULL, internal_user_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (tenant_id, binding_id, external_user_id),
  FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings(tenant_id, binding_id)
);
CREATE TABLE IF NOT EXISTS session_heads (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL,
  last_event_seq BIGINT NOT NULL DEFAULT 0, last_fence BIGINT NOT NULL DEFAULT 0, state_version BIGINT NOT NULL DEFAULT 0,
  state_json JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, session_id),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
);
CREATE TABLE IF NOT EXISTS message_events (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL,
  event_id TEXT NOT NULL, event_seq BIGINT NOT NULL, event_type TEXT NOT NULL, payload_json JSONB NOT NULL,
  state_delta_json JSONB, trace_id TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, session_id, event_seq), UNIQUE (tenant_id, event_id),
  FOREIGN KEY (tenant_id, app_id, user_id, session_id) REFERENCES session_heads(tenant_id, app_id, user_id, session_id)
);
CREATE TABLE IF NOT EXISTS session_summaries (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL,
  summary_version BIGINT NOT NULL, cutoff_event_seq BIGINT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, session_id, summary_version),
  UNIQUE (tenant_id, app_id, user_id, session_id, cutoff_event_seq),
  FOREIGN KEY (tenant_id, app_id, user_id, session_id) REFERENCES session_heads(tenant_id, app_id, user_id, session_id)
);
CREATE TABLE IF NOT EXISTS memory_entries (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, memory_id TEXT NOT NULL,
  source_session_id TEXT, source_event_id TEXT, source_event_seq BIGINT, version BIGINT NOT NULL, status TEXT NOT NULL,
  content TEXT NOT NULL, metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, memory_id, version),
  UNIQUE (tenant_id, app_id, user_id, source_event_id),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
);
CREATE TABLE IF NOT EXISTS inbox_messages (
  tenant_id TEXT NOT NULL, binding_id TEXT NOT NULL, external_message_id TEXT NOT NULL, app_id TEXT NOT NULL,
  user_id TEXT NOT NULL, session_id TEXT NOT NULL, status TEXT NOT NULL, attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ, claimed_at TIMESTAMPTZ, trace_id TEXT, last_error TEXT,
  payload_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, binding_id, external_message_id),
  FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings(tenant_id, binding_id)
);
CREATE TABLE IF NOT EXISTS outbox_messages (
  tenant_id TEXT NOT NULL, outbox_id TEXT NOT NULL, dedupe_key TEXT NOT NULL, binding_id TEXT NOT NULL,
  user_id TEXT NOT NULL, session_id TEXT NOT NULL, status TEXT NOT NULL, attempts INT NOT NULL DEFAULT 0,
  retry_at TIMESTAMPTZ, last_error TEXT, payload_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, outbox_id), UNIQUE (tenant_id, dedupe_key),
  FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings(tenant_id, binding_id)
);
CREATE TABLE IF NOT EXISTS audit_logs (
  tenant_id TEXT NOT NULL, audit_id TEXT NOT NULL, channel TEXT, user_id TEXT, session_id TEXT,
  agent_name TEXT, tool_name TEXT, decision TEXT NOT NULL, latency_ms BIGINT, error_type TEXT,
  cost NUMERIC(20,6), cost_micros BIGINT, trace_id TEXT, request_id TEXT, event_id TEXT, details_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (tenant_id, audit_id),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);
CREATE TABLE IF NOT EXISTS migration_jobs (
  tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, app_id TEXT, domain TEXT NOT NULL,
  source_backend TEXT NOT NULL, target_backend TEXT NOT NULL, status TEXT NOT NULL,
  cursor_json JSONB, checkpoint_json JSONB, lease_owner TEXT, lease_until TIMESTAMPTZ, last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, job_id),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_inbox_ready ON inbox_messages (tenant_id, status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_identity_internal_user ON identity_mappings (tenant_id, internal_user_id);
CREATE INDEX IF NOT EXISTS idx_outbox_ready ON outbox_messages (tenant_id, status, retry_at);
CREATE INDEX IF NOT EXISTS idx_audit_trace ON audit_logs (tenant_id, trace_id, created_at);
