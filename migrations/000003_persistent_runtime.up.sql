-- These tables mirror the public PostgreSQL service contracts in
-- trpc-agent-go/session/postgres and memory/postgres v1.11.0. Runtime
-- processes use WithSkipDBInit(true), so only the migration job needs DDL.
CREATE TABLE IF NOT EXISTS runtime_session_states (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  session_id VARCHAR(255) NOT NULL,
  state JSONB DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_session_states_unique_active
  ON runtime_session_states (app_name, user_id, session_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_session_states_expires
  ON runtime_session_states (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_session_events (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  session_id VARCHAR(255) NOT NULL,
  event JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_runtime_session_events_lookup
  ON runtime_session_events (app_name, user_id, session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_runtime_session_events_expires
  ON runtime_session_events (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_session_track_events (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  session_id VARCHAR(255) NOT NULL,
  track VARCHAR(255) NOT NULL,
  event JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_runtime_session_track_events_lookup
  ON runtime_session_track_events (app_name, user_id, session_id, track, created_at);
CREATE INDEX IF NOT EXISTS idx_runtime_session_track_events_expires
  ON runtime_session_track_events (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_session_summaries (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  session_id VARCHAR(255) NOT NULL,
  filter_key VARCHAR(255) NOT NULL DEFAULT '',
  summary JSONB DEFAULT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_session_summaries_unique_active
  ON runtime_session_summaries (app_name, user_id, session_id, filter_key) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_session_summaries_expires
  ON runtime_session_summaries (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_app_states (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  key VARCHAR(255) NOT NULL,
  value TEXT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_app_states_unique_active
  ON runtime_app_states (app_name, key) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_app_states_expires
  ON runtime_app_states (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_user_states (
  id BIGSERIAL PRIMARY KEY,
  app_name VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  key VARCHAR(255) NOT NULL,
  value TEXT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP DEFAULT NULL,
  deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_user_states_unique_active
  ON runtime_user_states (app_name, user_id, key) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_user_states_expires
  ON runtime_user_states (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_memories (
  memory_id TEXT PRIMARY KEY,
  app_name TEXT NOT NULL,
  user_id TEXT NOT NULL,
  memory_data JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TIMESTAMP NULL DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_runtime_memories_app_user
  ON runtime_memories (app_name, user_id);
CREATE INDEX IF NOT EXISTS idx_runtime_memories_updated_at
  ON runtime_memories (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_memories_deleted_at
  ON runtime_memories (deleted_at);

CREATE TABLE IF NOT EXISTS runtime_artifacts (
  tenant_id TEXT NOT NULL,
  app_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  revision INT NOT NULL,
  mime_type TEXT NOT NULL,
  artifact_url TEXT NOT NULL DEFAULT '',
  display_name TEXT NOT NULL DEFAULT '',
  data BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, session_id, filename, revision),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
);
CREATE INDEX IF NOT EXISTS idx_runtime_artifacts_session
  ON runtime_artifacts (tenant_id, app_id, user_id, session_id, filename);

CREATE TABLE IF NOT EXISTS runtime_knowledge_documents (
  tenant_id TEXT NOT NULL,
  app_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  content TEXT NOT NULL,
  metadata_json JSONB NOT NULL DEFAULT '{}'::JSONB,
  version BIGINT NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, document_id),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
);
