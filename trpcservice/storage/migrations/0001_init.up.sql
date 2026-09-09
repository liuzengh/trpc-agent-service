CREATE TABLE IF NOT EXISTS tenant (
  tenant_id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  quota_json JSON NOT NULL,
  policy_json JSON NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_app (
  app_id VARCHAR(64) NOT NULL,
  version INT NOT NULL,
  tenant_id VARCHAR(64) NOT NULL,
  app_name VARCHAR(128) NOT NULL,
  model_cfg_json JSON NOT NULL,
  tools_allowlist_json JSON NOT NULL,
  backends_json JSON NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (app_id, version),
  UNIQUE KEY uk_app_name_ver (app_name, version),
  KEY idx_agent_app_tenant (tenant_id)
);

CREATE TABLE IF NOT EXISTS channel_binding (
  binding_id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  app_id VARCHAR(64) NOT NULL,
  channel VARCHAR(16) NOT NULL,
  route_key VARCHAR(128) NOT NULL,
  config_json JSON NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_channel_route (channel, route_key),
  KEY idx_channel_binding_tenant_app (tenant_id, app_id)
);

CREATE TABLE IF NOT EXISTS session (
  session_id VARCHAR(255) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  app_id VARCHAR(64) NOT NULL,
  channel VARCHAR(16) NOT NULL,
  user_ref VARCHAR(128) NOT NULL,
  state_json JSON NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_session_tenant_app (tenant_id, app_id)
);

CREATE TABLE IF NOT EXISTS message_event (
  event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  session_id VARCHAR(255) NOT NULL,
  tenant_id VARCHAR(64) NOT NULL,
  role VARCHAR(16) NOT NULL,
  type VARCHAR(32) NOT NULL,
  payload_json JSON NOT NULL,
  msg_id VARCHAR(128) NULL,
  channel VARCHAR(16) NOT NULL,
  consumed BOOLEAN NOT NULL DEFAULT FALSE,
  ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_message_event_dedup (tenant_id, channel, msg_id),
  KEY idx_message_event_session (session_id, event_id)
);

CREATE TABLE IF NOT EXISTS memory (
  memory_id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL,
  session_id VARCHAR(255) NULL,
  source VARCHAR(16) NOT NULL,
  content TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_memory_tenant (tenant_id)
);

CREATE TABLE IF NOT EXISTS summary (
  summary_id VARCHAR(64) PRIMARY KEY,
  session_id VARCHAR(255) NOT NULL,
  tenant_id VARCHAR(64) NOT NULL,
  content TEXT NOT NULL,
  version INT NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_summary_session_version (session_id, version)
);

CREATE TABLE IF NOT EXISTS audit_log (
  log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id VARCHAR(64),
  channel VARCHAR(16),
  user_id VARCHAR(128),
  session_id VARCHAR(255),
  agent_name VARCHAR(128),
  tool_name VARCHAR(128),
  decision VARCHAR(32),
  latency_ms INT,
  error_type VARCHAR(64),
  cost DECIMAL(12,6),
  trace_id VARCHAR(64),
  request_id VARCHAR(64),
  ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_audit_log_tenant_ts (tenant_id, ts)
);

CREATE TABLE IF NOT EXISTS migration (
  migration_id VARCHAR(64) PRIMARY KEY,
  app_id VARCHAR(64) NOT NULL,
  from_backend VARCHAR(16) NOT NULL,
  to_backend VARCHAR(16) NOT NULL,
  phase VARCHAR(24) NOT NULL,
  detail_json JSON NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_migration_app (app_id)
);
