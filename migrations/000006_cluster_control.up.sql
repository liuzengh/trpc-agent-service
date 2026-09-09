CREATE TABLE IF NOT EXISTS run_statuses (
  tenant_id TEXT NOT NULL,
  binding_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  trace_id TEXT,
  status TEXT NOT NULL,
  reply TEXT,
  error TEXT,
  worker_id TEXT,
  cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, request_id),
  FOREIGN KEY (tenant_id, request_id) REFERENCES inbox_messages(tenant_id, inbox_id)
);
CREATE INDEX IF NOT EXISTS idx_run_statuses_binding_updated
  ON run_statuses (tenant_id, binding_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS worker_nodes (
  node_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL,
  draining BOOLEAN NOT NULL DEFAULT FALSE,
  stopped_at TIMESTAMPTZ,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_worker_nodes_live
  ON worker_nodes (last_heartbeat) WHERE NOT draining;

CREATE TABLE IF NOT EXISTS policy_budget_usage (
  tenant_id TEXT NOT NULL,
  period TEXT NOT NULL,
  used_micros BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, period),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);

CREATE TABLE IF NOT EXISTS policy_budget_reservations (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  period TEXT NOT NULL,
  reserved_micros BIGINT NOT NULL,
  actual_micros BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, request_id),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);

CREATE TABLE IF NOT EXISTS tool_approvals (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, request_id, tool_name),
  FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);
