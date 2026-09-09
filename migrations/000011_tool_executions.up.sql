CREATE TABLE IF NOT EXISTS tool_executions (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  arguments_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL,
  result_hash TEXT,
  error_type TEXT,
  trace_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, request_id, tool_call_id),
  UNIQUE (tenant_id, idempotency_key),
  CHECK (status IN ('running','completed','failed','outcome_unknown'))
);
CREATE INDEX IF NOT EXISTS idx_tool_executions_status ON tool_executions (tenant_id, status, updated_at);
