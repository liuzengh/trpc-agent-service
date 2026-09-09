CREATE INDEX IF NOT EXISTS idx_inbox_recovery_ready
  ON inbox_messages (status, next_attempt_at, lease_until, created_at)
  WHERE status IN ('processing', 'retry');
