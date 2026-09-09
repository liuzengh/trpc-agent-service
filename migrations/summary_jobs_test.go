package migrations

import (
	"strings"
	"testing"
)

func TestSummaryJobsMigrationPreservesDedupLeaseAndCASFields(t *testing.T) {
	up, err := files.ReadFile("023_summary_jobs.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS summary_jobs",
		"UNIQUE (tenant_id, agent_app_id, session_id, filter_key)",
		"status IN ('PENDING','PROCESSING','COMPLETED','FAILED')",
		"lease_version BIGINT",
		"max_attempts INTEGER",
		"next_attempt_at TIMESTAMPTZ",
		"CREATE INDEX IF NOT EXISTS idx_summary_jobs_claimable",
		"CREATE INDEX IF NOT EXISTS idx_summary_jobs_expired_processing",
		"CREATE TABLE IF NOT EXISTS summary_checkpoints",
		"max_event_sequence BIGINT",
		"content_sha256 CHAR(64)",
		"PRIMARY KEY (tenant_id, agent_app_id, session_id, filter_key)",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("summary migration is missing %q", required)
		}
	}
	if strings.Contains(text, "summary_jobs_last_error_length") ||
		strings.Contains(text, "summary_jobs_completed_sequence_consistency") {
		t.Error("023 migration was modified with post-release invariant constraints; use 024 instead")
	}
	down, err := files.ReadFile("023_summary_jobs.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	downText := string(down)
	if !strings.Contains(downText, "DROP TABLE IF EXISTS summary_checkpoints") ||
		!strings.Contains(downText, "DROP TABLE IF EXISTS summary_jobs") {
		t.Error("summary rollback does not remove both tables")
	}
	if strings.Index(downText, "summary_checkpoints") > strings.Index(downText, "summary_jobs") {
		t.Error("summary rollback drops the parent coordination table before its checkpoint table")
	}
}

func TestSummaryJobInvariantMigrationIsAdditive(t *testing.T) {
	up, err := files.ReadFile("024_summary_job_invariants.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"ALTER TABLE summary_jobs",
		"summary_jobs_last_error_length",
		"summary_jobs_completed_sequence_consistency",
		"octet_length(last_error) <= 4096",
		"completed_event_sequence >= target_event_sequence",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("invariant migration is missing %q", required)
		}
	}
	down, err := files.ReadFile("024_summary_job_invariants.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(down), "DROP CONSTRAINT IF EXISTS summary_jobs_completed_sequence_consistency") ||
		!strings.Contains(string(down), "DROP CONSTRAINT IF EXISTS summary_jobs_last_error_length") {
		t.Error("invariant rollback does not remove both constraints")
	}
}
