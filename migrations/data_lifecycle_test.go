package migrations

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDataLifecycleDDLAndChecksum(t *testing.T) {
	ddl, err := DataLifecycleDDL()
	if err != nil {
		t.Fatalf("DataLifecycleDDL() error = %v", err)
	}
	if !strings.Contains(ddl, "session_turn_summaries") ||
		!strings.Contains(ddl, "tenant_id text NOT NULL DEFAULT ''") ||
		!strings.Contains(ddl, "memory_visibility_watermarks") ||
		!strings.Contains(ddl, "audit_records") ||
		!strings.Contains(ddl, "data_migration_reconciliations") {
		t.Fatalf("data lifecycle DDL is missing one or more required tables")
	}
	if len(DataLifecycleChecksum()) != 64 {
		t.Fatalf("DataLifecycleChecksum() length = %d, want 64", len(DataLifecycleChecksum()))
	}
}

func TestVerifyDataLifecycle(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: DataLifecycleChecksum()}}
	if err := VerifyDataLifecycle(context.Background(), q); err != nil {
		t.Fatalf("VerifyDataLifecycle() error = %v", err)
	}
	q = &verifierQuerier{row: verifierRow{checksum: strings.Repeat("0", 64)}}
	if err := VerifyDataLifecycle(context.Background(), q); !errors.Is(err, ErrDataLifecycleChecksumMismatch) {
		t.Fatalf("VerifyDataLifecycle() drift error = %v, want checksum mismatch", err)
	}
}
