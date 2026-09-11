package migrations

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
)

func TestUsageBudgetEmbeddedMigration(t *testing.T) {
	body, err := UsageBudgetDDL()
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS budget_periods",
		"CREATE TABLE IF NOT EXISTS model_usage_calls",
		"'reserved', 'settled', 'unknown', 'released'",
		"UNIQUE (tenant_id, run_id, call_no)",
		"input_price_per_million_units bigint",
		"FOREIGN KEY (tenant_id, billing_period)",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded usage budget migration is missing %q", required)
		}
	}
	decoded, err := hex.DecodeString(UsageBudgetChecksum())
	if err != nil || len(decoded) != 32 {
		t.Fatalf("usage budget checksum is not SHA-256: %v", err)
	}
}

func TestVerifyUsageBudgetAppliedAndMismatch(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: UsageBudgetChecksum()}}
	if err := VerifyUsageBudget(context.Background(), q); err != nil {
		t.Fatalf("VerifyUsageBudget() error = %v", err)
	}
	if q.version != UsageBudgetVersion {
		t.Fatalf("queried version = %q, want %q", q.version, UsageBudgetVersion)
	}
	q.row = verifierRow{checksum: "bad"}
	if err := VerifyUsageBudget(context.Background(), q); err == nil || !strings.Contains(err.Error(), UsageBudgetVersion) {
		t.Fatalf("checksum mismatch = %v", err)
	}
}
