package migrations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestProductionSafetyDDLAndChecksum(t *testing.T) {
	ddl, err := ProductionSafetyDDL()
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"content_safety_decisions", "lease_expires_at", "status IN ('pending', 'allowed', 'blocked', 'unknown')"} {
		if !strings.Contains(ddl, required) {
			t.Fatalf("production safety DDL is missing %q", required)
		}
	}
	if len(ProductionSafetyChecksum()) != 64 {
		t.Fatalf("checksum length = %d", len(ProductionSafetyChecksum()))
	}
}

func TestVerifyProductionSafety(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: ProductionSafetyChecksum()}}
	if err := VerifyProductionSafety(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	q = &verifierQuerier{row: verifierRow{checksum: strings.Repeat("0", 64)}}
	if !errors.Is(VerifyProductionSafety(context.Background(), q), ErrProductionSafetyChecksumMismatch) {
		t.Fatal("checksum drift was accepted")
	}
	q = &verifierQuerier{row: verifierRow{err: pgx.ErrNoRows}}
	if !errors.Is(VerifyProductionSafety(context.Background(), q), ErrProductionSafetyNotApplied) {
		t.Fatal("missing production safety migration was accepted")
	}
}
