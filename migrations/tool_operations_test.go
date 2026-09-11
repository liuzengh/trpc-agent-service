package migrations

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestToolOperationsEmbeddedMigration(t *testing.T) {
	body, err := ToolOperationsDDL()
	if err != nil {
		t.Fatal(err)
	}
	firstDDL := strings.Index(body, "CREATE TABLE IF NOT EXISTS tool_operations")
	if firstDDL < 0 || strings.TrimSpace(body[firstDDL:]) != strings.TrimSpace(tooloperation.PostgreSQLSchemaDDL) {
		t.Fatal("migration 003 and tooloperation schema contract have drifted")
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS tool_operations",
		"CREATE TABLE IF NOT EXISTS tool_operation_attempts",
		"CREATE TABLE IF NOT EXISTS tool_operation_resolutions",
		"WHERE state = 'unknown'",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded tool migration is missing %q", required)
		}
	}
	checksum := ToolOperationsChecksum()
	decoded, err := hex.DecodeString(checksum)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("tool migration checksum %q is not SHA-256: %v", checksum, err)
	}
	if LatestVersion != ProductionSafetyVersion {
		t.Fatalf("latest migration = %q, want %q", LatestVersion, ProductionSafetyVersion)
	}
}

func TestToolOperationsDDLRejectsUnwrappedSource(t *testing.T) {
	original := toolOperationsSource
	t.Cleanup(func() { toolOperationsSource = original })
	toolOperationsSource = "SELECT 1;"
	if _, err := ToolOperationsDDL(); err == nil {
		t.Fatal("unwrapped tool migration was accepted")
	}
}

func TestApplyToolOperationsRejectsNilTransaction(t *testing.T) {
	if err := ApplyToolOperations(context.Background(), nil); err == nil {
		t.Fatal("nil tool migration transaction was accepted")
	}
}

func TestVerifyToolOperationsApplied(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: ToolOperationsChecksum()}}
	if err := VerifyToolOperations(context.Background(), q); err != nil {
		t.Fatalf("VerifyToolOperations() error = %v", err)
	}
	if q.version != ToolOperationsVersion {
		t.Fatalf("queried version = %v, want %q", q.version, ToolOperationsVersion)
	}
}

func TestVerifyToolOperationsNotApplied(t *testing.T) {
	for name, rowErr := range map[string]error{
		"missing record": pgx.ErrNoRows,
		"missing ledger": &pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
		"legacy ledger":  &pgconn.PgError{Code: "42703", Message: "column does not exist"},
	} {
		t.Run(name, func(t *testing.T) {
			err := VerifyToolOperations(context.Background(), &verifierQuerier{row: verifierRow{err: rowErr}})
			if !errors.Is(err, ErrToolOperationsNotApplied) {
				t.Fatalf("VerifyToolOperations() error = %v, want not applied", err)
			}
		})
	}
}

func TestVerifyToolOperationsChecksumDriftAndRedaction(t *testing.T) {
	err := VerifyToolOperations(context.Background(), &verifierQuerier{
		row: verifierRow{checksum: strings.Repeat("0", 64)},
	})
	if !errors.Is(err, ErrToolOperationsChecksumMismatch) {
		t.Fatalf("checksum error = %v", err)
	}

	const secret = "postgres://admin:secret@db.internal/runtime"
	err = VerifyToolOperations(context.Background(), &verifierQuerier{
		row: verifierRow{err: errors.New("driver failed for " + secret)},
	})
	if !errors.Is(err, ErrToolOperationsVerification) {
		t.Fatalf("verification error = %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "admin") {
		t.Fatalf("verification leaked driver details: %v", err)
	}
}
