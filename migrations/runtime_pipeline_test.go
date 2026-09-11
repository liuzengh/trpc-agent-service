package migrations

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type verifierRow struct {
	checksum string
	err      error
}

func (r verifierRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = r.checksum
	return nil
}

type verifierQuerier struct {
	row     pgx.Row
	version any
}

func (q *verifierQuerier) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	if len(args) > 0 {
		q.version = args[0]
	}
	return q.row
}

func TestRuntimePipelineEmbeddedMigration(t *testing.T) {
	body, err := RuntimePipelineDDL()
	if err != nil {
		t.Fatal(err)
	}
	trimmed := strings.TrimSpace(body)
	if strings.HasPrefix(trimmed, "BEGIN;") || strings.HasSuffix(trimmed, "COMMIT;") {
		t.Fatal("runtime-owned migration transaction retained an outer transaction statement")
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS runtime_inbox",
		"CREATE TABLE IF NOT EXISTS runtime_outbox",
		"CREATE TABLE IF NOT EXISTS session_turn_sessions",
		"pipeline_schema_version integer NOT NULL DEFAULT 0",
		"atomic_commit_mode text NOT NULL DEFAULT ''",
		"database_identity text NOT NULL DEFAULT ''",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded runtime migration is missing %q", required)
		}
	}

	checksum := RuntimePipelineChecksum()
	decoded, err := hex.DecodeString(checksum)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("runtime migration checksum %q is not SHA-256: %v", checksum, err)
	}
}

func TestRuntimePipelineDDLRejectsUnwrappedSource(t *testing.T) {
	original := runtimePipelineSource
	t.Cleanup(func() { runtimePipelineSource = original })
	runtimePipelineSource = "SELECT 1;"
	if _, err := RuntimePipelineDDL(); err == nil {
		t.Fatal("unwrapped runtime migration was accepted")
	}
}

func TestApplyRuntimePipelineRejectsNilTransaction(t *testing.T) {
	if err := ApplyRuntimePipeline(context.Background(), nil); err == nil {
		t.Fatal("nil runtime migration transaction was accepted")
	}
}

func TestVerifyRuntimePipelineApplied(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: RuntimePipelineChecksum()}}
	if err := VerifyRuntimePipeline(context.Background(), q); err != nil {
		t.Fatalf("VerifyRuntimePipeline() error = %v", err)
	}
	if q.version != RuntimePipelineVersion {
		t.Fatalf("queried version = %v, want %q", q.version, RuntimePipelineVersion)
	}
}

func TestVerifyRuntimePipelineNotApplied(t *testing.T) {
	for name, rowErr := range map[string]error{
		"missing record": pgx.ErrNoRows,
		"missing ledger": &pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
		"legacy ledger":  &pgconn.PgError{Code: "42703", Message: "column does not exist"},
	} {
		t.Run(name, func(t *testing.T) {
			err := VerifyRuntimePipeline(context.Background(), &verifierQuerier{row: verifierRow{err: rowErr}})
			if !errors.Is(err, ErrRuntimePipelineNotApplied) {
				t.Fatalf("VerifyRuntimePipeline() error = %v, want not applied", err)
			}
		})
	}
}

func TestVerifyRuntimePipelineChecksumDrift(t *testing.T) {
	err := VerifyRuntimePipeline(context.Background(), &verifierQuerier{row: verifierRow{checksum: strings.Repeat("0", 64)}})
	if !errors.Is(err, ErrRuntimePipelineChecksumMismatch) {
		t.Fatalf("VerifyRuntimePipeline() error = %v, want checksum mismatch", err)
	}
}

func TestVerifyRuntimePipelineRedactsQueryFailure(t *testing.T) {
	const secretDSN = "postgres://fixture-user:fixture-credential@db.internal/runtime"
	err := VerifyRuntimePipeline(context.Background(), &verifierQuerier{
		row: verifierRow{err: errors.New("driver failure for " + secretDSN)},
	})
	if !errors.Is(err, ErrRuntimePipelineVerificationFailed) {
		t.Fatalf("VerifyRuntimePipeline() error = %v, want verification failure", err)
	}
	if strings.Contains(err.Error(), secretDSN) || strings.Contains(err.Error(), "fixture-credential") {
		t.Fatalf("VerifyRuntimePipeline() leaked DSN: %v", err)
	}
}
