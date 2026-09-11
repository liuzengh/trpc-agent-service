package migrations

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestDataMigrationsEmbeddedLedger(t *testing.T) {
	body, err := DataMigrationsDDL()
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS data_migrations",
		"CREATE TABLE IF NOT EXISTS data_migration_objects",
		"mismatch_count bigint",
		"generation bigint",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded data migration is missing %q", required)
		}
	}
	decoded, err := hex.DecodeString(DataMigrationsChecksum())
	if err != nil || len(decoded) != 32 {
		t.Fatalf("data migration checksum is not SHA-256: %v", err)
	}
}

func TestVerifyDataMigrationsAppliedAndMismatch(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: DataMigrationsChecksum()}}
	if err := VerifyDataMigrations(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.version != DataMigrationsVersion {
		t.Fatalf("queried version = %v", q.version)
	}
	err := VerifyDataMigrations(context.Background(), &verifierQuerier{row: verifierRow{checksum: strings.Repeat("0", 64)}})
	if !errors.Is(err, ErrDataMigrationsChecksumMismatch) {
		t.Fatalf("checksum mismatch = %v", err)
	}
	err = VerifyDataMigrations(context.Background(), &verifierQuerier{row: verifierRow{err: pgx.ErrNoRows}})
	if !errors.Is(err, ErrDataMigrationsNotApplied) {
		t.Fatalf("not applied = %v", err)
	}
}

func TestApplyDataMigrationsRejectsNilTransaction(t *testing.T) {
	if err := ApplyDataMigrations(context.Background(), nil); err == nil {
		t.Fatal("nil transaction was accepted")
	}
}
