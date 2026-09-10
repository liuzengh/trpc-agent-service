package postgres

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
)

func TestMemoryLedgerPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, now := context.Background(), time.Now().UTC().Truncate(time.Microsecond)
	const tenantID, migrationID = "memory-ledger-tenant", "memory-ledger-move"
	// The ledger's authority check is the contract under test. Keep the setup
	// minimal and restore trigger enforcement before calling the ledger.
	if _, err := db.ExecContext(ctx, `SET session_replication_role='replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.backend_migration(
tenant_id,migration_id,domain,epoch,source_config_version,source_backend_profile_id,source_backend_version,
target_config_version,target_backend_profile_id,target_backend_version,state,created_at,updated_at)
VALUES($1,$2,'memory',1,1,'source',1,2,'target',1,'planned',$3,$3)`, tenantID, migrationID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `SET session_replication_role='origin'`); err != nil {
		t.Fatal(err)
	}
	ledger := New(db)
	record := memorydriver.RecordRequest{TenantID: tenantID, MigrationID: migrationID, MutationID: "u1-v1", Epoch: 1, ConfigVersion: 1,
		Direction: memorydriver.DirectionForward, Key: memorydriver.UserKey{TenantID: tenantID, AppName: tenantID + "/app", UserID: "u1"}, SourceDigest: strings.Repeat("a", 64), CreatedAt: now}
	first, err := ledger.Record(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := ledger.Record(ctx, record); err != nil || replay.Version != first.Version {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	claimed, err := ledger.Claim(ctx, memorydriver.ClaimRequest{TenantID: record.TenantID, MigrationID: record.MigrationID, WorkerID: "memory-contract", Limit: 1, Now: now.Add(time.Second), Lease: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if _, err := ledger.MarkApplied(ctx, memorydriver.CompleteRequest{TenantID: record.TenantID, MigrationID: record.MigrationID, MutationID: record.MutationID,
		WorkerID: claimed[0].LeaseOwner, Key: record.Key, ExpectedVersion: claimed[0].Version, TargetDigest: record.SourceDigest, At: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if outstanding, err := ledger.Outstanding(ctx, record.TenantID, record.MigrationID); err != nil || outstanding != 0 {
		t.Fatalf("outstanding=%d err=%v", outstanding, err)
	}
}
