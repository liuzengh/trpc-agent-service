package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestControlOperationsPostgreSQL16(t *testing.T) {
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
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, major)
	}

	ctx := context.Background()
	const tenantID, migrationID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAX", "control-contract"
	now := time.Now().UTC().Truncate(time.Microsecond)
	// This fixture isolates the repository's control-plane contract from the
	// profile/config fixture graph. Re-enable triggers before exercising any
	// repository method, so the migration state guard remains under test.
	if _, err := db.ExecContext(ctx, `SET session_replication_role='replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.backend_migration(
tenant_id,migration_id,domain,epoch,source_config_version,source_backend_profile_id,source_backend_version,
target_config_version,target_backend_profile_id,target_backend_version,state,created_at,updated_at)
VALUES($1,$2,'session',1,1,'source',1,2,'target',1,'planned',$3,$3)`, tenantID, migrationID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `SET session_replication_role='origin'`); err != nil {
		t.Fatal(err)
	}

	store := New(db)
	metadata := migration.ControlMetadata{ActorID: "operator", ReasonCode: "maintenance", CorrelationID: "control-1", TraceID: "trace-1"}
	paused, err := store.Pause(ctx, migration.ControlRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: 1, At: now.Add(time.Minute), Metadata: metadata})
	if err != nil || paused.State != migration.StatePaused || paused.PausedFrom != migration.StatePlanned || paused.Version != 2 {
		t.Fatalf("pause=%+v err=%v", paused, err)
	}
	var action, actor, reason, ref string
	if err := db.QueryRowContext(ctx, `SELECT c.action,c.actor_id,c.reason_code,o.payload_ref
FROM public.backend_migration_control c JOIN public.outbox o ON o.tenant_id=c.tenant_id AND o.event_seq=c.control_version
WHERE c.tenant_id=$1 AND c.migration_id=$2 AND c.control_version=2`, tenantID, migrationID).Scan(&action, &actor, &reason, &ref); err != nil {
		t.Fatal(err)
	}
	if action != "pause" || actor != metadata.ActorID || reason != metadata.ReasonCode || !strings.Contains(ref, "/2") {
		t.Fatalf("audit action=%q actor=%q reason=%q ref=%q", action, actor, reason, ref)
	}
	if _, err := store.Resume(ctx, migration.ControlRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: 1, At: now.Add(2 * time.Minute), Metadata: metadata}); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale resume=%v", err)
	}
	resumed, err := store.Resume(ctx, migration.ControlRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: paused.Version, At: now.Add(2 * time.Minute), Metadata: metadata})
	if err != nil || resumed.State != migration.StatePlanned || resumed.Version != 3 {
		t.Fatalf("resume=%+v err=%v", resumed, err)
	}
	aborted, err := store.Abort(ctx, migration.ControlRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: resumed.Version, At: now.Add(3 * time.Minute), Metadata: metadata})
	if err != nil || aborted.State != migration.StateAborted || aborted.Version != 4 {
		t.Fatalf("abort=%+v err=%v", aborted, err)
	}
}

func TestMemoryMigrationTransitionPublishesBundleInvalidationPostgreSQL16(t *testing.T) {
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
	const tenantID, migrationID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAY", "memory-invalidation"
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
	store := New(db)
	next, err := store.Transition(ctx, migration.TransitionRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: 1,
		To: migration.StateSnapshot, SnapshotWatermark: "memory-snapshot", At: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	var kind, payload string
	if err := db.QueryRowContext(ctx, `SELECT kind,payload_ref FROM public.outbox WHERE tenant_id=$1 AND outbox_id=$2`, tenantID,
		"memory-migration-invalidation:"+tenantID+":"+migrationID+":"+strconv.FormatInt(next.Version, 10)).Scan(&kind, &payload); err != nil {
		t.Fatal(err)
	}
	if kind != "config-invalidation" || !strings.HasPrefix(payload, "memory-migration://"+tenantID+"/"+migrationID+"/") {
		t.Fatalf("outbox kind=%q payload=%q", kind, payload)
	}
}

func TestPhaseJournalRecoversCommittedAuthorityWritePostgreSQL16(t *testing.T) {
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
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, major)
	}

	ctx := context.Background()
	const tenantID, migrationID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAX", "journal-contract"
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.ExecContext(ctx, `SET session_replication_role='replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.backend_migration(
tenant_id,migration_id,domain,epoch,source_config_version,source_backend_profile_id,source_backend_version,
target_config_version,target_backend_profile_id,target_backend_version,state,created_at,updated_at)
VALUES($1,$2,'session',2,1,'source',1,2,'target',1,'planned',$3,$3)`, tenantID, migrationID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `SET session_replication_role='origin'`); err != nil {
		t.Fatal(err)
	}

	store := New(db)
	request := migration.TransitionRequest{TenantID: tenantID, MigrationID: migrationID, ExpectedVersion: 1,
		To: migration.StateSnapshot, At: now.Add(time.Minute), SnapshotWatermark: "snapshot:journal"}
	if _, err := store.BeginPhaseIntent(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, request); err != nil {
		t.Fatal(err)
	}
	journaled := migration.NewJournaledRepository(store, store)
	recovered, err := journaled.RecoverPending(ctx, tenantID, migrationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].State != migration.StateSnapshot || recovered[0].Version != 2 {
		t.Fatalf("recovered=%+v", recovered)
	}
	var status string
	var resultVersion int64
	if err := db.QueryRowContext(ctx, `SELECT status,result_version FROM public.backend_migration_phase_intent
WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID).Scan(&status, &resultVersion); err != nil {
		t.Fatal(err)
	}
	if status != string(migration.PhaseIntentCompleted) || resultVersion != 2 {
		t.Fatalf("journal status=%q result=%d", status, resultVersion)
	}
	request.SnapshotWatermark = "snapshot:changed"
	if _, err := store.BeginPhaseIntent(ctx, request); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("changed phase intent=%v", err)
	}
}
