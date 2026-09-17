package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	migrationpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestLedgerPostgreSQL16(t *testing.T) {
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
	ledger := New(db)
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	record := knowledgedriver.RecordRequest{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", MigrationID: "knowledge-probe", ConfigVersion: 1,
		MutationID: "adapter-mutation", Epoch: 1, Key: knowledgedriver.ChunkKey{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			KnowledgeID: "kb-a", KnowledgeVersion: 1, ChunkID: "adapter-chunk"}, Operation: knowledgedriver.OperationUpsert,
		SourceRevision: 2, MutationDigest: strings.Repeat("4", 64), CreatedAt: createdAt}
	first, err := ledger.Record(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := ledger.Record(ctx, record); err != nil || replay.Version != first.Version {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	collision := record
	collision.Key.ChunkID = "other-chunk"
	if _, err := ledger.Record(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("coordinate collision=%v", err)
	}

	now := createdAt.Add(time.Second)
	claimed, err := ledger.Claim(ctx, knowledgedriver.ClaimRequest{TenantID: record.TenantID,
		MigrationID: record.MigrationID, WorkerID: "knowledge-contract", Limit: 10, Now: now, Lease: time.Minute})
	if err != nil || len(claimed) < 1 {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	for _, item := range claimed {
		if _, err := ledger.MarkApplied(ctx, knowledgedriver.CompleteRequest{TenantID: item.TenantID,
			MigrationID: item.MigrationID, MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key,
			ExpectedVersion: item.Version, TargetRevision: item.SourceRevision, TargetDigest: item.MutationDigest,
			At: now}); err != nil {
			t.Fatalf("apply %s: %v", item.MutationID, err)
		}
	}
	if outstanding, err := ledger.Outstanding(ctx, record.TenantID, record.MigrationID); err != nil || outstanding != 0 {
		t.Fatalf("outstanding=%d err=%v", outstanding, err)
	}
}

func TestPublisherCutoverObserveReverseAndRollbackPostgreSQL16(t *testing.T) {
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
	ctx := context.Background()
	const tenantID, migrationID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", "knowledge-probe"
	ledger := New(db)
	clearKnowledgeOutstanding(t, ctx, ledger, tenantID, migrationID, time.Now().UTC().Add(time.Minute))

	var tenantVersion int64
	if err := db.QueryRowContext(ctx, `UPDATE public.tenant SET active_config_version=1,version=version+1
WHERE tenant_id=$1 RETURNING version`, tenantID).Scan(&tenantVersion); err != nil {
		t.Fatal(err)
	}
	authority := migrationpg.New(db)
	current, err := authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		t.Fatal(err)
	}
	verifyAt := time.Now().UTC().Add(2 * time.Minute)
	current, err = authority.Transition(ctx, migration.TransitionRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedVersion: current.Version, To: migration.StateVerify, At: verifyAt})
	if err != nil {
		t.Fatal(err)
	}
	verification := migration.Verification{SourceCount: 1, TargetCount: 1, SourceDigest: strings.Repeat("a", 64),
		TargetDigest: strings.Repeat("a", 64), SourceWatermark: "knowledge-chunk-v1:verified",
		TargetWatermark: "knowledge-chunk-v1:verified", SampleDigest: strings.Repeat("b", 64)}
	publisher := NewPublisher(db)
	cut, err := publisher.Cutover(ctx, knowledgedriver.CutoverRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedTenantVersion: tenantVersion, ExpectedVersion: current.Version, Verification: verification,
		At: verifyAt.Add(time.Minute), Metadata: knowledgedriver.SwitchMetadata{SwitchID: "knowledge-cutover", ActorID: "contract",
			ReasonCode: "contract", CorrelationID: "contract", TraceID: "contract"}})
	if err != nil || cut.ActiveConfigVersion != 2 || cut.Migration.State != migration.StateCutover {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
	observed, err := publisher.BeginObserve(ctx, knowledgedriver.ObserveRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedTenantVersion: cut.TenantVersion, ExpectedVersion: cut.Migration.Version, At: verifyAt.Add(2 * time.Minute), ObserveUntil: verifyAt.Add(4 * time.Minute)})
	if err != nil || observed.Migration.State != migration.StateObserve {
		t.Fatalf("observed=%+v err=%v", observed, err)
	}

	reverse, err := ledger.Record(ctx, knowledgedriver.RecordRequest{TenantID: tenantID, MigrationID: migrationID,
		MutationID: "post-cutover-target-write", Epoch: observed.Migration.Epoch, ConfigVersion: 2,
		Direction: knowledgedriver.DirectionReverse, Key: knowledgedriver.ChunkKey{TenantID: tenantID, KnowledgeID: "kb-a", KnowledgeVersion: 1, ChunkID: "reverse"},
		Operation: knowledgedriver.OperationUpsert, SourceRevision: 1, MutationDigest: strings.Repeat("c", 64), CreatedAt: verifyAt.Add(3 * time.Minute)})
	if err != nil || reverse.Direction != knowledgedriver.DirectionReverse {
		t.Fatalf("reverse=%+v err=%v", reverse, err)
	}
	clearKnowledgeOutstanding(t, ctx, ledger, tenantID, migrationID, verifyAt.Add(4*time.Minute))
	rolled, err := publisher.Rollback(ctx, knowledgedriver.RollbackRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedTenantVersion: observed.TenantVersion, ExpectedVersion: observed.Migration.Version,
		RollbackSyncWatermark: verification.TargetWatermark, At: verifyAt.Add(5 * time.Minute),
		Metadata: knowledgedriver.SwitchMetadata{SwitchID: "knowledge-rollback", ActorID: "contract", ReasonCode: "contract", CorrelationID: "contract", TraceID: "contract"}})
	if err != nil || !rolled.RolledBack || rolled.ActiveConfigVersion != 1 || rolled.Migration.State != migration.StateObserve {
		t.Fatalf("rolled=%+v err=%v", rolled, err)
	}
}

func clearKnowledgeOutstanding(t *testing.T, ctx context.Context, ledger *Ledger, tenantID, migrationID string, now time.Time) {
	t.Helper()
	items, err := ledger.Claim(ctx, knowledgedriver.ClaimRequest{TenantID: tenantID, MigrationID: migrationID,
		WorkerID: "knowledge-publisher-contract", Limit: 100, Now: now, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if _, err := ledger.MarkApplied(ctx, knowledgedriver.CompleteRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
			MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key, ExpectedVersion: item.Version,
			TargetRevision: item.SourceRevision, TargetDigest: item.MutationDigest, At: now}); err != nil {
			t.Fatal(err)
		}
	}
	if outstanding, err := ledger.Outstanding(ctx, tenantID, migrationID); err != nil || outstanding != 0 {
		t.Fatalf("outstanding=%d err=%v", outstanding, err)
	}
}
