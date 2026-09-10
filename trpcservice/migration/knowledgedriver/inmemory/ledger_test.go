package inmemory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestLedgerReplayLeaseAndStaleCompletion(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	ledger := New()
	request := knowledgedriver.RecordRequest{TenantID: "tenant-a", MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 2, ConfigVersion: 1,
		Key:       knowledgedriver.ChunkKey{TenantID: "tenant-a", KnowledgeID: "kb-a", KnowledgeVersion: 3, ChunkID: "chunk-a"},
		Operation: knowledgedriver.OperationUpsert, SourceRevision: 4, MutationDigest: strings.Repeat("a", 64), CreatedAt: clock}
	first, err := ledger.Record(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := ledger.Record(ctx, request); err != nil || replay.Version != first.Version {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	collision := request
	collision.SourceRevision++
	if _, err := ledger.Record(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision=%v", err)
	}
	coordinateCollision := request
	coordinateCollision.Key.ChunkID = "chunk-b"
	if _, err := ledger.Record(ctx, coordinateCollision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("coordinate collision=%v", err)
	}
	claimed, err := ledger.Claim(ctx, knowledgedriver.ClaimRequest{TenantID: request.TenantID, MigrationID: request.MigrationID, WorkerID: "one", Limit: 1, Now: clock, Lease: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	reclaimed, err := ledger.Claim(ctx, knowledgedriver.ClaimRequest{TenantID: request.TenantID, MigrationID: request.MigrationID, WorkerID: "two", Limit: 1, Now: clock.Add(time.Minute), Lease: time.Minute})
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Attempt != 2 {
		t.Fatalf("reclaim=%+v err=%v", reclaimed, err)
	}
	_, err = ledger.MarkApplied(ctx, knowledgedriver.CompleteRequest{TenantID: request.TenantID, MigrationID: request.MigrationID, MutationID: request.MutationID, WorkerID: "one", Key: request.Key, ExpectedVersion: claimed[0].Version, TargetRevision: 4, TargetDigest: request.MutationDigest, At: clock.Add(30 * time.Second)})
	if !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale completion=%v", err)
	}
}
