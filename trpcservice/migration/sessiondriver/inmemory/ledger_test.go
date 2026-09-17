package inmemory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func TestLedgerReplayCollisionAndLeaseReclaim(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	store := New()
	record := sessiondriver.RecordRequest{TenantID: "tenant-a", MigrationID: "migration-a", MutationID: "commit-a", Epoch: 2,
		SessionKey:    sessionstore.SessionKey{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a"},
		SourceVersion: 3, MutationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: clock}
	first, err := store.Record(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := store.Record(ctx, record); err != nil || replay.Version != first.Version {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if first.Direction != sessiondriver.DirectionForward {
		t.Fatalf("default direction=%q", first.Direction)
	}
	directionCollision := record
	directionCollision.Direction = sessiondriver.DirectionReverse
	if _, err := store.Record(ctx, directionCollision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("direction collision=%v", err)
	}
	collision := record
	collision.SourceVersion++
	if _, err := store.Record(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision=%v", err)
	}
	claimed, err := store.Claim(ctx, sessiondriver.ClaimRequest{TenantID: record.TenantID,
		MigrationID: record.MigrationID, WorkerID: "worker-a", Limit: 1, Now: clock, Lease: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if second, err := store.Claim(ctx, sessiondriver.ClaimRequest{TenantID: record.TenantID,
		MigrationID: record.MigrationID, WorkerID: "worker-b", Limit: 1, Now: clock.Add(30 * time.Second),
		Lease: time.Minute}); err != nil || len(second) != 0 {
		t.Fatalf("live lease stolen=%+v err=%v", second, err)
	}
	reclaimed, err := store.Claim(ctx, sessiondriver.ClaimRequest{TenantID: record.TenantID,
		MigrationID: record.MigrationID, WorkerID: "worker-b", Limit: 1, Now: clock.Add(time.Minute), Lease: time.Minute})
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Attempt != 2 {
		t.Fatalf("reclaim=%+v err=%v", reclaimed, err)
	}
	if _, err := store.MarkApplied(ctx, sessiondriver.CompleteRequest{TenantID: record.TenantID,
		MigrationID: record.MigrationID, MutationID: record.MutationID, WorkerID: "worker-a",
		SessionKey: record.SessionKey, ExpectedVersion: claimed[0].Version, TargetVersion: 3,
		TargetDigest: record.MutationDigest, At: clock.Add(30 * time.Second)}); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale completion=%v", err)
	}
}
