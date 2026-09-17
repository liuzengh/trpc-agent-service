package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestJournaledTransitionCompletesWriteAheadIntent(t *testing.T) {
	ctx := context.Background()
	store := inmemory.New()
	current := createJournalMigration(t, ctx, store)
	journaled := migration.NewJournaledRepository(store, store)
	next, err := journaled.Transition(ctx, snapshotRequest(current))
	if err != nil {
		t.Fatal(err)
	}
	if next.State != migration.StateSnapshot || next.Version != 2 {
		t.Fatalf("next=%+v", next)
	}
	pending, err := store.ListPendingPhaseIntents(ctx, current.TenantID, current.MigrationID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

func TestJournalRecoveryCompletesEffectWrittenBeforeWorkerCrash(t *testing.T) {
	ctx := context.Background()
	store := inmemory.New()
	current := createJournalMigration(t, ctx, store)
	request := snapshotRequest(current)
	if _, err := store.BeginPhaseIntent(ctx, request); err != nil {
		t.Fatal(err)
	}
	// Model a process crash after the authority CAS commits but before it can
	// complete its intent. Recovery must observe the committed phase and must
	// not attempt the transition again.
	if _, err := store.Transition(ctx, request); err != nil {
		t.Fatal(err)
	}
	journaled := migration.NewJournaledRepository(store, store)
	recovered, err := journaled.RecoverPending(ctx, current.TenantID, current.MigrationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].State != migration.StateSnapshot || recovered[0].Version != 2 {
		t.Fatalf("recovered=%+v", recovered)
	}
	if _, err := journaled.RecoverPending(ctx, current.TenantID, current.MigrationID); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseIntentRejectsChangedPayloadForSameCASPhase(t *testing.T) {
	ctx := context.Background()
	store := inmemory.New()
	current := createJournalMigration(t, ctx, store)
	request := snapshotRequest(current)
	if _, err := store.BeginPhaseIntent(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.SnapshotWatermark = "snapshot:changed"
	if _, err := store.BeginPhaseIntent(ctx, request); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("changed intent error=%v", err)
	}
}

func createJournalMigration(t *testing.T, ctx context.Context, store *inmemory.Store) migration.Migration {
	t.Helper()
	clock := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	value, err := store.Create(ctx, migration.CreateRequest{TenantID: "tenant-journal", MigrationID: "migration-journal", Domain: "session", Epoch: 1,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func snapshotRequest(current migration.Migration) migration.TransitionRequest {
	return migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: migration.StateSnapshot, At: current.UpdatedAt.Add(time.Minute), SnapshotWatermark: "snapshot:1"}
}
