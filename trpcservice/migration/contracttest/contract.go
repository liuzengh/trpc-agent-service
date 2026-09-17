package contracttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Factory func(*testing.T) migration.Repository

func Run(t *testing.T, factory Factory) {
	t.Helper()
	store := factory(t)
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	create := migration.CreateRequest{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 1,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock}
	current, err := store.Create(ctx, create)
	if err != nil || current.State != migration.StatePlanned || current.Version != 1 {
		t.Fatalf("create=%+v err=%v", current, err)
	}
	if replay, err := store.Create(ctx, create); err != nil || replay.Version != current.Version {
		t.Fatalf("create replay=%+v err=%v", replay, err)
	}
	collision := create
	collision.Target.BackendVersion = 2
	if _, err := store.Create(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("expected create collision, got %v", err)
	}
	if _, err := store.Get(ctx, "tenant-b", create.MigrationID); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross tenant get=%v", err)
	}

	transition := func(to migration.State, alter func(*migration.TransitionRequest)) error {
		request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			ExpectedVersion: current.Version, To: to, At: clock.Add(time.Duration(current.Version) * time.Minute)}
		if alter != nil {
			alter(&request)
		}
		next, err := store.Transition(ctx, request)
		if err == nil {
			current = next
		}
		return err
	}
	if err := transition(migration.StateSnapshot, func(in *migration.TransitionRequest) { in.SnapshotWatermark = "snapshot:100" }); err != nil {
		t.Fatal(err)
	}
	if err := transition(migration.StateDualWrite, func(in *migration.TransitionRequest) { in.DualWriteRef = "outbox://migration-1" }); err != nil {
		t.Fatal(err)
	}
	control := migration.ControlRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		At: clock.Add(time.Duration(current.Version) * time.Minute), Metadata: migration.ControlMetadata{ActorID: "operator", ReasonCode: "maintenance", CorrelationID: "control-1", TraceID: "trace-1"}}
	paused, err := store.Pause(ctx, control)
	if err != nil || paused.State != migration.StatePaused || paused.PausedFrom != migration.StateDualWrite {
		t.Fatalf("pause=%+v err=%v", paused, err)
	}
	if _, err := store.Resume(ctx, control); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale resume=%v", err)
	}
	control.ExpectedVersion, control.At = paused.Version, paused.UpdatedAt.Add(time.Minute)
	current, err = store.Resume(ctx, control)
	if err != nil || current.State != migration.StateDualWrite || current.PausedFrom != "" {
		t.Fatalf("resume=%+v err=%v", current, err)
	}
	if err := transition(migration.StateBackfill, nil); err != nil {
		t.Fatal(err)
	}
	digest1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	batch1 := migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "batch-1",
		Epoch: 1, ExpectedVersion: current.Version, BatchSeq: 1, FromCheckpoint: "", ToCheckpoint: "cursor:10",
		Digest: digest1, RecordCount: 10, CommittedAt: clock.Add(5 * time.Minute)}
	result1, err := store.CommitBatch(ctx, batch1)
	if err != nil {
		t.Fatal(err)
	}
	current = result1.Migration
	if replay, err := store.CommitBatch(ctx, batch1); err != nil || replay.Batch.ResultVersion != result1.Batch.ResultVersion {
		t.Fatalf("batch replay=%+v err=%v", replay, err)
	}
	batchCollision := batch1
	batchCollision.Digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := store.CommitBatch(ctx, batchCollision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("expected batch collision, got %v", err)
	}
	batch2 := migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "batch-2",
		Epoch: 1, ExpectedVersion: current.Version, BatchSeq: 2, FromCheckpoint: "cursor:10", ToCheckpoint: "cursor:eof",
		Digest: digest1, RecordCount: 0, Complete: true, CommittedAt: clock.Add(6 * time.Minute)}
	result2, err := store.CommitBatch(ctx, batch2)
	if err != nil || !result2.Migration.BackfillComplete || result2.Migration.BackfillCount != 10 {
		t.Fatalf("complete batch=%+v err=%v", result2, err)
	}
	current = result2.Migration
	if replay, err := store.CommitBatch(ctx, batch1); err != nil ||
		replay.Batch.ResultVersion != result1.Batch.ResultVersion || replay.Migration.Version != current.Version {
		t.Fatalf("late batch replay=%+v err=%v", replay, err)
	}
	if err := transition(migration.StateVerify, nil); err != nil {
		t.Fatal(err)
	}
	verification := migration.Verification{SourceCount: 10, TargetCount: 10, SourceDigest: digest1, TargetDigest: digest1,
		SourceWatermark: "watermark:10", TargetWatermark: "watermark:10", SampleDigest: digest1}
	if err := transition(migration.StateCutover, func(in *migration.TransitionRequest) {
		in.Verification = verification
		in.CutoverConfigVersion = 2
	}); err != nil {
		t.Fatal(err)
	}
	observeUntil := clock.Add(24 * time.Hour)
	if err := transition(migration.StateObserve, func(in *migration.TransitionRequest) { in.ObserveUntil = observeUntil }); err != nil {
		t.Fatal(err)
	}
	if err := transition(migration.StateCleanup, func(in *migration.TransitionRequest) {
		in.At = observeUntil.Add(time.Minute)
		in.RollbackSyncWatermark = verification.TargetWatermark
	}); err != nil {
		t.Fatal(err)
	}
	if current.State != migration.StateCleanup {
		t.Fatalf("state=%s", current.State)
	}
}
