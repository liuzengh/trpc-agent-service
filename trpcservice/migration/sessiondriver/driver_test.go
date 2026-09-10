package sessiondriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	ledgermemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func TestDriverRepairBackfillAndShadowVerify(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	snapshot := fixtureSnapshot()
	mutationDigest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := ledger.Record(ctx, sessiondriver.RecordRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, MutationID: "commit-1", Epoch: current.Epoch,
		SessionKey: snapshot.Head.SessionKey, SourceVersion: snapshot.Head.Version,
		MutationDigest: mutationDigest, CreatedAt: clock.Add(4 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	target := newFakeTarget()
	target.fail = true
	fingerprint := sessiondriver.Fingerprint{Count: 1,
		Digest:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Watermark: "session:1", SampleDigest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	driver := sessiondriver.Driver{Authority: authority, Ledger: ledger, Source: fakeReader{snapshot},
		Backfill: fakePageSource{snapshot: snapshot}, Target: target,
		SourceInventory: fakeInventory{value: fingerprint}, TargetInventory: fakeInventory{value: fingerprint}}

	first, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, WorkerID: "repair-1", Limit: 10, Now: clock.Add(5 * time.Minute),
		Lease: time.Minute, RetryDelay: 30 * time.Second})
	if err != nil || first.Retried != 1 || first.Applied != 0 {
		t.Fatalf("first repair=%+v err=%v", first, err)
	}
	target.fail = false
	second, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, WorkerID: "repair-2", Limit: 10, Now: clock.Add(6 * time.Minute),
		Lease: time.Minute, RetryDelay: 0})
	if err != nil || second.Applied != 1 || second.Retried != 0 {
		t.Fatalf("second repair=%+v err=%v", second, err)
	}
	if outstanding, err := ledger.Outstanding(ctx, current.TenantID, current.MigrationID); err != nil || outstanding != 0 {
		t.Fatalf("outstanding=%d err=%v", outstanding, err)
	}

	batch, err := driver.BackfillOnce(ctx, sessiondriver.BackfillRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, Limit: 100, At: clock.Add(7 * time.Minute)})
	if err != nil || !batch.Migration.BackfillComplete || batch.Migration.BackfillCount != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	verifiedState, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, clock.Add(8*time.Minute))
	if err != nil || verifiedState.State != migration.StateVerify {
		t.Fatalf("verify state=%+v err=%v", verifiedState, err)
	}
	evidence, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID)
	if err != nil || evidence.SourceCount != 1 || evidence.SourceDigest != fingerprint.Digest || evidence.SampleDigest != fingerprint.SampleDigest {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
}

func TestDriverFailsClosedBeforeCheckpointAndVerification(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	snapshot := fixtureSnapshot()
	target := newFakeTarget()
	target.fail = true
	driver := sessiondriver.Driver{Authority: authority, Ledger: ledger, Source: fakeReader{snapshot},
		Backfill: fakePageSource{snapshot: snapshot}, Target: target}
	if _, err := driver.BackfillOnce(ctx, sessiondriver.BackfillRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, Limit: 10, At: clock.Add(5 * time.Minute)}); err == nil {
		t.Fatal("target failure advanced checkpoint")
	}
	unchanged, err := authority.Get(ctx, current.TenantID, current.MigrationID)
	if err != nil || unchanged.Version != current.Version || unchanged.BackfillCheckpoint != "" {
		t.Fatalf("authority changed=%+v err=%v", unchanged, err)
	}
	target.fail = false
	if _, err := driver.BackfillOnce(ctx, sessiondriver.BackfillRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, Limit: 10, At: clock.Add(6 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Record(ctx, sessiondriver.RecordRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, MutationID: "pending", Epoch: current.Epoch,
		SessionKey: snapshot.Head.SessionKey, SourceVersion: snapshot.Head.Version,
		MutationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CreatedAt:      clock.Add(7 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, clock.Add(8*time.Minute)); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("verify with repair backlog=%v", err)
	}
}

func TestShadowDivergenceFailsClosed(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	target := newFakeTarget()
	driver := sessiondriver.Driver{Authority: authority, Ledger: ledger, Backfill: fakePageSource{completeEmpty: true}, Target: target}
	if _, err := driver.BackfillOnce(ctx, sessiondriver.BackfillRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, Limit: 10, At: clock.Add(5 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.EnterVerify(ctx, current.TenantID, current.MigrationID, clock.Add(6*time.Minute)); err != nil {
		t.Fatal(err)
	}
	source := sessiondriver.Fingerprint{Count: 1, Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Watermark: "one", SampleDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	targetFingerprint := source
	targetFingerprint.DivergenceCount = 1
	driver.SourceInventory, driver.TargetInventory = fakeInventory{source}, fakeInventory{targetFingerprint}
	if _, err := driver.ShadowVerify(ctx, current.TenantID, current.MigrationID); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("divergence accepted=%v", err)
	}
}

func TestDriverRepairsReverseMutationIntoSource(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	authority, current := authorityAtBackfill(t, ctx, clock)
	ledger := ledgermemory.New()
	snapshot := fixtureSnapshot()
	if _, err := ledger.Record(ctx, sessiondriver.RecordRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, MutationID: "target-commit", Epoch: current.Epoch,
		Direction: sessiondriver.DirectionReverse, SessionKey: snapshot.Head.SessionKey,
		SourceVersion: snapshot.Head.Version, MutationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CreatedAt: clock.Add(4 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	sourceReplica := newFakeTarget()
	driver := sessiondriver.Driver{Authority: authority, Ledger: ledger,
		Source: fakeReader{snapshot: snapshot}, Target: newFakeTarget(),
		ReverseSource: fakeReader{snapshot: snapshot}, ReverseTarget: sourceReplica}
	result, err := driver.Repair(ctx, sessiondriver.RepairRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, WorkerID: "reverse-repair", Limit: 1,
		Now: clock.Add(5 * time.Minute), Lease: time.Minute})
	if err != nil || result.Applied != 1 || sourceReplica.applied["target-commit"].MutationID == "" {
		t.Fatalf("result=%+v applied=%+v err=%v", result, sourceReplica.applied, err)
	}
}

func TestPreparedVersionZeroImageIsMigratable(t *testing.T) {
	image := sessiondriver.SessionImage{Head: sessionstore.SessionHead{SessionKey: sessionstore.SessionKey{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "prepared"}, NextInputSeq: 1, State: map[string]any{}}}
	if digest, err := sessiondriver.SnapshotDigest(image); err != nil || len(digest) != 64 {
		t.Fatalf("digest=%q err=%v", digest, err)
	}
}

func TestSessionPhaseJournalRecoversAuthorityCommitCrashWindow(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	store, current := authorityAtBackfill(t, ctx, clock)
	batch, err := store.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		BatchID: "journal-empty", Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq,
		ToCheckpoint: "cursor:eof", Digest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		Complete: true, CommittedAt: clock.Add(4 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	request := migration.TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		ExpectedVersion: batch.Migration.Version, To: migration.StateVerify, At: clock.Add(5 * time.Minute)}
	if _, err := store.BeginPhaseIntent(ctx, request); err != nil {
		t.Fatal(err)
	}
	// Fault injection: authority CAS succeeds and the worker dies before it
	// marks the journal completed. Recovery must not reapply the transition.
	if _, err := store.Transition(ctx, request); err != nil {
		t.Fatal(err)
	}
	journaled := migration.NewJournaledRepository(store, store)
	recovered, err := journaled.RecoverPending(ctx, current.TenantID, current.MigrationID)
	if err != nil || len(recovered) != 1 || recovered[0].State != migration.StateVerify || recovered[0].Version != request.ExpectedVersion+1 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
}

func authorityAtBackfill(t *testing.T, ctx context.Context, clock time.Time) (*migrationmemory.Store, migration.Migration) {
	t.Helper()
	store := migrationmemory.New()
	current, err := store.Create(ctx, migration.CreateRequest{TenantID: "tenant-a", MigrationID: "session-migration",
		Domain: sessiondriver.Domain, Epoch: 1,
		Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateSnapshot,
		At: clock.Add(time.Minute), SnapshotWatermark: "snapshot:1"})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateDualWrite,
		At: clock.Add(2 * time.Minute), DualWriteRef: "session-migration-mutation://session-migration"})
	if err != nil {
		t.Fatal(err)
	}
	current, err = store.Transition(ctx, migration.TransitionRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, ExpectedVersion: current.Version, To: migration.StateBackfill,
		At: clock.Add(3 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return store, current
}

func fixtureSnapshot() sessiondriver.SessionImage {
	key := sessionstore.SessionKey{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a"}
	clock := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	return sessiondriver.SessionImage{Head: sessionstore.SessionHead{SessionKey: key, Version: 1,
		LastFence: 3, NextInputSeq: 2, State: map[string]any{}},
		LastAllocatedInputSeq: 1,
		SDK: &sessiondriver.SDKSessionImage{AppName: "tenant-a/app-a", UserID: "user-a", SessionID: key.SessionID,
			State: json.RawMessage(`{"id":"session-a","state":{"topic":"migration"}}`), CreatedAt: clock, UpdatedAt: clock,
			Events: []sessiondriver.SDKEventRecord{{Event: json.RawMessage(`{"id":"event-a","response":{"id":"response-a"}}`), CreatedAt: clock, UpdatedAt: clock}}},
		Commits: []sessiondriver.CommitRecord{{CommitID: "commit-a", RequestID: "request-a",
			RequestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Stage: "terminal",
			InputSeq: 1, Fence: 3, Outcome: runtime.OutcomeSucceeded, SessionVersion: 1, CreatedAt: clock}}}
}

type fakeReader struct{ snapshot sessiondriver.SessionImage }

func (f fakeReader) LoadSessionImage(_ context.Context, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if key != f.snapshot.Head.SessionKey {
		return sessiondriver.SessionImage{}, runtime.ErrNotFound
	}
	return f.snapshot, nil
}

type fakePageSource struct {
	snapshot      sessiondriver.SessionImage
	completeEmpty bool
}

func (f fakePageSource) PageSessions(_ context.Context, in sessiondriver.PageRequest) (sessiondriver.Page, error) {
	if in.TenantID != "tenant-a" || in.After != "" || in.SnapshotWatermark != "snapshot:1" {
		return sessiondriver.Page{}, runtime.ErrInvariantViolation
	}
	if f.completeEmpty {
		return sessiondriver.Page{NextCheckpoint: "cursor:eof", Complete: true}, nil
	}
	return sessiondriver.Page{Sessions: []sessiondriver.SessionImage{f.snapshot}, NextCheckpoint: "cursor:eof", Complete: true}, nil
}

type fakeTarget struct {
	fail    bool
	applied map[string]sessiondriver.ApplyRequest
}

func newFakeTarget() *fakeTarget {
	return &fakeTarget{applied: make(map[string]sessiondriver.ApplyRequest)}
}

func (f *fakeTarget) ApplySessionSnapshot(_ context.Context, in sessiondriver.ApplyRequest) (sessiondriver.ApplyResult, error) {
	if f.fail {
		return sessiondriver.ApplyResult{}, runtime.ErrBackendUnavailable
	}
	if existing, ok := f.applied[in.MutationID]; ok {
		if existing.Epoch != in.Epoch || existing.Image.Head.SessionKey != in.Image.Head.SessionKey ||
			existing.SnapshotDigest != in.SnapshotDigest || existing.Image.Head.Version != in.Image.Head.Version {
			return sessiondriver.ApplyResult{}, runtime.ErrIdempotencyCollision
		}
	} else {
		f.applied[in.MutationID] = in
	}
	return sessiondriver.ApplyResult{SessionVersion: in.Image.Head.Version, SnapshotDigest: in.SnapshotDigest}, nil
}

type fakeInventory struct{ value sessiondriver.Fingerprint }

func (f fakeInventory) Fingerprint(_ context.Context, _, _ string) (sessiondriver.Fingerprint, error) {
	return f.value, nil
}
