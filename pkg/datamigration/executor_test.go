package datamigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type scriptedSource struct {
	snapshot Batch
	changes  Batch
}

func (s scriptedSource) Snapshot(context.Context, string, Domain, string, int) (Batch, error) {
	return s.snapshot, nil
}
func (s scriptedSource) Changes(context.Context, string, Domain, int64, int) (Batch, error) {
	return s.changes, nil
}

type freshCursorStalledChangesSource struct {
	snapshot Batch
	calls    int
}

func (s *freshCursorStalledChangesSource) Snapshot(context.Context, string, Domain, string, int) (Batch, error) {
	return s.snapshot, nil
}

func (s *freshCursorStalledChangesSource) Changes(context.Context, string, Domain, int64, int) (Batch, error) {
	s.calls++
	return Batch{Records: []Record{migrationRecord("m-stalled", 0, "same")}, NextCursor: fmt.Sprintf("fresh-%d", s.calls), Watermark: 0, Done: false}, nil
}

type recordingTarget struct{ batches []Batch }

func (t *recordingTarget) Upsert(_ context.Context, _ string, _ Domain, _ LeaseFence, records []Record) error {
	t.batches = append(t.batches, Batch{Records: records})
	return nil
}

type failingRenewStore struct{ *MemoryStore }

type cancellationRenewMigrationStore struct {
	*MemoryStore
	renewStarted chan struct{}
	renewOnce    sync.Once
}

type recordingReleaseStore struct {
	*MemoryStore
	releaseContexts    []context.Context
	releaseErr         error
	releaseDeadline    time.Time
	releaseHasDeadline bool
	releaseTraceValue  any
}

type migrationTraceKey struct{}

type typedNilMigrationStore struct{ Store }
type typedNilMigrationSource struct{ Source }
type typedNilMigrationTarget struct{ Target }

func (s *failingRenewStore) Renew(context.Context, string, string, int64, time.Duration) (Job, error) {
	return Job{}, errors.New("renew backend unavailable token=secret-value")
}

func (s *cancellationRenewMigrationStore) Renew(ctx context.Context, id, owner string, version int64, ttl time.Duration) (Job, error) {
	s.renewOnce.Do(func() { close(s.renewStarted) })
	<-ctx.Done()
	return Job{}, ctx.Err()
}

func (s *recordingReleaseStore) Release(ctx context.Context, id, owner string, version int64) error {
	s.releaseContexts = append(s.releaseContexts, ctx)
	s.releaseErr = ctx.Err()
	s.releaseDeadline, s.releaseHasDeadline = ctx.Deadline()
	s.releaseTraceValue = ctx.Value(migrationTraceKey{})
	return s.MemoryStore.Release(ctx, id, owner, version)
}

func migrationRecord(key string, version int64, payload string) Record {
	digest := sha256.Sum256([]byte(payload))
	return Record{Key: key, Version: version, Hash: hex.EncodeToString(digest[:]), Payload: []byte(payload)}
}

func newTestMigration(t *testing.T) (*MemoryStore, Job) {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	store := NewMemoryStore(func() time.Time { return now })
	job, err := NewJob("migration-1", "tenant-a", DomainMemory, "redis-a", "postgres-a", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return store, job
}

func TestExecutorRejectsTypedNilDependencies(t *testing.T) {
	base := Executor{Owner: "worker-a", LeaseTTL: time.Minute, BatchSize: 1}
	var store *typedNilMigrationStore
	base.Store = store
	base.Source = scriptedSource{}
	base.Target = &recordingTarget{}
	if err := base.validate(); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("typed-nil store validation error=%v, want capability error", err)
	}
	base.Store = NewMemoryStore(nil)
	var source *typedNilMigrationSource
	base.Source = source
	if err := base.validate(); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("typed-nil source validation error=%v, want capability error", err)
	}
	base.Source = scriptedSource{}
	var target *typedNilMigrationTarget
	base.Target = target
	if err := base.validate(); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("typed-nil target validation error=%v, want capability error", err)
	}
}

func TestExecutorNormalizesNilContexts(t *testing.T) {
	newExecutor := func(t *testing.T, store *MemoryStore, jobID string, rollback func(context.Context, Job, LeaseFence) error) *Executor {
		t.Helper()
		source := scriptedSource{
			snapshot: Batch{Done: true},
			changes:  Batch{Done: true},
		}
		return &Executor{
			Store: store, Source: source, Target: &recordingTarget{}, Owner: "worker-a",
			LeaseTTL: time.Minute, BatchSize: 1,
			Hooks: Hooks{
				Prepare:         func(context.Context, Job, LeaseFence) error { return nil },
				EnableDualWrite: func(context.Context, Job, LeaseFence) error { return nil },
				Validate:        func(context.Context, Job, LeaseFence) error { return nil },
				ShadowRead:      func(context.Context, Job, LeaseFence) error { return nil },
				Cutover:         func(context.Context, Job, LeaseFence) error { return nil },
				Complete:        func(context.Context, Job, LeaseFence) error { return nil },
				Rollback:        rollback,
			},
		}
	}

	store, job := newTestMigration(t)
	executor := newExecutor(t, store, job.ID, func(context.Context, Job, LeaseFence) error { return nil })
	for step := 0; step < 7; step++ {
		var err error
		job, err = executor.RunOnce(nil, job.ID)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	completed, err := executor.Complete(nil, job.ID)
	if err != nil {
		t.Fatalf("complete with nil context: %v", err)
	}
	if completed.Phase != PhaseComplete {
		t.Fatalf("completed phase=%s", completed.Phase)
	}

	rollbackStore, rollbackJob := newTestMigration(t)
	rollbackExecutor := newExecutor(t, rollbackStore, rollbackJob.ID, func(context.Context, Job, LeaseFence) error { return nil })
	for step := 0; step < 7; step++ {
		var err error
		rollbackJob, err = rollbackExecutor.RunOnce(context.Background(), rollbackJob.ID)
		if err != nil {
			t.Fatalf("rollback setup step %d: %v", step, err)
		}
	}
	rolledBack, err := rollbackExecutor.Rollback(nil, rollbackJob.ID)
	if err != nil {
		t.Fatalf("rollback with nil context: %v", err)
	}
	if rolledBack.Phase != PhaseRolledBack {
		t.Fatalf("rolled back phase=%s", rolledBack.Phase)
	}
}

func TestExecutorReleasesLeaseWithDetachedContextAfterCancellation(t *testing.T) {
	newExecutor := func(t *testing.T, store *recordingReleaseStore) (*Executor, Job) {
		t.Helper()
		jobStore, job := newTestMigration(t)
		store.MemoryStore = jobStore
		executor := &Executor{
			Store: store, Source: scriptedSource{snapshot: Batch{Done: true}, changes: Batch{Done: true}},
			Target: &recordingTarget{}, Owner: "worker-a", LeaseTTL: time.Minute, BatchSize: 1,
			Hooks: Hooks{
				Prepare:         func(context.Context, Job, LeaseFence) error { return nil },
				EnableDualWrite: func(context.Context, Job, LeaseFence) error { return nil },
				Validate:        func(context.Context, Job, LeaseFence) error { return nil },
				ShadowRead:      func(context.Context, Job, LeaseFence) error { return nil },
				Cutover:         func(context.Context, Job, LeaseFence) error { return nil },
				Complete:        func(context.Context, Job, LeaseFence) error { return nil },
				Rollback:        func(context.Context, Job, LeaseFence) error { return nil },
			},
		}
		return executor, job
	}
	assertDetached := func(t *testing.T, store *recordingReleaseStore, parent context.Context) {
		t.Helper()
		if len(store.releaseContexts) == 0 {
			t.Fatal("executor did not release the migration lease")
		}
		if store.releaseErr != nil {
			t.Fatalf("lease release inherited cancelled context: %v", store.releaseErr)
		}
		deadline, ok := store.releaseDeadline, store.releaseHasDeadline
		if !ok || !deadline.After(time.Now()) || deadline.After(time.Now().Add(migrationPersistenceTimeout)) {
			t.Fatalf("lease release context deadline=%v, want bounded future deadline", deadline)
		}
		if got, want := store.releaseTraceValue, parent.Value(migrationTraceKey{}); got != want {
			t.Fatalf("lease release context lost trace value: got=%v want=%v", got, want)
		}
	}

	t.Run("RunOnce", func(t *testing.T) {
		store := &recordingReleaseStore{}
		executor, job := newExecutor(t, store)
		parent := context.WithValue(context.Background(), migrationTraceKey{}, "run-once")
		ctx, cancel := context.WithCancel(parent)
		executor.Hooks.Prepare = func(context.Context, Job, LeaseFence) error {
			cancel()
			return context.Canceled
		}
		if _, err := executor.RunOnce(ctx, job.ID); err == nil {
			t.Fatal("cancelled migration step unexpectedly succeeded")
		}
		assertDetached(t, store, parent)
	})

	for _, operation := range []string{"Complete", "Rollback"} {
		t.Run(operation, func(t *testing.T) {
			store := &recordingReleaseStore{}
			executor, job := newExecutor(t, store)
			for step := 0; step < 7; step++ {
				var err error
				job, err = executor.RunOnce(context.Background(), job.ID)
				if err != nil {
					t.Fatalf("prepare rollback window step %d: %v", step, err)
				}
			}
			if job.Phase != PhaseRollbackWindow {
				t.Fatalf("phase=%s, want rollback window", job.Phase)
			}
			store.releaseContexts = nil
			parent := context.WithValue(context.Background(), migrationTraceKey{}, operation)
			ctx, cancel := context.WithCancel(parent)
			cancel()
			var err error
			switch operation {
			case "Complete":
				_, err = executor.Complete(ctx, job.ID)
			case "Rollback":
				_, err = executor.Rollback(ctx, job.ID)
			}
			if err != nil {
				t.Fatalf("%s with cancelled context: %v", operation, err)
			}
			assertDetached(t, store, parent)
		})
	}
}

func TestMigrationLeaseRejectsSubMillisecondTTL(t *testing.T) {
	store, job := newTestMigration(t)
	if _, err := store.Claim(context.Background(), job.ID, "worker-a", 999*time.Microsecond); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("claim error=%v, want ErrInvalidMigration", err)
	}
	claimed, err := store.Claim(context.Background(), job.ID, "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Renew(context.Background(), job.ID, "worker-a", claimed.LeaseVersion, 999*time.Microsecond); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("renew error=%v, want ErrInvalidMigration", err)
	}
	executor := &Executor{Store: store, Source: scriptedSource{}, Target: &recordingTarget{}, Owner: "worker-a", LeaseTTL: 999 * time.Microsecond, BatchSize: 1}
	if err := executor.validate(); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("executor validation error=%v, want ErrInvalidMigration", err)
	}
}

func TestExecutorIgnoresRenewCancellationAfterStep(t *testing.T) {
	base, job := newTestMigration(t)
	store := &cancellationRenewMigrationStore{
		MemoryStore:  base,
		renewStarted: make(chan struct{}),
	}
	executor := &Executor{
		Store: store, Source: scriptedSource{}, Target: &recordingTarget{}, Owner: "worker-a",
		LeaseTTL: 30 * time.Millisecond, BatchSize: 1,
		Hooks: Hooks{Prepare: func(ctx context.Context, _ Job, _ LeaseFence) error {
			select {
			case <-store.renewStarted:
				return nil
			case <-time.After(time.Second):
				return errors.New("renewal did not start")
			}
		}},
	}
	got, err := executor.RunOnce(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("normal step completion was reported as renewal failure: %v", err)
	}
	if got.Phase != PhaseSnapshotCopy {
		t.Fatalf("phase=%s, want %s", got.Phase, PhaseSnapshotCopy)
	}
}

func TestExecutorRequiresApprovalForCutoverCompletion(t *testing.T) {
	store, job := newTestMigration(t)
	target := &recordingTarget{}
	source := scriptedSource{
		snapshot: Batch{Records: []Record{migrationRecord("m-1", 1, "one")}, NextCursor: "snapshot-done", Watermark: 1, Done: true},
		changes:  Batch{Watermark: 1, Done: true},
	}
	var hooks []string
	mark := func(name string) func(context.Context, Job, LeaseFence) error {
		return func(context.Context, Job, LeaseFence) error { hooks = append(hooks, name); return nil }
	}
	executor := &Executor{
		Store: store, Source: source, Target: target, Owner: "worker-a", LeaseTTL: time.Minute, BatchSize: 10,
		Hooks: Hooks{
			Prepare: mark("prepare"), EnableDualWrite: mark("dual"), Validate: mark("validate"),
			ShadowRead: mark("shadow"), Cutover: mark("cutover"), Complete: mark("complete"),
		},
	}
	for i := 0; i < 7; i++ {
		got, err := executor.RunOnce(context.Background(), job.ID)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		job = got
	}
	if job.Phase != PhaseRollbackWindow {
		t.Fatalf("phase=%s, want rollback window", job.Phase)
	}
	if _, err := executor.RunOnce(context.Background(), job.ID); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("run in rollback window error=%v", err)
	}
	completed, err := executor.Complete(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Phase != PhaseComplete {
		t.Fatalf("completed phase=%s", completed.Phase)
	}
	if !reflect.DeepEqual(hooks, []string{"prepare", "dual", "validate", "shadow", "cutover", "complete"}) {
		t.Fatalf("hooks=%v", hooks)
	}
	if len(target.batches) != 1 || len(target.batches[0].Records) != 1 {
		t.Fatalf("target batches=%#v", target.batches)
	}
}

func TestExecutorCatchUpPersistsCursorAndRejectsStalledBatch(t *testing.T) {
	store, job := newTestMigration(t)
	source := scriptedSource{
		snapshot: Batch{NextCursor: "snapshot-done", Watermark: 1, Done: true},
		changes: Batch{
			Records:    []Record{migrationRecord("m-2", 2, "two")},
			NextCursor: "change-1",
			Watermark:  2,
			Done:       false,
		},
	}
	executor := &Executor{
		Store: store, Source: source, Target: &recordingTarget{}, Owner: "worker-a",
		LeaseTTL: time.Minute, BatchSize: 10,
		Hooks: Hooks{
			Prepare:         func(context.Context, Job, LeaseFence) error { return nil },
			EnableDualWrite: func(context.Context, Job, LeaseFence) error { return nil },
		},
	}
	for step := 0; step < 4; step++ {
		var err error
		job, err = executor.RunOnce(context.Background(), job.ID)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	if job.Phase != PhaseCatchUp || job.Cursor != "change-1" || job.AppliedWatermark != 2 {
		t.Fatalf("catch-up checkpoint=%#v, want cursor and watermark persisted", job)
	}
	if _, err := executor.RunOnce(context.Background(), job.ID); !errors.Is(err, ErrCursorStalled) {
		t.Fatalf("repeated catch-up batch error=%v, want ErrCursorStalled", err)
	}
	current, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Cursor != "change-1" || current.AppliedWatermark != 2 {
		t.Fatalf("stalled batch changed checkpoint=%#v", current)
	}
}

func TestExecutorCatchUpRejectsUnchangedWatermarkWithFreshCursor(t *testing.T) {
	store, job := newTestMigration(t)
	source := &freshCursorStalledChangesSource{
		snapshot: Batch{NextCursor: "snapshot-done", Watermark: 0, Done: true},
	}
	executor := &Executor{
		Store: store, Source: source, Target: &recordingTarget{}, Owner: "worker-a",
		LeaseTTL: time.Minute, BatchSize: 10,
		Hooks: Hooks{
			Prepare:         func(context.Context, Job, LeaseFence) error { return nil },
			EnableDualWrite: func(context.Context, Job, LeaseFence) error { return nil },
		},
	}
	for step := 0; step < 3; step++ {
		var err error
		job, err = executor.RunOnce(context.Background(), job.ID)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	if _, err := executor.RunOnce(context.Background(), job.ID); !errors.Is(err, ErrCursorStalled) {
		t.Fatalf("unchanged watermark error=%v, want ErrCursorStalled", err)
	}
	if source.calls != 1 {
		t.Fatalf("source calls=%d, want one rejected batch", source.calls)
	}
}

func TestMemoryStoreRejectsMigrationCheckpointRegression(t *testing.T) {
	store, job := newTestMigration(t)
	claimed, err := store.Claim(context.Background(), job.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	watermark := int64(4)
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{SnapshotWatermark: &watermark}); err != nil {
		t.Fatal(err)
	}
	watermark = 3
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{SnapshotWatermark: &watermark}); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("snapshot watermark regression error=%v, want ErrInvalidMigration", err)
	}
	watermark = 4
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{AppliedWatermark: &watermark}); err != nil {
		t.Fatal(err)
	}
	watermark = 2
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{AppliedWatermark: &watermark}); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("applied watermark regression error=%v, want ErrInvalidMigration", err)
	}
}

func TestMemoryStoreRejectsClearingCursorOutsidePhaseBoundary(t *testing.T) {
	store, job := newTestMigration(t)
	claimed, err := store.Claim(context.Background(), job.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cursor := "snapshot-1"
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{Cursor: &cursor}); err != nil {
		t.Fatal(err)
	}
	cleared := ""
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion, JobPatch{Cursor: &cleared}); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("cursor clearing error=%v, want ErrInvalidMigration", err)
	}
}

func TestMemoryStoreRejectsClearingCursorOnCatchUpSelfTransition(t *testing.T) {
	store, job := newTestMigration(t)
	claimed, err := store.Claim(context.Background(), job.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	advance := func(phase Phase, cursor string) {
		t.Helper()
		_, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion,
			JobPatch{Phase: &phase, Cursor: &cursor})
		if err != nil {
			t.Fatalf("advance to %s: %v", phase, err)
		}
	}
	advance(PhaseSnapshotCopy, "snapshot")
	advance(PhaseDualWrite, "")
	advance(PhaseCatchUp, "change-1")
	cleared := ""
	samePhase := PhaseCatchUp
	if _, err := store.Advance(context.Background(), job.ID, claimed.LeaseOwner, claimed.LeaseVersion,
		JobPatch{Phase: &samePhase, Cursor: &cleared}); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("catch-up self-transition cursor clearing error=%v, want ErrInvalidMigration", err)
	}
	current, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Phase != PhaseCatchUp || current.Cursor != "change-1" {
		t.Fatalf("self-transition changed checkpoint=%#v", current)
	}
}

func TestExecutorRejectsStalledSourceCursor(t *testing.T) {
	store, job := newTestMigration(t)
	initial := "cursor"
	_, err := store.Advance(context.Background(), job.ID, "owner", 0, JobPatch{Cursor: &initial})
	if err == nil {
		t.Fatal("advance without a lease unexpectedly succeeded")
	}
	target := &recordingTarget{}
	source := scriptedSource{snapshot: Batch{NextCursor: "", Watermark: 0, Done: false}}
	executor := &Executor{
		Store: store, Source: source, Target: target, Owner: "worker-a", LeaseTTL: time.Minute, BatchSize: 1,
		Hooks: Hooks{Prepare: func(context.Context, Job, LeaseFence) error { return nil }},
	}
	if _, err := executor.RunOnce(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	_, err = executor.RunOnce(context.Background(), job.ID)
	if !errors.Is(err, ErrCursorStalled) {
		t.Fatalf("error=%v, want cursor stalled", err)
	}
}

func TestRecordValidationDetectsTampering(t *testing.T) {
	record := migrationRecord("key", 1, "payload")
	record.Payload[0] = 'P'
	if err := record.Validate(); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("error=%v, want invalid record", err)
	}
}

func TestMemoryStoreRejectsReclaimBySameOwnerWhileLeaseIsValid(t *testing.T) {
	store, job := newTestMigration(t)
	claimed, err := store.Claim(context.Background(), job.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(context.Background(), job.ID, "worker-a", time.Minute); !errors.Is(err, ErrMigrationLeaseHeld) {
		t.Fatalf("same-owner reclaim error=%v, want ErrMigrationLeaseHeld", err)
	}
	current, err := store.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LeaseVersion != claimed.LeaseVersion || current.LeaseOwner != claimed.LeaseOwner {
		t.Fatalf("same-owner reclaim changed lease: %#v", current)
	}
}

func TestMigrationErrorTextIsRedactedAndBounded(t *testing.T) {
	input := "provider failed dsn=postgres://user:password@db.internal/agent api_key=sk-live password=hunter2 Authorization: Bearer bearer-value bare Bearer alternate-bearer\n" + strings.Repeat("x", MaxMigrationErrorBytes)
	got := sanitizeMigrationErrorText(input)
	if len(got) > MaxMigrationErrorBytes || !utf8.ValidString(got) || strings.ContainsAny(got, "\x00\r\n") {
		t.Fatalf("unsafe migration error %q", got)
	}
	for _, secret := range []string{"password@db.internal", "sk-live", "hunter2", "bearer-value", "alternate-bearer"} {
		if strings.Contains(got, secret) {
			t.Fatalf("migration error leaked %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "Authorization=<redacted>") || !strings.Contains(got, "Bearer <redacted>") {
		t.Fatalf("bearer token was not redacted: %q", got)
	}
}

func TestMigrationJobRejectsUnsafePersistentMetadata(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	valid, err := NewJob("migration-1", "tenant-a", DomainMemory, "redis-a", "postgres-a", now)
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*Job){
		"oversized source profile":         func(job *Job) { job.SourceProfile = strings.Repeat("p", maxMigrationProfileBytes+1) },
		"control character target profile": func(job *Job) { job.TargetProfile = "postgres-a\nother" },
		"invalid UTF-8 source profile":     func(job *Job) { job.SourceProfile = string([]byte{'p', 0xff}) },
		"unredacted error":                 func(job *Job) { job.LastError = "migration failed password=hunter2" },
	} {
		t.Run(name, func(t *testing.T) {
			job := valid
			mutate(&job)
			if err := job.Validate(); !errors.Is(err, ErrInvalidMigration) {
				t.Fatalf("Validate() error=%v, want ErrInvalidMigration", err)
			}
		})
	}

	valid.LastError = sanitizeMigrationErrorText("migration failed password=hunter2")
	if err := valid.Validate(); err != nil {
		t.Fatalf("sanitized job was rejected: %v", err)
	}
}

func TestExecutorCancelsStepWhenLeaseRenewalFails(t *testing.T) {
	base, job := newTestMigration(t)
	store := &failingRenewStore{MemoryStore: base}
	started := make(chan struct{})
	executor := &Executor{
		Store: store, Source: scriptedSource{}, Target: &recordingTarget{}, Owner: "worker-a",
		LeaseTTL: 30 * time.Millisecond, BatchSize: 1,
		Hooks: Hooks{Prepare: func(ctx context.Context, got Job, leaseFence LeaseFence) error {
			close(started)
			fence, err := LeaseFenceFromContext(ctx)
			if err != nil || fence != leaseFence || fence.MigrationID != got.ID || fence.Owner != "worker-a" || fence.Version != got.LeaseVersion {
				return fmt.Errorf("invalid step fence: %v", err)
			}
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := executor.RunOnce(context.Background(), job.ID)
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("migration step did not start")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrMigrationFence) {
			t.Fatalf("RunOnce error=%v, want ErrMigrationFence", err)
		}
		if strings.Contains(err.Error(), "secret-value") {
			t.Fatalf("renew credential leaked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunOnce did not stop after renewal failure")
	}
}
