package datamigration

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type memoryStore struct {
	mu  sync.Mutex
	job Job
}

func (s *memoryStore) Create(_ context.Context, job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job = cloneJob(job)
	return nil
}

func (s *memoryStore) Get(_ context.Context, id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.MigrationID != id {
		return Job{}, ErrNotFound
	}
	return cloneJob(s.job), nil
}

func (s *memoryStore) CompareAndSwap(_ context.Context, generation int64, job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.Generation != generation {
		return ErrConflict
	}
	job.Generation = generation + 1
	s.job = cloneJob(job)
	return nil
}

type operatorFunc func(context.Context, Phase, Job) (Result, error)

func (f operatorFunc) ExecutePhase(ctx context.Context, phase Phase, job Job) (Result, error) {
	return f(ctx, phase, job)
}

func testJob() Job {
	return Job{MigrationID: "migration-1", TenantID: "tenant-a", AppName: "assistant",
		ResourceKind: "session", Source: "redis", Target: "sql", Phase: PhasePrepare,
		Status: StatusPending, Checkpoint: map[string]any{}}
}

func TestEngineAdvancesOneIdempotentBatchAtATime(t *testing.T) {
	store := &memoryStore{job: testJob()}
	calls := 0
	engine, err := New(store, operatorFunc(func(_ context.Context, phase Phase, job Job) (Result, error) {
		calls++
		if calls == 1 {
			return Result{Checkpoint: map[string]any{"cursor": "42"}, Copied: 2}, nil
		}
		return Result{Done: true, Copied: 1}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	first, err := engine.Advance(context.Background(), "migration-1")
	if err != nil || first.Phase != PhasePrepare || first.Checkpoint["cursor"] != "42" || first.Copied != 2 {
		t.Fatalf("first batch = %+v, err=%v", first, err)
	}
	second, err := engine.Advance(context.Background(), "migration-1")
	if err != nil || second.Phase != PhaseSnapshot || second.Copied != 3 || len(second.Checkpoint) != 0 {
		t.Fatalf("completed phase = %+v, err=%v", second, err)
	}
}

func TestEngineParksVerificationMismatch(t *testing.T) {
	job := testJob()
	job.Phase = PhaseShadowRead
	store := &memoryStore{job: job}
	engine, _ := New(store, operatorFunc(func(context.Context, Phase, Job) (Result, error) {
		return Result{Done: true, Verified: 5, Mismatches: 1}, nil
	}))
	got, err := engine.Advance(context.Background(), job.MigrationID)
	if !errors.Is(err, ErrVerificationMismatch) || got.Status != StatusPaused || got.Phase != PhaseShadowRead {
		t.Fatalf("mismatch result = %+v, err=%v", got, err)
	}
}

func TestEngineParksMismatchBeforeFinalBatch(t *testing.T) {
	job := testJob()
	job.Phase = PhaseShadowRead
	store := &memoryStore{job: job}
	engine, _ := New(store, operatorFunc(func(context.Context, Phase, Job) (Result, error) {
		return Result{Done: false, Checkpoint: map[string]any{"cursor": "next"}, Verified: 2, Mismatches: 1}, nil
	}))
	got, err := engine.Advance(context.Background(), job.MigrationID)
	if !errors.Is(err, ErrVerificationMismatch) || got.Status != StatusPaused || len(got.Checkpoint) != 0 {
		t.Fatalf("early mismatch result = %+v, err=%v", got, err)
	}
}

func TestEngineDoesNotAdvanceCheckpointOnPartialBatchFailure(t *testing.T) {
	job := testJob()
	job.Phase = PhaseSnapshot
	job.Checkpoint = map[string]any{"cursor": "before"}
	store := &memoryStore{job: job}
	engine, _ := New(store, operatorFunc(func(context.Context, Phase, Job) (Result, error) {
		return Result{Checkpoint: map[string]any{"cursor": "after"}, Copied: 1, Failed: 1}, errors.New("partial failure")
	}))
	got, err := engine.Advance(context.Background(), job.MigrationID)
	if !errors.Is(err, ErrPhaseExecution) || got.Checkpoint["cursor"] != "before" {
		t.Fatalf("partial failure result=%+v err=%v", got, err)
	}
}

func TestEngineRollbackNeverReversesThroughSnapshotPhases(t *testing.T) {
	job := testJob()
	job.Phase = PhaseCutover
	job.Generation = 7
	store := &memoryStore{job: job}
	engine, _ := New(store, operatorFunc(func(_ context.Context, phase Phase, _ Job) (Result, error) {
		if phase != PhaseRollback {
			t.Fatalf("operator phase = %s", phase)
		}
		return Result{Done: true}, nil
	}))
	requested, err := engine.RequestRollback(context.Background(), job.MigrationID)
	if err != nil || requested.Phase != PhaseRollback || requested.Generation != 8 {
		t.Fatalf("rollback request = %+v, err=%v", requested, err)
	}
	done, err := engine.Advance(context.Background(), job.MigrationID)
	if err != nil || done.Status != StatusRolledBack || done.Phase != PhaseComplete {
		t.Fatalf("rollback completion = %+v, err=%v", done, err)
	}
}
