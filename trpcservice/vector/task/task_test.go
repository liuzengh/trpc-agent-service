package task

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

type fakeLeaseStore struct {
	mu       sync.Mutex
	renewErr error
	released int
}

func (s *fakeLeaseStore) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error) {
	return storage.Lease{TenantID: "tenant-a", ResourceID: "task", SessionID: "task", OwnerID: "worker-a", FenceToken: 1, Epoch: 1, Backend: storage.BackendRedis, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (s *fakeLeaseStore) Renew(ctx context.Context, tc tenant.TenantContext, l storage.Lease, ttl time.Duration) (storage.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.renewErr != nil {
		return l, s.renewErr
	}
	l.ExpiresAt = time.Now().Add(ttl)
	return l, nil
}
func (s *fakeLeaseStore) Release(context.Context, tenant.TenantContext, storage.Lease) error {
	s.mu.Lock()
	s.released++
	s.mu.Unlock()
	return nil
}
func (s *fakeLeaseStore) Validate(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}

type fakeStore struct {
	mu               sync.Mutex
	upserts, deletes int
	err              error
}

func (s *fakeStore) Ready(context.Context) error { return nil }
func (s *fakeStore) Upsert(_ context.Context, r vector.UpsertRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts++
	return s.err
}
func (s *fakeStore) Delete(_ context.Context, r vector.DeleteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return s.err
}
func (s *fakeStore) Search(context.Context, vector.SearchRequest) ([]vector.SearchResult, error) {
	return nil, nil
}
func (s *fakeStore) Close(context.Context) error { return nil }

type fakeRepo struct {
	mu                       sync.Mutex
	task                     Task
	candidate                bool
	completeCalls, failCalls int
	lastFailure              Failure
	forceCompleteConflict    bool
	head                     HeadKey
	headOK                   bool
}

func (r *fakeRepo) Enqueue(_ context.Context, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (EnqueueOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, _ := TaskID(ref)
	if r.task.TaskID != "" {
		return EnqueueOutcome{Task: r.task}, nil
	}
	r.task = Task{TenantID: tc.TenantID, TaskID: id, SourceType: ref.SourceType, SourceID: ref.SourceID, ProjectionScope: ref.ProjectionScope, DocumentID: ref.DocumentID, Operation: ref.Operation, SourceVersion: ref.SourceVersion, SourceSequence: ref.SourceSequence, ContentHash: ref.ContentHash, Model: ref.Model, ModelVersion: ref.ModelVersion, Dimension: ref.Dimension, SchemaVersion: ref.SchemaVersion, State: StatePending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
	r.candidate = true
	return EnqueueOutcome{Task: r.task, Created: true}, nil
}
func (r *fakeRepo) Get(context.Context, tenant.TenantContext, string) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.task, nil
}
func (r *fakeRepo) Candidate(_ context.Context, now time.Time) (Candidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.candidate && (r.task.State == StatePending || (r.task.State == StateRetryWait && !r.task.NextAttemptAt.After(now))) {
		return Candidate{TenantID: r.task.TenantID, TaskID: r.task.TaskID}, nil
	}
	return Candidate{}, ErrNotFound
}
func (r *fakeRepo) Claim(_ context.Context, c Candidate, l LeaseRef, now time.Time) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.TaskID != r.task.TaskID || !r.candidate || !l.Valid() {
		return Task{}, ErrNotClaimable
	}
	r.task.State = StateRunning
	r.task.Attempt++
	r.task.LeaseOwner = l.Owner
	r.task.LeaseEpoch = l.Epoch
	r.task.LeaseFence = l.Fence
	r.task.LeaseExpiresAt = l.ExpiresAt
	r.task.ClaimedAt = now
	r.candidate = false
	return r.task, nil
}
func (r *fakeRepo) ExtendLease(_ context.Context, c Candidate, l LeaseRef, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.task.TaskID != c.TaskID || r.task.LeaseOwner != l.Owner || r.task.LeaseFence != l.Fence {
		return ErrConflictOwnership
	}
	r.task.LeaseExpiresAt = l.ExpiresAt
	r.task.UpdatedAt = now
	return nil
}
func (r *fakeRepo) Complete(_ context.Context, c Candidate, l LeaseRef, now time.Time) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.forceCompleteConflict {
		return Task{}, ErrConflictOwnership
	}
	if r.task.TaskID != c.TaskID || r.task.State != StateRunning || r.task.LeaseOwner != l.Owner || r.task.LeaseFence != l.Fence {
		return Task{}, ErrConflictOwnership
	}
	r.task.State = StateSucceeded
	r.task.CompletedAt = now
	r.task.UpdatedAt = now
	r.completeCalls++
	return r.task, nil
}
func (r *fakeRepo) Fail(_ context.Context, c Candidate, l LeaseRef, f Failure, now time.Time) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.task.TaskID != c.TaskID || r.task.State != StateRunning || r.task.LeaseOwner != l.Owner || r.task.LeaseFence != l.Fence {
		return Task{}, ErrConflictOwnership
	}
	r.failCalls++
	r.lastFailure = f
	r.task.LastError = string(f.Category)
	r.task.UpdatedAt = now
	switch f.Kind {
	case FailureRetry:
		if r.task.Attempt >= r.task.MaxAttempts {
			r.task.State = StateDeadLetter
			r.task.CompletedAt = now
			r.task.DeadLetteredAt = now
		} else {
			r.task.State = StateRetryWait
			r.task.NextAttemptAt = f.NextAttempt
			r.candidate = true
		}
	case FailureRelease:
		r.task.State = StatePending
		r.task.NextAttemptAt = now
		r.candidate = true
	case FailureStale:
		r.task.State = StateStale
		r.task.CompletedAt = now
	case FailureDeadLetter:
		r.task.State = StateDeadLetter
		r.task.CompletedAt = now
		r.task.DeadLetteredAt = now
	}
	return r.task, nil
}
func (r *fakeRepo) Cancel(context.Context, tenant.TenantContext, string, time.Time) (Task, error) {
	return Task{}, nil
}
func (r *fakeRepo) Head(_ context.Context, _, _ string) (HeadKey, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head, r.headOK, nil
}
func (r *fakeRepo) Counts(context.Context) (map[State]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[State]int64{r.task.State: 1}, nil
}

func validTaskSource(t *testing.T) (tenant.TenantContext, vector.SourceDocument, vector.VectorDocumentRef) {
	t.Helper()
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-a", ProjectionScope: "memory:user", SourceVersion: 1, SourceSequence: 1, Content: "hello", Model: "test-model", ModelVersion: "v1", Dimension: 8, SchemaVersion: "schema-v1"}
	ctx := tenant.WithContext(context.Background(), tc)
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	return tc, source, ref
}
func testWorker(t *testing.T, repo *fakeRepo, leases *fakeLeaseStore, store *fakeStore, source vector.SourceDocument, clock *fakeClock, max int) (*Worker, error) {
	embed, err := vector.NewDeterministicEmbedder(source.Model, source.ModelVersion, source.Dimension, 1024)
	if err != nil {
		return nil, err
	}
	return NewWorker(Config{WorkerID: "worker-a", Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseTTL: time.Second, LeaseRenewInterval: 100 * time.Millisecond, MaxAttempts: max, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond, TaskTimeout: time.Second, ShutdownTimeout: time.Second}, Dependencies{Repository: repo, Leases: leases, Store: store, Embedder: embed, Source: SourceProjectorFunc(func(context.Context, tenant.TenantContext, Task) (vector.SourceDocument, error) { return source, nil }), Clock: clock})
}

func TestTaskIDStableAndContentIndependent(t *testing.T) {
	_, source, ref := validTaskSource(t)
	id, err := TaskID(ref)
	if err != nil {
		t.Fatal(err)
	}
	source.Content = "changed"
	ctx := tenant.WithContext(context.Background(), tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}})
	newer, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := TaskID(newer)
	if err != nil {
		t.Fatal(err)
	}
	if id != id2 {
		t.Fatalf("content changed task identity: %s != %s", id, id2)
	}
}
func TestHeadDeleteDominatesEqualOrder(t *testing.T) {
	up := HeadKey{Version: 2, Sequence: 3, Operation: vector.OperationUpsert}
	del := HeadKey{Version: 2, Sequence: 3, Operation: vector.OperationDelete}
	if !up.Less(del) || del.Less(up) {
		t.Fatalf("unexpected order up=%v delete=%v", up, del)
	}
}
func TestWorkerCompletesOutsideDatabaseAndStops(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &fakeClock{now: now}
	repo := &fakeRepo{}
	if _, err := repo.Enqueue(context.Background(), tc, ref, now); err != nil {
		t.Fatal(err)
	}
	leases := &fakeLeaseStore{}
	store := &fakeStore{}
	w, err := testWorker(t, repo, leases, store, source, clock, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		repo.mu.Lock()
		done := repo.completeCalls == 1
		repo.mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker did not complete")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = w.Stop(); err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.task.State != StateSucceeded || store.upserts != 1 {
		t.Fatalf("state=%s upserts=%d", repo.task.State, store.upserts)
	}
}
func TestWorkerUnknownProviderOutcomeRetries(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	now := time.Now().UTC()
	clock := &fakeClock{now: now}
	repo := &fakeRepo{}
	if _, err := repo.Enqueue(context.Background(), tc, ref, now); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{err: vector.ErrUnknown}
	w, err := testWorker(t, repo, &fakeLeaseStore{}, store, source, clock, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		repo.mu.Lock()
		done := repo.failCalls == 1
		state := repo.task.State
		repo.mu.Unlock()
		if done {
			if state != StateRetryWait {
				t.Fatalf("state=%s", state)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker did not retry")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = w.Stop(); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerRetryBudgetDeadLetters(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	now := time.Now().UTC()
	clock := &fakeClock{now: now}
	repo := &fakeRepo{}
	out, err := repo.Enqueue(context.Background(), tc, ref, now)
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	repo.task.MaxAttempts = 1
	repo.mu.Unlock()
	_ = out
	w, err := testWorker(t, repo, &fakeLeaseStore{}, &fakeStore{err: vector.ErrUnknown}, source, clock, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		repo.mu.Lock()
		done := repo.failCalls == 1
		state := repo.task.State
		repo.mu.Unlock()
		if done {
			if state != StateDeadLetter || repo.lastFailure.Category != CategoryUnknown {
				t.Fatalf("state=%s category=%s", state, repo.lastFailure.Category)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker did not dead-letter")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = w.Stop(); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerRejectsInvalidDependencies(t *testing.T) {
	_, err := NewWorker(Config{WorkerID: "worker-a", LeaseTTL: time.Second, TaskTimeout: time.Second}, Dependencies{})
	if !errors.Is(err, ErrInvalidWorker) {
		t.Fatalf("err=%v", err)
	}
}

func TestClassifyVectorErrorTable(t *testing.T) {
	cases := []struct {
		err      error
		category Category
		kind     FailureKind
	}{
		{context.Canceled, CategoryCancelled, FailureRelease},
		{context.DeadlineExceeded, CategoryDeadline, FailureRetry},
		{vector.ErrCancelled, CategoryCancelled, FailureRelease},
		{vector.ErrTimeout, CategoryDeadline, FailureRetry},
		{vector.ErrUnknown, CategoryUnknown, FailureRetry},
		{vector.ErrUnavailable, CategoryRetryable, FailureRetry},
		{vector.ErrRetryable, CategoryRetryable, FailureRetry},
		{vector.ErrConflict, CategoryRetryable, FailureRetry},
		{vector.ErrStale, CategoryStaleTask, FailureStale},
		{vector.ErrPermanent, CategoryPermanent, FailureDeadLetter},
		{vector.ErrNotFound, CategoryPermanent, FailureDeadLetter},
		{vector.ErrInvalidDimension, CategoryDimensionMismatch, FailureDeadLetter},
		{vector.ErrInvalidModel, CategoryModelMismatch, FailureDeadLetter},
		{vector.ErrInvalidSchema, CategorySchemaMismatch, FailureDeadLetter},
		{vector.ErrInvalidConfig, CategoryInvalidRequest, FailureDeadLetter},
		{vector.ErrDisabled, CategoryInvalidRequest, FailureDeadLetter},
		{errors.New("provider text"), CategoryUnknown, FailureRetry},
	}
	for _, tc := range cases {
		category, kind := classifyVectorError(tc.err)
		if category != tc.category || kind != tc.kind {
			t.Fatalf("classify(%v)=%s/%s want %s/%s", tc.err, category, kind, tc.category, tc.kind)
		}
	}
	if category, kind := classifyVectorError(nil); category != "" || kind != "" {
		t.Fatalf("nil error classified as %s/%s", category, kind)
	}
}

func TestConfigBackoffDoublesAndCaps(t *testing.T) {
	cfg := Config{WorkerID: "w", LeaseTTL: time.Second, TaskTimeout: time.Second, RetryBackoff: time.Second, MaxRetryBackoff: 5 * time.Second}
	var err error
	if cfg, err = cfg.withDefaults(); err != nil {
		t.Fatal(err)
	}
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 5 * time.Second, 9: 5 * time.Second} {
		if got := cfg.backoff(attempt); got != want {
			t.Fatalf("backoff(%d)=%s want %s", attempt, got, want)
		}
	}
}

func TestWorkerReleasesOnCancellation(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	now := time.Now().UTC()
	clock := &fakeClock{now: now}
	repo := &fakeRepo{}
	if _, err := repo.Enqueue(context.Background(), tc, ref, now); err != nil {
		t.Fatal(err)
	}
	w, err := testWorker(t, repo, &fakeLeaseStore{}, &fakeStore{}, source, clock, 5)
	if err != nil {
		t.Fatal(err)
	}
	w.source = SourceProjectorFunc(func(ctx context.Context, tc tenant.TenantContext, task Task) (vector.SourceDocument, error) {
		return vector.SourceDocument{}, context.Canceled
	})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		repo.mu.Lock()
		done := repo.failCalls >= 1 && repo.task.State == StatePending && repo.lastFailure.Category == CategoryCancelled
		repo.mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			repo.mu.Lock()
			t.Fatalf("worker did not release cancelled task: state=%s failCalls=%d category=%q candidate=%v", repo.task.State, repo.failCalls, repo.lastFailure.Category, repo.candidate)
			repo.mu.Unlock()
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerMarksStaleOnNewerHeadConflict(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	now := time.Now().UTC()
	clock := &fakeClock{now: now}
	repo := &fakeRepo{forceCompleteConflict: true, headOK: true, head: HeadKey{Version: ref.SourceVersion + 1, Sequence: ref.SourceSequence, Operation: vector.OperationUpsert}}
	if _, err := repo.Enqueue(context.Background(), tc, ref, now); err != nil {
		t.Fatal(err)
	}
	w, err := testWorker(t, repo, &fakeLeaseStore{}, &fakeStore{}, source, clock, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		repo.mu.Lock()
		done := repo.failCalls >= 1 && repo.task.State == StateStale && repo.lastFailure.Category == CategoryStaleTask
		repo.mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			repo.mu.Lock()
			t.Fatalf("worker did not mark stale: state=%s failCalls=%d completeCalls=%d category=%q", repo.task.State, repo.failCalls, repo.completeCalls, repo.lastFailure.Category)
			repo.mu.Unlock()
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyLoadedSourceRejectsDrift(t *testing.T) {
	tc, source, ref := validTaskSource(t)
	ctx := tenant.WithContext(context.Background(), tc)
	ref2 := ref
	task := Task{TenantID: tc.TenantID, SourceType: ref.SourceType, SourceID: ref.SourceID, ProjectionScope: ref.ProjectionScope, DocumentID: ref.DocumentID, Operation: ref.Operation, SourceVersion: ref.SourceVersion, SourceSequence: ref.SourceSequence, ContentHash: ref.ContentHash, Model: ref.Model, ModelVersion: ref.ModelVersion, Dimension: ref.Dimension, SchemaVersion: ref.SchemaVersion}
	if failure := verifyLoadedSource(task, source); failure.Kind != "" {
		t.Fatalf("matching source rejected: %+v", failure)
	}
	drifted := source
	drifted.SourceVersion++
	if failure := verifyLoadedSource(task, drifted); failure.Kind != FailureStale {
		t.Fatalf("version drift: %+v", failure)
	}
	drifted = source
	drifted.Model = "other-model"
	if failure := verifyLoadedSource(task, drifted); failure.Kind != FailureDeadLetter || failure.Category != CategoryModelMismatch {
		t.Fatalf("model drift: %+v", failure)
	}
	drifted = source
	drifted.Content = "tampered"
	if failure := verifyLoadedSource(task, drifted); failure.Kind != FailureStale {
		t.Fatalf("content drift: %+v", failure)
	}
	ref2, err := vector.BuildDocumentRef(ctx, drifted)
	if err != nil {
		t.Fatal(err)
	}
	_ = ref2
}

func TestTaskRefRestoresServerIdentity(t *testing.T) {
	_, _, ref := validTaskSource(t)
	task := Task{TenantID: ref.TenantID, SourceType: ref.SourceType, SourceID: ref.SourceID, ProjectionScope: ref.ProjectionScope, DocumentID: ref.DocumentID, Operation: ref.Operation, SourceVersion: ref.SourceVersion, SourceSequence: ref.SourceSequence, ContentHash: ref.ContentHash, Model: ref.Model, ModelVersion: ref.ModelVersion, Dimension: ref.Dimension, SchemaVersion: ref.SchemaVersion}
	restored, err := task.Ref()
	if err != nil {
		t.Fatal(err)
	}
	if restored.DocumentID != ref.DocumentID {
		t.Fatalf("document identity changed: %s != %s", restored.DocumentID, ref.DocumentID)
	}
	broken := task
	broken.ContentHash = "not-hex"
	if _, err := broken.Ref(); err == nil {
		t.Fatal("structurally invalid content hash accepted")
	}
}
