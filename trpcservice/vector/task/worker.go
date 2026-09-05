package task

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

var (
	ErrInvalidWorker    = errors.New("vector task: invalid worker")
	ErrWorkerStarted    = errors.New("vector task: worker already started")
	ErrWorkerNotStarted = errors.New("vector task: worker is not started")
	ErrWorkerStopped    = errors.New("vector task: worker stopped")
	ErrShutdownTimeout  = errors.New("vector task: shutdown timeout")
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Dependencies struct {
	Repository Repository
	Leases     storage.LeaseStore
	Store      vector.VectorStore
	Embedder   vector.EmbeddingProvider
	// Embedders is the optional server-owned projection embedder registry.
	// When present it resolves the embedder per task projection (model,
	// version, dimension); Embedder remains the fallback for projections the
	// registry does not cover. Nil keeps the single-embedder behavior.
	Embedders EmbedderRegistry
	Source    SourceProjector
	Clock     Clock
}

// EmbedderRegistry resolves the production embedder for a server-owned
// projection. Unknown projections fail closed.
type EmbedderRegistry interface {
	EmbedderFor(model, modelVersion string, dimension int) (vector.EmbeddingProvider, error)
}

type Worker struct {
	repo      Repository
	leases    storage.LeaseStore
	store     vector.VectorStore
	embedder  vector.EmbeddingProvider
	embedders EmbedderRegistry
	source    SourceProjector
	clock     Clock
	cfg       Config

	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	done    chan struct{}
	err     error
}

func NewWorker(cfg Config, deps Dependencies) (*Worker, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if deps.Repository == nil || deps.Leases == nil || deps.Store == nil || deps.Embedder == nil || deps.Source == nil {
		return nil, ErrInvalidWorker
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	return &Worker{repo: deps.Repository, leases: deps.Leases, store: deps.Store, embedder: deps.Embedder, embedders: deps.Embedders, source: deps.Source, clock: deps.Clock, cfg: cfg}, nil
}

// Start uses a process-owned background context. StartContext is useful to
// bind worker lifetime to a service context while retaining Stop/Wait.
func (w *Worker) Start() error { return w.StartContext(context.Background()) }
func (w *Worker) StartContext(ctx context.Context) error {
	if w == nil || ctx == nil {
		return ErrInvalidWorker
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return ErrWorkerStarted
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	w.started, w.stop, w.done = true, cancel, make(chan struct{})
	go w.run(workerCtx)
	return nil
}

// Run starts the worker and waits for its terminal result. Context cancellation
// initiates a bounded drain exactly like Stop.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.StartContext(ctx); err != nil {
		return err
	}
	return w.Wait()
}

func (w *Worker) run(ctx context.Context) {
	var workers sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); w.consume(ctx) }()
	}
	workers.Wait()
	w.mu.Lock()
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		w.err = ctx.Err()
	}
	close(w.done)
	w.mu.Unlock()
}

func (w *Worker) consume(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		candidate, err := w.repo.Candidate(ctx, w.clock.Now())
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				if !wait(ctx, w.cfg.PollInterval) {
					return
				}
				continue
			}
			if !wait(ctx, w.cfg.PollInterval) {
				return
			}
			continue
		}
		w.process(ctx, candidate)
	}
}

func (w *Worker) process(parent context.Context, candidate Candidate) {
	if strings.TrimSpace(candidate.TenantID) == "" || strings.TrimSpace(candidate.TaskID) == "" {
		return
	}
	tc := workerTenantContext(candidate.TenantID, w.cfg.WorkerID)
	leaseCtx, cancelLease := context.WithTimeout(parent, w.cfg.TaskTimeout)
	defer cancelLease()
	lease, err := w.leases.Acquire(leaseCtx, tc, candidate.TaskID, w.cfg.WorkerID, w.cfg.LeaseTTL)
	if err != nil {
		return
	}
	ref := LeaseRef{Owner: lease.OwnerID, Epoch: lease.Epoch, Fence: lease.FenceToken, ExpiresAt: lease.ExpiresAt}
	if !ref.Valid() {
		_ = w.releaseLease(tc, lease)
		return
	}
	task, err := w.repo.Claim(leaseCtx, candidate, ref, w.clock.Now())
	if err != nil {
		_ = w.releaseLease(tc, lease)
		return
	}
	w.execute(parent, tc, candidate, task, lease, ref)
}

func (w *Worker) execute(parent context.Context, tc tenant.TenantContext, candidate Candidate, task Task, lease storage.Lease, initial LeaseRef) {
	callCtx, cancel := context.WithTimeout(parent, w.cfg.TaskTimeout)
	defer cancel()
	callCtx = tenant.WithContext(callCtx, tc)
	leaseCtx, stopRenew := context.WithCancel(callCtx)
	var renewMu sync.Mutex
	current := initial
	var renewErr error
	var renewDone = make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(w.cfg.LeaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				updated, err := w.leases.Renew(leaseCtx, tc, lease, w.cfg.LeaseTTL)
				if err == nil {
					lease = updated
					candidateLease := LeaseRef{Owner: updated.OwnerID, Epoch: updated.Epoch, Fence: updated.FenceToken, ExpiresAt: updated.ExpiresAt}
					if err = w.repo.ExtendLease(leaseCtx, candidate, candidateLease, w.clock.Now()); err == nil {
						renewMu.Lock()
						current = candidateLease
						renewMu.Unlock()
						continue
					}
				}
				if leaseCtx.Err() == nil {
					renewMu.Lock()
					renewErr = err
					renewMu.Unlock()
					cancel()
				}
				return
			}
		}
	}()

	failure := Failure{}
	source, err := w.source.Load(callCtx, tc, task)
	if err == nil {
		failure = verifyLoadedSource(task, source)
	}
	embedder := w.embedder
	if err == nil && failure.Kind == "" && w.embedders != nil {
		embedder, err = w.embedders.EmbedderFor(source.Model, source.ModelVersion, source.Dimension)
		if err != nil {
			failure = Failure{Kind: FailureDeadLetter, Category: CategoryModelMismatch}
		}
	}
	if err == nil && failure.Kind == "" {
		switch task.Operation {
		case vector.OperationUpsert:
			var embedding vector.Embedding
			embedding, err = embedder.Embed(callCtx, source.Content)
			if err == nil {
				var request vector.UpsertRequest
				request, err = vector.BuildUpsertRequest(callCtx, source, embedding)
				if err == nil {
					err = w.store.Upsert(callCtx, request)
				}
			}
		case vector.OperationDelete:
			var request vector.DeleteRequest
			request, err = vector.BuildDeleteRequest(callCtx, source)
			if err == nil {
				err = w.store.Delete(callCtx, request)
			}
		default:
			failure = Failure{Kind: FailureDeadLetter, Category: CategoryInvalidRequest}
		}
	}
	stopRenew()
	<-renewDone
	renewMu.Lock()
	leaseFailure := renewErr
	finalLease := current
	renewMu.Unlock()
	if leaseFailure != nil {
		_ = w.releaseLease(tc, lease)
		return
	}
	if callCtx.Err() != nil && err == nil {
		err = callCtx.Err()
	}
	if err != nil && failure.Kind == "" {
		category, kind := classifyVectorError(err)
		failure = Failure{Kind: kind, Category: category}
		if kind == FailureRetry {
			failure.NextAttempt = w.clock.Now().Add(w.cfg.backoff(task.Attempt))
		}
	}
	if failure.Kind != "" {
		if failure.Kind == FailureRetry && failure.NextAttempt.IsZero() {
			failure.NextAttempt = w.clock.Now().Add(w.cfg.backoff(task.Attempt))
		}
		_, _ = w.repo.Fail(context.Background(), candidate, finalLease, failure, w.clock.Now())
		_ = w.releaseLease(tc, lease)
		return
	}
	_, completeErr := w.repo.Complete(context.Background(), candidate, finalLease, w.clock.Now())
	if errors.Is(completeErr, ErrConflictOwnership) {
		if head, ok, headErr := w.repo.Head(context.Background(), task.TenantID, task.DocumentID); headErr == nil && ok {
			currentHead := HeadKey{Version: task.SourceVersion, Sequence: task.SourceSequence, Operation: task.Operation}
			if currentHead.Less(head) {
				_, _ = w.repo.Fail(context.Background(), candidate, finalLease, Failure{Kind: FailureStale, Category: CategoryStaleTask}, w.clock.Now())
			}
		}
	}
	_ = w.releaseLease(tc, lease)
}

func (w *Worker) releaseLease(tc tenant.TenantContext, lease storage.Lease) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.ShutdownTimeout)
	defer cancel()
	return w.leases.Release(ctx, tc, lease)
}

func (w *Worker) Stop() error {
	if w == nil {
		return ErrInvalidWorker
	}
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return ErrWorkerNotStarted
	}
	stop, done := w.stop, w.done
	w.mu.Unlock()
	stop()
	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return ErrShutdownTimeout
	}
}

func (w *Worker) Wait() error {
	if w == nil {
		return ErrInvalidWorker
	}
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return ErrWorkerNotStarted
	}
	done := w.done
	w.mu.Unlock()
	<-done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *Worker) Done() <-chan struct{} {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

func workerTenantContext(tenantID, workerID string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: tenantID, AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", InternalUser: workerID, RequestID: "vector-worker", MessageID: "vector-worker", TraceID: "vector-worker", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
