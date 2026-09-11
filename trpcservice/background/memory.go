package background

import (
	"context"
	"errors"
	"sync"
	"time"
)

type memoryJob struct {
	job         Job
	nextAttempt time.Time
	lockedBy    string
	lockedUntil time.Time
}

type MemoryRepository struct {
	mu     sync.Mutex
	closed bool
	jobs   map[string]*memoryJob
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{jobs: make(map[string]*memoryJob)}
}

func (r *MemoryRepository) Enqueue(
	ctx context.Context,
	request EnqueueRequest,
) (EnqueueResult, error) {
	if err := contextErr(ctx); err != nil {
		return EnqueueResult{}, err
	}
	if err := validateEnqueue(&request); err != nil {
		return EnqueueResult{}, err
	}
	id := StableJobID(request.TenantID, request.Type, request.DedupeKey)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return EnqueueResult{}, ErrClosed
	}
	if existing := r.jobs[id]; existing != nil {
		return EnqueueResult{Job: existing.job, Duplicate: true}, nil
	}
	job := Job{
		ID: id, TenantID: request.TenantID, AppID: request.AppID,
		RevisionID: request.RevisionID, Type: request.Type, DedupeKey: request.DedupeKey,
		Payload: append([]byte(nil), request.Payload...), Status: "pending",
		MaxAttempts: request.MaxAttempts, TraceParent: request.TraceParent, CreatedAt: time.Now().UTC(),
	}
	r.jobs[id] = &memoryJob{job: job, nextAttempt: time.Now()}
	return EnqueueResult{Job: job}, nil
}

func (r *MemoryRepository) Claim(
	ctx context.Context,
	workerID string,
	lease time.Duration,
) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Job{}, ErrClosed
	}
	now := time.Now()
	for _, item := range r.jobs {
		if item.job.Status == "completed" || item.job.Status == "dead" ||
			item.nextAttempt.After(now) ||
			(item.lockedBy != "" && item.lockedUntil.After(now)) {
			continue
		}
		item.job.Status = "running"
		item.job.AttemptCount++
		item.lockedBy = workerID
		item.lockedUntil = now.Add(lease)
		return item.job, nil
	}
	return Job{}, ErrNoJob
}

func (r *MemoryRepository) Complete(_ context.Context, jobID string, workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.jobs[jobID]
	if item == nil || item.lockedBy != workerID {
		return errors.New("background job ownership mismatch")
	}
	item.job.Status = "completed"
	item.job.CompletedAt = time.Now().UTC()
	item.job.LastError = ""
	item.lockedBy = ""
	item.lockedUntil = time.Time{}
	return nil
}

func (r *MemoryRepository) Fail(
	_ context.Context,
	job Job,
	workerID string,
	retryAt time.Time,
	cause error,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.jobs[job.ID]
	if item == nil || item.lockedBy != workerID {
		return errors.New("background job ownership mismatch")
	}
	if item.job.AttemptCount >= item.job.MaxAttempts {
		item.job.Status = "dead"
		item.job.CompletedAt = time.Now().UTC()
	} else {
		item.job.Status = "pending"
		item.nextAttempt = retryAt
	}
	if cause != nil {
		item.job.LastError = cause.Error()
	}
	item.lockedBy = ""
	item.lockedUntil = time.Time{}
	return nil
}

func (r *MemoryRepository) Get(
	ctx context.Context,
	tenantID string,
	jobID string,
) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.jobs[jobID]
	if item == nil || item.job.TenantID != tenantID {
		return Job{}, ErrJobNotFound
	}
	job := item.job
	job.Payload = append([]byte(nil), job.Payload...)
	return job, nil
}

func (r *MemoryRepository) Retry(ctx context.Context, tenantID string, jobID string) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.jobs[jobID]
	if item == nil || item.job.TenantID != tenantID {
		return ErrJobNotFound
	}
	if item.job.Status != "dead" {
		return ErrJobConflict
	}
	item.job.Status = "pending"
	item.job.AttemptCount = 0
	item.job.LastError = ""
	item.job.CompletedAt = time.Time{}
	item.nextAttempt = time.Now()
	return nil
}

func (r *MemoryRepository) Ready(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	return nil
}

func (r *MemoryRepository) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

var _ Repository = (*MemoryRepository)(nil)
