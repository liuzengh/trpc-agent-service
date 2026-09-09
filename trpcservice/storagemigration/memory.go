package storagemigration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is an offline/test job store; production always uses SQLStore.
type MemoryStore struct {
	mu   sync.Mutex
	jobs map[string]Job
	now  func() time.Time
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{jobs: make(map[string]Job), now: time.Now} }

func (store *MemoryStore) Create(_ context.Context, job Job) (Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, existing := range store.jobs {
		if existing.TenantID == job.TenantID && existing.AppID == job.AppID && existing.ConfigVersion == job.ConfigVersion && existing.Domain == job.Domain {
			return Job{}, ErrConflict
		}
	}
	key := memoryKey(job.TenantID, job.JobID)
	if _, ok := store.jobs[key]; ok {
		return Job{}, ErrConflict
	}
	now := store.now().UTC()
	job.Status = StatusPending
	job.CreatedAt = now
	job.UpdatedAt = now
	store.jobs[key] = cloneJob(job)
	return cloneJob(job), nil
}
func (store *MemoryStore) List(_ context.Context, tenantID string) ([]Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var result []Job
	for _, job := range store.jobs {
		if job.TenantID == tenantID {
			result = append(result, cloneJob(job))
		}
	}
	return result, nil
}
func (store *MemoryStore) Get(_ context.Context, tenantID, jobID string) (Job, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	job, ok := store.jobs[memoryKey(tenantID, jobID)]
	if !ok {
		return Job{}, ErrNotFound
	}
	return cloneJob(job), nil
}
func (store *MemoryStore) Claim(_ context.Context, owner string, ttl time.Duration) (Job, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now().UTC()
	for key, job := range store.jobs {
		if job.Status == StatusPending || (job.Status == StatusRetrying && !job.LeaseUntil.After(now)) || (job.Status == StatusRunning && !job.LeaseUntil.After(now)) {
			job.Status = StatusRunning
			job.Attempts++
			job.ClaimOwner = owner
			job.ClaimToken = fmt.Sprintf("claim-%d", job.Attempts)
			job.LeaseUntil = now.Add(ttl)
			job.UpdatedAt = now
			store.jobs[key] = job
			return cloneJob(job), true, nil
		}
	}
	return Job{}, false, nil
}
func (store *MemoryStore) Save(_ context.Context, claim Job, progress Progress) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := memoryKey(claim.TenantID, claim.JobID)
	job, ok := store.jobs[key]
	if !ok || job.ClaimOwner != claim.ClaimOwner || job.ClaimToken != claim.ClaimToken {
		return ErrClaim
	}
	job.Checkpoint = append([]byte(nil), progress.Checkpoint...)
	job.SourceRows = progress.SourceRows
	job.CopiedRows = progress.CopiedRows
	job.Status = StatusPending
	job.ClaimOwner = ""
	job.ClaimToken = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = store.now().UTC()
	if progress.Done {
		job.Status = StatusCompleted
		completed := job.UpdatedAt
		job.CompletedAt = &completed
	}
	store.jobs[key] = job
	return nil
}
func (store *MemoryStore) Fail(_ context.Context, claim Job, cause error, retryAt time.Time, maxAttempts int) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := memoryKey(claim.TenantID, claim.JobID)
	job, ok := store.jobs[key]
	if !ok || job.ClaimToken != claim.ClaimToken {
		return ErrClaim
	}
	job.Status = StatusRetrying
	if job.Attempts >= maxAttempts {
		job.Status = StatusFailed
	}
	job.LastErrorType = fmt.Sprintf("%T", cause)
	job.LeaseUntil = retryAt
	job.ClaimOwner = ""
	job.ClaimToken = ""
	store.jobs[key] = job
	return nil
}
func (store *MemoryStore) Cancel(_ context.Context, tenantID, jobID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := memoryKey(tenantID, jobID)
	job, ok := store.jobs[key]
	if !ok {
		return ErrNotFound
	}
	if job.Status == StatusRunning || job.Status == StatusCompleted {
		return ErrConflict
	}
	job.Status = StatusCanceled
	store.jobs[key] = job
	return nil
}
func memoryKey(tenantID, jobID string) string { return tenantID + "\x00" + jobID }
func cloneJob(job Job) Job {
	job.Source = job.Source.Clone()
	job.Target = job.Target.Clone()
	job.Checkpoint = append([]byte(nil), job.Checkpoint...)
	if job.CompletedAt != nil {
		value := *job.CompletedAt
		job.CompletedAt = &value
	}
	return job
}

var _ Store = (*MemoryStore)(nil)
