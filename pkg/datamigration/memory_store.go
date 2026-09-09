package datamigration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is a deterministic store for unit tests and single-process
// dry-runs. Production deployments must use a durable Store implementation.
type MemoryStore struct {
	mu    sync.Mutex
	jobs  map[string]Job
	clock func() time.Time
}

func NewMemoryStore(clock func() time.Time) *MemoryStore {
	if clock == nil {
		clock = time.Now
	}
	return &MemoryStore{jobs: make(map[string]Job), clock: clock}
}

func (s *MemoryStore) Create(_ context.Context, job Job) error {
	if s == nil {
		return fmt.Errorf("%w: store is unavailable", ErrMigrationCapability)
	}
	if err := job.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; ok {
		return fmt.Errorf("migration %q already exists", job.ID)
	}
	for _, existing := range s.jobs {
		if existing.TenantID == job.TenantID && existing.Domain == job.Domain && !terminalPhase(existing.Phase) {
			return ErrMigrationConflict
		}
	}
	now := s.clock().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrMigrationNotFound
	}
	return cloneJob(job), nil
}

func (s *MemoryStore) Claim(_ context.Context, id, owner string, ttl time.Duration) (Job, error) {
	if err := validateMigrationLease(owner, ttl); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrMigrationNotFound
	}
	now := s.clock().UTC()
	if terminalPhase(job.Phase) {
		return Job{}, ErrMigrationTerminal
	}
	if job.LeaseOwner != "" && job.LeaseUntil.After(now) {
		return Job{}, ErrMigrationLeaseHeld
	}
	job.LeaseOwner = owner
	job.LeaseVersion++
	job.LeaseUntil = now.Add(ttl)
	job.UpdatedAt = now
	s.jobs[id] = cloneJob(job)
	return cloneJob(job), nil
}

func (s *MemoryStore) Renew(_ context.Context, id, owner string, version int64, ttl time.Duration) (Job, error) {
	if version <= 0 {
		return Job{}, fmt.Errorf("%w: invalid lease version", ErrInvalidMigration)
	}
	if err := validateMigrationLease(owner, ttl); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrMigrationNotFound
	}
	now := s.clock().UTC()
	if err := checkLease(job, owner, version, now); err != nil {
		return Job{}, err
	}
	job.LeaseUntil = now.Add(ttl)
	job.UpdatedAt = now
	s.jobs[id] = cloneJob(job)
	return cloneJob(job), nil
}

func (s *MemoryStore) Advance(_ context.Context, id, owner string, version int64, patch JobPatch) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrMigrationNotFound
	}
	if err := checkLease(job, owner, version, s.clock().UTC()); err != nil {
		return Job{}, err
	}
	previousPhase := job.Phase
	if patch.Phase != nil {
		if !canTransition(job.Phase, *patch.Phase) {
			return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidMigration, job.Phase, *patch.Phase)
		}
		job.Phase = *patch.Phase
	}
	if patch.Paused != nil {
		job.Paused = *patch.Paused
	}
	if patch.Cursor != nil {
		// Phase has already been applied above. These are the only legal phase
		// transitions whose cursor is intentionally reset; canTransition keeps
		// the reset from being smuggled into another state.
		boundaryReset := patch.Phase != nil &&
			((previousPhase == PhaseSnapshotCopy && *patch.Phase == PhaseDualWrite) ||
				(previousPhase == PhaseDualWrite && *patch.Phase == PhaseCatchUp))
		if *patch.Cursor == "" && job.Cursor != "" && !boundaryReset {
			return Job{}, fmt.Errorf("%w: migration cursor cannot be cleared outside the snapshot-to-dual-write boundary", ErrInvalidMigration)
		}
		job.Cursor = *patch.Cursor
	}
	if patch.SnapshotWatermark != nil {
		if *patch.SnapshotWatermark < job.SnapshotWatermark {
			return Job{}, fmt.Errorf("%w: snapshot watermark cannot move backwards", ErrInvalidMigration)
		}
		job.SnapshotWatermark = *patch.SnapshotWatermark
	}
	if patch.AppliedWatermark != nil {
		if *patch.AppliedWatermark < job.AppliedWatermark {
			return Job{}, fmt.Errorf("%w: applied watermark cannot move backwards", ErrInvalidMigration)
		}
		job.AppliedWatermark = *patch.AppliedWatermark
	}
	if patch.LastError != nil {
		job.LastError = sanitizeMigrationErrorText(*patch.LastError)
	}
	job.UpdatedAt = s.clock().UTC()
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	s.jobs[id] = cloneJob(job)
	return cloneJob(job), nil
}

func (s *MemoryStore) Release(_ context.Context, id, owner string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return ErrMigrationNotFound
	}
	if err := checkLease(job, owner, version, s.clock().UTC()); err != nil {
		return err
	}
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = s.clock().UTC()
	s.jobs[id] = cloneJob(job)
	return nil
}

func checkLease(job Job, owner string, version int64, now time.Time) error {
	if owner == "" || job.LeaseOwner != owner || job.LeaseVersion != version || !job.LeaseUntil.After(now) {
		return ErrMigrationFence
	}
	return nil
}

func cloneJob(job Job) Job { return job }
