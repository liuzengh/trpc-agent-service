package summary

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// MemoryStore is a deterministic, process-local implementation for unit
// tests and dry-runs. It must not be used as the coordination store for a
// horizontally scaled deployment.
type MemoryStore struct {
	mu     sync.Mutex
	clock  func() time.Time
	nextID int64
	byID   map[int64]Job
	byKey  map[Key]int64
}

// ValidateLease checks a claimed job against the current in-memory row. It is
// useful when a MemorySink is used with Processor in a deterministic test.
func (s *MemoryStore) ValidateLease(claimed Job) error {
	if s == nil {
		return ErrStoreUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[claimed.ID]
	if !ok {
		return ErrJobNotFound
	}
	if !validLease(current, claimed, s.clock().UTC()) {
		return ErrStaleLease
	}
	return nil
}

// NewMemoryStore constructs an empty summary job store. A clock can be
// supplied by tests to exercise expiry and retry boundaries deterministically.
func NewMemoryStore(clock func() time.Time) *MemoryStore {
	if clock == nil {
		clock = time.Now
	}
	return &MemoryStore{
		clock: clock,
		byID:  make(map[int64]Job),
		byKey: make(map[Key]int64),
	}
}

func (s *MemoryStore) Enqueue(ctx context.Context, request EnqueueRequest) (EnqueueResult, error) {
	if err := contextErr(ctx); err != nil {
		return EnqueueResult{}, err
	}
	if err := request.Validate(); err != nil {
		return EnqueueResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock().UTC()
	if id, ok := s.byKey[request.Key]; ok {
		job := s.byID[id]
		if _, err := mergeEnqueue(&job, request, now); err != nil {
			return EnqueueResult{}, err
		}
		s.byID[id] = job
		return EnqueueResult{Job: cloneJob(job), Coalesced: true}, nil
	}

	s.nextID++
	maxAttempts := request.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	job := Job{
		ID:                  s.nextID,
		Key:                 request.Key,
		AgentVersionID:      request.AgentVersionID,
		TargetEventSequence: request.TargetEventSequence,
		Status:              StatusPending,
		MaxAttempts:         maxAttempts,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if request.TargetEventSequence == 0 {
		job.TargetResolutionLeaseVersion = 1
	}
	if err := job.Validate(); err != nil {
		return EnqueueResult{}, err
	}
	s.byID[job.ID] = job
	s.byKey[job.Key] = job.ID
	return EnqueueResult{Job: cloneJob(job), Created: true}, nil
}

func (s *MemoryStore) Get(ctx context.Context, id int64) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.byID[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return cloneJob(job), nil
}

func (s *MemoryStore) Claim(ctx context.Context, owner string, ttl time.Duration) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	if err := validateLeaseRequest(owner, ttl); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock().UTC()
	for id := int64(1); id <= s.nextID; id++ {
		job, ok := s.byID[id]
		if !ok || !claimable(job, now) {
			continue
		}
		job.Status = StatusProcessing
		job.Attempts++
		job.LeaseOwner = owner
		job.LeaseVersion++
		job.LeaseUntil = now.Add(ttl)
		job.NextAttemptAt = time.Time{}
		job.UpdatedAt = now
		s.byID[id] = job
		return cloneJob(job), nil
	}
	return Job{}, ErrNoWork
}

func (s *MemoryStore) Renew(ctx context.Context, claimed Job, ttl time.Duration) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	if err := validateLeaseRequest(claimed.LeaseOwner, ttl); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[claimed.ID]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	now := s.clock().UTC()
	if !validLease(current, claimed, now) {
		return Job{}, ErrStaleLease
	}
	current.LeaseUntil = now.Add(ttl)
	current.UpdatedAt = now
	s.byID[current.ID] = current
	return cloneJob(current), nil
}

func (s *MemoryStore) ResolveTarget(ctx context.Context, claimed Job, sequence int64) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	if sequence <= 0 {
		return Job{}, fmt.Errorf("%w: resolved target event sequence", ErrInvalidJob)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[claimed.ID]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	now := s.clock().UTC()
	if !validLease(current, claimed, now) {
		return Job{}, ErrStaleLease
	}
	if current.TargetEventSequence == 0 || (claimed.TargetResolutionLeaseVersion > 0 && current.TargetResolutionLeaseVersion > 0) {
		current.TargetEventSequence = max(current.TargetEventSequence, sequence)
		if current.TargetResolutionLeaseVersion <= claimed.LeaseVersion {
			current.TargetResolutionLeaseVersion = 0
		}
		current.UpdatedAt = now
		if err := current.Validate(); err != nil {
			return Job{}, err
		}
		s.byID[current.ID] = current
	}
	return cloneJob(current), nil
}

func (s *MemoryStore) Fail(ctx context.Context, claimed Job, cause error, retryAt time.Time) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[claimed.ID]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	now := s.clock().UTC()
	if !validLease(current, claimed, now) {
		return Job{}, ErrStaleLease
	}
	current.Status = StatusFailed
	current.LastError = errorText(cause)
	current.LeaseOwner = ""
	current.LeaseUntil = time.Time{}
	if current.Attempts < current.MaxAttempts && retryAt.After(now) {
		current.NextAttemptAt = retryAt.UTC()
	} else {
		current.NextAttemptAt = time.Time{}
	}
	current.UpdatedAt = now
	s.byID[current.ID] = current
	return cloneJob(current), nil
}

func (s *MemoryStore) Complete(ctx context.Context, claimed Job, observedSequence int64) (Job, error) {
	if err := contextErr(ctx); err != nil {
		return Job{}, err
	}
	if observedSequence < 0 {
		return Job{}, fmt.Errorf("%w: observed sequence", ErrInvalidCandidate)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[claimed.ID]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	now := s.clock().UTC()
	if !validLease(current, claimed, now) {
		return Job{}, ErrStaleLease
	}
	if observedSequence < current.CompletedEventSequence {
		return Job{}, ErrSummaryStale
	}
	if observedSequence >= current.TargetEventSequence && current.TargetResolutionLeaseVersion == 0 {
		current.Status = StatusCompleted
	} else {
		current.Status = StatusPending
		current.Attempts = 0
		current.NextAttemptAt = time.Time{}
	}
	if observedSequence > current.CompletedEventSequence {
		current.CompletedEventSequence = observedSequence
	}
	current.LastError = ""
	current.LeaseOwner = ""
	current.LeaseUntil = time.Time{}
	current.UpdatedAt = now
	if err := current.Validate(); err != nil {
		return Job{}, err
	}
	s.byID[current.ID] = current
	return cloneJob(current), nil
}

func claimable(job Job, now time.Time) bool {
	if job.Attempts >= job.MaxAttempts || job.Status == StatusCompleted {
		return false
	}
	switch job.Status {
	case StatusPending:
		return true
	case StatusFailed:
		return job.NextAttemptAt.IsZero() || !job.NextAttemptAt.After(now)
	case StatusProcessing:
		return !job.LeaseUntil.After(now)
	default:
		return false
	}
}

func validLease(current, claimed Job, now time.Time) bool {
	return current.Status == StatusProcessing &&
		current.LeaseOwner != "" && current.LeaseOwner == claimed.LeaseOwner &&
		current.LeaseVersion == claimed.LeaseVersion && current.LeaseUntil.After(now)
}

func validateLeaseRequest(owner string, ttl time.Duration) error {
	if owner == "" || len(owner) > 128 || !utf8.ValidString(owner) || strings.ContainsAny(owner, "\x00\r\n") ||
		ttl < time.Millisecond || ttl > 24*time.Hour {
		return fmt.Errorf("%w: invalid lease owner or duration", ErrInvalidJob)
	}
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func cloneJob(job Job) Job { return job }

// MemorySink is a CAS-protected checkpoint sink for tests and local dry-runs.
type MemorySink struct {
	mu            sync.Mutex
	clock         func() time.Time
	values        map[Key]Checkpoint
	validateLease func(Job) error
}

func NewMemorySink(clock func() time.Time) *MemorySink {
	return NewMemorySinkWithLeaseValidator(clock, nil)
}

// NewMemorySinkWithLeaseValidator adds an optional job-store fence check to
// the in-memory sink. Without a validator, PublishFenced still checks the
// shape and expiry of the supplied lease but cannot observe replacement
// claims made by another process.
func NewMemorySinkWithLeaseValidator(clock func() time.Time, validator func(Job) error) *MemorySink {
	if clock == nil {
		clock = time.Now
	}
	return &MemorySink{clock: clock, values: make(map[Key]Checkpoint), validateLease: validator}
}

func (s *MemorySink) PublishFenced(ctx context.Context, candidate Candidate, claimed Job) (PublishResult, error) {
	if claimed.Status != StatusProcessing || claimed.LeaseOwner == "" ||
		!claimed.LeaseUntil.After(s.clock().UTC()) {
		return PublishResult{}, ErrStaleLease
	}
	if s.validateLease != nil {
		if err := s.validateLease(claimed); err != nil {
			return PublishResult{}, err
		}
	}
	return s.Publish(ctx, candidate)
}

func (s *MemorySink) Publish(ctx context.Context, candidate Candidate) (PublishResult, error) {
	if err := contextErr(ctx); err != nil {
		return PublishResult{}, err
	}
	if err := candidate.Validate(); err != nil {
		return PublishResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.values[candidate.Key]
	if ok {
		if candidate.EventSequence < current.EventSequence {
			return PublishResult{Checkpoint: current}, ErrSummaryStale
		}
		if candidate.EventSequence == current.EventSequence {
			if !strings.EqualFold(candidate.ContentSHA256, current.ContentSHA256) ||
				!candidate.CutoffAt.Equal(current.CutoffAt) || candidate.LastEventID != current.LastEventID {
				return PublishResult{Checkpoint: current}, ErrSummaryConflict
			}
			return PublishResult{Checkpoint: current}, nil
		}
	}
	checkpoint := Checkpoint{
		Key: candidate.Key, EventSequence: candidate.EventSequence,
		Content: candidate.Content, ContentSHA256: candidate.ContentSHA256,
		CutoffAt: candidate.CutoffAt, LastEventID: candidate.LastEventID,
		UpdatedAt: s.clock().UTC(),
	}
	s.values[candidate.Key] = checkpoint
	return PublishResult{Checkpoint: checkpoint, Applied: true}, nil
}

func (s *MemorySink) Get(ctx context.Context, key Key) (Checkpoint, bool, error) {
	if err := contextErr(ctx); err != nil {
		return Checkpoint{}, false, err
	}
	if err := key.Validate(); err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint, ok := s.values[key]
	return checkpoint, ok, nil
}
