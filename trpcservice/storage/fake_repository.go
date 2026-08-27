package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type FakeRepository struct {
	mu        sync.Mutex
	sessions  map[string]session.Session
	memories  map[string]memory.Memory
	summaries map[string]session.Summary
	artifacts map[string]artifact.Artifact
	audits    map[string]audit.AuditLog
	claims    map[string]Claim
	leases    map[string]Lease
	outbox    map[string]OutboxMessage
	ownerSeq  uint64
	fenceSeq  uint64
}

func NewFakeRepository() *FakeRepository {
	return &FakeRepository{
		sessions: map[string]session.Session{}, memories: map[string]memory.Memory{}, summaries: map[string]session.Summary{},
		artifacts: map[string]artifact.Artifact{}, audits: map[string]audit.AuditLog{}, claims: map[string]Claim{},
		leases: map[string]Lease{}, outbox: map[string]OutboxMessage{},
	}
}

func (f *FakeRepository) contextOK(ctx context.Context, tc tenant.TenantContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ensureTenantContext(tc.TenantID, tc.Validate)
}

func (f *FakeRepository) Get(ctx context.Context, tc tenant.TenantContext, id string) (session.Session, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return session.Session{}, err
	}
	if err := validateSessionID(id); err != nil {
		return session.Session{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.sessions[tc.TenantID+"\x00"+id]
	if !ok {
		return session.Session{}, ErrNotFound
	}
	return value, nil
}

func (f *FakeRepository) Create(ctx context.Context, tc tenant.TenantContext, value session.Session) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := value.TenantID + "\x00" + value.ID
	if _, exists := f.sessions[key]; exists {
		return ErrConflict
	}
	f.sessions[key] = value
	return nil
}

func (f *FakeRepository) AppendEvent(ctx context.Context, tc tenant.TenantContext, expectedVersion int64, event session.SessionEvent) (session.Session, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return session.Session{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.sessions[event.TenantID+"\x00"+event.SessionID]
	if !ok {
		return session.Session{}, ErrNotFound
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return session.Session{}, err
	}
	if expectedVersion != value.StateVersion {
		return session.Session{}, ErrConflict
	}
	if err := validateEventForSession(value, event); err != nil {
		return session.Session{}, err
	}
	value.LastEventSeq = event.Seq
	value.StateVersion++
	value.UpdatedAt = time.Now().UTC()
	f.sessions[value.TenantID+"\x00"+value.ID] = value
	return value, nil
}

func (f *FakeRepository) AcquireLease(ctx context.Context, tc tenant.TenantContext, id string, ttl time.Duration) (Lease, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return Lease{}, err
	}
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("%w: lease ttl must be positive", ErrInvalidArgument)
	}
	if _, err := f.Get(ctx, tc, id); err != nil {
		return Lease{}, err
	}
	now := time.Now().UTC()
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.leases[tc.TenantID+"\x00"+id]; ok && current.ExpiresAt.After(now) {
		return Lease{}, ErrLeaseLost
	}
	fence := atomic.AddUint64(&f.fenceSeq, 1)
	owner := fmt.Sprintf("fake-worker-%d", atomic.AddUint64(&f.ownerSeq, 1))
	lease := Lease{SessionID: id, OwnerID: owner, FenceToken: fence, ExpiresAt: now.Add(ttl)}
	f.leases[tc.TenantID+"\x00"+id] = lease
	return lease, nil
}

func (f *FakeRepository) Claim(ctx context.Context, tc tenant.TenantContext, externalMessageID string, ttl time.Duration) (Claim, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return Claim{}, err
	}
	if externalMessageID == "" || ttl <= 0 {
		return Claim{}, fmt.Errorf("%w: message id and ttl are required", ErrInvalidArgument)
	}
	key := DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: externalMessageID}
	if err := validateDedupKey(key); err != nil {
		return Claim{}, err
	}
	now := time.Now().UTC()
	f.mu.Lock()
	defer f.mu.Unlock()
	claimKey := dedupKeyString(key)
	if current, ok := f.claims[claimKey]; ok {
		if current.Status == ClaimCompleted {
			return current, nil
		}
		if current.ExpiresAt.After(now) {
			return current, nil
		}
	}
	attempt := 1
	if current, ok := f.claims[claimKey]; ok {
		attempt = current.Attempt + 1
	}
	fence := atomic.AddUint64(&f.fenceSeq, 1)
	claim := Claim{Key: key, Status: ClaimAcquired, OwnerID: fmt.Sprintf("fake-claim-%d", atomic.AddUint64(&f.ownerSeq, 1)), Attempt: attempt, FenceToken: fence, ClaimedAt: now, ExpiresAt: now.Add(ttl)}
	f.claims[claimKey] = claim
	return claim, nil
}

func (f *FakeRepository) Complete(ctx context.Context, tc tenant.TenantContext, key DedupKey, responseRef string, fenceToken uint64) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := validateDedupKey(key); err != nil || responseRef == "" {
		if err != nil {
			return err
		}
		return ErrInvalidArgument
	}
	if err := sameTenant(tc.TenantID, key.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.claims[dedupKeyString(key)]
	if !ok {
		return ErrNotFound
	}
	if current.FenceToken != fenceToken {
		return ErrFenceRejected
	}
	current.Status, current.ResponseRef = ClaimCompleted, responseRef
	f.claims[dedupKeyString(key)] = current
	return nil
}

func (f *FakeRepository) Fail(ctx context.Context, tc tenant.TenantContext, key DedupKey, fenceToken uint64, retryable bool) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := validateDedupKey(key); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, key.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.claims[dedupKeyString(key)]
	if !ok {
		return ErrNotFound
	}
	if current.FenceToken != fenceToken {
		return ErrFenceRejected
	}
	if retryable {
		current.ExpiresAt = time.Now().UTC()
	} else {
		delete(f.claims, dedupKeyString(key))
	}
	if retryable {
		f.claims[dedupKeyString(key)] = current
	}
	return nil
}

func (f *FakeRepository) Put(ctx context.Context, tc tenant.TenantContext, value memory.Memory) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := value.TenantID + "\x00" + value.ID
	if old, ok := f.memories[key]; ok && old.Version >= value.Version {
		return ErrConflict
	}
	f.memories[key] = value
	return nil
}

func (f *FakeRepository) Search(ctx context.Context, tc tenant.TenantContext, query string, limit int) ([]memory.Memory, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidArgument
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]memory.Memory, 0, limit)
	for _, value := range f.memories {
		if value.TenantID == tc.TenantID && !value.Deleted && (query == "" || value.Content == query) {
			result = append(result, value)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (f *FakeRepository) UpsertIfNewer(ctx context.Context, tc tenant.TenantContext, value session.Summary) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := value.TenantID + "\x00" + value.SessionID
	if old, ok := f.summaries[key]; ok && (old.Version > value.Version || old.CoveredSeq > value.CoveredSeq) {
		return ErrConflict
	}
	f.summaries[key] = value
	return nil
}

func (f *FakeRepository) CreateArtifact(ctx context.Context, tc tenant.TenantContext, value artifact.Artifact) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := value.TenantID + "\x00" + value.ID
	if _, ok := f.artifacts[key]; ok {
		return ErrConflict
	}
	f.artifacts[key] = value
	return nil
}

func (f *FakeRepository) PresignedURL(ctx context.Context, tc tenant.TenantContext, id string, ttl time.Duration) (string, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.artifacts[tc.TenantID+"\x00"+id]; !ok {
		return "", ErrNotFound
	}
	return fakeURL(tc.TenantID, id, ttl)
}

func (f *FakeRepository) Append(ctx context.Context, tc tenant.TenantContext, value audit.AuditLog) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	value.Metadata = audit.RedactMetadata(value.Metadata)
	if err := value.Validate(); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := value.TenantID + "\x00" + value.AuditID
	if _, ok := f.audits[key]; ok {
		return ErrConflict
	}
	f.audits[key] = value
	return nil
}

func (f *FakeRepository) Enqueue(ctx context.Context, tc tenant.TenantContext, value OutboxMessage) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	if err := ValidateOutboxMessage(value); err != nil {
		return err
	}
	if err := sameTenant(tc.TenantID, value.TenantID); err != nil {
		return err
	}
	if len(value.Payload) == 0 {
		value.Payload = []byte("{}")
	} else {
		value.Payload = append([]byte(nil), value.Payload...)
	}
	now := time.Now().UTC()
	value.Status = OutboxPending
	value.Attempt = 1
	if value.NextAttempt.IsZero() {
		value.NextAttempt = now
	}
	value.CreatedAt, value.UpdatedAt = now, now
	f.mu.Lock()
	defer f.mu.Unlock()
	key := outboxKey(value.TenantID, value.ID)
	if _, ok := f.outbox[key]; ok {
		return ErrConflict
	}
	if value.DedupKey != "" {
		for _, existing := range f.outbox {
			if existing.TenantID == value.TenantID && existing.DedupKey == value.DedupKey {
				return fmt.Errorf("%w: %w", ErrDedupConflict, ErrConflict)
			}
		}
	}
	f.outbox[key] = value
	return nil
}

func (f *FakeRepository) ClaimBatch(ctx context.Context, tc tenant.TenantContext, workerID string, limit int) ([]OutboxMessage, error) {
	if err := f.contextOK(ctx, tc); err != nil {
		return nil, err
	}
	if workerID == "" || limit < 1 || limit > 100 {
		return nil, ErrInvalidArgument
	}
	now := time.Now().UTC()
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]OutboxMessage, 0, limit)
	keys := make([]string, 0, len(f.outbox))
	for key, value := range f.outbox {
		if value.TenantID == tc.TenantID {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(result) == limit {
			break
		}
		value := f.outbox[key]
		if ((value.Status == OutboxPending || value.Status == OutboxRetry) && !value.NextAttempt.After(now)) || (value.Status == OutboxProcessing && !value.LockedUntil.After(now)) {
			if value.Status == OutboxProcessing {
				value.Attempt++
			}
			value.Status, value.LockedBy, value.LockedUntil = OutboxProcessing, workerID, now.Add(DefaultOutboxLockDuration)
			value.UpdatedAt = now
			f.outbox[key] = value
			result = append(result, value)
		}
	}
	return result, nil
}

func (f *FakeRepository) updateOutbox(ctx context.Context, tc tenant.TenantContext, workerID, id string, mutate func(*OutboxMessage) error) error {
	if err := f.contextOK(ctx, tc); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC()
	key := outboxKey(tc.TenantID, id)
	value, ok := f.outbox[key]
	if !ok {
		return ErrNotFound
	}
	if value.Status == OutboxCompleted {
		return ErrAlreadyCompleted
	}
	if value.Status == OutboxDead {
		return ErrAlreadyDead
	}
	if value.Status != OutboxProcessing {
		return ErrConflict
	}
	if value.LockedBy != workerID {
		return fmt.Errorf("%w: %w", ErrOutboxLockLost, ErrFenceRejected)
	}
	if !value.LockedUntil.After(now) {
		return fmt.Errorf("%w: %w", ErrOutboxLockExpired, ErrLeaseLost)
	}
	if err := mutate(&value); err != nil {
		return err
	}
	value.UpdatedAt = now
	f.outbox[key] = value
	return nil
}

func (f *FakeRepository) MarkCompleted(ctx context.Context, tc tenant.TenantContext, workerID, id string) error {
	return f.updateOutbox(ctx, tc, workerID, id, func(value *OutboxMessage) error { value.Status = OutboxCompleted; return nil })
}

func (f *FakeRepository) MarkRetry(ctx context.Context, tc tenant.TenantContext, workerID, id string, nextAttempt time.Time, errorType string) error {
	if err := ValidateOutboxFailureCode(errorType); err != nil {
		return err
	}
	if nextAttempt.IsZero() {
		return ErrInvalidArgument
	}
	return f.updateOutbox(ctx, tc, workerID, id, func(value *OutboxMessage) error {
		value.Status, value.NextAttempt, value.LastError = OutboxRetry, nextAttempt, errorType
		value.Attempt++
		return nil
	})
}

func (f *FakeRepository) MoveToDLQ(ctx context.Context, tc tenant.TenantContext, workerID, id, reason string) error {
	if err := ValidateOutboxFailureCode(reason); err != nil {
		return err
	}
	return f.updateOutbox(ctx, tc, workerID, id, func(value *OutboxMessage) error { value.Status, value.LastError = OutboxDead, reason; return nil })
}

type FakeArtifactRepository struct {
	*FakeRepository
}

func NewFakeArtifactRepository() *FakeArtifactRepository {
	return &FakeArtifactRepository{FakeRepository: NewFakeRepository()}
}

func (f *FakeArtifactRepository) Create(ctx context.Context, tc tenant.TenantContext, value artifact.Artifact) error {
	return f.CreateArtifact(ctx, tc, value)
}

var _ ArtifactRepository = (*FakeArtifactRepository)(nil)

var _ SessionRepository = (*FakeRepository)(nil)
