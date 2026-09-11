package tooloperation

import (
	"context"
	"sort"
	"sync"
	"time"
)

type memoryEntry struct {
	record         Record
	leaseOwnerHash string
	attempts       map[int]*Attempt
}

// Memory is a deterministic, concurrency-safe Ledger implementation. It is
// suitable for tests and single-process development; production callers use
// the PostgreSQL implementation so fences survive process failure.
type Memory struct {
	mu          sync.Mutex
	entries     map[string]*memoryEntry
	resolutions map[string]Resolution
}

// NewMemory creates an empty in-memory operation ledger.
func NewMemory() *Memory {
	return &Memory{
		entries:     make(map[string]*memoryEntry),
		resolutions: make(map[string]Resolution),
	}
}

func (m *Memory) Reserve(
	_ context.Context,
	req ReserveRequest,
	now time.Time,
) (*ReserveResult, error) {
	if err := validateReserve(req); err != nil || now.IsZero() {
		return nil, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	key := operationKey(req.TenantID, req.OperationKey)
	if existing := m.entries[key]; existing != nil {
		if existing.record.PayloadHash != req.PayloadHash || !sameMetadata(existing.record.Metadata, req.Metadata) {
			return nil, ErrConflict
		}
		record := cloneRecord(existing.record)
		result := &ReserveResult{Record: record, Existing: true}
		if record.State == StateConfirmed {
			result.Replayed = true
			result.Confirmation = cloneConfirmation(record.Confirmation)
		}
		return result, nil
	}

	record := Record{
		TenantID:      req.TenantID,
		OperationKey:  req.OperationKey,
		PayloadHash:   req.PayloadHash,
		Metadata:      req.Metadata,
		State:         StateReserved,
		StateVersion:  1,
		NextAttemptAt: now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	m.entries[key] = &memoryEntry{record: record, attempts: make(map[int]*Attempt)}
	return &ReserveResult{Record: cloneRecord(record)}, nil
}

func (m *Memory) Lease(_ context.Context, req LeaseRequest) ([]LeasedOperation, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	type candidate struct {
		key   string
		entry *memoryEntry
	}
	candidates := make([]candidate, 0)
	for key, entry := range m.entries {
		record := &entry.record
		if record.TenantID != req.TenantID || (record.State != StateReserved && record.State != StateRetryableNotApplied) {
			continue
		}
		if !record.NextAttemptAt.IsZero() && record.NextAttemptAt.After(req.Now) {
			continue
		}
		if !record.LeaseExpiresAt.IsZero() {
			if record.LeaseExpiresAt.After(req.Now) {
				continue
			}
			// An expired non-executing attempt is proved not applied. Finalize
			// its history before issuing the next fencing number.
			m.expireReservedAttempt(entry, req.Now)
		}
		candidates = append(candidates, candidate{key: key, entry: entry})
	}
	sort.Slice(candidates, func(i, j int) bool {
		a := candidates[i].entry.record
		b := candidates[j].entry.record
		if !a.NextAttemptAt.Equal(b.NextAttemptAt) {
			return a.NextAttemptAt.Before(b.NextAttemptAt)
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return candidates[i].key < candidates[j].key
	})
	if len(candidates) > req.Limit {
		candidates = candidates[:req.Limit]
	}

	ownerDigest := ownerHash(req.Owner)
	leased := make([]LeasedOperation, 0, len(candidates))
	for _, candidate := range candidates {
		leased = append(leased, leaseMemoryEntry(candidate.entry, req.Owner, ownerDigest, req.Now, req.TTL))
	}
	return leased, nil
}

func (m *Memory) LeaseOperation(
	_ context.Context,
	tenantID, operationID, owner string,
	now time.Time,
	ttl time.Duration,
) (*LeasedOperation, error) {
	if err := validateLeaseOperation(tenantID, operationID, owner, now, ttl); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.entries[operationKey(tenantID, operationID)]
	if entry == nil {
		return nil, ErrNotFound
	}
	if entry.record.State == StateUnknown {
		return nil, ErrUnknownRequiresResolution
	}
	if entry.record.State != StateReserved && entry.record.State != StateRetryableNotApplied {
		return nil, ErrInvalidTransition
	}
	if !entry.record.NextAttemptAt.IsZero() && entry.record.NextAttemptAt.After(now) {
		return nil, ErrInvalidTransition
	}
	if !entry.record.LeaseExpiresAt.IsZero() {
		if entry.record.LeaseExpiresAt.After(now) {
			return nil, ErrInvalidTransition
		}
		m.expireReservedAttempt(entry, now)
	}

	leased := leaseMemoryEntry(entry, owner, ownerHash(owner), now, ttl)
	return &leased, nil
}

func leaseMemoryEntry(
	entry *memoryEntry,
	owner, ownerDigest string,
	now time.Time,
	ttl time.Duration,
) LeasedOperation {
	record := &entry.record
	record.State = StateReserved
	record.AttemptCount++
	record.CurrentAttempt = record.AttemptCount
	record.LeaseExpiresAt = now.Add(ttl)
	record.NextAttemptAt = time.Time{}
	record.OutcomeCode = ""
	record.StateVersion++
	record.UpdatedAt = now
	entry.leaseOwnerHash = ownerDigest
	entry.attempts[record.CurrentAttempt] = &Attempt{
		TenantID:     record.TenantID,
		OperationKey: record.OperationKey,
		AttemptNo:    record.CurrentAttempt,
		OwnerHash:    ownerDigest,
		Phase:        AttemptReserved,
		StartedAt:    now,
	}
	return LeasedOperation{
		Record: cloneRecord(*record),
		Fence: Fence{
			TenantID:     record.TenantID,
			OperationKey: record.OperationKey,
			AttemptNo:    record.CurrentAttempt,
			Owner:        owner,
		},
	}
}

func (m *Memory) Renew(_ context.Context, fence Fence, now time.Time, ttl time.Duration) error {
	if err := validateFence(fence); err != nil || now.IsZero() || ttl <= 0 {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.activeEntry(fence, now)
	if err != nil {
		return err
	}
	entry.record.LeaseExpiresAt = now.Add(ttl)
	entry.record.UpdatedAt = now
	return nil
}

func (m *Memory) MarkExecuting(_ context.Context, fence Fence, now time.Time) (*Record, error) {
	if err := validateFence(fence); err != nil || now.IsZero() {
		return nil, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.activeEntry(fence, now)
	if err != nil {
		return nil, err
	}
	attempt := entry.attempts[fence.AttemptNo]
	if attempt == nil || attempt.OwnerHash != ownerHash(fence.Owner) {
		return nil, ErrLeaseLost
	}
	switch entry.record.State {
	case StateReserved:
		if attempt.Phase != AttemptReserved {
			return nil, ErrInvalidTransition
		}
		entry.record.State = StateExecuting
		entry.record.StateVersion++
		entry.record.UpdatedAt = now
		attempt.Phase = AttemptExecuting
		attempt.ExecutingAt = now
	case StateExecuting:
		if attempt.Phase != AttemptExecuting {
			return nil, ErrInvalidTransition
		}
		// Replaying an ambiguous database acknowledgement is safe: the first
		// external request could only start after this transition committed.
	default:
		return nil, ErrInvalidTransition
	}
	record := cloneRecord(entry.record)
	return &record, nil
}

func (m *Memory) Finish(_ context.Context, req FinishRequest, now time.Time) (*Record, error) {
	if err := validateFinish(req, now); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.entries[operationKey(req.Fence.TenantID, req.Fence.OperationKey)]
	if entry == nil {
		return nil, ErrNotFound
	}
	attempt := entry.attempts[req.Fence.AttemptNo]
	if attempt != nil && attempt.Phase == AttemptFinished {
		if entry.record.CurrentAttempt == req.Fence.AttemptNo &&
			attempt.OwnerHash == ownerHash(req.Fence.Owner) && finishMatches(attempt, req) &&
			entry.record.State == req.Outcome {
			record := cloneRecord(entry.record)
			return &record, nil
		}
		return nil, ErrInvalidTransition
	}
	if _, err := m.activeEntry(req.Fence, now); err != nil {
		return nil, err
	}
	if attempt == nil || attempt.OwnerHash != ownerHash(req.Fence.Owner) {
		return nil, ErrLeaseLost
	}
	if entry.record.State == StateReserved {
		if req.Outcome != StateRetryableNotApplied && req.Outcome != StatePermanentRejected {
			return nil, ErrInvalidTransition
		}
	} else if entry.record.State != StateExecuting {
		return nil, ErrInvalidTransition
	}

	attempt.Phase = AttemptFinished
	attempt.Outcome = req.Outcome
	attempt.OutcomeCode = req.OutcomeCode
	attempt.ResultHash = req.ResultHash
	attempt.ReplayReference = req.ReplayReference
	attempt.FinishedAt = now
	attempt.RetryAt = req.RetryAt
	entry.record.State = req.Outcome
	entry.record.StateVersion++
	entry.record.LeaseExpiresAt = time.Time{}
	entry.record.NextAttemptAt = time.Time{}
	entry.record.OutcomeCode = req.OutcomeCode
	entry.record.Confirmation = nil
	entry.record.UpdatedAt = now
	entry.leaseOwnerHash = ""
	switch req.Outcome {
	case StateConfirmed:
		entry.record.Confirmation = &Confirmation{
			ResultHash:      req.ResultHash,
			ReplayReference: req.ReplayReference,
			OutcomeCode:     req.OutcomeCode,
		}
	case StateRetryableNotApplied:
		entry.record.NextAttemptAt = req.RetryAt
	}
	record := cloneRecord(entry.record)
	return &record, nil
}

func (m *Memory) Get(_ context.Context, tenantID, operationID string) (*Record, error) {
	if !validTenant(tenantID) || !validOperationKey(operationID) {
		return nil, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.entries[operationKey(tenantID, operationID)]
	if entry == nil {
		return nil, ErrNotFound
	}
	record := cloneRecord(entry.record)
	return &record, nil
}

func (m *Memory) GetAttempt(
	_ context.Context,
	tenantID, operationID string,
	attemptNo int,
) (*Attempt, error) {
	if !validTenant(tenantID) || !validOperationKey(operationID) || attemptNo <= 0 {
		return nil, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.entries[operationKey(tenantID, operationID)]
	if entry == nil {
		return nil, ErrNotFound
	}
	attempt := entry.attempts[attemptNo]
	if attempt == nil {
		return nil, ErrNotFound
	}
	clone := *attempt
	return &clone, nil
}

func (m *Memory) ReclaimExpired(_ context.Context, now time.Time, limit int) (ReclaimResult, error) {
	if now.IsZero() || limit <= 0 || limit > maxLeaseBatch {
		return ReclaimResult{}, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]*memoryEntry, 0)
	for _, entry := range m.entries {
		if entry.record.LeaseExpiresAt.IsZero() || entry.record.LeaseExpiresAt.After(now) {
			continue
		}
		if entry.record.State == StateReserved || entry.record.State == StateExecuting {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].record, entries[j].record
		if !a.LeaseExpiresAt.Equal(b.LeaseExpiresAt) {
			return a.LeaseExpiresAt.Before(b.LeaseExpiresAt)
		}
		if a.TenantID != b.TenantID {
			return a.TenantID < b.TenantID
		}
		return a.OperationKey < b.OperationKey
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}

	result := ReclaimResult{}
	for _, entry := range entries {
		if entry.record.State == StateReserved {
			m.expireReservedAttempt(entry, now)
			result.Retryable++
			continue
		}
		attempt := entry.attempts[entry.record.CurrentAttempt]
		if attempt == nil || attempt.Phase != AttemptExecuting {
			// Corrupt state must fail closed. Persisting unknown is safer than
			// reconstructing a possibly emitted side effect.
			if attempt == nil {
				attempt = &Attempt{
					TenantID: entry.record.TenantID, OperationKey: entry.record.OperationKey,
					AttemptNo: entry.record.CurrentAttempt, OwnerHash: entry.leaseOwnerHash,
					StartedAt: now,
				}
				entry.attempts[entry.record.CurrentAttempt] = attempt
			}
		}
		attempt.Phase = AttemptFinished
		attempt.Outcome = StateUnknown
		attempt.OutcomeCode = leaseExpiredAfterExec
		attempt.FinishedAt = now
		entry.record.State = StateUnknown
		entry.record.StateVersion++
		entry.record.LeaseExpiresAt = time.Time{}
		entry.record.NextAttemptAt = time.Time{}
		entry.record.OutcomeCode = leaseExpiredAfterExec
		entry.record.UpdatedAt = now
		entry.leaseOwnerHash = ""
		result.Unknown++
	}
	return result, nil
}

func (m *Memory) ListUnknown(_ context.Context, tenantID string, limit int) ([]Record, error) {
	if !validTenant(tenantID) || limit <= 0 || limit > maxLeaseBatch {
		return nil, ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	records := make([]Record, 0)
	for _, entry := range m.entries {
		if entry.record.TenantID == tenantID && entry.record.State == StateUnknown {
			records = append(records, cloneRecord(entry.record))
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if !records[i].UpdatedAt.Equal(records[j].UpdatedAt) {
			return records[i].UpdatedAt.Before(records[j].UpdatedAt)
		}
		return records[i].OperationKey < records[j].OperationKey
	})
	if len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (m *Memory) ResolveUnknown(
	_ context.Context,
	req ResolveRequest,
	now time.Time,
) (*Record, error) {
	if err := validateResolve(req, now); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	resolutionID := resolutionKey(req.TenantID, req.ResolutionID)
	if previous, ok := m.resolutions[resolutionID]; ok {
		if !sameResolutionRequest(previous, req) {
			return nil, ErrConflict
		}
		entry := m.entries[operationKey(req.TenantID, req.OperationKey)]
		if entry == nil {
			return nil, ErrNotFound
		}
		if entry.record.StateVersion != previous.ToVersion {
			return nil, ErrInvalidTransition
		}
		record := cloneRecord(entry.record)
		return &record, nil
	}

	entry := m.entries[operationKey(req.TenantID, req.OperationKey)]
	if entry == nil {
		return nil, ErrNotFound
	}
	if entry.record.State != StateUnknown {
		return nil, ErrUnknownRequiresResolution
	}
	if entry.record.StateVersion != req.ExpectedVersion {
		return nil, ErrInvalidTransition
	}

	fromVersion := entry.record.StateVersion
	entry.record.StateVersion++
	entry.record.OutcomeCode = req.ReasonCode
	entry.record.Confirmation = nil
	entry.record.NextAttemptAt = time.Time{}
	entry.record.UpdatedAt = now
	switch req.Action {
	case ResolveConfirm:
		entry.record.State = StateConfirmed
		entry.record.Confirmation = &Confirmation{
			ResultHash: req.ResultHash, ReplayReference: req.ReplayReference,
			OutcomeCode: req.ReasonCode,
		}
	case ResolveRetryNotApplied:
		entry.record.State = StateRetryableNotApplied
		entry.record.NextAttemptAt = req.RetryAt
	case ResolveReject:
		entry.record.State = StatePermanentRejected
	}
	m.resolutions[resolutionID] = Resolution{
		ResolutionID: req.ResolutionID, TenantID: req.TenantID, OperationKey: req.OperationKey,
		FromVersion: fromVersion, ToVersion: entry.record.StateVersion, Action: req.Action,
		ActorHash: req.ActorHash, ReasonCode: req.ReasonCode, ResultHash: req.ResultHash,
		ReplayReference: req.ReplayReference, ResolvedAt: now,
		RetryAt: req.RetryAt,
	}
	record := cloneRecord(entry.record)
	return &record, nil
}

func (m *Memory) activeEntry(fence Fence, now time.Time) (*memoryEntry, error) {
	entry := m.entries[operationKey(fence.TenantID, fence.OperationKey)]
	if entry == nil {
		return nil, ErrNotFound
	}
	if entry.record.CurrentAttempt != fence.AttemptNo || entry.leaseOwnerHash != ownerHash(fence.Owner) ||
		entry.record.LeaseExpiresAt.IsZero() || !entry.record.LeaseExpiresAt.After(now) {
		return nil, ErrLeaseLost
	}
	if entry.record.State != StateReserved && entry.record.State != StateExecuting {
		return nil, ErrLeaseLost
	}
	return entry, nil
}

func (m *Memory) expireReservedAttempt(entry *memoryEntry, now time.Time) {
	attempt := entry.attempts[entry.record.CurrentAttempt]
	if attempt != nil && attempt.Phase != AttemptFinished {
		attempt.Phase = AttemptFinished
		attempt.Outcome = StateRetryableNotApplied
		attempt.OutcomeCode = leaseExpiredBeforeExec
		attempt.FinishedAt = now
		attempt.RetryAt = now
	}
	entry.record.State = StateReserved
	entry.record.StateVersion++
	entry.record.LeaseExpiresAt = time.Time{}
	entry.record.NextAttemptAt = now
	entry.record.OutcomeCode = leaseExpiredBeforeExec
	entry.record.UpdatedAt = now
	entry.leaseOwnerHash = ""
}

func finishMatches(attempt *Attempt, req FinishRequest) bool {
	return attempt.Outcome == req.Outcome && attempt.OutcomeCode == req.OutcomeCode &&
		attempt.ResultHash == req.ResultHash && attempt.ReplayReference == req.ReplayReference &&
		attempt.RetryAt.Equal(req.RetryAt)
}

func sameResolutionRequest(previous Resolution, req ResolveRequest) bool {
	return previous.ResolutionID == req.ResolutionID && previous.TenantID == req.TenantID &&
		previous.OperationKey == req.OperationKey && previous.FromVersion == req.ExpectedVersion &&
		previous.Action == req.Action && previous.ActorHash == req.ActorHash &&
		previous.ReasonCode == req.ReasonCode && previous.ResultHash == req.ResultHash &&
		previous.ReplayReference == req.ReplayReference && previous.RetryAt.Equal(req.RetryAt)
}

var _ Ledger = (*Memory)(nil)
