package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
)

// Memory is an in-memory Store used by tests and by deployments that want the
// durable pipeline semantics (separate Agent execution and delivery, retry
// with backoff, dead letters) without an external database. It is obviously
// not durable across process restarts.
type Memory struct {
	mu          sync.Mutex
	inbox       map[string]*InboxRecord
	outbox      map[string]*OutboxRecord
	orderIn     map[string]uint64
	orderOut    map[string]uint64
	nextOrder   uint64
	leaseIn     map[string]string // inbox_id -> owner
	leaseOut    map[string]string // outbox_id -> owner
	nextIn      map[string]time.Time
	nextOut     map[string]time.Time
	leaseExpIn  map[string]time.Time
	leaseExpOut map[string]time.Time
	attempts    map[string]map[int]*OutboxAttempt // outbox_id -> attempt_no -> attempt
	resolutions map[string]memoryResolution       // resolution_id -> applied decision
}

type memoryResolution struct {
	Request        ResolveRequest
	AppliedVersion int64
}

func NewMemory() *Memory {
	return &Memory{
		inbox:       make(map[string]*InboxRecord),
		outbox:      make(map[string]*OutboxRecord),
		orderIn:     make(map[string]uint64),
		orderOut:    make(map[string]uint64),
		leaseIn:     make(map[string]string),
		leaseOut:    make(map[string]string),
		nextIn:      make(map[string]time.Time),
		nextOut:     make(map[string]time.Time),
		leaseExpIn:  make(map[string]time.Time),
		leaseExpOut: make(map[string]time.Time),
		attempts:    make(map[string]map[int]*OutboxAttempt),
		resolutions: make(map[string]memoryResolution),
	}
}

func (*Memory) EnsureSchema(context.Context) error { return nil }
func (*Memory) Close() error                       { return nil }

func (m *Memory) InsertInbox(_ context.Context, rec *InboxRecord, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.inbox {
		if existing.DedupKey == rec.DedupKey {
			return ErrDuplicate
		}
	}
	stored := *rec
	stored.Payload = append([]byte(nil), rec.Payload...)
	stored.TraceCarrier = cloneCarrier(rec.TraceCarrier)
	stored.PartitionKey = rec.PartitionKey
	stored.Status = InboxReceived
	stored.AttemptCount = 0
	m.inbox[stored.InboxID] = &stored
	m.nextOrder++
	m.orderIn[stored.InboxID] = m.nextOrder
	m.nextIn[stored.InboxID] = now
	return nil
}

func (m *Memory) LeaseInbox(_ context.Context, owner string, now time.Time, leaseTTL time.Duration, limit int) ([]InboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	// Insertion order mirrors (created_at, inbox_id) in Postgres. UUID lexical
	// order is deliberately not used because it can reorder one session.
	ids := make([]string, 0, len(m.inbox))
	for id := range m.inbox {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return m.orderIn[ids[i]] < m.orderIn[ids[j]] })
	leased := make([]InboxRecord, 0, limit)
	for _, id := range ids {
		if len(leased) >= limit {
			break
		}
		rec := m.inbox[id]
		if rec.Status != InboxReceived && rec.Status != InboxRetry {
			continue
		}
		if m.nextIn[id].After(now) {
			continue
		}
		if m.hasEarlierActiveInbox(rec, m.orderIn[id]) {
			continue
		}
		rec.Status = InboxProcessing
		rec.AttemptCount++
		m.leaseIn[id] = owner
		m.leaseExpIn[id] = now.Add(leaseTTL)
		snapshot := *rec
		snapshot.Payload = append([]byte(nil), rec.Payload...)
		snapshot.TraceCarrier = cloneCarrier(rec.TraceCarrier)
		leased = append(leased, snapshot)
	}
	return leased, nil
}

func (m *Memory) RenewInboxLease(_ context.Context, inboxID, owner string, now time.Time, leaseTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.inbox[inboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.Status != InboxProcessing || m.leaseIn[inboxID] != owner || !m.leaseExpIn[inboxID].After(now) {
		return ErrLeaseLost
	}
	m.leaseExpIn[inboxID] = now.Add(leaseTTL)
	return nil
}

func (m *Memory) CompleteInbox(ctx context.Context, inboxID, owner string, outbox *OutboxRecord, now time.Time) error {
	if outbox == nil {
		return m.CompleteInboxBatch(ctx, inboxID, owner, nil, now)
	}
	return m.CompleteInboxBatch(ctx, inboxID, owner, []OutboxRecord{*outbox}, now)
}

func (m *Memory) CompleteInboxBatch(
	_ context.Context,
	inboxID, owner string,
	outboxes []OutboxRecord,
	now time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.inbox[inboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.Status != InboxProcessing || m.leaseIn[inboxID] != owner || !m.leaseExpIn[inboxID].After(now) {
		return ErrLeaseLost
	}

	// Validate and normalize the whole batch before changing the Inbox or any
	// Outbox map. This is the in-memory equivalent of the PostgreSQL completion
	// transaction: one conflicting part rolls the entire completion back.
	staged := make([]OutboxRecord, 0, len(outboxes))
	byOperation := make(map[string]*OutboxRecord, len(m.outbox)+len(outboxes))
	byDedup := make(map[string]*OutboxRecord, len(m.outbox)+len(outboxes))
	for _, existing := range m.outbox {
		byOperation[operationIdentity(existing)] = existing
		if existing.DedupKey != "" {
			byDedup[existing.DedupKey] = existing
		}
	}
	newIDs := make(map[string]*OutboxRecord, len(outboxes))
	for i := range outboxes {
		stored, err := normalizeOutbox(outboxes[i], rec.PartitionKey)
		if err != nil {
			return err
		}
		collisions := make([]*OutboxRecord, 0, 3)
		if existing := byOperation[stored.OperationKey]; existing != nil {
			collisions = append(collisions, existing)
		}
		if existing := byDedup[stored.DedupKey]; stored.DedupKey != "" && existing != nil {
			collisions = append(collisions, existing)
		}
		if existing := m.outbox[stored.OutboxID]; existing != nil {
			collisions = append(collisions, existing)
		} else if existing := newIDs[stored.OutboxID]; existing != nil {
			collisions = append(collisions, existing)
		}
		if len(collisions) > 0 {
			canonicalID := collisions[0].OutboxID
			for _, existing := range collisions {
				if existing.OutboxID != canonicalID || !sameDeliveryOperation(existing, &stored) {
					return ErrOperationConflict
				}
			}
			// An exact operation replay is idempotent. Its original queue position
			// and canonical Outbox ID remain authoritative.
			continue
		}
		staged = append(staged, stored)
		candidate := &staged[len(staged)-1]
		byOperation[stored.OperationKey] = candidate
		if stored.DedupKey != "" {
			byDedup[stored.DedupKey] = candidate
		}
		newIDs[stored.OutboxID] = candidate
	}

	for i := range staged {
		stored := staged[i]
		m.outbox[stored.OutboxID] = &stored
		m.nextOrder++
		m.orderOut[stored.OutboxID] = m.nextOrder
		m.nextOut[stored.OutboxID] = now
	}
	rec.Status = InboxProcessed
	delete(m.leaseIn, inboxID)
	delete(m.leaseExpIn, inboxID)
	return nil
}

func normalizeOutbox(rec OutboxRecord, partitionKey string) (OutboxRecord, error) {
	if rec.OutboxID == "" || rec.DedupKey == "" {
		return OutboxRecord{}, ErrOperationConflict
	}
	if rec.OperationKey == "" {
		rec.OperationKey = "legacy:" + rec.OutboxID
	}
	if rec.PartCount == 0 {
		// Records written through the legacy one-Outbox API are one-part plans.
		rec.PartCount = 1
	}
	if rec.OperationVersion < 0 || rec.PartCount <= 0 || rec.PartIndex < 0 || rec.PartIndex >= rec.PartCount {
		return OutboxRecord{}, ErrOperationConflict
	}
	payloadSum := sha256.Sum256(rec.Payload)
	computedHash := hex.EncodeToString(payloadSum[:])
	if rec.PayloadHash != "" && rec.PayloadHash != computedHash {
		return OutboxRecord{}, ErrOperationConflict
	}
	rec.PayloadHash = computedHash
	rec.PartitionKey = partitionKey
	rec.Payload = append([]byte(nil), rec.Payload...)
	rec.TraceCarrier = cloneCarrier(rec.TraceCarrier)
	rec.Status = OutboxPending
	rec.DeliveryState = DeliveryPending
	rec.StateVersion = 0
	rec.AttemptCount = 0
	rec.AttemptPhase = ""
	rec.LastErrorType = ""
	rec.ProviderCode = ""
	rec.ProviderMessageID = ""
	rec.ProviderRequestID = ""
	rec.ResponseHash = ""
	rec.LeaseExpiresAt = time.Time{}
	return rec, nil
}

func operationIdentity(rec *OutboxRecord) string {
	if rec == nil {
		return ""
	}
	if rec.OperationKey != "" {
		return rec.OperationKey
	}
	if rec.OutboxID != "" {
		return "legacy:" + rec.OutboxID
	}
	return ""
}

func sameDeliveryOperation(a, b *OutboxRecord) bool {
	if a == nil || b == nil {
		return a == b
	}
	return operationIdentity(a) == operationIdentity(b) &&
		a.OperationVersion == b.OperationVersion &&
		a.PartIndex == b.PartIndex && a.PartCount == b.PartCount &&
		a.PayloadHash == b.PayloadHash && bytes.Equal(a.Payload, b.Payload) &&
		a.TenantID == b.TenantID && a.ChannelType == b.ChannelType &&
		a.BindingID == b.BindingID && a.DedupKey == b.DedupKey &&
		a.PartitionKey == b.PartitionKey
}

func (m *Memory) RetryInbox(_ context.Context, inboxID, owner, errType string, now, retryAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.inbox[inboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.Status != InboxProcessing || m.leaseIn[inboxID] != owner || !m.leaseExpIn[inboxID].After(now) {
		return ErrLeaseLost
	}
	rec.Status = InboxRetry
	rec.LastErrorType = errType
	m.nextIn[inboxID] = retryAt
	delete(m.leaseIn, inboxID)
	delete(m.leaseExpIn, inboxID)
	return nil
}

func (m *Memory) DeadLetterInbox(_ context.Context, inboxID, owner, errType string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.inbox[inboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.Status != InboxProcessing || m.leaseIn[inboxID] != owner || !m.leaseExpIn[inboxID].After(now) {
		return ErrLeaseLost
	}
	rec.Status = InboxDead
	rec.LastErrorType = errType
	delete(m.leaseIn, inboxID)
	delete(m.leaseExpIn, inboxID)
	return nil
}

func (m *Memory) LeaseOutbox(_ context.Context, owner string, now time.Time, leaseTTL time.Duration, limit int) ([]OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	ids := make([]string, 0, len(m.outbox))
	for id := range m.outbox {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return m.orderOut[ids[i]] < m.orderOut[ids[j]] })
	leased := make([]OutboxRecord, 0, limit)
	for _, id := range ids {
		if len(leased) >= limit {
			break
		}
		rec := m.outbox[id]
		if rec.Status != OutboxPending && rec.Status != OutboxRetry {
			continue
		}
		if m.nextOut[id].After(now) {
			continue
		}
		if m.hasEarlierActiveOutbox(rec, m.orderOut[id]) {
			continue
		}
		rec.Status = OutboxSending
		rec.DeliveryState = DeliveryInFlight
		rec.AttemptCount++
		rec.AttemptPhase = AttemptLeased
		rec.StateVersion++
		m.leaseOut[id] = owner
		m.leaseExpOut[id] = now.Add(leaseTTL)
		rec.LeaseExpiresAt = m.leaseExpOut[id]
		if m.attempts[id] == nil {
			m.attempts[id] = make(map[int]*OutboxAttempt)
		}
		m.attempts[id][rec.AttemptCount] = &OutboxAttempt{
			OutboxID: rec.OutboxID, OperationKey: rec.OperationKey,
			AttemptNo: rec.AttemptCount, LeaseOwner: owner,
			Phase: AttemptLeased, StartedAt: now,
		}
		leased = append(leased, cloneOutboxRecord(rec))
	}
	return leased, nil
}

func (m *Memory) hasEarlierActiveInbox(current *InboxRecord, order uint64) bool {
	for id, candidate := range m.inbox {
		if m.orderIn[id] >= order || !samePartition(
			candidate.TenantID, candidate.ChannelType, candidate.BindingID, candidate.PartitionKey,
			current.TenantID, current.ChannelType, current.BindingID, current.PartitionKey,
		) {
			continue
		}
		switch candidate.Status {
		case InboxReceived, InboxRetry, InboxProcessing:
			return true
		}
	}
	return false
}

func (m *Memory) hasEarlierActiveOutbox(current *OutboxRecord, order uint64) bool {
	for id, candidate := range m.outbox {
		if m.orderOut[id] >= order || !samePartition(
			candidate.TenantID, candidate.ChannelType, candidate.BindingID, candidate.PartitionKey,
			current.TenantID, current.ChannelType, current.BindingID, current.PartitionKey,
		) {
			continue
		}
		switch candidate.Status {
		case OutboxPending, OutboxRetry, OutboxSending:
			return true
		}
	}
	return false
}

func samePartition(tenantA, channelA, bindingA, partitionA, tenantB, channelB, bindingB, partitionB string) bool {
	if tenantA != tenantB || channelA != channelB || bindingA != bindingB {
		return false
	}
	// Empty is the rolling-upgrade marker used by legacy writers. It is
	// conservatively compatible with every exact session in this binding.
	return partitionA == "" || partitionB == "" || partitionA == partitionB
}

func (m *Memory) RenewOutboxLease(_ context.Context, outboxID, owner string, now time.Time, leaseTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.Status != OutboxSending || rec.DeliveryState != DeliveryInFlight ||
		m.leaseOut[outboxID] != owner || !m.leaseExpOut[outboxID].After(now) {
		return ErrLeaseLost
	}
	m.leaseExpOut[outboxID] = now.Add(leaseTTL)
	rec.LeaseExpiresAt = m.leaseExpOut[outboxID]
	return nil
}

func (m *Memory) MarkOutboxDispatched(
	_ context.Context,
	outboxID, owner string,
	attemptNo int,
	now time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	if !m.hasLiveOutboxLease(rec, owner, now) {
		return ErrLeaseLost
	}
	if rec.AttemptCount != attemptNo {
		return ErrInvalidTransition
	}
	attempt := m.attempt(outboxID, attemptNo)
	if attempt == nil || attempt.LeaseOwner != owner {
		return ErrInvalidTransition
	}
	if attempt.Phase == AttemptDispatched && rec.AttemptPhase == AttemptDispatched {
		return nil
	}
	if attempt.Phase != AttemptLeased || rec.AttemptPhase != AttemptLeased {
		return ErrInvalidTransition
	}
	attempt.Phase = AttemptDispatched
	attempt.DispatchedAt = now
	rec.AttemptPhase = AttemptDispatched
	rec.StateVersion++
	return nil
}

func (m *Memory) FinishOutboxAttempt(
	_ context.Context,
	outboxID, owner string,
	attemptNo int,
	result delivery.Result,
	now, retryAt time.Time,
	exhausted bool,
) error {
	if err := validateMemoryDeliveryResult(result); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finishOutboxAttemptLocked(outboxID, owner, attemptNo, result, now, retryAt, exhausted, false)
}

func (m *Memory) finishOutboxAttemptLocked(
	outboxID, owner string,
	attemptNo int,
	result delivery.Result,
	now, retryAt time.Time,
	exhausted, legacy bool,
) error {
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	if !m.hasLiveOutboxLease(rec, owner, now) {
		return ErrLeaseLost
	}
	if rec.AttemptCount != attemptNo {
		return ErrInvalidTransition
	}
	attempt := m.attempt(outboxID, attemptNo)
	if attempt == nil || attempt.LeaseOwner != owner || attempt.Phase == AttemptFinished {
		return ErrInvalidTransition
	}
	if attempt.Phase != AttemptDispatched {
		if !legacy {
			return ErrInvalidTransition
		}
	}

	attempt.Phase = AttemptFinished
	attempt.Outcome = result.Outcome
	attempt.ErrorType = result.ErrorType
	attempt.ProviderCode = result.ProviderCode
	attempt.HTTPStatus = result.HTTPStatus
	attempt.ProviderMessageID = result.ProviderMessageID
	attempt.ProviderRequestID = result.ProviderRequestID
	attempt.ResponseHash = result.ResponseHash
	attempt.FinishedAt = now

	rec.AttemptPhase = AttemptFinished
	rec.LastErrorType = result.ErrorType
	rec.ProviderCode = result.ProviderCode
	rec.ProviderMessageID = result.ProviderMessageID
	rec.ProviderRequestID = result.ProviderRequestID
	rec.ResponseHash = result.ResponseHash
	rec.StateVersion++
	delete(m.leaseOut, outboxID)
	delete(m.leaseExpOut, outboxID)
	rec.LeaseExpiresAt = time.Time{}

	switch result.Outcome {
	case delivery.Confirmed:
		rec.Status = OutboxSent
		rec.DeliveryState = DeliveryConfirmed
		delete(m.nextOut, outboxID)
	case delivery.RetryableNotSent:
		if exhausted {
			rec.Status = OutboxDead
			rec.DeliveryState = DeliveryRetryExhausted
			delete(m.nextOut, outboxID)
		} else {
			rec.Status = OutboxRetry
			rec.DeliveryState = DeliveryRetryableNotSent
			m.nextOut[outboxID] = retryAt
		}
	case delivery.PermanentRejected:
		rec.Status = OutboxDead
		rec.DeliveryState = DeliveryPermanentRejected
		delete(m.nextOut, outboxID)
	case delivery.Unknown:
		// Park as legacy status=sending without a lease. Both new and old FIFO
		// readers treat it as active, while an old reclaimer sees no deadline.
		rec.Status = OutboxSending
		rec.DeliveryState = DeliveryUnknown
		delete(m.nextOut, outboxID)
	}
	return nil
}

const (
	memoryMaxDeliveryCategoryBytes = 128
	memoryMaxDeliveryIDBytes       = 512
	memoryMaxResolutionReasonBytes = 1024
)

func validateMemoryDeliveryResult(result delivery.Result) error {
	if err := result.Validate(); err != nil {
		return errors.Join(ErrInvalidTransition, err)
	}
	if result.HTTPStatus < 0 || result.HTTPStatus > 999 ||
		!validLedgerIdentifier(result.ErrorType, memoryMaxDeliveryCategoryBytes) ||
		!validLedgerIdentifier(result.ProviderCode, memoryMaxDeliveryCategoryBytes) ||
		!validLedgerIdentifier(result.ProviderMessageID, memoryMaxDeliveryIDBytes) ||
		!validLedgerIdentifier(result.ProviderRequestID, memoryMaxDeliveryIDBytes) ||
		!validLedgerIdentifier(result.ResponseHash, memoryMaxDeliveryCategoryBytes) {
		return ErrInvalidTransition
	}
	return nil
}

func (m *Memory) hasLiveOutboxLease(rec *OutboxRecord, owner string, now time.Time) bool {
	return rec != nil && rec.Status == OutboxSending && rec.DeliveryState == DeliveryInFlight &&
		m.leaseOut[rec.OutboxID] == owner && m.leaseExpOut[rec.OutboxID].After(now)
}

func (m *Memory) attempt(outboxID string, attemptNo int) *OutboxAttempt {
	if byNumber := m.attempts[outboxID]; byNumber != nil {
		return byNumber[attemptNo]
	}
	return nil
}

func (m *Memory) CompleteOutbox(_ context.Context, outboxID, owner string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	return m.finishOutboxAttemptLocked(outboxID, owner, rec.AttemptCount, delivery.Result{
		Outcome: delivery.Confirmed,
	}, now, time.Time{}, false, true)
}

func (m *Memory) RetryOutbox(_ context.Context, outboxID, owner, errType string, now, retryAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	return m.finishOutboxAttemptLocked(outboxID, owner, rec.AttemptCount, delivery.Result{
		Outcome: delivery.RetryableNotSent, ErrorType: errType,
	}, now, retryAt, false, true)
}

func (m *Memory) DeadLetterOutbox(_ context.Context, outboxID, owner, errType string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.outbox[outboxID]
	if !ok {
		return ErrNotFound
	}
	return m.finishOutboxAttemptLocked(outboxID, owner, rec.AttemptCount, delivery.Result{
		Outcome: delivery.PermanentRejected, ErrorType: errType,
	}, now, time.Time{}, false, true)
}

func (m *Memory) ListUncertainOutbox(_ context.Context, limit int) ([]OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	ids := make([]string, 0)
	for id, rec := range m.outbox {
		if rec.DeliveryState == DeliveryUnknown {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return m.orderOut[ids[i]] < m.orderOut[ids[j]] })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	result := make([]OutboxRecord, 0, len(ids))
	for _, id := range ids {
		result = append(result, cloneOutboxRecord(m.outbox[id]))
	}
	return result, nil
}

func (m *Memory) ResolveOutbox(_ context.Context, req ResolveRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.ResolutionID == "" || req.OutboxID == "" || req.ExpectedVersion < 0 ||
		req.ExpectedAttempt < 1 || len(req.Actor) == 0 || len(req.Reason) == 0 ||
		len(req.Actor) > memoryMaxDeliveryIDBytes || len(req.Reason) > memoryMaxResolutionReasonBytes {
		return ErrInvalidTransition
	}
	switch req.Action {
	case ResolveAssumeDelivered, ResolveRetry, ResolveCancel:
	default:
		return ErrInvalidTransition
	}
	if previous, exists := m.resolutions[req.ResolutionID]; exists {
		if sameResolution(previous.Request, req) {
			return nil
		}
		return ErrOperationConflict
	}
	rec, ok := m.outbox[req.OutboxID]
	if !ok {
		return ErrNotFound
	}
	if rec.DeliveryState != DeliveryUnknown || rec.Status != OutboxSending ||
		rec.StateVersion != req.ExpectedVersion || rec.AttemptCount != req.ExpectedAttempt {
		return ErrInvalidTransition
	}
	switch req.Action {
	case ResolveAssumeDelivered:
		rec.Status = OutboxSent
		rec.DeliveryState = DeliveryConfirmed
		delete(m.nextOut, rec.OutboxID)
	case ResolveRetry:
		rec.Status = OutboxRetry
		rec.DeliveryState = DeliveryRetryableNotSent
		rec.LastErrorType = "operator_retry"
		m.nextOut[rec.OutboxID] = req.Now
	case ResolveCancel:
		rec.Status = OutboxDead
		rec.DeliveryState = DeliveryCanceled
		delete(m.nextOut, rec.OutboxID)
	}
	delete(m.leaseOut, rec.OutboxID)
	delete(m.leaseExpOut, rec.OutboxID)
	rec.LeaseExpiresAt = time.Time{}
	rec.StateVersion++
	m.resolutions[req.ResolutionID] = memoryResolution{Request: req, AppliedVersion: rec.StateVersion}
	return nil
}

func sameResolution(a, b ResolveRequest) bool {
	// Now is the local application clock and may be regenerated when an HTTP
	// client safely retries the same ResolutionID. All decision-bearing fields
	// must nevertheless remain identical.
	return a.ResolutionID == b.ResolutionID && a.OutboxID == b.OutboxID &&
		a.ExpectedVersion == b.ExpectedVersion && a.ExpectedAttempt == b.ExpectedAttempt &&
		a.Action == b.Action && a.Actor == b.Actor && a.Reason == b.Reason
}

func (m *Memory) ReclaimExpired(_ context.Context, now time.Time) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inboxCount := 0
	for id, rec := range m.inbox {
		if rec.Status == InboxProcessing && !m.leaseExpIn[id].After(now) {
			rec.Status = InboxRetry
			rec.LastErrorType = "lease_expired"
			m.nextIn[id] = now
			delete(m.leaseIn, id)
			delete(m.leaseExpIn, id)
			inboxCount++
		}
	}
	outboxCount := 0
	for id, rec := range m.outbox {
		if rec.Status != OutboxSending || rec.DeliveryState == DeliveryUnknown {
			continue
		}
		deadline, hasDeadline := m.leaseExpOut[id]
		if hasDeadline && deadline.After(now) {
			continue
		}
		// A legacy sending row may not carry delivery_state or an attempt record.
		// Treat it as potentially dispatched: retrying it would recreate the exact
		// duplicate window this ledger is meant to close.
		attempt := m.attempt(id, rec.AttemptCount)
		leasedOnly := rec.DeliveryState == DeliveryInFlight && rec.AttemptPhase == AttemptLeased &&
			attempt != nil && attempt.Phase == AttemptLeased
		if attempt == nil {
			attemptNo := rec.AttemptCount
			if attemptNo <= 0 {
				attemptNo = 1
				rec.AttemptCount = attemptNo
			}
			if m.attempts[id] == nil {
				m.attempts[id] = make(map[int]*OutboxAttempt)
			}
			attempt = &OutboxAttempt{
				OutboxID: rec.OutboxID, OperationKey: operationIdentity(rec),
				AttemptNo: attemptNo, LeaseOwner: m.leaseOut[id],
				Phase: AttemptFinished, StartedAt: now,
			}
			m.attempts[id][attemptNo] = attempt
		}
		attempt.Phase = AttemptFinished
		attempt.FinishedAt = now
		if leasedOnly {
			attempt.Outcome = delivery.RetryableNotSent
			attempt.ErrorType = "lease_expired_before_dispatch"
			rec.Status = OutboxRetry
			rec.DeliveryState = DeliveryRetryableNotSent
			rec.LastErrorType = attempt.ErrorType
			m.nextOut[id] = now
		} else {
			attempt.Outcome = delivery.Unknown
			attempt.ErrorType = "lease_expired_after_dispatch"
			rec.Status = OutboxSending
			rec.DeliveryState = DeliveryUnknown
			rec.LastErrorType = attempt.ErrorType
			delete(m.nextOut, id)
		}
		rec.AttemptPhase = AttemptFinished
		rec.StateVersion++
		rec.LeaseExpiresAt = time.Time{}
		delete(m.leaseOut, id)
		delete(m.leaseExpOut, id)
		outboxCount++
	}
	return inboxCount, outboxCount, nil
}

func cloneCarrier(carrier map[string]string) map[string]string {
	if len(carrier) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(carrier))
	for key, value := range carrier {
		cloned[key] = value
	}
	return cloned
}

func cloneOutboxRecord(rec *OutboxRecord) OutboxRecord {
	if rec == nil {
		return OutboxRecord{}
	}
	cloned := *rec
	cloned.Payload = append([]byte(nil), rec.Payload...)
	cloned.TraceCarrier = cloneCarrier(rec.TraceCarrier)
	return cloned
}

func (m *Memory) Depths(context.Context) (int, int, int, int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox := 0, 0, 0, 0, 0
	for _, rec := range m.inbox {
		switch rec.Status {
		case InboxReceived, InboxRetry, InboxProcessing:
			runnableInbox++
		case InboxDead:
			deadInbox++
		}
	}
	for _, rec := range m.outbox {
		if rec.DeliveryState == DeliveryUnknown {
			uncertainOutbox++
			continue
		}
		switch rec.Status {
		case OutboxPending, OutboxRetry, OutboxSending:
			pendingOutbox++
		case OutboxDead:
			deadOutbox++
		}
	}
	return runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox, nil
}
