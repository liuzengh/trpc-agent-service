package reliable

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MemoryStore implements the same state machine as PostgresStore for unit
// tests and local demos. It is process-local and must not be used for a
// horizontally scaled deployment.
type MemoryStore struct {
	mu              sync.Mutex
	now             func() time.Time
	nextInboxID     int64
	nextOutboxID    int64
	inbox           map[int64]*InboxMessage
	outbox          map[int64]*OutboxMessage
	idempotencyKey  map[string]int64
	outboxByInbox   map[int64]int64
	sessionSequence map[sessionPartition]int64
	queueSchedule   map[string]*tenantQueueSchedule
	fairVirtualTime int64
	replayAudit     []ReplayAuditRecord
}

type sessionPartition struct {
	tenantID  string
	agentApp  string
	sessionID string
}

// NewMemoryStore constructs an empty deterministic store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		now:             time.Now,
		inbox:           make(map[int64]*InboxMessage),
		outbox:          make(map[int64]*OutboxMessage),
		idempotencyKey:  make(map[string]int64),
		outboxByInbox:   make(map[int64]int64),
		sessionSequence: make(map[sessionPartition]int64),
		queueSchedule:   make(map[string]*tenantQueueSchedule),
	}
}

type tenantQueueSchedule struct {
	policy         TenantQueuePolicy
	virtualRuntime int64
	lastClaimedAt  time.Time
}

func (s *MemoryStore) EnqueueInbox(_ context.Context, msg *InboxMessage) (bool, error) {
	return s.enqueueInbox(msg, false)
}

// EnqueueInboxWithAdmission applies the operator-owned MaxQueued policy under
// the same mutex as idempotency and session-sequence allocation.
func (s *MemoryStore) EnqueueInboxWithAdmission(_ context.Context, msg *InboxMessage) (bool, error) {
	return s.enqueueInbox(msg, true)
}

func (s *MemoryStore) enqueueInbox(msg *InboxMessage, enforceAdmission bool) (bool, error) {
	if err := prepareInbox(msg); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := inboxKey(msg)
	if id, ok := s.idempotencyKey[key]; ok {
		existing := s.inbox[id]
		if !inboxIdentityMatches(existing, msg) {
			return false, ErrIdempotencyConflict
		}
		// Return the canonical stored record. In particular, callers that retry
		// an accepted message observe the original durable stream sequence rather
		// than an incomplete local request copy.
		*msg = *cloneInbox(existing)
		return false, nil
	}
	if enforceAdmission {
		if state := s.queueSchedule[msg.TenantID]; state != nil && state.policy.MaxQueued > 0 {
			var queued int64
			for _, existing := range s.inbox {
				if existing != nil && existing.TenantID == msg.TenantID && automaticInboxStatus(existing.Status) {
					queued++
				}
			}
			if queued >= state.policy.MaxQueued {
				return false, ErrTenantQueueFull
			}
		}
	}
	if msg.MaxAttempts == 0 {
		msg.MaxAttempts = 5
	}
	partition := inboxPartition(msg)
	s.sessionSequence[partition]++
	s.nextInboxID++
	now := s.now().UTC()
	stored := cloneInbox(msg)
	stored.ID = s.nextInboxID
	stored.SessionSequence = s.sessionSequence[partition]
	stored.Status = InboxReceived
	stored.AttemptCount = 0
	stored.NextAttemptAt = nil
	stored.Lease = Lease{}
	stored.LastError = ""
	stored.CreatedAt = now
	stored.UpdatedAt = now
	s.inbox[stored.ID] = stored
	s.idempotencyKey[key] = stored.ID
	*msg = *cloneInbox(stored)
	return true, nil
}

func (s *MemoryStore) ClaimInbox(_ context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim inbox: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim inbox: lease duration of at least 1ms is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for id := int64(1); id <= s.nextInboxID; id++ {
		msg := s.inbox[id]
		if msg == nil {
			continue
		}
		approvalPending := msg.Status == InboxWaitingApproval
		if (approvalPending && (msg.ApprovalDeadline == nil || !msg.ApprovalDeadline.After(now))) ||
			(!approvalPending && msg.AttemptCount >= msg.MaxAttempts) || !claimableInbox(msg, now) ||
			s.hasUnfinishedInboxPredecessor(msg) {
			continue
		}
		msg.Status = InboxProcessing
		// Approval waits are pre-execution pauses. Reclaiming one after an
		// operator grant must not exhaust the ordinary transient-attempt budget.
		if !approvalPending {
			msg.AttemptCount++
		}
		msg.NextAttemptAt = nil
		msg.ApprovalDeadline = nil
		msg.Lease.Owner = owner
		msg.Lease.Fence++
		msg.Lease.Until = now.Add(leaseDuration)
		msg.UpdatedAt = now
		return cloneInbox(msg), nil
	}
	return nil, ErrNoWork
}

// UpsertTenantQueuePolicy installs operator-owned fair scheduling state. The
// in-memory implementation mirrors the durable control-plane seam for tests;
// it is never a substitute for a shared production schedule.
func (s *MemoryStore) UpsertTenantQueuePolicy(_ context.Context, policy TenantQueuePolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.queueSchedule[policy.TenantID]
	if state == nil {
		state = &tenantQueueSchedule{}
		s.queueSchedule[policy.TenantID] = state
	}
	state.policy = policy
	return nil
}

func (s *MemoryStore) DeleteTenantQueuePolicy(_ context.Context, tenantID string) error {
	policy := TenantQueuePolicy{TenantID: tenantID, Weight: 1}
	if err := policy.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.queueSchedule[tenantID]
	if state == nil {
		state = &tenantQueueSchedule{}
		s.queueSchedule[tenantID] = state
	}
	state.policy = policy
	return nil
}

// CheckFairInboxReady keeps the optional readiness seam usable in local
// compositions. MemoryStore has no external schema to migrate.
func (s *MemoryStore) CheckFairInboxReady(context.Context) error {
	if s == nil {
		return ErrStoreUnavailable
	}
	return nil
}

// ClaimInboxFair applies weighted virtual-runtime scheduling over currently
// eligible session heads. The selection, MaxInflight check, lease issuance and
// schedule update share one mutex here and one SQL transaction in PostgresStore.
func (s *MemoryStore) ClaimInboxFair(_ context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim inbox fair: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim inbox fair: lease duration of at least 1ms is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	type candidate struct {
		msg      *InboxMessage
		inflight int64
		schedule *tenantQueueSchedule
		runtime  int64
	}
	candidates := make(map[string]candidate)
	for _, msg := range s.inbox {
		approvalPending := msg != nil && msg.Status == InboxWaitingApproval
		if msg == nil || !claimableInbox(msg, now) ||
			(approvalPending && (msg.ApprovalDeadline == nil || !msg.ApprovalDeadline.After(now))) ||
			(!approvalPending && msg.AttemptCount >= msg.MaxAttempts) ||
			s.hasUnfinishedInboxPredecessor(msg) {
			continue
		}
		inflight := int64(0)
		for _, other := range s.inbox {
			if other != nil && other.TenantID == msg.TenantID && other.Status == InboxProcessing && other.Lease.Until.After(now) {
				inflight++
			}
		}
		state := s.queueSchedule[msg.TenantID]
		if state == nil {
			state = &tenantQueueSchedule{policy: TenantQueuePolicy{TenantID: msg.TenantID, Weight: 1}}
			s.queueSchedule[msg.TenantID] = state
		}
		if state.policy.MaxInflight > 0 && inflight >= state.policy.MaxInflight {
			continue
		}
		current, ok := candidates[msg.TenantID]
		if !ok || msg.SessionSequence < current.msg.SessionSequence ||
			(msg.SessionSequence == current.msg.SessionSequence && msg.ID < current.msg.ID) {
			candidates[msg.TenantID] = candidate{
				msg: msg, inflight: inflight, schedule: state,
				runtime: max(state.virtualRuntime, s.fairVirtualTime),
			}
		}
	}
	if len(candidates) == 0 {
		return nil, ErrNoWork
	}
	var selected candidate
	var selectedTenant string
	for tenantID, item := range candidates {
		if selected.msg == nil || item.runtime < selected.runtime ||
			(item.runtime == selected.runtime && fairScheduleBefore(item.schedule, selected.schedule, tenantID, selectedTenant)) {
			selected, selectedTenant = item, tenantID
		}
	}
	msg := selected.msg
	approvalPending := msg.Status == InboxWaitingApproval
	msg.Status = InboxProcessing
	if !approvalPending {
		msg.AttemptCount++
	}
	msg.NextAttemptAt = nil
	msg.ApprovalDeadline = nil
	msg.Lease.Owner = owner
	msg.Lease.Fence++
	msg.Lease.Until = now.Add(leaseDuration)
	msg.UpdatedAt = now
	// New and idle tenants enter at the current service start, not at time zero.
	// Advancing the clock to the finish instead would erase weighted fairness.
	s.fairVirtualTime = selected.runtime
	selected.schedule.virtualRuntime = saturatingAdd(selected.runtime, fairQueueServiceCost(selected.schedule.policy.Weight))
	selected.schedule.lastClaimedAt = now
	return cloneInbox(msg), nil
}

func (s *MemoryStore) RenewInbox(_ context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error) {
	if leaseDuration < time.Millisecond {
		return Lease{}, fmt.Errorf("renew inbox: lease duration of at least 1ms is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return Lease{}, ErrStaleLease
	}
	msg.Lease.Until = s.now().UTC().Add(leaseDuration)
	return msg.Lease, nil
}

func (s *MemoryStore) CompleteInbox(_ context.Context, id int64, lease Lease, reply OutboxReply) (*OutboxMessage, error) {
	if err := prepareOutboxReply(&reply); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return nil, ErrStaleLease
	}
	if _, exists := s.outboxByInbox[id]; exists {
		return nil, fmt.Errorf("complete inbox: %w for inbox %d", ErrOutboxConflict, id)
	}
	now := s.now().UTC()
	s.nextOutboxID++
	storedReply := &OutboxMessage{
		ID:               s.nextOutboxID,
		InboxID:          id,
		TenantID:         msg.TenantID,
		AgentApp:         msg.AgentApp,
		SessionID:        msg.SessionID,
		SessionSequence:  msg.SessionSequence,
		ChannelType:      msg.ChannelType,
		ChannelAccountID: msg.ChannelAccountID,
		ConversationID:   msg.ConversationID,
		ReplyToID:        msg.ReplyToID,
		ContentType:      reply.ContentType,
		Content:          reply.Content,
		TraceParent:      reply.TraceParent,
		MaxAttempts:      defaultOutboxMaxAttempts,
		Status:           OutboxPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	s.outbox[storedReply.ID] = storedReply
	s.outboxByInbox[id] = storedReply.ID
	msg.Status = InboxCompleted
	releaseLease(&msg.Lease)
	msg.LastError = ""
	msg.ApprovalDeadline = nil
	msg.UpdatedAt = now
	return cloneOutbox(storedReply), nil
}

func (s *MemoryStore) RetryInbox(_ context.Context, id int64, lease Lease, cause error, nextAttempt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.LastError = errorText(cause)
	releaseLease(&msg.Lease)
	msg.UpdatedAt = s.now().UTC()
	if msg.AttemptCount >= msg.MaxAttempts {
		msg.Status = InboxDeadLetter
		msg.NextAttemptAt = nil
	} else {
		msg.Status = InboxRetryWait
		next := nextAttempt.UTC()
		msg.NextAttemptAt = &next
	}
	msg.ApprovalDeadline = nil
	return nil
}

func (s *MemoryStore) RetryInboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("retry inbox: negative delay is invalid")
	}
	return s.RetryInbox(ctx, id, lease, cause, s.now().UTC().Add(delay))
}

// WaitInboxApproval parks a claimed message until the approval capability is
// granted. It deliberately preserves AttemptCount and the session FIFO.
func (s *MemoryStore) WaitInboxApproval(_ context.Context, id int64, lease Lease, cause error, delay time.Duration, deadline time.Time) error {
	if delay < 0 {
		return fmt.Errorf("wait inbox approval: negative delay is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	now := s.now().UTC()
	deadline = deadline.UTC()
	if deadline.IsZero() || !deadline.After(now) {
		return fmt.Errorf("wait inbox approval: %w", ErrApprovalDeadlineInvalid)
	}
	if deadline.After(now.Add(MaxApprovalWait)) {
		return fmt.Errorf("wait inbox approval: %w", ErrApprovalDeadlineInvalid)
	}
	msg.Status = InboxWaitingApproval
	msg.LastError = errorText(cause)
	next := now.Add(delay)
	if next.After(deadline) {
		next = deadline
	}
	msg.NextAttemptAt = &next
	msg.ApprovalDeadline = &deadline
	releaseLease(&msg.Lease)
	msg.UpdatedAt = now
	return nil
}

// BlockInbox stops automatic retries when the downstream execution outcome is
// unknown. It preserves the attempt count and requires an explicit audited
// ReplayInbox after operator reconciliation.
func (s *MemoryStore) BlockInbox(_ context.Context, id int64, lease Lease, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.Status = InboxWaitingReconciliation
	msg.LastError = errorText(cause)
	msg.NextAttemptAt = nil
	msg.ApprovalDeadline = nil
	releaseLease(&msg.Lease)
	msg.UpdatedAt = s.now().UTC()
	return nil
}

// DeadLetterInbox records a deterministic, non-retryable failure immediately.
func (s *MemoryStore) DeadLetterInbox(_ context.Context, id int64, lease Lease, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || msg.Status != InboxProcessing || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.Status = InboxDeadLetter
	msg.LastError = errorText(cause)
	msg.NextAttemptAt = nil
	msg.ApprovalDeadline = nil
	releaseLease(&msg.Lease)
	msg.UpdatedAt = s.now().UTC()
	return nil
}

func (s *MemoryStore) ReplayInbox(_ context.Context, id int64, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("replay inbox: actor and reason are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.inbox[id]
	if msg == nil || (msg.Status != InboxDeadLetter && msg.Status != InboxWaitingReconciliation) {
		return fmt.Errorf("replay inbox: message is not awaiting replay")
	}
	msg.Status = InboxReceived
	msg.AttemptCount = 0
	msg.NextAttemptAt = nil
	msg.ApprovalDeadline = nil
	msg.Lease.Fence++
	msg.Lease.Owner = ""
	msg.Lease.Until = time.Time{}
	msg.LastError = ""
	now := s.now().UTC()
	msg.UpdatedAt = now
	s.replayAudit = append(s.replayAudit, ReplayAuditRecord{
		Queue:       ReplayQueueInbox,
		MessageID:   msg.ID,
		TenantID:    msg.TenantID,
		RequestedBy: actor,
		Reason:      reason,
		Mode:        OutboxReplayRestart,
		CreatedAt:   now,
	})
	return nil
}

func (s *MemoryStore) ClaimOutbox(_ context.Context, owner string, leaseDuration time.Duration) (*OutboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim outbox: lease duration of at least 1ms is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for id := int64(1); id <= s.nextOutboxID; id++ {
		msg := s.outbox[id]
		if msg == nil {
			continue
		}
		if msg.AttemptCount >= msg.MaxAttempts || !claimableOutbox(msg, now) ||
			s.hasUnfinishedOutboxPredecessor(msg) {
			continue
		}
		msg.Status = OutboxDelivering
		msg.AttemptCount++
		msg.NextAttemptAt = nil
		msg.Lease.Owner = owner
		msg.Lease.Fence++
		msg.Lease.Until = now.Add(leaseDuration)
		msg.UpdatedAt = now
		return cloneOutbox(msg), nil
	}
	return nil, ErrNoWork
}

// ReapExpired performs bounded maintenance that is deliberately separate from
// ClaimInbox and ClaimOutbox. It preserves a previous final-attempt error for
// diagnosis, but records an approval timeout as the terminal cause because the
// authorization window, rather than the worker, made the invocation fail.
func (s *MemoryStore) ReapExpired(_ context.Context, batchSize int) (ExpiredWorkReapResult, error) {
	if err := ValidateExpiredWorkReapBatchSize(batchSize); err != nil {
		return ExpiredWorkReapResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	result := ExpiredWorkReapResult{}
	for id := int64(1); id <= s.nextInboxID && result.InboxFinalAttemptExpired+result.InboxApprovalExpired < batchSize; id++ {
		msg := s.inbox[id]
		if msg == nil {
			continue
		}
		switch {
		case msg.Status == InboxProcessing &&
			!msg.Lease.Until.After(now) && msg.AttemptCount >= msg.MaxAttempts:
			msg.Status = InboxDeadLetter
			msg.NextAttemptAt = nil
			msg.ApprovalDeadline = nil
			if msg.LastError == "" {
				msg.LastError = ErrLeaseExpiredAfterFinalAttempt.Error()
			}
			releaseLease(&msg.Lease)
			msg.UpdatedAt = now
			result.InboxFinalAttemptExpired++
		case msg.Status == InboxWaitingApproval &&
			(msg.ApprovalDeadline == nil || !msg.ApprovalDeadline.After(now)):
			msg.Status = InboxDeadLetter
			msg.NextAttemptAt = nil
			msg.ApprovalDeadline = nil
			msg.LastError = "tool approval expired"
			releaseLease(&msg.Lease)
			msg.UpdatedAt = now
			result.InboxApprovalExpired++
		}
	}
	outboxReaped := 0
	for id := int64(1); id <= s.nextOutboxID && outboxReaped < batchSize; id++ {
		msg := s.outbox[id]
		if msg == nil || msg.Status == OutboxDelivered || msg.Status == OutboxDeadLetter || msg.Status == OutboxWaitingReconciliation || msg.Lease.Until.After(now) {
			continue
		}
		if msg.Status == OutboxDispatchStarted {
			msg.Status = OutboxWaitingReconciliation
			msg.NextAttemptAt = nil
			msg.LastError = "dispatch lease expired; provider outcome unknown"
			releaseLease(&msg.Lease)
			msg.UpdatedAt = now
			result.OutboxDispatchUnknown++
			outboxReaped++
			continue
		}
		if msg.Status != OutboxDelivering || msg.AttemptCount < msg.MaxAttempts {
			continue
		}
		msg.Status = OutboxDeadLetter
		msg.NextAttemptAt = nil
		if msg.LastError == "" {
			msg.LastError = ErrLeaseExpiredAfterFinalAttempt.Error()
		}
		releaseLease(&msg.Lease)
		msg.UpdatedAt = now
		result.OutboxFinalAttemptExpired++
		outboxReaped++
	}
	return result, nil
}

// InspectQueue returns a consistent, read-only snapshot for local tests and
// demos. Production deployments should use the PostgreSQL implementation so
// the observation reflects all workers, not one process.
func (s *MemoryStore) InspectQueue(_ context.Context) (QueueStats, error) {
	if s == nil {
		return QueueStats{}, ErrStoreUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := QueueStats{ObservedAt: s.now().UTC()}
	for _, msg := range s.inbox {
		if !automaticInboxStatus(msg.Status) {
			continue
		}
		stats.InboxDepth++
		if stats.InboxOldest.IsZero() || msg.CreatedAt.Before(stats.InboxOldest) {
			stats.InboxOldest = msg.CreatedAt
		}
	}
	for _, msg := range s.outbox {
		if !automaticOutboxStatus(msg.Status) {
			continue
		}
		stats.OutboxDepth++
		if stats.OutboxOldest.IsZero() || msg.CreatedAt.Before(stats.OutboxOldest) {
			stats.OutboxOldest = msg.CreatedAt
		}
	}
	return stats, nil
}

func (s *MemoryStore) RenewOutbox(_ context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error) {
	if leaseDuration < time.Millisecond {
		return Lease{}, fmt.Errorf("renew outbox: lease duration of at least 1ms is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || (msg.Status != OutboxDelivering && msg.Status != OutboxDispatchStarted) || !validLease(msg.Lease, lease, s.now()) {
		return Lease{}, ErrStaleLease
	}
	msg.Lease.Until = s.now().UTC().Add(leaseDuration)
	return msg.Lease, nil
}

// MarkDispatchStarted fences the transition across the provider side-effect
// boundary. A row in this state is never automatically reclaimed.
func (s *MemoryStore) MarkDispatchStarted(_ context.Context, id int64, lease Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || msg.Status != OutboxDelivering || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	now := s.now().UTC()
	msg.Status = OutboxDispatchStarted
	msg.UpdatedAt = now
	return nil
}

func (s *MemoryStore) MarkDelivered(_ context.Context, id int64, lease Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || msg.Status != OutboxDispatchStarted || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	now := s.now().UTC()
	msg.Status = OutboxDelivered
	msg.DeliveredAt = &now
	releaseLease(&msg.Lease)
	msg.LastError = ""
	msg.UpdatedAt = now
	return nil
}

func (s *MemoryStore) AdvanceOutbox(_ context.Context, id int64, lease Lease, nextCursor int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || msg.Status != OutboxDispatchStarted || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	if nextCursor <= msg.DeliveryCursor {
		return fmt.Errorf("advance outbox: cursor must increase")
	}
	msg.DeliveryCursor = nextCursor
	msg.Status = OutboxPending
	msg.AttemptCount = 0
	msg.NextAttemptAt = nil
	releaseLease(&msg.Lease)
	msg.LastError = ""
	now := s.now().UTC()
	msg.UpdatedAt = now
	return nil
}

// ReplayAuditRecords returns a copied snapshot. Callers cannot mutate the
// store's audit history through the returned slice.
func (s *MemoryStore) ReplayAuditRecords() []ReplayAuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]ReplayAuditRecord, len(s.replayAudit))
	copy(records, s.replayAudit)
	return records
}

func (s *MemoryStore) RetryOutbox(_ context.Context, id int64, lease Lease, cause error, nextAttempt time.Time, cursor int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || (msg.Status != OutboxDelivering && msg.Status != OutboxDispatchStarted) || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.LastError = errorText(cause)
	if cursor > msg.DeliveryCursor {
		msg.DeliveryCursor = cursor
	}
	releaseLease(&msg.Lease)
	msg.UpdatedAt = s.now().UTC()
	if msg.AttemptCount >= msg.MaxAttempts {
		msg.Status = OutboxDeadLetter
		msg.NextAttemptAt = nil
	} else {
		msg.Status = OutboxRetryWait
		next := nextAttempt.UTC()
		msg.NextAttemptAt = &next
	}
	return nil
}

func (s *MemoryStore) RetryOutboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration, cursor int) error {
	if delay < 0 {
		return fmt.Errorf("retry outbox: negative delay is invalid")
	}
	return s.RetryOutbox(ctx, id, lease, cause, s.now().UTC().Add(delay), cursor)
}

func (s *MemoryStore) DeadLetterOutbox(_ context.Context, id int64, lease Lease, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || (msg.Status != OutboxDelivering && msg.Status != OutboxDispatchStarted) || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.Status = OutboxDeadLetter
	msg.NextAttemptAt = nil
	releaseLease(&msg.Lease)
	msg.LastError = errorText(cause)
	msg.UpdatedAt = s.now().UTC()
	return nil
}

// BlockOutbox stops automatic delivery when tenant or provider state requires
// operator reconciliation. The existing lease fence remains authoritative.
func (s *MemoryStore) BlockOutbox(_ context.Context, id int64, lease Lease, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || (msg.Status != OutboxDelivering && msg.Status != OutboxDispatchStarted) || !validLease(msg.Lease, lease, s.now()) {
		return ErrStaleLease
	}
	msg.Status = OutboxWaitingReconciliation
	msg.NextAttemptAt = nil
	msg.LastError = errorText(cause)
	releaseLease(&msg.Lease)
	msg.UpdatedAt = s.now().UTC()
	return nil
}

func (s *MemoryStore) ReplayOutbox(ctx context.Context, id int64, actor, reason string) error {
	return s.replayOutbox(ctx, "", id, actor, reason, OutboxReplayResume)
}

// ReplayOutboxForTenant performs the replay only when the persisted message
// belongs to the requested tenant. The check and state transition share the
// store mutex, matching the transactional PostgreSQL implementation.
func (s *MemoryStore) ReplayOutboxForTenant(ctx context.Context, tenantID string, id int64, actor, reason string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("replay outbox: tenant id is required")
	}
	return s.replayOutbox(ctx, tenantID, id, actor, reason, OutboxReplayResume)
}

func (s *MemoryStore) RestartOutbox(ctx context.Context, id int64, actor, reason string) error {
	return s.replayOutbox(ctx, "", id, actor, reason, OutboxReplayRestart)
}

func (s *MemoryStore) replayOutbox(_ context.Context, expectedTenant string, id int64, actor, reason string, mode OutboxReplayMode) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("replay outbox: actor and reason are required")
	}
	if err := validateOutboxReplayMode(mode); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := s.outbox[id]
	if msg == nil || (expectedTenant != "" && msg.TenantID != expectedTenant) || (msg.Status != OutboxDeadLetter && msg.Status != OutboxWaitingReconciliation) {
		return fmt.Errorf("replay outbox: message is not dead-lettered or awaiting reconciliation")
	}
	msg.Status = OutboxPending
	msg.AttemptCount = 0
	if mode == OutboxReplayRestart {
		msg.DeliveryCursor = 0
	}
	msg.NextAttemptAt = nil
	msg.Lease.Fence++
	msg.Lease.Owner = ""
	msg.Lease.Until = time.Time{}
	msg.LastError = ""
	now := s.now().UTC()
	msg.UpdatedAt = now
	s.replayAudit = append(s.replayAudit, ReplayAuditRecord{
		Queue:       ReplayQueueOutbox,
		MessageID:   msg.ID,
		TenantID:    msg.TenantID,
		RequestedBy: actor,
		Reason:      reason,
		Mode:        mode,
		CreatedAt:   now,
	})
	return nil
}

func (s *MemoryStore) PingContext(context.Context) error { return nil }
func (s *MemoryStore) Close() error                      { return nil }

func claimableInbox(msg *InboxMessage, now time.Time) bool {
	switch msg.Status {
	case InboxReceived:
		return true
	case InboxRetryWait:
		return msg.NextAttemptAt != nil && !msg.NextAttemptAt.After(now)
	case InboxWaitingApproval:
		return msg.NextAttemptAt != nil && !msg.NextAttemptAt.After(now)
	case InboxProcessing:
		return !msg.Lease.Until.After(now)
	default:
		return false
	}
}

func (s *MemoryStore) hasUnfinishedInboxPredecessor(candidate *InboxMessage) bool {
	partition := inboxPartition(candidate)
	for _, earlier := range s.inbox {
		if inboxPartition(earlier) == partition &&
			earlier.SessionSequence < candidate.SessionSequence &&
			earlier.Status != InboxCompleted {
			return true
		}
	}
	return false
}

func (s *MemoryStore) hasUnfinishedOutboxPredecessor(candidate *OutboxMessage) bool {
	for _, earlier := range s.outbox {
		if earlier.TenantID == candidate.TenantID &&
			earlier.AgentApp == candidate.AgentApp &&
			earlier.SessionID == candidate.SessionID &&
			earlier.SessionSequence < candidate.SessionSequence &&
			earlier.Status != OutboxDelivered {
			return true
		}
	}
	return false
}

func inboxPartition(msg *InboxMessage) sessionPartition {
	return sessionPartition{
		tenantID:  msg.TenantID,
		agentApp:  msg.AgentApp,
		sessionID: msg.SessionID,
	}
}

func claimableOutbox(msg *OutboxMessage, now time.Time) bool {
	switch msg.Status {
	case OutboxPending:
		return true
	case OutboxRetryWait:
		return msg.NextAttemptAt != nil && !msg.NextAttemptAt.After(now)
	case OutboxDelivering:
		return !msg.Lease.Until.After(now)
	default:
		return false
	}
}

func automaticInboxStatus(status InboxStatus) bool {
	switch status {
	case InboxReceived, InboxProcessing, InboxRetryWait, InboxWaitingApproval:
		return true
	default:
		return false
	}
}

func automaticOutboxStatus(status OutboxStatus) bool {
	switch status {
	case OutboxPending, OutboxDelivering, OutboxDispatchStarted, OutboxRetryWait:
		return true
	default:
		return false
	}
}

func validLease(current, presented Lease, now time.Time) bool {
	return current.Owner == presented.Owner && current.Fence == presented.Fence && current.Until.After(now)
}

func releaseLease(lease *Lease) {
	lease.Owner = ""
	lease.Until = time.Time{}
}

func saturatingAdd(value, delta int64) int64 {
	const maxInt64 = int64(1<<63 - 1)
	const minInt64 = -maxInt64 - 1
	if delta > 0 && value > maxInt64-delta {
		return maxInt64
	}
	if delta < 0 && value < minInt64-delta {
		return minInt64
	}
	return value + delta
}

const fairQueueQuantum int64 = 1_000_000

func fairQueueServiceCost(weight int64) int64 {
	if weight <= 0 {
		return fairQueueQuantum
	}
	return (fairQueueQuantum + weight - 1) / weight
}

func fairScheduleBefore(left, right *tenantQueueSchedule, leftTenant, rightTenant string) bool {
	if right == nil {
		return true
	}
	if left.lastClaimedAt.IsZero() != right.lastClaimedAt.IsZero() {
		return left.lastClaimedAt.IsZero()
	}
	if !left.lastClaimedAt.Equal(right.lastClaimedAt) {
		return left.lastClaimedAt.Before(right.lastClaimedAt)
	}
	return leftTenant < rightTenant
}

func inboxKey(msg *InboxMessage) string {
	return msg.TenantID + "\x00" + msg.ChannelType + "\x00" + msg.ChannelAccountID + "\x00" + msg.ExternalMessageID
}

func cloneInbox(msg *InboxMessage) *InboxMessage {
	copy := *msg
	copy.Payload = append([]byte(nil), msg.Payload...)
	if msg.NextAttemptAt != nil {
		next := *msg.NextAttemptAt
		copy.NextAttemptAt = &next
	}
	if msg.ApprovalDeadline != nil {
		deadline := *msg.ApprovalDeadline
		copy.ApprovalDeadline = &deadline
	}
	return &copy
}

func cloneOutbox(msg *OutboxMessage) *OutboxMessage {
	copy := *msg
	if msg.NextAttemptAt != nil {
		next := *msg.NextAttemptAt
		copy.NextAttemptAt = &next
	}
	if msg.DeliveredAt != nil {
		delivered := *msg.DeliveredAt
		copy.DeliveredAt = &delivered
	}
	return &copy
}
