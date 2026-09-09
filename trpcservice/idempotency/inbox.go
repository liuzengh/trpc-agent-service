// Package idempotency owns durable inbound-message claims.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// ErrClaimOwner indicates that a different worker owns the active claim.
var ErrClaimOwner = errors.New("idempotency: claim owned by another worker")

// Status is the durable Inbox processing state.
type Status string

const (
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusRetry      Status = "retry"
	StatusCanceled   Status = "canceled"
	StatusRejected   Status = "rejected"
	StatusDLQ        Status = "dlq"
)

// ExecutionStage records durable progress after a claim has entered Runner.
// It allows a reclaimed claim to resume derived writes without invoking the
// model or side-effecting tools a second time.
type ExecutionStage string

const (
	ExecutionNone             ExecutionStage = "none"
	ExecutionRunnerCommitted  ExecutionStage = "runner_committed"
	ExecutionDerivedCommitted ExecutionStage = "derived_committed"
	ExecutionOutboxCommitted  ExecutionStage = "outbox_committed"
)

type ExecutionRecord struct {
	Stage   ExecutionStage
	Reply   string
	EventID string
}

// Claim is one Inbox ownership attempt.
type Claim struct {
	InboxID, Owner, ClaimToken string
	Attempt                    int
	InboxSeq                   uint64
	Status                     Status
	LeaseUntil                 time.Time
	Message                    gateway.InboundMessage
}

// RunRequest reconstructs the exact durable Worker request owned by the claim.
func (claim Claim) RunRequest() gateway.RunRequest {
	message := claim.Message
	return gateway.RunRequest{
		InboxID: claim.InboxID, InboxSeq: claim.InboxSeq,
		TenantID: message.TenantID, AppID: message.AppID, BindingID: message.BindingID,
		ExternalMessageID: message.ExternalMessageID, ExternalUserID: message.ExternalUserID,
		ConversationID: message.ConversationID, UserID: message.UserID, SessionID: message.SessionID,
		Text: message.Text, Attachments: message.Attachments, TraceID: message.TraceID, TraceContext: message.TraceContext,
		ConfigVersion: message.ConfigVersion, ClaimOwner: claim.Owner, ClaimToken: claim.ClaimToken,
		ClaimAttempt: claim.Attempt, ClaimLeaseUntil: claim.LeaseUntil,
	}
}

// Store claims duplicate deliveries and records terminal/retry state.
type Store interface {
	Claim(context.Context, gateway.InboundMessage, string, time.Duration) (Claim, bool, error)
	Renew(context.Context, Claim, time.Duration) (Claim, error)
	Cancel(context.Context, Claim) error
	Reject(context.Context, Claim) error
	Complete(context.Context, Claim) error
	Fail(context.Context, Claim, error, time.Time) error
	// Defer yields capacity-constrained work without consuming its retry budget.
	Defer(context.Context, Claim, time.Time) error
}

// ExecutionStore is an optional durable extension implemented by production
// SQL and the deterministic memory store.
type ExecutionStore interface {
	GetExecution(context.Context, Claim) (ExecutionRecord, error)
	SaveExecution(context.Context, Claim, ExecutionRecord) error
}

// ReadyStore atomically reclaims retryable or lease-expired Inbox work for a
// background Worker. Implementations must not return later work from a session
// while an earlier inbox_seq remains non-terminal.
type ReadyStore interface {
	Store
	ClaimReady(context.Context, string, time.Duration, int) ([]Claim, error)
}

func (store *MemoryStore) Reject(_ context.Context, claim Claim) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	item.claim.Status = StatusRejected
	return nil
}

// Cancel marks an explicitly canceled active claim terminal.
func (store *MemoryStore) Cancel(_ context.Context, claim Claim) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	item.claim.Status = StatusCanceled
	return nil
}

// Defer returns an exact active claim to retry without counting capacity
// backpressure as a processing failure.
func (store *MemoryStore) Defer(_ context.Context, claim Claim, retryAt time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	item.claim.Status = StatusRetry
	if item.claim.Attempt > 0 {
		item.claim.Attempt--
	}
	item.claim.Owner, item.claim.ClaimToken = "", ""
	item.claim.LeaseUntil = time.Time{}
	item.nextAttempt = retryAt.UTC()
	item.lastError = ""
	return nil
}

// Renew extends an unexpired claim only for its exact owner and token.
func (store *MemoryStore) Renew(_ context.Context, claim Claim, ttl time.Duration) (Claim, error) {
	if ttl <= 0 {
		return Claim{}, errors.New("idempotency: positive ttl is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	now := store.now().UTC()
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !now.Before(item.claim.LeaseUntil) {
		return Claim{}, ErrClaimOwner
	}
	item.claim.LeaseUntil = now.Add(ttl)
	return item.claim, nil
}

type record struct {
	claim       Claim
	nextAttempt time.Time
	lastError   string
	execution   ExecutionRecord
}

// MemoryStore is a concurrency-safe reference Inbox implementation.
type MemoryStore struct {
	mu          sync.Mutex
	records     map[string]*record
	sequences   map[gateway.SessionKey]uint64
	now         func() time.Time
	maxAttempts int
}

// NewMemoryStore creates an empty Inbox store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]*record), sequences: make(map[gateway.SessionKey]uint64), now: time.Now, maxAttempts: 5}
}

// InboxID returns a deterministic tenant-scoped delivery identity.
func InboxID(message gateway.InboundMessage) string {
	return message.TenantID + "/" + message.BindingID + "/" + message.ExternalMessageID
}

// Claim atomically wins a new or expired delivery; duplicates do not run.
func (store *MemoryStore) Claim(_ context.Context, message gateway.InboundMessage, owner string, ttl time.Duration) (Claim, bool, error) {
	if message.TenantID == "" || message.BindingID == "" || message.ExternalMessageID == "" || owner == "" || ttl <= 0 {
		return Claim{}, false, errors.New("idempotency: tenant, binding, external message, owner, and positive ttl are required")
	}
	if message.Media != nil {
		return Claim{}, false, errors.New("idempotency: unresolved media reference is not durable")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now().UTC()
	id := InboxID(message)
	existing := store.records[id]
	if existing != nil {
		if terminal(existing.claim.Status) {
			return existing.claim, false, nil
		}
		if existing.claim.Status == StatusProcessing && now.Before(existing.claim.LeaseUntil) {
			return existing.claim, false, nil
		}
		if existing.claim.Status == StatusRetry && now.Before(existing.nextAttempt) {
			return existing.claim, false, nil
		}
		if existing.claim.Attempt >= store.attemptLimit() {
			existing.claim.Status = StatusDLQ
			return existing.claim, false, nil
		}
		existing.claim.Owner = owner
		existing.claim.Attempt++
		existing.claim.ClaimToken = fmt.Sprintf("%s#%d#%d", id, existing.claim.Attempt, now.UnixNano())
		existing.claim.Status = StatusProcessing
		existing.claim.LeaseUntil = now.Add(ttl)
		existing.claim.Message = message
		return existing.claim, true, nil
	}
	key := gateway.SessionKey{TenantID: message.TenantID, AppID: message.AppID, UserID: message.UserID, SessionID: message.SessionID}
	store.sequences[key]++
	claim := Claim{InboxID: id, Owner: owner, ClaimToken: fmt.Sprintf("%s#1#%d", id, now.UnixNano()), Attempt: 1, InboxSeq: store.sequences[key], Status: StatusProcessing, LeaseUntil: now.Add(ttl), Message: message}
	store.records[id] = &record{claim: claim}
	return claim, true, nil
}

// ClaimReady reclaims due work in deterministic session order.
func (store *MemoryStore) ClaimReady(_ context.Context, owner string, ttl time.Duration, limit int) ([]Claim, error) {
	if owner == "" || ttl <= 0 || limit <= 0 {
		return nil, errors.New("idempotency: owner, positive ttl, and limit are required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now().UTC()
	records := make([]*record, 0, len(store.records))
	for _, item := range store.records {
		if ready(item, now) && item.claim.Attempt >= store.attemptLimit() {
			item.claim.Status = StatusDLQ
			continue
		}
		if ready(item, now) {
			records = append(records, item)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i].claim, records[j].claim
		if left.Message.ReceivedAt.Equal(right.Message.ReceivedAt) {
			return left.InboxID < right.InboxID
		}
		return left.Message.ReceivedAt.Before(right.Message.ReceivedAt)
	})
	claims := make([]Claim, 0, min(limit, len(records)))
	for _, item := range records {
		if len(claims) == limit || store.hasEarlierNonTerminal(item.claim) {
			continue
		}
		item.claim.Attempt++
		item.claim.Owner = owner
		item.claim.ClaimToken = claimToken(item.claim.InboxID, item.claim.Attempt, now)
		item.claim.Status = StatusProcessing
		item.claim.LeaseUntil = now.Add(ttl)
		item.nextAttempt = time.Time{}
		item.lastError = ""
		claims = append(claims, item.claim)
	}
	return claims, nil
}

func (store *MemoryStore) hasEarlierNonTerminal(candidate Claim) bool {
	key := gateway.SessionKey{TenantID: candidate.Message.TenantID, AppID: candidate.Message.AppID, UserID: candidate.Message.UserID, SessionID: candidate.Message.SessionID}
	for _, item := range store.records {
		other := item.claim
		otherKey := gateway.SessionKey{TenantID: other.Message.TenantID, AppID: other.Message.AppID, UserID: other.Message.UserID, SessionID: other.Message.SessionID}
		if otherKey == key && other.InboxSeq < candidate.InboxSeq && !terminal(other.Status) {
			return true
		}
	}
	return false
}

func ready(item *record, now time.Time) bool {
	return item != nil && ((item.claim.Status == StatusProcessing && !item.claim.LeaseUntil.After(now)) || (item.claim.Status == StatusRetry && !item.nextAttempt.After(now)))
}

func terminal(status Status) bool {
	return status == StatusCompleted || status == StatusCanceled || status == StatusRejected || status == StatusDLQ
}

func (store *MemoryStore) attemptLimit() int {
	if store.maxAttempts > 0 {
		return store.maxAttempts
	}
	return 5
}

// Complete marks a claim terminal only for its current owner.
func (store *MemoryStore) Complete(_ context.Context, claim Claim) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil {
		return fmt.Errorf("idempotency: inbox %q not found", claim.InboxID)
	}
	if item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	item.claim.Status = StatusCompleted
	return nil
}

func (store *MemoryStore) GetExecution(_ context.Context, claim Claim) (ExecutionRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken {
		return ExecutionRecord{}, ErrClaimOwner
	}
	return item.execution, nil
}

func (store *MemoryStore) SaveExecution(_ context.Context, claim Claim, execution ExecutionRecord) error {
	if execution.Stage == ExecutionNone || execution.Reply == "" || execution.EventID == "" {
		return errors.New("idempotency: execution stage, reply, and event ID are required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil || item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	if stageRank(execution.Stage) < stageRank(item.execution.Stage) {
		return nil
	}
	if execution.Stage == item.execution.Stage && item.execution.Stage != ExecutionNone && execution != item.execution {
		return errors.New("idempotency: execution checkpoint conflict")
	}
	item.execution = execution
	return nil
}

func stageRank(stage ExecutionStage) int {
	switch stage {
	case ExecutionRunnerCommitted:
		return 1
	case ExecutionDerivedCommitted:
		return 2
	case ExecutionOutboxCommitted:
		return 3
	default:
		return 0
	}
}

// Fail schedules a retry only for the current owner.
func (store *MemoryStore) Fail(_ context.Context, claim Claim, cause error, retryAt time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	item := store.records[claim.InboxID]
	if item == nil {
		return fmt.Errorf("idempotency: inbox %q not found", claim.InboxID)
	}
	if item.claim.Owner != claim.Owner || item.claim.ClaimToken != claim.ClaimToken || item.claim.Status != StatusProcessing || !store.now().UTC().Before(item.claim.LeaseUntil) {
		return ErrClaimOwner
	}
	item.claim.Status = StatusRetry
	item.nextAttempt = retryAt.UTC()
	if cause != nil {
		item.lastError = sanitizeError(cause.Error())
	}
	return nil
}
