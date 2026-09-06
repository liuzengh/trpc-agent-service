package approval

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type MemoryRepository struct {
	mu      sync.Mutex
	closed  bool
	records map[string]Record
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{records: make(map[string]Record)}
}

func (r *MemoryRepository) Request(ctx context.Context, request Request) (Record, error) {
	if err := contextError(ctx); err != nil {
		return Record{}, err
	}
	if err := validateRequest(&request); err != nil {
		return Record{}, err
	}
	id := StableID(request.TenantID, request.RequestID, request.ToolCallID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Record{}, errors.New("approval repository is closed")
	}
	if existing, ok := r.records[id]; ok {
		if existing.ToolName != request.ToolName || existing.ArgumentsHash != request.ArgumentsHash {
			return Record{}, ErrConflict
		}
		return existing, nil
	}
	record := Record{
		ApprovalID: id, TenantID: request.TenantID, AppID: request.AppID,
		RevisionID: request.RevisionID, ChannelBindingID: request.ChannelBindingID,
		RequestID: request.RequestID, MessageID: request.MessageID, UserID: request.UserID,
		SessionID: request.SessionID, ToolCallID: request.ToolCallID, ToolName: request.ToolName,
		ArgumentsHash: request.ArgumentsHash, ResumeText: request.ResumeText,
		ReplyTarget: request.ReplyTarget, Status: StatusPending,
		ExpiresAt: request.ExpiresAt.UTC(), CreatedAt: time.Now().UTC(),
	}
	r.records[id] = record
	return record, nil
}

func (r *MemoryRepository) ListPendingByRequest(
	ctx context.Context,
	tenantID string,
	requestID string,
) ([]Record, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("approval repository is closed")
	}
	now := time.Now()
	result := make([]Record, 0)
	for id, record := range r.records {
		if record.Status == StatusPending && !record.ExpiresAt.After(now) {
			record.Status = StatusExpired
			r.records[id] = record
		}
		if record.TenantID == tenantID && record.RequestID == requestID && record.Status == StatusPending {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (r *MemoryRepository) Decide(ctx context.Context, decision Decision) (Record, error) {
	if err := contextError(ctx); err != nil {
		return Record{}, err
	}
	if !validDecisionStatus(decision.Status) || decision.ExternalMessageID == "" {
		return Record{}, errors.New("approval decision is invalid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.records[decision.ApprovalID]
	if !ok {
		return Record{}, ErrNotFound
	}
	if record.TenantID != decision.TenantID || record.ChannelBindingID != decision.ChannelBindingID ||
		record.UserID != decision.UserID || record.SessionID != decision.SessionID {
		return Record{}, ErrForbidden
	}
	if record.Status == StatusPending && !record.ExpiresAt.After(time.Now()) {
		record.Status = StatusExpired
		r.records[record.ApprovalID] = record
		return Record{}, ErrExpired
	}
	if record.Status != StatusPending {
		if record.Status == StatusExpired {
			return Record{}, ErrExpired
		}
		if record.Status == decision.Status {
			return record, nil
		}
		return Record{}, ErrConflict
	}
	for _, existing := range r.records {
		if existing.ChannelBindingID == decision.ChannelBindingID &&
			existing.DecisionMessageID == decision.ExternalMessageID {
			return Record{}, ErrConflict
		}
	}
	record.Status = decision.Status
	record.DecisionMessageID = decision.ExternalMessageID
	record.DecisionReason = decision.Reason
	record.DecidedAt = time.Now().UTC()
	r.records[record.ApprovalID] = record
	return record, nil
}

func (r *MemoryRepository) ListPendingBySession(ctx context.Context, tenantID, bindingID, userID, sessionID string) ([]Record, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("approval repository is closed")
	}
	result := make([]Record, 0)
	for _, record := range r.records {
		if record.TenantID == tenantID && record.ChannelBindingID == bindingID && record.UserID == userID &&
			record.SessionID == sessionID && record.Status == StatusPending && record.ExpiresAt.After(time.Now()) {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	if len(result) > 10 {
		result = result[:10]
	}
	return result, nil
}

func (r *MemoryRepository) MarkResumed(ctx context.Context, approvalID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.records[approvalID]
	if !ok {
		return ErrNotFound
	}
	if record.ResumedAt.IsZero() {
		record.ResumedAt = time.Now().UTC()
		r.records[approvalID] = record
	}
	return nil
}

func (r *MemoryRepository) Ready(ctx context.Context) error { return contextError(ctx) }

func (r *MemoryRepository) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func contextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

var _ Repository = (*MemoryRepository)(nil)
