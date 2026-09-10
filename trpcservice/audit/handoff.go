package audit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrHandoffNotFound indicates that a handoff does not exist.
	ErrHandoffNotFound = errors.New("audit handoff not found")
	// ErrHandoffConflict indicates a conflicting handoff transition.
	ErrHandoffConflict = errors.New("audit handoff conflict")
)

// HandoffState identifies a handoff lifecycle state.
type HandoffState string

const (
	// HandoffPending indicates a reserved handoff.
	HandoffPending HandoffState = "pending"
	// HandoffFinalized indicates a completed handoff.
	HandoffFinalized HandoffState = "finalized"
	// HandoffRepairable indicates a handoff needing repair.
	HandoffRepairable HandoffState = "repairable"
)

// ExecutionHandoff tracks a cross-component execution handoff.
type ExecutionHandoff struct {
	TenantID  string
	HandoffID string
	RequestID string
	TraceID   string
	EventID   string
	State     HandoffState
	Result    ExecutionResult
	ErrorType string
	LatencyMS *int64
	Cost      *Usage
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Clone returns an isolated copy of the handoff.
func (h ExecutionHandoff) Clone() ExecutionHandoff {
	h.Cost = h.Cost.Clone()
	h.LatencyMS = cloneInt64(h.LatencyMS)
	return h
}

// HandoffStore persists execution handoffs.
type HandoffStore interface {
	Reserve(context.Context, ExecutionHandoff) (ExecutionHandoff, error)
	Finalize(context.Context, ExecutionHandoff) (ExecutionHandoff, error)
	Get(context.Context, string, string) (ExecutionHandoff, error)
}

// InMemoryHandoffStore stores handoffs in memory.
type InMemoryHandoffStore struct {
	mu     sync.Mutex
	values map[string]ExecutionHandoff
}

// NewInMemoryHandoffStore creates an in-memory handoff store.
func NewInMemoryHandoffStore() *InMemoryHandoffStore {
	return &InMemoryHandoffStore{values: map[string]ExecutionHandoff{}}
}

// Reserve stores a pending handoff.
func (s *InMemoryHandoffStore) Reserve(ctx context.Context, value ExecutionHandoff) (ExecutionHandoff, error) {
	if err := handoffContext(ctx); err != nil {
		return ExecutionHandoff{}, err
	}
	if value.TenantID == "" || value.HandoffID == "" || value.RequestID == "" || value.State != HandoffPending {
		return ExecutionHandoff{}, ErrInvalid
	}
	now := time.Now().UTC()
	value.CreatedAt = now
	value.UpdatedAt = now
	s.mu.Lock()
	defer s.mu.Unlock()
	key := value.TenantID + "\x00" + value.HandoffID
	if old, ok := s.values[key]; ok {
		if old.RequestID == value.RequestID {
			return old.Clone(), nil
		}
		return ExecutionHandoff{}, ErrHandoffConflict
	}
	s.values[key] = value.Clone()
	return value.Clone(), nil
}

// Finalize marks a handoff complete.
func (s *InMemoryHandoffStore) Finalize(ctx context.Context, value ExecutionHandoff) (ExecutionHandoff, error) {
	if err := handoffContext(ctx); err != nil {
		return ExecutionHandoff{}, err
	}
	if value.TenantID == "" || value.HandoffID == "" || value.State != HandoffFinalized {
		return ExecutionHandoff{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := value.TenantID + "\x00" + value.HandoffID
	old, ok := s.values[key]
	if !ok {
		return ExecutionHandoff{}, ErrHandoffNotFound
	}
	if old.State == HandoffFinalized {
		return old.Clone(), nil
	}
	value.CreatedAt = old.CreatedAt
	value.UpdatedAt = time.Now().UTC()
	value.RequestID = old.RequestID
	value.TraceID = old.TraceID
	value.EventID = old.EventID
	s.values[key] = value.Clone()
	return value.Clone(), nil
}

// Get retrieves a handoff by tenant and ID.
func (s *InMemoryHandoffStore) Get(ctx context.Context, tenantID, handoffID string) (ExecutionHandoff, error) {
	if err := handoffContext(ctx); err != nil {
		return ExecutionHandoff{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[tenantID+"\x00"+handoffID]
	if !ok {
		return ExecutionHandoff{}, ErrHandoffNotFound
	}
	return value.Clone(), nil
}

func handoffContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}
