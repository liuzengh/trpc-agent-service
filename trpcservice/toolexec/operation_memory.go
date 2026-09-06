package toolexec

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

type MemoryOperations struct {
	mu      sync.Mutex
	records map[string]Operation
}

func NewMemoryOperations() *MemoryOperations {
	return &MemoryOperations{records: map[string]Operation{}}
}
func (s *MemoryOperations) Reserve(ctx context.Context, op Operation) (Operation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, false, err
	}
	if !validOperation(op) {
		return Operation{}, false, ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.records[op.ID]; ok {
		if !sameOperation(old, op) {
			return Operation{}, false, ErrOperationConflict
		}
		return cloneOperation(old), false, nil
	}
	op.Status, op.CreatedAt, op.UpdatedAt = StatusRunning, time.Now().UTC(), time.Now().UTC()
	op.Result, op.ErrorType = nil, ""
	s.records[op.ID] = cloneOperation(op)
	return cloneOperation(op), true, nil
}
func (s *MemoryOperations) Get(ctx context.Context, tenantID, id string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.records[id]
	if !ok || op.TenantID != tenantID {
		return Operation{}, ErrNotFound
	}
	return cloneOperation(op), nil
}
func (s *MemoryOperations) List(ctx context.Context, tenantID, status, after string, limit int) ([]Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []Operation{}
	for _, op := range s.records {
		if op.TenantID == tenantID && (status == "" || op.Status == status) && op.ID > after {
			result = append(result, cloneOperation(op))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (s *MemoryOperations) Finish(ctx context.Context, tenantID, id, status string, result json.RawMessage, errorType string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	if !validOutcome(status) {
		return Operation{}, ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.records[id]
	if !ok || op.TenantID != tenantID {
		return Operation{}, ErrNotFound
	}
	// Committed facts dominate late timeouts/not-found observations.
	if terminal(op.Status) {
		return cloneOperation(op), nil
	}
	op.Status, op.Result, op.ErrorType, op.UpdatedAt = status, append(json.RawMessage(nil), result...), errorType, time.Now().UTC()
	s.records[id] = op
	return cloneOperation(op), nil
}
func (s *MemoryOperations) Ready(ctx context.Context) error { return ctx.Err() }
