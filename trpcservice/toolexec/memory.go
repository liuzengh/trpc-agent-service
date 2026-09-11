package toolexec

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type MemoryJournal struct {
	mu      sync.Mutex
	closed  bool
	records map[string]Execution
}

func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{records: make(map[string]Execution)}
}

func (j *MemoryJournal) Start(ctx context.Context, execution Execution) (StartResult, error) {
	if ctx != nil && ctx.Err() != nil {
		return StartResult{}, context.Cause(ctx)
	}
	if err := validate(&execution); err != nil {
		return StartResult{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return StartResult{}, errors.New("tool execution journal is closed")
	}
	if existing, ok := j.records[execution.ID]; ok {
		if !sameExecution(existing, execution) {
			return StartResult{}, ErrConflict
		}
		return StartResult{Execution: existing, Existing: true}, nil
	}
	execution.Status = StatusRunning
	execution.StartedAt = time.Now().UTC()
	j.records[execution.ID] = execution
	return StartResult{Execution: execution}, nil
}

func (j *MemoryJournal) Complete(
	ctx context.Context,
	executionID string,
	status string,
	resultHash string,
	errorType string,
) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if !validOutcome(status) {
		return ErrConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("tool execution journal is closed")
	}
	execution, ok := j.records[executionID]
	if !ok {
		return ErrNotFound
	}
	if terminal(execution.Status) {
		if execution.Status == status && execution.ResultHash == resultHash && execution.ErrorType == errorType {
			return nil
		}
		return ErrConflict
	}
	execution.Status = status
	execution.ResultHash = resultHash
	execution.ErrorType = errorType
	execution.CompletedAt = time.Now().UTC()
	j.records[executionID] = execution
	return nil
}

func (j *MemoryJournal) Ready(context.Context) error { return nil }

func (j *MemoryJournal) ListByRequest(ctx context.Context, tenantID, requestID string) ([]Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("tool execution journal is closed")
	}
	result := make([]Execution, 0)
	for _, item := range j.records {
		if item.TenantID == tenantID && item.RequestID == requestID {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, k int) bool { return result[i].ToolCallID < result[k].ToolCallID })
	return result, nil
}
func (j *MemoryJournal) Close() error {
	j.mu.Lock()
	j.closed = true
	j.mu.Unlock()
	return nil
}

func (j *MemoryJournal) Get(ctx context.Context, tenantID, id string) (Execution, error) {
	if err := ctx.Err(); err != nil {
		return Execution{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Execution{}, errors.New("tool execution journal is closed")
	}
	record, ok := j.records[id]
	if !ok || record.TenantID != tenantID {
		return Execution{}, ErrNotFound
	}
	return record, nil
}

func (j *MemoryJournal) LinkOperation(ctx context.Context, tenantID, id, operationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if operationID == "" {
		return ErrConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("tool execution journal is closed")
	}
	record, ok := j.records[id]
	if !ok || record.TenantID != tenantID {
		return ErrNotFound
	}
	if record.OperationID != "" && record.OperationID != operationID {
		return ErrConflict
	}
	record.OperationID = operationID
	j.records[id] = record
	return nil
}

func (j *MemoryJournal) ResolveOperation(ctx context.Context, tenantID, operationID, status, resultHash, errorType string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if operationID == "" || !validOutcome(status) {
		return ErrConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("tool execution journal is closed")
	}
	for id, record := range j.records {
		if record.TenantID != tenantID || record.OperationID != operationID || terminal(record.Status) {
			continue
		}
		record.Status, record.ResultHash, record.ErrorType = status, resultHash, errorType
		record.CompletedAt = time.Now().UTC()
		j.records[id] = record
	}
	return nil
}

var _ Journal = (*MemoryJournal)(nil)
