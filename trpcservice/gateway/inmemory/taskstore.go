// Package inmemory provides local task authority for the P0 vertical slice.
package inmemory

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type executionKey struct{ tenantID, requestID string }
type sessionKey struct{ tenantID, appID, sessionID string }

type TaskStore struct {
	mu         sync.RWMutex
	executions map[executionKey]gateway.ExecutionStatus
	nextInput  map[sessionKey]uint64
	parks      map[executionKey]gateway.ParkResult
}

func NewTaskStore() *TaskStore {
	return &TaskStore{executions: make(map[executionKey]gateway.ExecutionStatus), nextInput: make(map[sessionKey]uint64), parks: make(map[executionKey]gateway.ParkResult)}
}

func (s *TaskStore) PrepareDispatch(ctx context.Context, in gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	if err := ctx.Err(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if err := in.Tenant.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if err := in.Binding.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if in.RequestID == "" || in.SessionID == "" || in.UserID == "" || in.PayloadRef == "" {
		return gateway.PreparedDispatch{}, runtime.ErrInvalidEnvelope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ek := executionKey{in.Tenant.TenantID, in.RequestID}
	if existing, ok := s.executions[ek]; ok {
		candidate := envelope(in, existing.Envelope.InputSeq, existing.Envelope.CreatedAt)
		// Trace correlation is observability metadata, not part of stable run
		// identity. Preserve the first accepted value across protocol retries.
		candidate.TraceParent = existing.Envelope.TraceParent
		if !reflect.DeepEqual(existing.Envelope, candidate) {
			return gateway.PreparedDispatch{}, runtime.ErrCommitConflict
		}
		return gateway.PreparedDispatch{Envelope: existing.Envelope, Accepted: true}, nil
	}
	sk := sessionKey{in.Tenant.TenantID, in.Tenant.AgentAppID, in.SessionID}
	seq := s.nextInput[sk]
	if seq == 0 {
		seq = 1
	}
	s.nextInput[sk] = seq + 1
	e := envelope(in, seq, time.Now().UTC())
	if err := e.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	s.executions[ek] = gateway.ExecutionStatus{Envelope: e, Outcome: runtime.OutcomeQueued}
	return gateway.PreparedDispatch{Envelope: e, Accepted: true}, nil
}

func envelope(in gateway.PrepareDispatchRequest, seq uint64, createdAt time.Time) runtime.ExecutionEnvelope {
	return runtime.ExecutionEnvelope{SchemaVersion: runtime.CurrentEnvelopeSchemaVersion, TenantID: in.Tenant.TenantID, TenantVersion: in.Tenant.TenantVersion, AgentAppID: in.Tenant.AgentAppID, AgentAppVersion: in.Binding.AgentAppVersion, AgentAppRevision: in.Binding.AgentAppRevision, AgentContentDigest: in.Binding.AgentContentDigest, ConfigVersion: in.Binding.ConfigVersion, PolicyVersion: in.Binding.PolicyVersion, ExecutionBudget: in.Binding.ExecutionBudget, RequestID: in.RequestID, SessionID: in.SessionID, UserID: in.UserID, Channel: in.Tenant.Channel, InputSeq: seq, PayloadRef: in.PayloadRef, TraceParent: in.TraceParent, CreatedAt: createdAt}
}

func (s *TaskStore) GetExecution(ctx context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	if err := ctx.Err(); err != nil {
		return gateway.ExecutionStatus{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, ok := s.executions[executionKey{key.TenantID, key.RequestID}]
	if !ok {
		return gateway.ExecutionStatus{}, runtime.ErrNotFound
	}
	return status, nil
}

func (s *TaskStore) RequestCancel(ctx context.Context, in gateway.CancelRequest) (gateway.CancelResult, error) {
	if err := ctx.Err(); err != nil {
		return gateway.CancelResult{}, err
	}
	if in.TenantID == "" || in.RequestID == "" {
		return gateway.CancelResult{}, runtime.ErrTenantScope
	}
	if in.ExpectedVersion < 0 {
		return gateway.CancelResult{}, runtime.ErrVersionConflict
	}
	if in.ActorID == "" || in.ReasonCode == "" {
		return gateway.CancelResult{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := executionKey{in.TenantID, in.RequestID}
	status, ok := s.executions[k]
	if !ok {
		return gateway.CancelResult{}, runtime.ErrNotFound
	}
	if status.Outcome.Terminal() {
		return gateway.CancelResult{Accepted: false, Version: status.Version, CancelVersion: status.CancelVersion}, nil
	}
	if status.CancelRequested {
		return gateway.CancelResult{Accepted: true, Version: status.Version, CancelVersion: status.CancelVersion}, nil
	}
	if status.Version != in.ExpectedVersion {
		return gateway.CancelResult{}, runtime.ErrVersionConflict
	}
	status.Version++
	status.CancelRequested = true
	status.CancelVersion++
	s.executions[k] = status
	return gateway.CancelResult{Accepted: true, Version: status.Version, CancelVersion: status.CancelVersion}, nil
}

func (s *TaskStore) ParkInput(ctx context.Context, in gateway.ParkRequest) (gateway.ParkResult, error) {
	if err := ctx.Err(); err != nil {
		return gateway.ParkResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := executionKey{in.TenantID, in.RequestID}
	status, ok := s.executions[k]
	if !ok {
		return gateway.ParkResult{}, runtime.ErrNotFound
	}
	if status.Envelope.InputSeq != in.InputSeq {
		return gateway.ParkResult{}, runtime.ErrCommitConflict
	}
	if status.Outcome.Terminal() {
		return gateway.ParkResult{Disposition: gateway.ParkInputTerminal}, nil
	}
	// nextInput is the allocator, while the in-memory SessionStore owns the
	// execution gate. The local implementation therefore preserves an existing
	// park idempotently and is used only by local contract tests.
	if existing, exists := s.parks[k]; exists && status.Outcome == runtime.OutcomePending {
		return existing, nil
	}
	status.Outcome = runtime.OutcomePending
	s.executions[k] = status
	result := gateway.ParkResult{Disposition: gateway.ParkedInput, Attempt: 1, Version: 1, NotBefore: time.Now().UTC()}
	s.parks[k] = result
	return result, nil
}
