// Package postgres implements the shared PostgreSQL TaskStore.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type TaskStore struct {
	db         *sql.DB
	parkPolicy gateway.ParkPolicy
}

func NewTaskStore(db *sql.DB) *TaskStore {
	return &TaskStore{db: db, parkPolicy: gateway.DefaultParkPolicy()}
}

func NewTaskStoreWithParkPolicy(db *sql.DB, policy gateway.ParkPolicy) (*TaskStore, error) {
	if db == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &TaskStore{db: db, parkPolicy: policy}, nil
}

func (s *TaskStore) PrepareDispatch(ctx context.Context, in gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	if err := in.Tenant.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if err := in.Binding.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	var input sql.NullInt64
	var accepted bool
	var reason sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT input_seq,accepted,terminal_reason FROM prepare_dispatch(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, in.Tenant.TenantID, in.Tenant.TenantVersion,
		in.Tenant.AgentAppID, in.Binding.AgentAppVersion, in.Binding.AgentAppRevision,
		in.Binding.AgentContentDigest, in.Binding.ConfigVersion, in.Binding.PolicyVersion,
		in.RequestID, in.SessionID, in.UserID, in.Tenant.Channel, in.PayloadRef, nullable(in.TraceParent)).
		Scan(&input, &accepted, &reason)
	if err != nil {
		return gateway.PreparedDispatch{}, translate(err)
	}
	if !accepted {
		return gateway.PreparedDispatch{Accepted: false, TerminalReason: reason.String}, nil
	}
	status, err := s.GetExecution(ctx, gateway.ExecutionKey{TenantID: in.Tenant.TenantID, RequestID: in.RequestID})
	if err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if status.Envelope.InputSeq != uint64(input.Int64) {
		return gateway.PreparedDispatch{}, runtime.ErrInvariantViolation
	}
	return gateway.PreparedDispatch{Envelope: status.Envelope, Accepted: true}, nil
}

func (s *TaskStore) GetExecution(ctx context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	var result gateway.ExecutionStatus
	var created time.Time
	err := s.db.QueryRowContext(ctx, `SELECT e.tenant_id,e.tenant_version,e.agent_app_id,e.agent_app_version,
e.agent_app_revision,e.agent_content_digest,e.config_version,e.policy_version,e.request_id,e.session_id,e.user_id,e.channel,
e.input_seq,e.payload_ref,COALESCE(e.traceparent,''),e.max_llm_calls,e.max_tool_calls,e.max_parallel_tools,e.execution_timeout_seconds,e.outcome,COALESCE(e.result_ref,''),e.created_at,e.version,
(e.cancel_requested_at IS NOT NULL OR t.status='disabled'),
CASE WHEN e.cancel_requested_at IS NOT NULL THEN e.cancel_version WHEN t.status='disabled' THEN t.version ELSE 0 END
FROM execution_record e JOIN tenant t ON t.tenant_id=e.tenant_id
WHERE e.tenant_id=$1 AND e.request_id=$2`, key.TenantID, key.RequestID).
		Scan(&result.Envelope.TenantID, &result.Envelope.TenantVersion, &result.Envelope.AgentAppID,
			&result.Envelope.AgentAppVersion, &result.Envelope.AgentAppRevision, &result.Envelope.AgentContentDigest,
			&result.Envelope.ConfigVersion, &result.Envelope.PolicyVersion, &result.Envelope.RequestID,
			&result.Envelope.SessionID, &result.Envelope.UserID, &result.Envelope.Channel, &result.Envelope.InputSeq,
			&result.Envelope.PayloadRef, &result.Envelope.TraceParent, &result.Envelope.ExecutionBudget.MaxLLMCalls,
			&result.Envelope.ExecutionBudget.MaxToolCalls, &result.Envelope.ExecutionBudget.MaxParallelTools,
			&result.Envelope.ExecutionBudget.ExecutionTimeoutSeconds, &result.Outcome, &result.ResultRef, &created,
			&result.Version, &result.CancelRequested, &result.CancelVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return gateway.ExecutionStatus{}, runtime.ErrNotFound
	}
	if err != nil {
		return gateway.ExecutionStatus{}, err
	}
	result.Envelope.SchemaVersion = runtime.CurrentEnvelopeSchemaVersion
	result.Envelope.CreatedAt = created
	return result, nil
}

func (s *TaskStore) RequestCancel(ctx context.Context, in gateway.CancelRequest) (gateway.CancelResult, error) {
	if in.TenantID == "" || in.RequestID == "" {
		return gateway.CancelResult{}, runtime.ErrTenantScope
	}
	if in.ExpectedVersion < 0 {
		return gateway.CancelResult{}, runtime.ErrVersionConflict
	}
	if in.ActorID == "" || in.ReasonCode == "" {
		return gateway.CancelResult{}, runtime.ErrCommitConflict
	}
	var result gateway.CancelResult
	err := s.db.QueryRowContext(ctx, `SELECT accepted,execution_version,cancel_version FROM request_cancel_execution($1,$2,$3,$4,$5,$6)`,
		in.TenantID, in.RequestID, in.ExpectedVersion, in.ActorID, in.ReasonCode, nullable(in.TraceParent)).
		Scan(&result.Accepted, &result.Version, &result.CancelVersion)
	return result, translate(err)
}

func (s *TaskStore) ParkInput(ctx context.Context, in gateway.ParkRequest) (gateway.ParkResult, error) {
	if in.TenantID == "" || in.RequestID == "" {
		return gateway.ParkResult{}, runtime.ErrTenantScope
	}
	if in.InputSeq < 1 {
		return gateway.ParkResult{}, runtime.ErrCommitConflict
	}
	var result gateway.ParkResult
	var notBefore, deadline sql.NullTime
	policy := s.parkPolicy
	if err := policy.Validate(); err != nil {
		return gateway.ParkResult{}, err
	}
	err := s.db.QueryRowContext(ctx, `SELECT disposition,attempt,execution_version,not_before,deadline
FROM park_execution($1,$2,$3,$4,$5,$6,$7)`, in.TenantID, in.RequestID, in.InputSeq,
		int64(policy.BaseDelay/time.Second), int64(policy.MaxDelay/time.Second),
		int64(policy.Deadline/time.Second), policy.MaxAttempts).
		Scan(&result.Disposition, &result.Attempt, &result.Version, &notBefore, &deadline)
	result.NotBefore, result.Deadline = notBefore.Time, deadline.Time
	return result, translate(err)
}

func (s *TaskStore) InspectWakeup(ctx context.Context, key gateway.ExecutionKey) (gateway.WakeupCandidate, error) {
	if key.TenantID == "" || key.RequestID == "" {
		return gateway.WakeupCandidate{}, runtime.ErrTenantScope
	}
	var result gateway.WakeupCandidate
	var created time.Time
	result.Execution.Envelope.SchemaVersion = runtime.CurrentEnvelopeSchemaVersion
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id,tenant_version,agent_app_id,agent_app_version,
agent_app_revision,agent_content_digest,config_version,policy_version,request_id,session_id,user_id,channel,
input_seq,payload_ref,COALESCE(traceparent,''),outcome,COALESCE(result_ref,''),created_at,ready,blocked,execution_version
FROM inspect_execution_wakeup($1,$2)`, key.TenantID, key.RequestID).
		Scan(&result.Execution.Envelope.TenantID, &result.Execution.Envelope.TenantVersion,
			&result.Execution.Envelope.AgentAppID, &result.Execution.Envelope.AgentAppVersion,
			&result.Execution.Envelope.AgentAppRevision, &result.Execution.Envelope.AgentContentDigest,
			&result.Execution.Envelope.ConfigVersion, &result.Execution.Envelope.PolicyVersion,
			&result.Execution.Envelope.RequestID, &result.Execution.Envelope.SessionID,
			&result.Execution.Envelope.UserID, &result.Execution.Envelope.Channel,
			&result.Execution.Envelope.InputSeq, &result.Execution.Envelope.PayloadRef,
			&result.Execution.Envelope.TraceParent, &result.Execution.Outcome,
			&result.Execution.ResultRef, &created, &result.Ready, &result.Blocked, &result.Version)
	if err != nil {
		return gateway.WakeupCandidate{}, translate(err)
	}
	result.Execution.Envelope.CreatedAt = created
	status, statusErr := s.GetExecution(ctx, key)
	if statusErr != nil {
		return gateway.WakeupCandidate{}, statusErr
	}
	result.Execution.Envelope.ExecutionBudget = status.Envelope.ExecutionBudget
	return result, nil
}

func (s *TaskStore) MarkWoken(ctx context.Context, key gateway.ExecutionKey, expectedVersion int64) error {
	if key.TenantID == "" || key.RequestID == "" {
		return runtime.ErrTenantScope
	}
	if expectedVersion < 0 {
		return runtime.ErrVersionConflict
	}
	result, err := s.db.ExecContext(ctx, `UPDATE execution_record SET outcome='queued',version=version+1
WHERE tenant_id=$1 AND request_id=$2 AND outcome='pending' AND version=$3`, key.TenantID, key.RequestID, expectedVersion)
	if err != nil {
		return translate(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *TaskStore) ListActionableParkedInputs(ctx context.Context, before time.Time, limit int) ([]gateway.ExecutionKey, error) {
	if before.IsZero() || limit < 1 {
		return nil, runtime.ErrCommitConflict
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.tenant_id,e.request_id
FROM execution_record e
JOIN session_head h ON h.tenant_id=e.tenant_id AND h.agent_app_id=e.agent_app_id AND h.session_id=e.session_id
WHERE e.outcome='pending' AND ((e.input_seq=h.next_input_seq AND COALESCE(e.not_before,'-infinity'::timestamptz)<=$1)
  OR (e.park_deadline IS NOT NULL AND e.park_deadline<=$1))
ORDER BY COALESCE(e.park_deadline,'infinity'::timestamptz),e.not_before,e.created_at
LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []gateway.ExecutionKey
	for rows.Next() {
		var key gateway.ExecutionKey
		if err := rows.Scan(&key.TenantID, &key.RequestID); err != nil {
			return nil, err
		}
		result = append(result, key)
	}
	return result, rows.Err()
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return runtime.ErrVersionConflict
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "42501":
			return runtime.ErrTenantScope
		case "P0904":
			return runtime.ErrPreprocessNotReady
		case "XX001":
			return runtime.ErrInvariantViolation
		case "P0002":
			return runtime.ErrNotFound
		case "55000":
			if strings.Contains(strings.ToLower(err.Error()), "park") {
				return runtime.ErrCommitConflict
			}
			return runtime.ErrVersionMismatch
		}
	}
	return err
}

var _ gateway.TaskStore = (*TaskStore)(nil)
var _ gateway.WakeupStore = (*TaskStore)(nil)
