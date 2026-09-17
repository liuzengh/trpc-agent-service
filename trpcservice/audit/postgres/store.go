package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) ResolveAuditEvent(ctx context.Context, record messaging.OutboxRecord) (audit.Event, error) {
	if s == nil || s.db == nil || record.Kind != "audit" || record.TenantID == "" || record.OutboxID == "" || record.CreatedAt.IsZero() {
		return audit.Event{}, runtime.ErrInvariantViolation
	}
	id, err := audit.StableID(record.TenantID, record.OutboxID)
	if err != nil {
		return audit.Event{}, err
	}
	event := audit.Event{SchemaVersion: 1, AuditID: id, TenantID: record.TenantID,
		Action: auditAction(record), Decision: auditDecision(record), OccurredAt: record.CreatedAt.UTC()}
	if record.PayloadRef != "" {
		if err := validateScopedRef(record.PayloadRef, record.TenantID); err != nil {
			return audit.Event{}, err
		}
		event.ResourceRefs = []string{record.PayloadRef}
	}
	event.TraceID = traceID(record.TraceParent)

	switch {
	case strings.HasPrefix(record.PayloadRef, "artifact-quarantine://"):
		if err := s.resolveArtifactQuarantine(ctx, record, &event); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "governance://"):
		if err := s.resolveGovernance(ctx, record, &event); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "confirmation://"):
		if err := s.resolveConfirmation(ctx, record, &event, "ask"); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "confirmation-decision://"):
		parts, splitErr := scopedParts(record.PayloadRef, "confirmation-decision://", record.TenantID, 3)
		if splitErr != nil {
			return audit.Event{}, splitErr
		}
		if err := s.resolveConfirmation(ctx, record, &event, parts[1]); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "confirmation-consumed://"):
		if err := s.resolveConfirmation(ctx, record, &event, "consumed"); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "execution-budget://"):
		if err := s.resolveExecutionBudget(ctx, record, &event); err != nil {
			return audit.Event{}, err
		}
	case strings.HasPrefix(record.PayloadRef, "execution-terminal://"):
		if err := s.resolveExecutionTerminal(ctx, record, &event); err != nil {
			return audit.Event{}, err
		}
	default:
		if err := s.resolveExecution(ctx, record.TenantID, record.AggregateID, &event); err == nil {
			event.RequestID = record.AggregateID
		} else if !errors.Is(err, runtime.ErrNotFound) {
			return audit.Event{}, err
		}
	}
	if err := audit.Validate(event); err != nil {
		return audit.Event{}, err
	}
	return event, nil
}

func (s *Store) resolveExecutionTerminal(ctx context.Context, record messaging.OutboxRecord, event *audit.Event) error {
	parts, err := scopedParts(record.PayloadRef, "execution-terminal://", record.TenantID, 2)
	if err != nil || len(parts) != 2 || parts[0] != record.AggregateID || record.EventSeq != 1 {
		return runtime.ErrInvalidEnvelope
	}
	switch runtime.Outcome(parts[1]) {
	case runtime.OutcomeSucceeded, runtime.OutcomeDenied, runtime.OutcomeFailed, runtime.OutcomeCancelled,
		runtime.OutcomeConfirmationDenied, runtime.OutcomeConfirmationTimeout:
	default:
		return runtime.ErrInvalidEnvelope
	}
	event.Action, event.Decision, event.RequestID = "execution.terminal", parts[1], parts[0]
	return s.resolveExecution(ctx, record.TenantID, parts[0], event)
}

func (s *Store) resolveExecutionBudget(ctx context.Context, record messaging.OutboxRecord, event *audit.Event) error {
	parts, err := scopedParts(record.PayloadRef, "execution-budget://", record.TenantID, 2)
	if err != nil || len(parts) != 2 || parts[0] != record.AggregateID {
		return runtime.ErrInvalidEnvelope
	}
	switch parts[1] {
	case "max_llm_calls", "max_tool_calls", "max_parallel_tools", "execution_timeout_seconds":
	default:
		return runtime.ErrInvalidEnvelope
	}
	event.Action, event.Decision, event.ReasonCode = "execution.budget", "denied", parts[1]
	event.RequestID = parts[0]
	return s.resolveExecution(ctx, record.TenantID, parts[0], event)
}

func (s *Store) resolveArtifactQuarantine(ctx context.Context, record messaging.OutboxRecord, event *audit.Event) error {
	parts, err := scopedParts(record.PayloadRef, "artifact-quarantine://", record.TenantID, 3)
	if err != nil || len(parts) != 3 || (parts[0] != "upload" && parts[0] != "retention") {
		return runtime.ErrInvalidEnvelope
	}
	version, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || version < 1 || record.AggregateID != parts[1] || record.EventSeq != uint64(version) {
		return runtime.ErrVersionMismatch
	}
	var requestID, contentDigest, state, errorClass string
	var occurredAt timeValue
	if parts[0] == "upload" {
		err = s.db.QueryRowContext(ctx, `SELECT request_id,content_digest,state,last_error_class,quarantined_at
FROM artifact_object_upload WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3`, record.TenantID, parts[1], version).Scan(
			&requestID, &contentDigest, &state, &errorClass, &occurredAt)
	} else {
		err = s.db.QueryRowContext(ctx, `SELECT request_id,content_digest,lifecycle_state,last_error_class,quarantined_at
FROM media_artifact WHERE tenant_id=$1 AND artifact_id=$2 AND lifecycle_version=$3`, record.TenantID, parts[1], version).Scan(
			&requestID, &contentDigest, &state, &errorClass, &occurredAt)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	if err != nil {
		return err
	}
	if state != "quarantined" || errorClass == "" || occurredAt.Time.IsZero() {
		return runtime.ErrInvariantViolation
	}
	event.RequestID, event.Action, event.Decision = requestID, "artifact.quarantine", "alert"
	event.ErrorType, event.ContentDigest, event.OccurredAt = errorClass, contentDigest, occurredAt.Time.UTC()
	return nil
}

func (s *Store) resolveGovernance(ctx context.Context, record messaging.OutboxRecord, event *audit.Event) error {
	parts, err := scopedParts(record.PayloadRef, "governance://", record.TenantID, 1)
	if err != nil {
		return err
	}
	var stage, decision, reason, requestID string
	var policyVersion int64
	var occurredAt timeValue
	err = s.db.QueryRowContext(ctx, `SELECT request_id,stage,action,reason_code,policy_version,created_at
FROM governance_decision WHERE tenant_id=$1 AND decision_id=$2`, record.TenantID, parts[0]).Scan(
		&requestID, &stage, &decision, &reason, &policyVersion, &occurredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	if err != nil {
		return err
	}
	event.RequestID, event.Action, event.Decision, event.ReasonCode, event.PolicyVersion = requestID, "governance."+stage, decision, reason, policyVersion
	event.OccurredAt = occurredAt.Time
	return s.resolveExecution(ctx, record.TenantID, requestID, event)
}

func (s *Store) resolveConfirmation(ctx context.Context, record messaging.OutboxRecord, event *audit.Event, decision string) error {
	prefix := "confirmation://"
	minimum := 1
	if strings.HasPrefix(record.PayloadRef, "confirmation-decision://") {
		prefix, minimum = "confirmation-decision://", 3
	} else if strings.HasPrefix(record.PayloadRef, "confirmation-consumed://") {
		prefix, minimum = "confirmation-consumed://", 2
	}
	parts, err := scopedParts(record.PayloadRef, prefix, record.TenantID, minimum)
	if err != nil {
		return err
	}
	confirmationID := parts[0]
	var requestID, toolName string
	var occurredAt timeValue
	err = s.db.QueryRowContext(ctx, `SELECT request_id,tool_id,created_at FROM confirmation WHERE tenant_id=$1 AND confirmation_id=$2`,
		record.TenantID, confirmationID).Scan(&requestID, &toolName, &occurredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	if err != nil {
		return err
	}
	event.RequestID, event.ToolName, event.Action, event.Decision = requestID, toolName, "tool_confirmation", decision
	if decision == "ask" {
		event.OccurredAt = occurredAt.Time
	}
	return s.resolveExecution(ctx, record.TenantID, requestID, event)
}

func (s *Store) resolveExecution(ctx context.Context, tenantID, requestID string, event *audit.Event) error {
	if requestID == "" {
		return runtime.ErrNotFound
	}
	err := s.db.QueryRowContext(ctx, `SELECT channel,user_id,session_id,agent_app_id,agent_app_revision,config_version,policy_version
FROM execution_record WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).Scan(&event.Channel, &event.UserID, &event.SessionID,
		&event.AgentAppID, &event.AgentAppRevision, &event.ConfigVersion, &event.PolicyVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	return err
}

func (s *Store) Emit(ctx context.Context, event audit.Event) error {
	if s == nil || s.db == nil {
		return runtime.ErrInvariantViolation
	}
	digest, err := audit.Digest(event)
	if err != nil {
		return err
	}
	resources, err := json.Marshal(event.ResourceRefs)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO audit_event(tenant_id,audit_id,schema_version,channel,user_id,session_id,request_id,
agent_app_id,agent_app_revision,agent_name,tool_name,action,decision,reason_code,latency_ms,error_type,cost_micros,currency,
input_tokens,output_tokens,config_version,policy_version,content_digest,trace_id,resource_refs,occurred_at,event_digest)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)
ON CONFLICT (tenant_id,audit_id) DO NOTHING`, event.TenantID, event.AuditID, event.SchemaVersion, event.Channel, event.UserID,
		event.SessionID, event.RequestID, event.AgentAppID, event.AgentAppRevision, event.AgentName, event.ToolName, event.Action,
		event.Decision, event.ReasonCode, event.LatencyMS, event.ErrorType, event.CostMicros, event.Currency, event.InputTokens,
		event.OutputTokens, event.ConfigVersion, event.PolicyVersion, event.ContentDigest, event.TraceID, resources, event.OccurredAt.UTC(), digest)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows == 1 {
		return err
	}
	var stored string
	if err := s.db.QueryRowContext(ctx, `SELECT event_digest FROM audit_event WHERE tenant_id=$1 AND audit_id=$2`, event.TenantID, event.AuditID).Scan(&stored); err != nil {
		return err
	}
	if stored != digest {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) AuditBacklog(ctx context.Context, now time.Time) (audit.Backlog, error) {
	if s == nil || s.db == nil || now.IsZero() {
		return audit.Backlog{}, runtime.ErrInvariantViolation
	}
	value := audit.Backlog{ObservedAt: now.UTC()}
	var oldestSeconds float64
	err := s.db.QueryRowContext(ctx, `SELECT
count(*) FILTER (WHERE state='pending'),count(*) FILTER (WHERE state='claimed'),
count(*) FILTER (WHERE state='retry_wait'),count(*) FILTER (WHERE state='dead_letter'),
GREATEST(0,COALESCE(EXTRACT(EPOCH FROM ($1::timestamptz-(min(created_at) FILTER (WHERE state IN ('pending','claimed','retry_wait'))))),0))
FROM outbox WHERE kind='audit'`, now.UTC()).Scan(&value.Pending, &value.Claimed, &value.RetryWait, &value.DeadLetter, &oldestSeconds)
	if err != nil {
		return audit.Backlog{}, err
	}
	value.OldestAge = time.Duration(oldestSeconds * float64(time.Second))
	if err := audit.ValidateBacklog(value); err != nil {
		return audit.Backlog{}, err
	}
	return value, nil
}

func auditAction(record messaging.OutboxRecord) string {
	value := record.IdempotencyKey
	if index := strings.IndexByte(value, ':'); index > 0 {
		value = value[:index]
	}
	if value == "" {
		value = "audit"
	}
	return value
}

func auditDecision(record messaging.OutboxRecord) string {
	switch {
	case strings.HasPrefix(record.IdempotencyKey, "cancel-intent:"):
		return "requested"
	case strings.HasPrefix(record.IdempotencyKey, "cancel:"):
		return "cancelled"
	case strings.HasPrefix(record.IdempotencyKey, "park-blocked:"):
		return "blocked"
	default:
		return "recorded"
	}
}

func scopedParts(ref, prefix, tenantID string, minimum int) ([]string, error) {
	value := strings.TrimPrefix(ref, prefix)
	parts := strings.Split(value, "/")
	if len(parts) < minimum+1 || parts[0] != tenantID {
		return nil, runtime.ErrTenantScope
	}
	for _, part := range parts {
		if part == "" || strings.TrimSpace(part) != part {
			return nil, runtime.ErrInvalidEnvelope
		}
	}
	return parts[1:], nil
}

func validateScopedRef(ref, tenantID string) error {
	separator := strings.Index(ref, "://")
	if separator < 1 || separator+3 >= len(ref) {
		return runtime.ErrInvalidEnvelope
	}
	parts := strings.Split(strings.TrimPrefix(ref[separator+3:], "/"), "/")
	if len(parts) == 0 || parts[0] != tenantID {
		return runtime.ErrTenantScope
	}
	return nil
}

func traceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) == 4 && len(parts[1]) == 32 {
		for _, value := range parts[1] {
			if !strings.ContainsRune("0123456789abcdef", value) {
				return ""
			}
		}
		return parts[1]
	}
	return ""
}

// timeValue keeps Scan sites explicit without leaking nullable timestamps
// into the stable audit contract.
type timeValue struct{ Time time.Time }

func (v *timeValue) Scan(source any) error {
	value, ok := source.(time.Time)
	if !ok {
		return runtime.ErrInvalidEnvelope
	}
	v.Time = value.UTC()
	return nil
}

var _ audit.Resolver = (*Store)(nil)
var _ audit.Sink = (*Store)(nil)
var _ audit.BacklogSource = (*Store)(nil)
