package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

func controlPlaneAuditEvent(
	ctx context.Context,
	tenantID, appID, configVersion, eventType, decision string,
) platformaudit.Event {
	requestID := "control-plane-" + uuid.NewString()
	actorID, actorRole, ok := platformaudit.ControlPlaneActorFromContext(ctx)
	if !ok {
		actorID = "control-plane"
		actorRole = "system"
	}
	return platformaudit.Event{
		TenantID:      tenantID,
		AppID:         appID,
		ActorID:       actorID,
		ActorRole:     actorRole,
		Decision:      decision,
		TraceID:       requestID,
		RequestID:     requestID,
		ConfigVersion: configVersion,
		EventType:     eventType,
	}
}

func recordControlPlaneAuditTx(
	ctx context.Context,
	tx pgx.Tx,
	event platformaudit.Event,
) error {
	if err := insertAuditEventTx(ctx, tx, event); err != nil {
		return fmt.Errorf("record control-plane audit: %w", err)
	}
	return nil
}

var _ platformaudit.Sink = (*Store)(nil)

// Record persists metadata-only audit events in the exact tenant/app scope.
// Callers use this as a best-effort sink; an audit failure must not be used to
// roll back execution or reply state.
func (s *Store) Record(ctx context.Context, event platformaudit.Event) error {
	if err := s.validate(); err != nil {
		return err
	}
	// This is the final persistence boundary. Callers may have different
	// policy paths, but stored audit metadata is always redacted before it is
	// written.
	event = platformaudit.RedactEvent(event)
	if err := event.Validate(); err != nil {
		return err
	}
	return insertAuditEvent(ctx, s.pool, event)
}

func insertAuditEvent(ctx context.Context, executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, event platformaudit.Event) error {
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := executor.Exec(ctx, `
INSERT INTO platform.audit_event (
    tenant_id, app_id, actor_id, actor_role, requested_tenant_id,
    requested_app_id, query_digest, result_count, channel, user_id, session_id,
    agent_name, tool_name,
    decision, policy_rule_id, policy_reason, latency, error_type, input_tokens, output_tokens, total_tokens,
    cost, trace_id, request_id, config_version, event_type, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
		event.TenantID,
		event.AppID,
		event.ActorID,
		event.ActorRole,
		event.RequestedTenantID,
		event.RequestedAppID,
		event.QueryDigest,
		event.ResultCount,
		event.Channel,
		event.UserID,
		event.SessionID,
		event.AgentName,
		event.ToolName,
		event.Decision,
		event.PolicyRuleID,
		event.PolicyReason,
		event.Latency.Milliseconds(),
		event.ErrorType,
		event.InputTokens,
		event.OutputTokens,
		event.TotalTokens,
		event.Cost,
		event.TraceID,
		event.RequestID,
		event.ConfigVersion,
		event.EventType,
		createdAt,
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func insertAuditEventTx(ctx context.Context, tx pgx.Tx, event platformaudit.Event) error {
	event = platformaudit.RedactEvent(event)
	if err := event.Validate(); err != nil {
		return err
	}
	return insertAuditEvent(ctx, tx, event)
}

func (s *Store) recordAuditBestEffort(ctx context.Context, event platformaudit.Event) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.Record(auditCtx, event); err != nil {
		log.Printf("audit write failed tenant=%s app=%s event=%s: %s", event.TenantID, event.AppID, event.EventType, platformlog.SafeError(err))
		if s.metrics != nil {
			s.metrics.RecordAuditFailure(ctx, platformmetrics.Labels{
				TenantID: event.TenantID,
				AppID:    event.AppID,
				Channel:  event.Channel,
			})
		}
	}
}

// ListAuditEvents reads only the requested tenant/application partition.
func (s *Store) ListAuditEvents(ctx context.Context, tenantID, appID string, limit int) ([]platformaudit.Event, error) {
	return s.ListAuditEventsQuery(ctx, platformaudit.Query{TenantID: tenantID, AppID: appID, Limit: limit})
}

// ListAuditEventsQuery reads metadata-only events with optional filters from
// one exact tenant/application partition.
func (s *Store) ListAuditEventsQuery(ctx context.Context, query platformaudit.Query) ([]platformaudit.Event, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	clauses := []string{"tenant_id = $1", "app_id = $2"}
	args := []any{query.TenantID, query.AppID}
	if query.EventType != "" {
		args = append(args, query.EventType)
		clauses = append(clauses, fmt.Sprintf("event_type = $%d", len(args)))
	}
	if query.ToolName != "" {
		args = append(args, query.ToolName)
		clauses = append(clauses, fmt.Sprintf("tool_name = $%d", len(args)))
	}
	if query.TraceID != "" {
		args = append(args, query.TraceID)
		clauses = append(clauses, fmt.Sprintf("trace_id = $%d", len(args)))
	}
	if query.CreatedAfter != nil {
		args = append(args, query.CreatedAfter.UTC())
		clauses = append(clauses, fmt.Sprintf("created_at >= $%d", len(args)))
	}
	if query.CreatedBefore != nil {
		args = append(args, query.CreatedBefore.UTC())
		clauses = append(clauses, fmt.Sprintf("created_at <= $%d", len(args)))
	}
	args = append(args, limit)
	limitPosition := len(args)
	args = append(args, query.Offset)
	offsetPosition := len(args)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT tenant_id, app_id, channel, user_id, session_id, agent_name, tool_name,
       actor_id, actor_role, requested_tenant_id, requested_app_id, query_digest,
       result_count, decision, policy_rule_id, policy_reason, latency, error_type, input_tokens, output_tokens,
       total_tokens, cost, trace_id, request_id, config_version, event_type,
       created_at
FROM platform.audit_event
WHERE %s
ORDER BY created_at DESC, audit_event_id DESC
LIMIT $%d OFFSET $%d`, strings.Join(clauses, " AND "), limitPosition, offsetPosition), args...)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()
	result := make([]platformaudit.Event, 0)
	for rows.Next() {
		var event platformaudit.Event
		var latency int64
		if err := rows.Scan(
			&event.TenantID,
			&event.AppID,
			&event.Channel,
			&event.UserID,
			&event.SessionID,
			&event.AgentName,
			&event.ToolName,
			&event.ActorID,
			&event.ActorRole,
			&event.RequestedTenantID,
			&event.RequestedAppID,
			&event.QueryDigest,
			&event.ResultCount,
			&event.Decision,
			&event.PolicyRuleID,
			&event.PolicyReason,
			&latency,
			&event.ErrorType,
			&event.InputTokens,
			&event.OutputTokens,
			&event.TotalTokens,
			&event.Cost,
			&event.TraceID,
			&event.RequestID,
			&event.ConfigVersion,
			&event.EventType,
			&event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		event.Latency = time.Duration(latency) * time.Millisecond
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return result, nil
}
