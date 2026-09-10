// Package postgres implements the tenant-bound PostgreSQL audit contract.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

// Store is permanently bound to one tenant. It cannot be retargeted after
// construction, which keeps tenant scope out of request/query payloads.
type Store struct {
	db       *sql.DB
	tenantID string
}

var _ audit.Writer = (*Store)(nil)
var _ audit.Reader = (*Store)(nil)
var _ audit.Aggregator = (*Store)(nil)

// New creates a tenant-bound PostgreSQL audit store. The database may be nil
// for wiring/tests, but operations return ErrStorage until a pool is supplied.
func New(db *sql.DB, tenantID string) (*Store, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || hasControl(tenantID) {
		return nil, audit.ErrInvalid
	}
	return &Store{db: db, tenantID: tenantID}, nil
}

// NewRepository is an explicit alias matching other PostgreSQL adapters.
func NewRepository(db *sql.DB, tenantID string) (*Store, error) { return New(db, tenantID) }

func (s *Store) check(ctx context.Context) error {
	if ctx == nil {
		return audit.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.tenantID == "" || s.db == nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) scopedTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := begin(ctx, s.db)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.tenant_id', $1, true)", s.tenantID); err != nil {
		rollback(tx)
		return nil, mapDBError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	return tx, nil
}

// Append persists an audit event.
func (s *Store) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	if err := s.check(ctx); err != nil {
		return audit.AppendResult{}, err
	}
	if event.TenantID != s.tenantID {
		return audit.AppendResult{}, audit.ErrTenantScope
	}
	digest, err := event.Digest()
	if err != nil {
		return audit.AppendResult{}, err
	}
	tx, err := s.scopedTx(ctx)
	if err != nil {
		return audit.AppendResult{}, err
	}
	defer rollback(tx)
	var storedDigest string
	var duplicate, conflict bool
	if err := tx.QueryRowContext(ctx, auditAppendSQL, appendEventArgs(event, digest)...).Scan(&storedDigest, &duplicate, &conflict); err != nil {
		return audit.AppendResult{}, mapDBError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	if conflict {
		return audit.AppendResult{}, audit.ErrConflict
	}
	if err := commit(ctx, tx); err != nil {
		return audit.AppendResult{}, err
	}
	return audit.AppendResult{Event: event.Clone(), Duplicate: duplicate, Digest: storedDigest}, nil
}

// Get retrieves an audit event by ID.
func (s *Store) Get(ctx context.Context, eventID string) (audit.Event, error) {
	if err := s.check(ctx); err != nil {
		return audit.Event{}, err
	}
	if strings.TrimSpace(eventID) == "" || hasControl(eventID) {
		return audit.Event{}, audit.ErrInvalid
	}
	tx, err := s.scopedTx(ctx)
	if err != nil {
		return audit.Event{}, err
	}
	defer rollback(tx)
	value, err := scanEvent(tx.QueryRowContext(ctx, auditSelectSQL+" WHERE tenant_id = $1 AND event_id = $2", s.tenantID, strings.TrimSpace(eventID)))
	if err != nil {
		return audit.Event{}, mapDBError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return audit.Event{}, err
	}
	return value, nil
}

// List retrieves audit events matching query.
func (s *Store) List(ctx context.Context, query audit.Query) ([]audit.Event, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	for _, typ := range query.EventTypes {
		if !validEventType(typ) {
			return nil, audit.ErrInvalid
		}
	}
	tx, err := s.scopedTx(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	statement := auditSelectSQL + " WHERE tenant_id = $1"
	args := []any{s.tenantID}
	if len(query.EventTypes) > 0 {
		placeholders := make([]string, len(query.EventTypes))
		for i, typ := range query.EventTypes {
			placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
			args = append(args, string(typ))
		}
		statement += " AND event_type IN (" + strings.Join(placeholders, ",") + ")"
	}
	if !query.Since.IsZero() {
		statement += fmt.Sprintf(" AND occurred_at >= $%d", len(args)+1)
		args = append(args, query.Since.UTC())
	}
	if !query.Until.IsZero() {
		statement += fmt.Sprintf(" AND occurred_at <= $%d", len(args)+1)
		args = append(args, query.Until.UTC())
	}
	statement += " ORDER BY occurred_at, event_id"
	rows, err := tx.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, mapDBError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	values := make([]audit.Event, 0)
	for rows.Next() {
		value, scanErr := scanEvent(rows)
		if scanErr != nil {
			return nil, ErrStorage
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBError(ctx, err, audit.ErrNotFound, audit.ErrConflict, audit.ErrConflict, audit.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return values, nil
}

// AggregateUsage computes bounded usage totals.
func (s *Store) AggregateUsage(ctx context.Context, query audit.UsageQuery) ([]audit.UsageTotal, error) {
	for _, group := range query.GroupBy {
		if !validGroup(group) {
			return nil, audit.ErrInvalid
		}
	}
	events, err := s.List(ctx, query.Query)
	if err != nil {
		return nil, err
	}
	groups := map[string]*audit.UsageTotal{}
	for _, event := range events {
		if event.Cost == nil {
			continue
		}
		key := aggregateKey(event, query.GroupBy)
		total := groups[key]
		if total == nil {
			total = &audit.UsageTotal{TenantID: s.tenantID, AppID: event.AgentAppID, Channel: event.Channel, Provider: event.Cost.Provider, Model: event.Cost.Model, Currency: event.Cost.Currency}
			groups[key] = total
		}
		total.EventCount++
		addUsage(total, event.Cost)
	}
	result := make([]audit.UsageTotal, 0, len(groups))
	for _, total := range groups {
		result = append(result, *total)
	}
	sort.Slice(result, func(i, j int) bool {
		return aggregateTotalKey(result[i], query.GroupBy) < aggregateTotalKey(result[j], query.GroupBy)
	})
	return result, nil
}

const auditAppendSQL = `SELECT * FROM public.audit_append_event($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34)`
const auditSelectSQL = `SELECT tenant_id,event_id,schema_version,event_type,channel,user_id,session_id,agent_app_id,revision,model_profile_id,tool_name,decision,latency_ms,error_type,input_tokens,output_tokens,model_cost_minor,tool_cost_minor,currency,budget_used_tokens,budget_used_minor,execution_result,provider,model,request_id,trace_id,correlation_id,actor_type,actor_id,reason,previous_version,next_version,occurred_at,digest FROM public.audit_event`

func appendEventArgs(event audit.Event, digest string) []any {
	var cost *audit.Usage
	if event.Cost != nil {
		cost = event.Cost
	}
	return []any{event.TenantID, event.EventID, event.SchemaVersion, string(event.EventType), nullable(event.Channel), nullable(event.UserID), nullable(event.SessionID), nullable(event.AgentAppID), event.Revision, nullable(event.ModelProfileID), nullable(event.ToolName), nullable(string(event.Decision)), event.LatencyMS, nullable(event.ErrorType), usageInt(cost, func(v *audit.Usage) *int64 { return v.InputTokens }), usageInt(cost, func(v *audit.Usage) *int64 { return v.OutputTokens }), usageInt(cost, func(v *audit.Usage) *int64 { return v.ModelCostMinor }), usageInt(cost, func(v *audit.Usage) *int64 { return v.ToolCostMinor }), usageString(cost, func(v *audit.Usage) string { return v.Currency }), usageInt(cost, func(v *audit.Usage) *int64 { return v.BudgetUsedTokens }), usageInt(cost, func(v *audit.Usage) *int64 { return v.BudgetUsedMinor }), usageString(cost, func(v *audit.Usage) string { return string(v.ExecutionResult) }), usageString(cost, func(v *audit.Usage) string { return v.Provider }), usageString(cost, func(v *audit.Usage) string { return v.Model }), nullable(event.RequestID), nullable(event.TraceID), nullable(event.CorrelationID), nullable(event.ActorType), nullable(event.ActorID), nullable(event.Reason), event.PreviousVersion, event.NextVersion, event.OccurredAt.UTC(), digest}
}

func scanEvent(row rowScanner) (audit.Event, error) {
	var value audit.Event
	var typ, channel, userID, sessionID, appID, modelProfile, tool, decision, errorType sql.NullString
	var input, output, modelCost, toolCost, budgetTokens, budgetMinor sql.NullInt64
	var currency, result, provider, model, requestID, traceID, correlationID, actorType, actorID, reason sql.NullString
	var revision, latency, previous, next sql.NullInt64
	var occurred time.Time
	var digest string
	if err := row.Scan(&value.TenantID, &value.EventID, &value.SchemaVersion, &typ, &channel, &userID, &sessionID, &appID, &revision, &modelProfile, &tool, &decision, &latency, &errorType, &input, &output, &modelCost, &toolCost, &currency, &budgetTokens, &budgetMinor, &result, &provider, &model, &requestID, &traceID, &correlationID, &actorType, &actorID, &reason, &previous, &next, &occurred, &digest); err != nil {
		return audit.Event{}, err
	}
	value.EventType = audit.EventType(typ.String)
	value.Channel = channel.String
	value.UserID = userID.String
	value.SessionID = sessionID.String
	value.AgentAppID = appID.String
	value.ModelProfileID = modelProfile.String
	value.ToolName = tool.String
	value.Decision = audit.Decision(decision.String)
	value.ErrorType = errorType.String
	value.RequestID = requestID.String
	value.TraceID = traceID.String
	value.CorrelationID = correlationID.String
	value.ActorType = actorType.String
	value.ActorID = actorID.String
	value.Reason = reason.String
	value.Revision = nullableInt(revision)
	value.LatencyMS = nullableInt(latency)
	value.PreviousVersion = nullableInt(previous)
	value.NextVersion = nullableInt(next)
	value.OccurredAt = occurred.UTC()
	if input.Valid || output.Valid || modelCost.Valid || toolCost.Valid || currency.Valid || budgetTokens.Valid || budgetMinor.Valid || result.Valid || provider.Valid || model.Valid {
		value.Cost = &audit.Usage{InputTokens: nullableInt(input), OutputTokens: nullableInt(output), ModelCostMinor: nullableInt(modelCost), ToolCostMinor: nullableInt(toolCost), Currency: strings.TrimSpace(currency.String), BudgetUsedTokens: nullableInt(budgetTokens), BudgetUsedMinor: nullableInt(budgetMinor), ExecutionResult: audit.ExecutionResult(result.String), Provider: provider.String, Model: model.String}
	}
	if err := value.Validate(); err != nil {
		return audit.Event{}, ErrStorage
	}
	_ = digest
	return value.Clone(), nil
}

func addUsage(total *audit.UsageTotal, usage *audit.Usage) {
	if usage.InputTokens != nil {
		total.InputTokens += *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		total.OutputTokens += *usage.OutputTokens
	}
	if usage.ModelCostMinor != nil {
		total.ModelCostMinor += *usage.ModelCostMinor
	}
	if usage.ToolCostMinor != nil {
		total.ToolCostMinor += *usage.ToolCostMinor
	}
	if usage.BudgetUsedTokens != nil {
		total.BudgetUsedTokens += *usage.BudgetUsedTokens
	}
	if usage.BudgetUsedMinor != nil {
		total.BudgetUsedMinor += *usage.BudgetUsedMinor
	}
}
func aggregateKey(event audit.Event, groups []audit.GroupBy) string {
	parts := []string{event.Cost.Currency}
	for _, group := range groups {
		switch group {
		case audit.GroupTenant:
			parts = append(parts, event.TenantID)
		case audit.GroupApp:
			parts = append(parts, event.AgentAppID)
		case audit.GroupChannel:
			parts = append(parts, event.Channel)
		case audit.GroupProvider:
			parts = append(parts, event.Cost.Provider)
		case audit.GroupModel:
			parts = append(parts, event.Cost.Model)
		}
	}
	return strings.Join(parts, "\x00")
}
func aggregateTotalKey(total audit.UsageTotal, groups []audit.GroupBy) string {
	parts := []string{total.Currency}
	for _, group := range groups {
		switch group {
		case audit.GroupTenant:
			parts = append(parts, total.TenantID)
		case audit.GroupApp:
			parts = append(parts, total.AppID)
		case audit.GroupChannel:
			parts = append(parts, total.Channel)
		case audit.GroupProvider:
			parts = append(parts, total.Provider)
		case audit.GroupModel:
			parts = append(parts, total.Model)
		}
	}
	return strings.Join(parts, "\x00")
}
func validGroup(group audit.GroupBy) bool {
	return group == audit.GroupTenant || group == audit.GroupApp || group == audit.GroupChannel || group == audit.GroupProvider || group == audit.GroupModel
}
func validEventType(value audit.EventType) bool {
	switch value {
	case audit.EventControlPlaneChanged, audit.EventExecutionStarted, audit.EventExecutionCompleted, audit.EventExecutionFailed, audit.EventExecutionCanceled, audit.EventExecutionTimedOut, audit.EventExecutionFallback, audit.EventCanarySelected, audit.EventToolAllowed, audit.EventToolDenied, audit.EventToolApprovalRequired, audit.EventToolExecuted, audit.EventIMAuthorizationAllowed, audit.EventIMAuthorizationDenied, audit.EventIMIngressAccepted, audit.EventIMIngressDuplicate, audit.EventIMDeliverySent, audit.EventIMDeliveryRetryScheduled, audit.EventIMDeliveryDeadLettered, audit.EventIMDeliveryReconciled, audit.EventBudgetRejected, audit.EventContentRedacted, audit.EventAuditIncomplete:
		return true
	}
	return false
}
func nullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
func usageInt(cost *audit.Usage, pick func(*audit.Usage) *int64) any {
	if cost == nil {
		return nil
	}
	return pick(cost)
}
func usageString(cost *audit.Usage, pick func(*audit.Usage) string) any {
	if cost == nil {
		return nil
	}
	return nullable(pick(cost))
}
func nullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}
func hasControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
