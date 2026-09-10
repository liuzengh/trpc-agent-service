// Package postgres implements Tenant Repository against the control-plane SQL
// functions. A driver is supplied by the process composition root.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct{ db *sql.DB }

func New(db *sql.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, in tenant.CreateInput) (tenant.Tenant, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	value := in.Tenant
	value.Status = tenant.StatusActive
	value.Version = 1
	if value.BillingCurrency == "" {
		value.BillingCurrency = "USD"
	}
	if value.AuditRetentionDays == 0 {
		value.AuditRetentionDays = 180
	}
	if value.AuditPayloadMode == "" {
		value.AuditPayloadMode = tenant.AuditRedacted
	}
	if value.LogMaskingLevel == "" {
		value.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return tenant.Tenant{}, classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	rules, err := marshalRules(value.RedactionRules)
	if err != nil {
		return tenant.Tenant{}, fmt.Errorf("encode redaction rules: %w", err)
	}
	row := tx.QueryRowContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name,status,request_limit_per_minute,max_concurrent_executions,monthly_token_budget,monthly_cost_budget_micros,billing_currency,audit_retention_days,audit_payload_mode,log_masking_level,redaction_rules,trace_sampling_rate,version) VALUES($1,$2,$3,'active',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1) RETURNING created_at,updated_at`, value.TenantID, value.TenantKey, value.DisplayName, value.RequestLimitPerMinute, value.MaxConcurrentExecutions, value.MonthlyTokenBudget, value.MonthlyCostBudgetMicros, value.BillingCurrency, value.AuditRetentionDays, value.AuditPayloadMode, value.LogMaskingLevel, rules, value.TraceSamplingRate)
	if err = row.Scan(&value.CreatedAt, &value.UpdatedAt); err != nil {
		return tenant.Tenant{}, classify(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref) VALUES($1,$2,'audit',$1,1,$3,$4),($1,$5,'tenant-control',$1,1,$6,$4)`, value.TenantID, "tenant-create-audit:"+value.TenantID, "tenant-create:"+value.TenantID+":audit", "tenant://"+value.TenantID, "tenant-create-control:"+value.TenantID, "tenant-create:"+value.TenantID+":control")
	if err != nil {
		return tenant.Tenant{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return tenant.Tenant{}, classify(err)
	}
	return value, nil
}

func (r *Repository) Get(ctx context.Context, id string) (tenant.Tenant, error) {
	row := r.db.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id=$1`, id)
	value, err := scanTenant(row)
	if err != nil {
		return tenant.Tenant{}, classify(err)
	}
	return value, nil
}

func (r *Repository) UpdateConfiguration(ctx context.Context, in tenant.UpdateConfigurationInput) (tenant.ChangeResult, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	if err := in.Tenant.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	rules, err := marshalRules(in.Tenant.RedactionRules)
	if err != nil {
		return tenant.ChangeResult{}, fmt.Errorf("encode redaction rules: %w", err)
	}
	var next int64
	err = r.db.QueryRowContext(ctx, `SELECT update_tenant_configuration($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`, in.Tenant.TenantID, in.ExpectedVersion, in.Tenant.DisplayName, in.Tenant.RequestLimitPerMinute, in.Tenant.MaxConcurrentExecutions, in.Tenant.MonthlyTokenBudget, in.Tenant.MonthlyCostBudgetMicros, in.Tenant.BillingCurrency, in.Tenant.AuditRetentionDays, in.Tenant.AuditPayloadMode, in.Tenant.LogMaskingLevel, rules, in.Tenant.TraceSamplingRate, nullString(in.Tenant.DefaultAgentAppID), nullString(in.Tenant.DefaultBackendProfileID), nullInt64(in.Tenant.ActiveConfigVersion), in.ChangeMetadata.ActorID, in.ChangeMetadata.ReasonCode, in.ChangeMetadata.CorrelationID, in.ChangeMetadata.TraceID, nil).Scan(&next)
	if err != nil {
		return tenant.ChangeResult{}, classify(err)
	}
	value, err := r.Get(ctx, in.Tenant.TenantID)
	if err != nil {
		return tenant.ChangeResult{}, err
	}
	if value.Version != next {
		return tenant.ChangeResult{}, fmt.Errorf("%w: persisted version mismatch", tenant.ErrVersionConflict)
	}
	return tenant.ChangeResult{Tenant: value}, nil
}

func (r *Repository) TransitionStatus(ctx context.Context, in tenant.TransitionStatusInput) (tenant.ChangeResult, error) {
	if err := in.ChangeMetadata.Validate(); err != nil {
		return tenant.ChangeResult{}, err
	}
	var next int64
	err := r.db.QueryRowContext(ctx, `SELECT transition_tenant_status($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, in.TenantID, in.ExpectedVersion, in.NextStatus, in.ChangeMetadata.ActorType, in.ChangeMetadata.ActorID, in.ChangeMetadata.ReasonCode, nullString(in.ChangeMetadata.ReasonRef), in.ChangeMetadata.CorrelationID, in.ChangeMetadata.TraceID, nil).Scan(&next)
	if err != nil {
		return tenant.ChangeResult{}, classify(err)
	}
	value, err := r.Get(ctx, in.TenantID)
	if err != nil {
		return tenant.ChangeResult{}, err
	}
	if value.Version != next {
		return tenant.ChangeResult{}, tenant.ErrVersionConflict
	}
	return tenant.ChangeResult{Tenant: value}, nil
}

const tenantSelect = `SELECT tenant_id,tenant_key,display_name,status,request_limit_per_minute,max_concurrent_executions,monthly_token_budget,monthly_cost_budget_micros,billing_currency,audit_retention_days,audit_payload_mode,log_masking_level,redaction_rules,trace_sampling_rate,default_agent_app_id,default_backend_profile_id,active_config_version,version,created_at,updated_at FROM tenant`

type scanner interface{ Scan(...any) error }

func scanTenant(row scanner) (tenant.Tenant, error) {
	var value tenant.Tenant
	var request, token, cost, active sql.NullInt64
	var concurrent sql.NullInt64
	var defaultApp, defaultBackend sql.NullString
	var rulesJSON []byte
	err := row.Scan(&value.TenantID, &value.TenantKey, &value.DisplayName, &value.Status, &request, &concurrent, &token, &cost, &value.BillingCurrency, &value.AuditRetentionDays, &value.AuditPayloadMode, &value.LogMaskingLevel, &rulesJSON, &value.TraceSamplingRate, &defaultApp, &defaultBackend, &active, &value.Version, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return tenant.Tenant{}, err
	}
	value.RequestLimitPerMinute = int64Ptr(request)
	if concurrent.Valid {
		v := int(concurrent.Int64)
		value.MaxConcurrentExecutions = &v
	}
	value.MonthlyTokenBudget = int64Ptr(token)
	value.MonthlyCostBudgetMicros = int64Ptr(cost)
	if defaultApp.Valid {
		value.DefaultAgentAppID = defaultApp.String
	}
	if defaultBackend.Valid {
		value.DefaultBackendProfileID = defaultBackend.String
	}
	if active.Valid {
		value.ActiveConfigVersion = active.Int64
	}
	if err := json.Unmarshal(rulesJSON, &value.RedactionRules); err != nil {
		return tenant.Tenant{}, fmt.Errorf("decode redaction rules: %w", err)
	}
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, err
	}
	return value, nil
}
func int64Ptr(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Int64
	return &v
}
func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func marshalRules(rules []redaction.Rule) ([]byte, error) {
	if len(rules) == 0 {
		return []byte("[]"), nil
	}
	return json.Marshal(rules)
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return tenant.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return fmt.Errorf("%w: %v", tenant.ErrVersionConflict, err)
		case "23505":
			return fmt.Errorf("%w: %v", tenant.ErrKeyConflict, err)
		case "P0002":
			return fmt.Errorf("%w: %v", tenant.ErrNotFound, err)
		case "55000":
			return fmt.Errorf("%w: %v", tenant.ErrStatusConflict, err)
		case "22023", "23514", "23503":
			return fmt.Errorf("%w: %v", tenant.ErrInvalid, err)
		}
	}
	return err
}

var _ tenant.Repository = (*Repository)(nil)
