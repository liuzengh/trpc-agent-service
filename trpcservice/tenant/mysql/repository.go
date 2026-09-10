// Package mysql provides the MySQL implementation of the Tenant
// repository.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	storagemysql "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// TenantRepository persists Tenant roots in MySQL.
type TenantRepository struct {
	db *sql.DB
}

var _ tenant.Repository = (*TenantRepository)(nil)

// List returns a stable page of only the requested tenant scopes.
//
//nolint:gocyclo // Collection listing coordinates scope, filter, and paging boundaries.
func (r *TenantRepository) List(ctx context.Context, scopes []string, query, status, cursor string, limit int) ([]*tenant.Tenant, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	visible := make(map[string]struct{}, len(scopes))
	all := false
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" {
			all = true
		} else if scope != "" {
			visible[scope] = struct{}{}
		}
	}
	if !all && len(visible) == 0 {
		return []*tenant.Tenant{}, "", nil
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	querySQL := `SELECT tenant_id FROM tenant ORDER BY tenant_id`
	var args []any
	if !all {
		ids := make([]string, 0, len(visible))
		for id := range visible {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		args = make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
		}
		// Placeholders are generated from validated scope count; values remain bound arguments.
		querySQL = `SELECT tenant_id FROM tenant WHERE tenant_id IN (` + placeholders + `) ORDER BY tenant_id` //nolint:gosec // placeholder list contains no user-controlled SQL
	}
	rows, err := r.db.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, "", ErrStorage
	}
	var tenantIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, "", ErrStorage
		}
		tenantIDs = append(tenantIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", ErrStorage
	}
	_ = rows.Close()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*tenant.Tenant, 0)
	for _, id := range tenantIDs {
		value, err := r.Get(ctx, id)
		if err != nil {
			return nil, "", ErrStorage
		}
		if status != "" && string(value.Status) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.TenantID+" "+value.TenantKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, value)
	}
	if offset >= len(items) {
		return []*tenant.Tenant{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = fmt.Sprintf("%d", end)
	}
	return items[offset:end], next, nil
}

func decodeListCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}

// NewRepository creates a Tenant repository over an owned or borrowed pool.
func NewRepository(db *sql.DB) *TenantRepository { return &TenantRepository{db: db} }

// Create persists a tenant root after validating its configuration.
func (r *TenantRepository) Create(ctx context.Context, input tenant.CreateInput) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, err := tenant.NewTenant(input)
	if err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO tenant (
			tenant_id, tenant_key, display_name, status, rate_limit_rpm,
			max_concurrent_executions, monthly_token_budget, monthly_spend_limit_minor,
			billing_currency, audit_retention_days, log_masking_level, trace_sampling_rate,
			default_agent_app_id, default_backend_profile_id, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		value.TenantID, value.TenantKey, value.DisplayName, string(value.Status),
		value.RateLimitRPM, value.MaxConcurrentExecutions, value.MonthlyTokenBudget,
		value.MonthlySpendLimitMinor, nullableText(value.BillingCurrency),
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate,
		value.DefaultAgentAppID, value.DefaultBackendProfileID, value.Version,
		value.CreatedAt, value.UpdatedAt,
	)
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	stored, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ?`, value.TenantID))
	if err != nil {
		if err == ErrStorage {
			return nil, err
		}
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return stored, nil
}

// CreateFirst serializes the bootstrap-only global tenant creation gate with
// a transaction-scoped advisory lock, so multiple service processes cannot all
// observe an empty control plane and create competing roots.
func (r *TenantRepository) CreateFirst(ctx context.Context, input tenant.CreateInput) (*tenant.Tenant, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	value, err := tenant.NewTenant(input)
	if err != nil {
		return nil, false, err
	}
	if r == nil || r.db == nil {
		return nil, false, ErrStorage
	}
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	defer func() { _ = conn.Close() }()
	if err := acquireLock(ctx, conn, "trpc-agent-service:first-tenant", 30); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	defer func() { _ = releaseLock(context.Background(), conn, "trpc-agent-service:first-tenant") }()
	tx, err := beginConn(ctx, conn)
	if err != nil {
		return nil, false, err
	}
	defer rollback(tx)
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM tenant").Scan(&count); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if count > 0 {
		return nil, false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant (
		tenant_id, tenant_key, display_name, status, rate_limit_rpm,
		max_concurrent_executions, monthly_token_budget, monthly_spend_limit_minor,
		billing_currency, audit_retention_days, log_masking_level, trace_sampling_rate,
		default_agent_app_id, default_backend_profile_id, version, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		value.TenantID, value.TenantKey, value.DisplayName, string(value.Status),
		value.RateLimitRPM, value.MaxConcurrentExecutions, value.MonthlyTokenBudget,
		value.MonthlySpendLimitMinor, nullableText(value.BillingCurrency),
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate,
		value.DefaultAgentAppID, value.DefaultBackendProfileID, value.Version,
		value.CreatedAt, value.UpdatedAt,
	); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	stored, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+" WHERE tenant_id = ?", value.TenantID))
	if err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, false, err
	}
	return stored, true, nil
}

// Get loads a tenant by its stable identifier.
func (r *TenantRepository) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := scanTenant(r.db.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ?`, tenantID))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", tenant.ErrNotFound, tenantID)
		}
		if err == ErrStorage {
			return nil, err
		}
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	return value, nil
}

// Count returns the durable tenant count used by the first-tenant admin
// authorization boundary.
func (r *TenantRepository) Count(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r == nil || r.db == nil {
		return 0, ErrStorage
	}
	var count int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tenant").Scan(&count); err != nil {
		return 0, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	return count, nil
}

// UpdateConfiguration applies an expected-version tenant configuration update.
func (r *TenantRepository) UpdateConfiguration(ctx context.Context, input tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, err := normalizeTenantConfigurationInput(input)
	if err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	current, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ? FOR UPDATE`, normalized.TenantID))
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if current.Status == tenant.StatusDisabled {
		return nil, tenant.ErrDisabled
	}
	if current.Version != normalized.ExpectedVersion {
		return nil, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, normalized.ExpectedVersion, current.Version)
	}
	if err := validateTenantDefaults(ctx, tx, normalized); err != nil {
		return nil, err
	}
	now := storagemysql.MonotonicNow(current.UpdatedAt)
	result, err := tx.ExecContext(ctx, `UPDATE tenant SET
		display_name = ?, rate_limit_rpm = ?, max_concurrent_executions = ?,
		monthly_token_budget = ?, monthly_spend_limit_minor = ?, billing_currency = ?,
		audit_retention_days = ?, log_masking_level = ?, trace_sampling_rate = ?,
		default_agent_app_id = ?, default_backend_profile_id = ?, version = version + 1,
		updated_at = ? WHERE tenant_id = ? AND version = ? AND status <> 'disabled'`,
		strings.TrimSpace(normalized.DisplayName), normalized.RateLimitRPM, normalized.MaxConcurrentExecutions,
		normalized.MonthlyTokenBudget, normalized.MonthlySpendLimitMinor, nullableText(normalized.BillingCurrency),
		normalized.AuditRetentionDays, string(normalized.LogMaskingLevel), normalized.TraceSamplingRate,
		normalized.DefaultAgentAppID, normalized.DefaultBackendProfileID, now,
		normalized.TenantID, normalized.ExpectedVersion)
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, normalized.ExpectedVersion, current.Version)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO tenant_configuration_outbox (tenant_id, previous_version, next_version, occurred_at) VALUES (?, ?, ?, ?)", input.TenantID, current.Version, current.Version+1, now); err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	updated, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ?`, input.TenantID))
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return updated, nil
}

func normalizeTenantConfigurationInput(input tenant.UpdateConfigurationInput) (tenant.UpdateConfigurationInput, error) {
	if input.AuditRetentionDays == 0 {
		input.AuditRetentionDays = 90
	}
	if input.LogMaskingLevel == "" {
		input.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := tenant.ValidateConfiguration(input.DisplayName, input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget, input.MonthlySpendLimitMinor, input.BillingCurrency, input.AuditRetentionDays, input.LogMaskingLevel, input.TraceSamplingRate); err != nil {
		return tenant.UpdateConfigurationInput{}, err
	}
	return input, nil
}

func validateTenantDefaults(ctx context.Context, tx *sql.Tx, input tenant.UpdateConfigurationInput) error {
	if input.DefaultAgentAppID != nil && strings.TrimSpace(*input.DefaultAgentAppID) != "" {
		var status string
		if err := tx.QueryRowContext(ctx, "SELECT status FROM agent_app WHERE tenant_id = ? AND app_id = ? FOR UPDATE", input.TenantID, *input.DefaultAgentAppID).Scan(&status); err != nil || status != "active" {
			return fmt.Errorf("%w: default agent app must exist in the tenant and be active", tenant.ErrInvalid)
		}
	}
	if input.DefaultBackendProfileID != nil && strings.TrimSpace(*input.DefaultBackendProfileID) != "" {
		var status string
		if err := tx.QueryRowContext(ctx, "SELECT status FROM backend_profile WHERE tenant_id = ? AND profile_id = ? FOR UPDATE", input.TenantID, *input.DefaultBackendProfileID).Scan(&status); err != nil || status != "active" {
			return fmt.Errorf("%w: default backend profile must exist in the tenant and be active", tenant.ErrInvalid)
		}
	}
	return nil
}

// TransitionStatus changes a tenant status with optimistic concurrency.
func (r *TenantRepository) TransitionStatus(ctx context.Context, input tenant.TransitionStatusInput) (*tenant.Tenant, tenant.StatusChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if err := validateTenantMetadata(input.Metadata); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, tenant.StatusChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ? FOR UPDATE`, input.TenantID))
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if current.Status == tenant.StatusDisabled {
		return nil, tenant.StatusChangeEvent{}, tenant.ErrDisabled
	}
	if current.Version != input.ExpectedVersion {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, current.Version)
	}
	if !validTenantTransition(current.Status, input.NextStatus) {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: %s -> %s", tenant.ErrInvalidTransition, current.Status, input.NextStatus)
	}
	var eventID int64
	now := storagemysql.MonotonicNow(current.UpdatedAt)
	result, err := tx.ExecContext(ctx, `UPDATE tenant SET status = ?, version = version + 1, updated_at = ?
		WHERE tenant_id = ? AND version = ? AND status <> 'disabled'`,
		string(input.NextStatus), now, input.TenantID, input.ExpectedVersion)
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, current.Version)
	}
	eventResult, err := tx.ExecContext(ctx, `INSERT INTO tenant_status_change_outbox (
		tenant_id, previous_status, next_status, actor_type, actor_id, reason,
		previous_version, next_version, correlation_id, occurred_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.TenantID, string(current.Status), string(input.NextStatus),
		strings.TrimSpace(input.Metadata.ActorType), strings.TrimSpace(input.Metadata.ActorID),
		strings.TrimSpace(input.Metadata.Reason), input.ExpectedVersion, input.ExpectedVersion+1,
		strings.TrimSpace(input.Metadata.CorrelationID), now)
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	eventID, err = eventResult.LastInsertId()
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	updated, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = ?`, input.TenantID))
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	event, err := scanTenantStatusEvent(tx.QueryRowContext(ctx, `
		SELECT tenant_id, previous_status, next_status, actor_type, actor_id, reason,
		       previous_version, next_version, occurred_at
		FROM tenant_status_change_outbox WHERE event_id = ?
	`, eventID))
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	return updated, event, nil
}

const tenantSelect = `SELECT tenant_id, tenant_key, display_name, status,
       rate_limit_rpm, max_concurrent_executions, monthly_token_budget,
       monthly_spend_limit_minor, billing_currency, audit_retention_days,
       log_masking_level, trace_sampling_rate, default_agent_app_id,
       default_backend_profile_id, version, created_at, updated_at
FROM tenant`

func scanTenant(row rowScanner) (*tenant.Tenant, error) {
	var value tenant.Tenant
	var status, masking string
	var currency sql.NullString
	var rate, concurrent, tokens, spend sql.NullInt64
	var defaultApp, defaultBackend sql.NullString
	if err := row.Scan(&value.TenantID, &value.TenantKey, &value.DisplayName, &status,
		&rate, &concurrent, &tokens, &spend, &currency, &value.AuditRetentionDays,
		&masking, &value.TraceSamplingRate, &defaultApp, &defaultBackend,
		&value.Version, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.Status = tenant.Status(status)
	value.RateLimitRPM = nullableInt(rate)
	value.MaxConcurrentExecutions = nullableInt(concurrent)
	value.MonthlyTokenBudget = nullableInt(tokens)
	value.MonthlySpendLimitMinor = nullableInt(spend)
	if currency.Valid {
		value.BillingCurrency = strings.TrimSpace(currency.String)
	}
	value.LogMaskingLevel = tenant.LogMaskingLevel(masking)
	value.DefaultAgentAppID = nullableString(defaultApp)
	value.DefaultBackendProfileID = nullableString(defaultBackend)
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

func scanTenantStatusEvent(row rowScanner) (tenant.StatusChangeEvent, error) {
	var event tenant.StatusChangeEvent
	var previousStatus, nextStatus string
	if err := row.Scan(&event.TenantID, &previousStatus, &nextStatus, &event.ActorType,
		&event.ActorID, &event.Reason, &event.PreviousVersion, &event.NextVersion,
		&event.OccurredAt); err != nil {
		return tenant.StatusChangeEvent{}, err
	}
	event.PreviousStatus = tenant.Status(previousStatus)
	event.NextStatus = tenant.Status(nextStatus)
	event.OccurredAt = asUTC(event.OccurredAt)
	return event, nil
}

func validateTenantMetadata(metadata tenant.TransitionMetadata) error {
	metadata.ActorType = strings.TrimSpace(metadata.ActorType)
	metadata.ActorID = strings.TrimSpace(metadata.ActorID)
	metadata.Reason = strings.TrimSpace(metadata.Reason)
	metadata.CorrelationID = strings.TrimSpace(metadata.CorrelationID)
	if metadata.ActorType == "" || metadata.ActorID == "" || metadata.Reason == "" || metadata.CorrelationID == "" {
		return fmt.Errorf("%w: transition metadata requires actor, reason, and correlation ID", tenant.ErrInvalid)
	}
	if len([]rune(metadata.Reason)) > 1000 {
		return fmt.Errorf("%w: transition reason must contain at most 1000 characters", tenant.ErrInvalid)
	}
	return nil
}

func validTenantTransition(from, to tenant.Status) bool {
	return (from == tenant.StatusActive && (to == tenant.StatusSuspended || to == tenant.StatusDisabled)) ||
		(from == tenant.StatusSuspended && (to == tenant.StatusActive || to == tenant.StatusDisabled))
}
