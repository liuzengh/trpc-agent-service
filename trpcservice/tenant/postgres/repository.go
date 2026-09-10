// Package postgres provides the PostgreSQL implementation of the Tenant
// repository.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// List filters and orders tenant roots in SQL before applying offset pagination.
// Cursors are numeric offsets, not snapshots: concurrent changes may shift a page.
func (r *TenantRepository) List(ctx context.Context, scopes []string, query, status, cursor string, limit int) ([]*tenant.Tenant, string, error) {
	if ctx == nil {
		return nil, "", ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	scopeClause, arguments, allowed := tenantScopeClause(scopes)
	if !allowed {
		return []*tenant.Tenant{}, "", nil
	}
	if r == nil || r.db == nil {
		return nil, "", ErrStorage
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := 0
	if cursor != "" {
		var err error
		offset, err = strconv.Atoi(cursor)
		if err != nil || offset < 0 || strconv.Itoa(offset) != cursor {
			return nil, "", fmt.Errorf("%w: invalid cursor", tenant.ErrInvalid)
		}
	}
	querySQL, arguments := tenantListQuery(scopeClause, arguments, query, status, offset, limit)
	rows, err := r.db.QueryContext(ctx, querySQL, arguments...)
	if err != nil {
		return nil, "", mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	defer rows.Close()
	items := make([]*tenant.Tenant, 0, limit+1)
	for rows.Next() {
		v, scanErr := scanTenant(rows)
		if scanErr != nil {
			return nil, "", ErrStorage
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		return nil, "", mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	next := ""
	if len(items) > limit {
		if offset > int(^uint(0)>>1)-limit {
			return nil, "", fmt.Errorf("%w: cursor overflow", tenant.ErrInvalid)
		}
		next = strconv.Itoa(offset + limit)
		items = items[:limit]
	}
	return items, next, nil
}

func tenantListQuery(scopeClause string, arguments []any, query, status string, offset, limit int) (string, []any) {
	querySQL := tenantSelect + scopeClause
	separator := " WHERE "
	if scopeClause != "" {
		separator = " AND "
	}
	if status != "" {
		arguments = append(arguments, status)
		querySQL += separator + fmt.Sprintf("status = $%d", len(arguments))
		separator = " AND "
	}
	if query = strings.ToLower(strings.TrimSpace(query)); query != "" {
		arguments = append(arguments, query)
		// STRPOS preserves literal substring matching, including percent and underscore.
		querySQL += separator + fmt.Sprintf("STRPOS(LOWER(tenant_id || ' ' || tenant_key || ' ' || display_name), $%d) > 0", len(arguments))
	}
	arguments = append(arguments, limit+1, offset)
	querySQL += fmt.Sprintf(" ORDER BY tenant_id LIMIT $%d OFFSET $%d", len(arguments)-1, len(arguments))
	return querySQL, arguments
}

// tenantScopeClause returns a deterministic SQL predicate and arguments for
// the tenant IDs visible to one Admin principal. The wildcard is explicit;
// an empty scope remains an empty result rather than an unrestricted query.
func tenantScopeClause(scopes []string) (string, []any, bool) {
	visible := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" {
			return "", nil, true
		}
		if scope != "" {
			visible[scope] = struct{}{}
		}
	}
	if len(visible) == 0 {
		return "", nil, false
	}
	scopeIDs := make([]string, 0, len(visible))
	for scope := range visible {
		scopeIDs = append(scopeIDs, scope)
	}
	sort.Strings(scopeIDs)
	placeholders := make([]string, len(scopeIDs))
	arguments := make([]any, len(scopeIDs))
	for index, scope := range scopeIDs {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
		arguments[index] = scope
	}
	return ` WHERE tenant_id IN (` + strings.Join(placeholders, ", ") + `) `, arguments, true
}

// TenantRepository persists Tenant roots in PostgreSQL.
type TenantRepository struct {
	db *sql.DB
}

var _ tenant.Repository = (*TenantRepository)(nil)

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
		SELECT public.control_plane_create_tenant(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)`,
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
	stored, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1`, value.TenantID))
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
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, false, err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('trpc-agent-service:first-tenant', 0))"); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&count); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if count > 0 {
		return nil, false, nil
	}
	if _, err := tx.ExecContext(ctx, "SELECT public.control_plane_create_tenant($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
		value.TenantID, value.TenantKey, value.DisplayName, string(value.Status),
		value.RateLimitRPM, value.MaxConcurrentExecutions, value.MonthlyTokenBudget,
		value.MonthlySpendLimitMinor, nullableText(value.BillingCurrency),
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate,
		value.DefaultAgentAppID, value.DefaultBackendProfileID, value.Version,
		value.CreatedAt, value.UpdatedAt,
	); err != nil {
		return nil, false, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	stored, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+" WHERE tenant_id = $1", value.TenantID))
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
	value, err := scanTenant(r.db.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1`, tenantID))
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
	if r == nil || r.db == nil {
		return 0, storagepostgres.ErrStorage
	}
	var count int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&count); err != nil {
		return 0, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	return count, nil
}

// UpdateConfiguration applies an expected-version tenant configuration update.
func (r *TenantRepository) UpdateConfiguration(ctx context.Context, input tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if input.AuditRetentionDays == 0 {
		input.AuditRetentionDays = 90
	}
	if input.LogMaskingLevel == "" {
		input.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := tenant.ValidateConfiguration(input.DisplayName, input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget, input.MonthlySpendLimitMinor, input.BillingCurrency, input.AuditRetentionDays, input.LogMaskingLevel, input.TraceSamplingRate); err != nil {
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
	current, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1 FOR UPDATE`, input.TenantID))
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if current.Status == tenant.StatusDisabled {
		return nil, tenant.ErrDisabled
	}
	if current.Version != input.ExpectedVersion {
		return nil, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, current.Version)
	}
	var nextVersion int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.update_tenant_configuration(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)`,
		input.TenantID, input.ExpectedVersion, strings.TrimSpace(input.DisplayName),
		input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget,
		input.MonthlySpendLimitMinor, nullableText(input.BillingCurrency), input.AuditRetentionDays,
		string(input.LogMaskingLevel), input.TraceSamplingRate, input.DefaultAgentAppID,
		input.DefaultBackendProfileID,
	).Scan(&nextVersion)
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	updated, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1`, input.TenantID))
	if err != nil {
		return nil, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return updated, nil
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
	current, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1 FOR UPDATE`, input.TenantID))
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
	err = tx.QueryRowContext(ctx, `
		SELECT public.transition_tenant_status($1, $2, $3, $4, $5, $6, $7)
	`, input.TenantID, input.ExpectedVersion, string(input.NextStatus),
		strings.TrimSpace(input.Metadata.ActorType), strings.TrimSpace(input.Metadata.ActorID),
		strings.TrimSpace(input.Metadata.Reason), strings.TrimSpace(input.Metadata.CorrelationID)).Scan(&eventID)
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	updated, err := scanTenant(tx.QueryRowContext(ctx, tenantSelect+` WHERE tenant_id = $1`, input.TenantID))
	if err != nil {
		return nil, tenant.StatusChangeEvent{}, mapDBError(ctx, err, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
	}
	event, err := scanTenantStatusEvent(tx.QueryRowContext(ctx, `
		SELECT tenant_id, previous_status, next_status, actor_type, actor_id, reason,
		       previous_version, next_version, occurred_at
		FROM public.tenant_status_change_outbox WHERE event_id = $1
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
FROM public.tenant`

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
