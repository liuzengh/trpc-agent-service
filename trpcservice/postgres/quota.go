package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type quotaPeriod struct {
	kind       string
	start      time.Time
	tokenLimit int64
	costLimit  float64
}

type quotaReservation struct {
	appID, scopeKind, periodKind string
	periodStart                  time.Time
	tokens                       int64
	cost                         float64
}

func isTerminalExecutionStatus(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "UNCERTAIN", "CANCELED":
		return true
	default:
		return false
	}
}

// reserveExecutionQuota serializes admission against the shared usage rows.
// The reservation is bounded by the pinned application's per-execution caps,
// so concurrent workers cannot admit unbounded work past a period quota.
func reserveExecutionQuota(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, requestID string,
	tenantQuota tenant.QuotaPolicy,
	appBudget tenant.BudgetPolicy,
) error {
	if !tenantQuota.HasLimit() && !appBudget.QuotaPolicy().HasLimit() {
		return nil
	}
	if appBudget.MaxTokensPerExecution <= 0 &&
		(tenantQuota.DailyTokenQuota > 0 || tenantQuota.MonthlyTokenQuota > 0) {
		return errors.New("tenant token quota requires max_tokens_per_execution")
	}
	if appBudget.MaxCostPerExecution <= 0 &&
		(tenantQuota.DailyCostQuota > 0 || tenantQuota.MonthlyCostQuota > 0) {
		return errors.New("tenant cost quota requires max_cost_per_execution")
	}
	var day, month time.Time
	if err := tx.QueryRow(ctx, `
SELECT timezone('UTC', clock_timestamp())::date,
       date_trunc('month', timezone('UTC', clock_timestamp()))::date`).Scan(&day, &month); err != nil {
		return fmt.Errorf("read quota periods: %w", err)
	}
	periods := []quotaPeriod{
		{kind: "DAY", start: day, tokenLimit: tenantQuota.DailyTokenQuota, costLimit: tenantQuota.DailyCostQuota},
		{kind: "MONTH", start: month, tokenLimit: tenantQuota.MonthlyTokenQuota, costLimit: tenantQuota.MonthlyCostQuota},
	}
	appQuota := appBudget.QuotaPolicy()
	appPeriods := []quotaPeriod{
		{kind: "DAY", start: day, tokenLimit: appQuota.DailyTokenQuota, costLimit: appQuota.DailyCostQuota},
		{kind: "MONTH", start: month, tokenLimit: appQuota.MonthlyTokenQuota, costLimit: appQuota.MonthlyCostQuota},
	}
	for _, period := range periods {
		if err := reserveQuotaPeriod(ctx, tx, tenantID, "", requestID, "TENANT", period,
			int64(appBudget.MaxTokensPerExecution), appBudget.MaxCostPerExecution); err != nil {
			return err
		}
	}
	for _, period := range appPeriods {
		if err := reserveQuotaPeriod(ctx, tx, tenantID, appID, requestID, "APP", period,
			int64(appBudget.MaxTokensPerExecution), appBudget.MaxCostPerExecution); err != nil {
			return err
		}
	}
	return nil
}

func reserveQuotaPeriod(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, requestID, scopeKind string,
	period quotaPeriod,
	reservedTokens int64,
	reservedCost float64,
) error {
	if period.tokenLimit <= 0 && period.costLimit <= 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.quota_usage (tenant_id, app_id, scope_kind, period_kind, period_start)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING`, tenantID, appID, scopeKind, period.kind, period.start); err != nil {
		return fmt.Errorf("initialize quota usage: %w", err)
	}
	var usedTokens, reservedTokensSoFar int64
	var usedCost, reservedCostSoFar float64
	if err := tx.QueryRow(ctx, `
SELECT token_used, cost_used, token_reserved, cost_reserved
FROM platform.quota_usage
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind=$3 AND period_kind=$4 AND period_start=$5
FOR UPDATE`, tenantID, appID, scopeKind, period.kind, period.start).
		Scan(&usedTokens, &usedCost, &reservedTokensSoFar, &reservedCostSoFar); err != nil {
		return fmt.Errorf("lock quota usage: %w", err)
	}
	if period.tokenLimit > 0 && usedTokens+reservedTokensSoFar+reservedTokens > period.tokenLimit {
		return tenant.ErrQuotaExceeded
	}
	if period.costLimit > 0 && usedCost+reservedCostSoFar+reservedCost > period.costLimit {
		return tenant.ErrQuotaExceeded
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.quota_reservation (
    tenant_id, app_id, request_id, scope_kind, period_kind, period_start,
    reserved_tokens, reserved_cost
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		tenantID, appID, requestID, scopeKind, period.kind, period.start,
		reservedTokens, reservedCost); err != nil {
		return fmt.Errorf("create quota reservation: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE platform.quota_usage
SET token_reserved = token_reserved + $6,
    cost_reserved = cost_reserved + $7,
    updated_at = clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind=$3 AND period_kind=$4 AND period_start=$5`,
		tenantID, appID, scopeKind, period.kind, period.start, reservedTokens, reservedCost); err != nil {
		return fmt.Errorf("update quota reservation: %w", err)
	}
	return nil
}

// RecordExecutionUsage accounts one worker attempt exactly once. When release
// is true the execution's remaining reservation is returned to the period.
func (s *Store) RecordExecutionUsage(
	ctx context.Context,
	claim queue.Claim,
	result worker.RunResult,
	release bool,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	tenantID := claim.Job.Tenant().TenantID
	appID := claim.Job.Tenant().AppID
	requestID := claim.Job.RequestID()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin quota usage: %w", err)
	}
	defer func() { rollback(tx) }()
	usage := worker.ExecutionUsage{
		InputTokens:  result.InputTokens,
		OutputTokens: result.OutputTokens,
		TotalTokens:  result.TotalTokens,
		Cost:         result.Cost,
	}
	if err := recordExecutionUsageTx(ctx, tx, tenantID, appID, requestID,
		claim.Job.Tenant().ConfigVersion, claim.Attempt, usage, release); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit quota usage: %w", err)
	}
	return nil
}

func recordExecutionUsageTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, requestID, configVersion string,
	attempt int,
	usage worker.ExecutionUsage,
	release bool,
) error {
	if tenantID == "" || appID == "" || requestID == "" || configVersion == "" || attempt <= 0 {
		return errors.New("quota usage execution identity is incomplete")
	}
	reservations, err := lockQuotaReservationsTx(ctx, tx, tenantID, appID, requestID)
	if err != nil {
		return err
	}
	if len(reservations) == 0 {
		return nil
	}
	tenantQuota, appBudget, err := loadQuotaPoliciesTx(ctx, tx, tenantID, appID, configVersion)
	if err != nil {
		return err
	}
	totalTokens := usage.TotalTokens
	if totalTokens <= 0 {
		totalTokens = usage.InputTokens + usage.OutputTokens
	}
	if totalTokens < 0 || usage.InputTokens < 0 || usage.OutputTokens < 0 {
		return errors.New("quota usage is negative")
	}
	cost, err := usageCost(usage.Cost, appBudget.MaxCostPerExecution)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO platform.quota_usage_record (
    tenant_id, app_id, request_id, attempt, input_tokens, output_tokens,
    total_tokens, cost
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (tenant_id, app_id, request_id, attempt) DO NOTHING`,
		tenantID, appID, requestID, attempt, usage.InputTokens, usage.OutputTokens,
		totalTokens, cost)
	if err != nil {
		return fmt.Errorf("record quota usage: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if release {
			if err := releaseExecutionQuotaTx(ctx, tx, tenantID, appID, requestID); err != nil {
				return err
			}
		}
		return nil
	}
	for _, value := range reservations {
		limits, err := quotaLimits(value.scopeKind, value.periodKind, tenantQuota, appBudget.QuotaPolicy())
		if err != nil {
			return err
		}
		if !usageWithinQuota(ctx, tx, tenantID, value, totalTokens, cost, limits) {
			return tenant.ErrQuotaExceeded
		}
		if _, err := tx.Exec(ctx, `
UPDATE platform.quota_usage
SET token_used = token_used + $6,
    cost_used = cost_used + $7,
    updated_at = clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind=$3 AND period_kind=$4 AND period_start=$5`,
			tenantID, value.appID, value.scopeKind, value.periodKind, value.periodStart,
			totalTokens, cost); err != nil {
			return fmt.Errorf("apply quota usage: %w", err)
		}
	}
	if release {
		if err := releaseExecutionQuotaTx(ctx, tx, tenantID, appID, requestID); err != nil {
			return err
		}
	}
	return nil
}

func lockQuotaReservationsTx(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string) ([]quotaReservation, error) {
	rows, err := tx.Query(ctx, `
SELECT app_id, scope_kind, period_kind, period_start, reserved_tokens, reserved_cost
FROM platform.quota_reservation
WHERE tenant_id=$1 AND app_id IN ('', $2) AND request_id=$3
FOR UPDATE`, tenantID, appID, requestID)
	if err != nil {
		return nil, fmt.Errorf("lock quota reservations: %w", err)
	}
	defer rows.Close()
	reservations := make([]quotaReservation, 0, 4)
	for rows.Next() {
		var value quotaReservation
		if err := rows.Scan(&value.appID, &value.scopeKind, &value.periodKind, &value.periodStart, &value.tokens, &value.cost); err != nil {
			return nil, fmt.Errorf("scan quota reservation: %w", err)
		}
		reservations = append(reservations, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quota reservations: %w", err)
	}
	return reservations, nil
}

func loadQuotaPoliciesTx(ctx context.Context, tx pgx.Tx, tenantID, appID, configVersion string) (tenant.QuotaPolicy, tenant.BudgetPolicy, error) {
	var tenantJSON, auditJSON []byte
	if err := tx.QueryRow(ctx, `
SELECT t.quota_policy, c.audit_policy
FROM platform.tenant t
JOIN platform.app_config_version c
  ON c.tenant_id = t.tenant_id AND c.app_id = $2 AND c.version = $3
WHERE t.tenant_id = $1 AND c.status = 'PUBLISHED'`, tenantID, appID, configVersion).Scan(&tenantJSON, &auditJSON); err != nil {
		return tenant.QuotaPolicy{}, tenant.BudgetPolicy{}, fmt.Errorf("load quota policies: %w", err)
	}
	var tenantQuota tenant.QuotaPolicy
	if err := json.Unmarshal(tenantJSON, &tenantQuota); err != nil {
		return tenant.QuotaPolicy{}, tenant.BudgetPolicy{}, fmt.Errorf("decode tenant quota policy: %w", err)
	}
	var document auditPolicyDocument
	if err := json.Unmarshal(auditJSON, &document); err != nil {
		return tenant.QuotaPolicy{}, tenant.BudgetPolicy{}, fmt.Errorf("decode application budget policy: %w", err)
	}
	var appBudget tenant.BudgetPolicy
	if document.Budget != nil {
		appBudget = *document.Budget
	}
	return tenantQuota, appBudget, nil
}

func usageCost(cost *float64, fallback float64) (float64, error) {
	if cost == nil {
		if fallback < 0 || math.IsNaN(fallback) || math.IsInf(fallback, 0) {
			return 0, errors.New("quota usage cost must be finite and non-negative")
		}
		return fallback, nil
	}
	if *cost < 0 || math.IsNaN(*cost) || math.IsInf(*cost, 0) {
		return 0, errors.New("quota usage cost must be finite and non-negative")
	}
	return *cost, nil
}

func quotaLimits(scopeKind, periodKind string, tenantQuota, appQuota tenant.QuotaPolicy) (quotaPeriod, error) {
	quota := tenantQuota
	if scopeKind == "APP" {
		quota = appQuota
	} else if scopeKind != "TENANT" {
		return quotaPeriod{}, errors.New("quota reservation scope is invalid")
	}
	period := quotaPeriod{kind: periodKind}
	switch periodKind {
	case "DAY":
		period.tokenLimit = quota.DailyTokenQuota
		period.costLimit = quota.DailyCostQuota
	case "MONTH":
		period.tokenLimit = quota.MonthlyTokenQuota
		period.costLimit = quota.MonthlyCostQuota
	default:
		return quotaPeriod{}, errors.New("quota reservation period is invalid")
	}
	return period, nil
}

func usageWithinQuota(ctx context.Context, tx pgx.Tx, tenantID string, reservation quotaReservation, totalTokens int, cost float64, limits quotaPeriod) bool {
	var usedTokens int64
	var usedCost float64
	if err := tx.QueryRow(ctx, `
SELECT token_used, cost_used
FROM platform.quota_usage
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind=$3 AND period_kind=$4 AND period_start=$5
		FOR UPDATE`, tenantID, reservation.appID, reservation.scopeKind, reservation.periodKind, reservation.periodStart).Scan(&usedTokens, &usedCost); err != nil {
		return false
	}
	return usageWithinQuotaValues(usedTokens, usedCost, totalTokens, cost, limits)
}

func usageWithinQuotaValues(usedTokens int64, usedCost float64, totalTokens int, cost float64, limits quotaPeriod) bool {
	return (limits.tokenLimit <= 0 || usedTokens+int64(totalTokens) <= limits.tokenLimit) &&
		(limits.costLimit <= 0 || usedCost+cost <= limits.costLimit)
}

func releaseExecutionQuotaTx(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string) error {
	reservations, err := lockQuotaReservationsTx(ctx, tx, tenantID, appID, requestID)
	if err != nil {
		return err
	}
	for _, value := range reservations {
		tag, err := tx.Exec(ctx, `
UPDATE platform.quota_usage
SET token_reserved = GREATEST(0, token_reserved - $6),
    cost_reserved = GREATEST(0, cost_reserved - $7),
    updated_at = clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind=$3 AND period_kind=$4 AND period_start=$5`,
			tenantID, value.appID, value.scopeKind, value.periodKind, value.periodStart,
			value.tokens, value.cost)
		if err != nil {
			return fmt.Errorf("release quota usage: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errors.New("quota usage row is missing for reservation")
		}
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM platform.quota_reservation
WHERE tenant_id=$1 AND app_id IN ('', $2) AND request_id=$3`, tenantID, appID, requestID); err != nil {
		return fmt.Errorf("release quota reservation: %w", err)
	}
	return nil
}

// releaseTerminalQuotaReservationsTx is a recovery fence for terminal rows
// whose worker lost its lease before it could report usage. It is idempotent.
func releaseTerminalQuotaReservationsTx(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, `
SELECT r.tenant_id, e.app_id, r.request_id
FROM platform.quota_reservation r
JOIN platform.execution e
  ON e.tenant_id=r.tenant_id AND e.request_id=r.request_id
 AND (r.app_id='' OR r.app_id=e.app_id)
WHERE e.status IN ('SUCCEEDED','FAILED','UNCERTAIN','CANCELED')
FOR UPDATE OF r`)
	if err != nil {
		return fmt.Errorf("find terminal quota reservations: %w", err)
	}
	defer rows.Close()
	type executionKey struct{ tenantID, appID, requestID string }
	keys := make(map[executionKey]struct{})
	for rows.Next() {
		var key executionKey
		if err := rows.Scan(&key.tenantID, &key.appID, &key.requestID); err != nil {
			return fmt.Errorf("scan terminal quota reservation: %w", err)
		}
		keys[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate terminal quota reservations: %w", err)
	}
	rows.Close()
	for key := range keys {
		if err := releaseExecutionQuotaTx(ctx, tx, key.tenantID, key.appID, key.requestID); err != nil {
			return err
		}
	}
	return nil
}
