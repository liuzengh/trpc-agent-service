package policy

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// SQLBudgetStore atomically reserves and reconciles tenant monthly cost across
// every Worker node. Request IDs make retries idempotent.
type SQLBudgetStore struct {
	DB  *sql.DB
	Now func() time.Time
}

var _ BudgetStore = (*SQLBudgetStore)(nil)
var _ BudgetReconciler = (*SQLBudgetStore)(nil)

func (store *SQLBudgetStore) Reserve(ctx context.Context, request Request) error {
	if store == nil || store.DB == nil || ctx == nil || request.TenantID == "" || request.RequestID == "" {
		return errors.New("policy: SQL budget store and request scope are required")
	}
	if request.Policy.RequestTokenBudget > 0 && request.EstimatedTokens > request.Policy.RequestTokenBudget {
		return ErrBudgetExceeded
	}
	if request.Policy.MonthlyCostBudgetCents > 0 && request.EstimatedCostMicros <= 0 {
		return errors.New("policy: positive estimated cost is required for monthly budget")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM policy_budget_reservations WHERE tenant_id=$1 AND request_id=$2`, request.TenantID, request.RequestID).Scan(&existing)
	if err == nil {
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	period := store.now().UTC().Format("2006-01")
	reserved := int64(0)
	if request.Policy.MonthlyCostBudgetCents > 0 {
		reserved = request.EstimatedCostMicros
		if _, err := tx.ExecContext(ctx, `INSERT INTO policy_budget_usage (tenant_id,period,used_micros) VALUES ($1,$2,0) ON CONFLICT DO NOTHING`, request.TenantID, period); err != nil {
			return err
		}
		var used int64
		if err := tx.QueryRowContext(ctx, `SELECT used_micros FROM policy_budget_usage WHERE tenant_id=$1 AND period=$2 FOR UPDATE`, request.TenantID, period).Scan(&used); err != nil {
			return err
		}
		limit := request.Policy.MonthlyCostBudgetCents * 10000
		if used+reserved > limit {
			return ErrBudgetExceeded
		}
		if _, err := tx.ExecContext(ctx, `UPDATE policy_budget_usage SET used_micros=$3,updated_at=$4 WHERE tenant_id=$1 AND period=$2`, request.TenantID, period, used+reserved, store.now().UTC()); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_budget_reservations (tenant_id,request_id,period,reserved_micros) VALUES ($1,$2,$3,$4)`, request.TenantID, request.RequestID, period, reserved); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *SQLBudgetStore) Reconcile(ctx context.Context, request Request, actualTokens, actualCostMicros int64) error {
	if store == nil || store.DB == nil || ctx == nil || request.TenantID == "" || request.RequestID == "" {
		return errors.New("policy: SQL budget store and request scope are required")
	}
	if request.Policy.RequestTokenBudget > 0 && actualTokens > request.Policy.RequestTokenBudget {
		return ErrBudgetExceeded
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var period string
	var reserved int64
	var previous sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT period,reserved_micros,actual_micros FROM policy_budget_reservations WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, request.TenantID, request.RequestID).Scan(&period, &reserved, &previous); err != nil {
		return errors.New("policy: budget was not reserved")
	}
	if request.Policy.MonthlyCostBudgetCents > 0 {
		var used int64
		if err := tx.QueryRowContext(ctx, `SELECT used_micros FROM policy_budget_usage WHERE tenant_id=$1 AND period=$2 FOR UPDATE`, request.TenantID, period).Scan(&used); err != nil {
			return err
		}
		allocated := reserved
		if previous.Valid {
			allocated = previous.Int64
		}
		updated := used - allocated + actualCostMicros
		if updated > request.Policy.MonthlyCostBudgetCents*10000 {
			return ErrBudgetExceeded
		}
		if _, err := tx.ExecContext(ctx, `UPDATE policy_budget_usage SET used_micros=$3,updated_at=$4 WHERE tenant_id=$1 AND period=$2`, request.TenantID, period, updated, store.now().UTC()); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE policy_budget_reservations SET actual_micros=$3,updated_at=$4 WHERE tenant_id=$1 AND request_id=$2`, request.TenantID, request.RequestID, actualCostMicros, store.now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *SQLBudgetStore) now() time.Time {
	if store != nil && store.Now != nil {
		return store.Now()
	}
	return time.Now()
}

// SQLApprovals makes grants visible to a waiting Runner on any node.
type SQLApprovals struct {
	DB           *sql.DB
	PollInterval time.Duration
}

var _ ApprovalStore = (*SQLApprovals)(nil)
var _ ApprovalWaiter = (*SQLApprovals)(nil)

func (store *SQLApprovals) Grant(tenantID, requestID, toolName string) bool {
	if store == nil || store.DB == nil || tenantID == "" || requestID == "" || toolName == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := store.DB.ExecContext(ctx, `INSERT INTO tool_approvals (tenant_id,request_id,tool_name) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, tenantID, requestID, toolName)
	return err == nil
}

func (store *SQLApprovals) Approved(ctx context.Context, tenantID, requestID, toolName string) bool {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" || requestID == "" || toolName == "" {
		return false
	}
	var approved int
	err := store.DB.QueryRowContext(ctx, `SELECT 1 FROM tool_approvals WHERE tenant_id=$1 AND request_id=$2 AND tool_name=$3`, tenantID, requestID, toolName).Scan(&approved)
	return err == nil
}

func (store *SQLApprovals) Wait(ctx context.Context, tenantID, requestID, toolName string) bool {
	if store.Approved(ctx, tenantID, requestID, toolName) {
		return true
	}
	interval := store.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if store.Approved(ctx, tenantID, requestID, toolName) {
				return true
			}
		}
	}
}
