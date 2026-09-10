package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// ValidateExecutionLease checks the current durable owner before a Tool or
// Session side effect. Event and terminal writes keep their own transaction
// fence; this method closes the pre-execution gap for stale workers.
func (s *Store) ValidateExecutionLease(ctx context.Context, exec worker.Execution, lease queue.Lease) error {
	if s == nil || s.pool == nil {
		return errors.New("postgres store is not initialized")
	}
	if err := lease.Validate(); err != nil {
		return fmt.Errorf("execution lease: %w", err)
	}
	if err := exec.Tenant.Validate(); err != nil {
		return fmt.Errorf("execution tenant context: %w", err)
	}
	if exec.RequestID == "" {
		return errors.New("execution request_id is required")
	}
	var valid bool
	if err := s.pool.QueryRow(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM platform.execution
    WHERE tenant_id = $1
      AND app_id = $2
      AND request_id = $3
      AND status = 'RUNNING'
      AND lease_owner = $4
      AND run_token = $5
      AND lease_until > clock_timestamp()
      AND session_principal_id = $6
      AND session_id = $7
      AND user_id = $8
      AND config_version = $9
)`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		lease.Owner,
		lease.Token,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		exec.Tenant.UserID,
		exec.Tenant.ConfigVersion,
	).Scan(&valid); err != nil {
		return fmt.Errorf("query execution lease: %w", err)
	}
	if !valid {
		return queue.ErrLeaseLost
	}
	return nil
}

var _ worker.ExecutionLeaseValidator = (*Store)(nil)
