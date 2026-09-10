//go:build integration

package postgres_test

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

func TestExecutionLeaseValidatorFencesStaleOwner(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	claim, execution := claimIM05Execution(t, p, "message-execution-fence", "request-execution-fence")

	if err := p.store.ValidateExecutionLease(p.ctx, execution, claim.Lease); err != nil {
		t.Fatalf("validate current execution lease: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `
UPDATE platform.execution
SET lease_owner = 'worker-b',
    run_token = 'run-b',
    lease_until = clock_timestamp() + interval '1 minute'
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, execution.RequestID); err != nil {
		t.Fatalf("take over execution lease: %v", err)
	}

	if err := p.store.ValidateExecutionLease(p.ctx, execution, claim.Lease); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale lease error = %v, want %v", err, queue.ErrLeaseLost)
	}
	if err := p.store.ValidateExecutionLease(p.ctx, execution, queue.Lease{
		Owner: "worker-b", Token: "run-b", Until: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("validate takeover lease: %v", err)
	}
}
