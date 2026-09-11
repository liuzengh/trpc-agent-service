package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/backendmigration"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// WithMemoryMigrationScopes blocks new tenant Run admissions, verifies that
// the tenant has no active/PENDING work, and exposes only scopes derived from
// durable accepted Runs. Memory identity survives revision changes, so the
// whole fenced tenant is the discovery boundary. The callback executes while the fence is
// held so a successful copy and the accepted-root snapshot cannot diverge.
func (l *Ledger) WithMemoryMigrationScopes(ctx context.Context, tenant, deploymentRevision, agentID string, use func([]memorystore.Scope) error) error {
	if tenant == "" || deploymentRevision == "" || agentID == "" || use == nil {
		return domain.ErrInvalid
	}
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1,731004286))`, tenant); err != nil {
		return err
	}
	defer func() {
		unlock, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlock, `SELECT pg_advisory_unlock(hashtextextended($1,731004286))`, tenant)
	}()
	var busy bool
	if err = conn.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM execution_runs r
 LEFT JOIN execution_completions c ON c.tenant_id=r.tenant_id AND c.run_id=r.run_id
 WHERE r.tenant_id=$1
 AND (r.status IN ('QUEUED','RUNNING','RETRY_WAIT') OR c.memory_status='PENDING'))`, tenant).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return backendmigration.ErrBusy
	}
	// MemoryScopeID intentionally survives DeploymentRevision and Binding
	// changes. Enumerating only the source revision would miss identities whose
	// last interaction predates that publication, so derive candidate social
	// identities from every accepted Run in the fenced tenant.
	rows, err := conn.Query(ctx, `SELECT DISTINCT request_json FROM execution_runs
 WHERE tenant_id=$1`, tenant)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var scopes []memorystore.Scope
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		var requested domain.Requested
		if err = json.Unmarshal(raw, &requested); err != nil || requested.Validate() != nil || requested.Route.TenantID != tenant {
			return errors.New("invalid stored migration run")
		}
		id, err := requested.MemoryScopeID(agentID)
		if err != nil {
			return err
		}
		if !seen[id] {
			seen[id] = true
			scopes = append(scopes, memorystore.Scope{TenantID: tenant, ID: id})
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].ID < scopes[j].ID })
	return use(scopes)
}
