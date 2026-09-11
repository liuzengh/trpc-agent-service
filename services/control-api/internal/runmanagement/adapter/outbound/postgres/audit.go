package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

type AuditReader struct{ pool *pgxpool.Pool }

func NewAuditReader(pool *pgxpool.Pool) *AuditReader { return &AuditReader{pool: pool} }

const auditUnion = `
 SELECT 'agent:'||id AS event_id,'CONFIG' AS category,'CREATE' AS action,created_by AS actor_id,'agent' AS resource_type,id AS resource_id,created_at AS occurred_at FROM agents WHERE tenant_id=$1
 UNION ALL SELECT 'agent-version:'||id,'CONFIG','PUBLISH',published_by,'agent_version',id,published_at FROM agent_versions WHERE tenant_id=$1
 UNION ALL SELECT 'profile:'||id,'CONFIG','CREATE',created_by,'runtime_profile',id,created_at FROM runtime_profiles WHERE tenant_id=$1
 UNION ALL SELECT 'profile-revision:'||id,'CONFIG','PUBLISH',published_by,'profile_revision',id,published_at FROM runtime_profile_revisions WHERE tenant_id=$1
 UNION ALL SELECT 'deployment:'||id,'CONFIG','CREATE',created_by,'deployment',id,created_at FROM deployments WHERE tenant_id=$1
 UNION ALL SELECT 'deployment-revision:'||id,'CONFIG','PUBLISH',published_by,'deployment_revision',id,published_at FROM deployment_revisions WHERE tenant_id=$1
 UNION ALL SELECT 'channel-account:'||id,'CONFIG','CREATE',created_by,'channel_account',id,created_at FROM channel_accounts WHERE tenant_id=$1
 UNION ALL SELECT 'channel-binding:'||id,'CONFIG','CREATE',created_by,'channel_binding',id,created_at FROM channel_bindings WHERE tenant_id=$1`

func (r *AuditReader) List(ctx context.Context, tenant string, offset, limit int) (managementv1.AuditPage, error) {
	page := managementv1.AuditPage{Events: []managementv1.AuditEvent{}, Offset: offset, Limit: limit}
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM (`+auditUnion+`) events`, tenant).Scan(&page.Total); err != nil {
		return page, err
	}
	rows, err := r.pool.Query(ctx, `SELECT * FROM (`+auditUnion+`) events ORDER BY occurred_at DESC,event_id DESC OFFSET $2 LIMIT $3`, tenant, offset, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var event managementv1.AuditEvent
		if err = rows.Scan(&event.EventID, &event.Category, &event.Action, &event.ActorID, &event.ResourceType, &event.ResourceID, &event.OccurredAt); err != nil {
			return page, err
		}
		event.Source, event.Outcome = "control", "SUCCEEDED"
		event.Attributes = map[string]any{"tenant_id": tenant}
		page.Events = append(page.Events, event)
	}
	if err = rows.Err(); err != nil {
		return page, fmt.Errorf("audit read: %w", err)
	}
	return page, nil
}
