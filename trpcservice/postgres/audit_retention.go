package postgres

import (
	"context"
	"fmt"

	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

const (
	defaultAuditPurgeBatchSize = 1000
	maxAuditPurgeBatchSize     = 10000
)

type auditRetentionScope struct {
	tenantID      string
	appID         string
	configVersion string
	retentionDays int
}

// PurgeExpiredAuditEvents removes expired audit rows tenant by tenant and
// application configuration by configuration. An application retention of
// zero inherits a finite tenant limit; zero at both scopes means retain
// indefinitely. Each DELETE is bounded so a large tenant cannot monopolize
// one transaction, and every predicate keeps the tenant scope explicit.
func (s *Store) PurgeExpiredAuditEvents(ctx context.Context, batchSize int) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = defaultAuditPurgeBatchSize
	}
	if batchSize > maxAuditPurgeBatchSize {
		return 0, fmt.Errorf("audit purge batch size must be at most %d", maxAuditPurgeBatchSize)
	}

	rows, err := s.pool.Query(ctx, `
	SELECT c.tenant_id, c.app_id, c.version, t.audit_policy, c.audit_policy
	FROM platform.app_config_version c
	JOIN platform.tenant t ON t.tenant_id = c.tenant_id
	ORDER BY c.tenant_id, c.app_id, c.version`)
	if err != nil {
		return 0, fmt.Errorf("list audit retention policies: %w", err)
	}
	var scopes []auditRetentionScope
	for rows.Next() {
		var tenantID string
		var appID string
		var configVersion string
		var tenantEncoded []byte
		var appEncoded []byte
		if err := rows.Scan(&tenantID, &appID, &configVersion, &tenantEncoded, &appEncoded); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan audit retention policy: %w", err)
		}
		tenantPolicy, err := unmarshalAuditPolicy(tenantEncoded)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("tenant %s audit retention policy: %w", tenantID, err)
		}
		appPolicy, err := unmarshalAuditPolicy(appEncoded)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("tenant %s app %s audit retention policy: %w", tenantID, appID, err)
		}
		retentionDays := tenantPolicy.EffectiveRetentionDays(appPolicy)
		if retentionDays > 0 {
			scopes = append(scopes, auditRetentionScope{
				tenantID:      tenantID,
				appID:         appID,
				configVersion: configVersion,
				retentionDays: retentionDays,
			})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate audit retention policies: %w", err)
	}
	rows.Close()

	var deleted int64
	for _, scope := range scopes {
		var scopeDeleted int64
		for {
			if err := ctx.Err(); err != nil {
				return deleted, err
			}
			commandTag, err := s.pool.Exec(ctx, `
WITH doomed AS (
    SELECT audit_event_id
    FROM platform.audit_event
    WHERE tenant_id = $1 AND app_id = $2 AND config_version = $3
      AND created_at < clock_timestamp() - make_interval(days => $4)
    ORDER BY created_at, audit_event_id
    LIMIT $5
)
DELETE FROM platform.audit_event AS event
USING doomed
WHERE event.audit_event_id = doomed.audit_event_id`,
				scope.tenantID, scope.appID, scope.configVersion, scope.retentionDays, batchSize)
			if err != nil {
				return deleted, fmt.Errorf("purge audit events for tenant %s app %s config %s: %w", scope.tenantID, scope.appID, scope.configVersion, err)
			}
			count := commandTag.RowsAffected()
			deleted += count
			scopeDeleted += count
			if count == 0 {
				break
			}
		}
		if scopeDeleted > 0 && s.metrics != nil {
			s.metrics.RecordAuditPurge(ctx, platformmetrics.Labels{
				TenantID: scope.tenantID,
				AppID:    scope.appID,
			}, scopeDeleted)
		}
	}
	return deleted, nil
}
