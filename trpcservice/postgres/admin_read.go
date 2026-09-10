package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	platformadmin "github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ListTenants returns metadata-only tenants in stable order. An optional
// tenant filter lets scoped Admin roles avoid an unbounded cross-tenant read.
func (s *Store) ListTenants(ctx context.Context, options platformadmin.ListOptions) ([]platformadmin.TenantView, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if options.Limit < 0 || options.Limit > 1000 {
		return nil, errors.New("list limit is invalid")
	}
	clauses := make([]string, 0, 1)
	args := make([]any, 0, 2)
	if strings.TrimSpace(options.TenantID) != "" {
		args = append(args, options.TenantID)
		clauses = append(clauses, fmt.Sprintf("t.tenant_id = $%d", len(args)))
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	args = append(args, limit)
	where := ""
	if len(clauses) != 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
	SELECT t.tenant_id, t.name, t.status, t.audit_policy, t.quota_policy, t.updated_at,
       (SELECT count(*) FROM platform.agent_app AS app
        WHERE app.tenant_id = t.tenant_id),
       CASE WHEN t.status <> 'ACTIVE'
              OR EXISTS (
                  SELECT 1 FROM platform.agent_app AS app
                  WHERE app.tenant_id = t.tenant_id AND app.status <> 'ACTIVE'
              )
              OR EXISTS (
                  SELECT 1 FROM platform.channel_binding AS binding
                  WHERE binding.tenant_id = t.tenant_id
                    AND binding.status = 'ACTIVE'
                    AND binding.connection_status <> 'READY'
              )
              OR EXISTS (
                  SELECT 1 FROM platform.data_migration AS migration
                  WHERE migration.tenant_id = t.tenant_id
                    AND (migration.status = 'FAILED'
                         OR (migration.status IN ('DRAINING', 'COPYING', 'VERIFYING')
                             AND migration.updated_at < clock_timestamp() - interval '5 minutes'))
              )
              OR EXISTS (
                  SELECT 1 FROM platform.execution AS execution
                  WHERE execution.tenant_id = t.tenant_id
                    AND execution.status IN ('FAILED', 'UNCERTAIN')
                    AND execution.updated_at >= clock_timestamp() - interval '15 minutes'
              )
            THEN 'DEGRADED' ELSE 'READY' END
FROM platform.tenant AS t
%s
ORDER BY t.tenant_id
LIMIT $%d`, where, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()
	values := make([]platformadmin.TenantView, 0)
	for rows.Next() {
		var value platformadmin.TenantView
		var encoded, encodedQuota []byte
		if err := rows.Scan(
			&value.ID, &value.Name, &value.Status, &encoded,
			&encodedQuota, &value.UpdatedAt, &value.AgentAppCount, &value.AnomalyStatus,
		); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		value.Audit, err = unmarshalAuditPolicy(encoded)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(encodedQuota, &value.Quota); err != nil {
			return nil, fmt.Errorf("unmarshal tenant quota: %w", err)
		}
		if err := (tenant.Tenant{
			ID: value.ID, Name: value.Name, Status: value.Status, Audit: value.Audit, Quota: value.Quota,
		}).Validate(); err != nil {
			return nil, fmt.Errorf("stored tenant: %w", err)
		}
		value.UpdatedAt = value.UpdatedAt.UTC()
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}
	return values, nil
}

func backendSummaries(config tenant.BackendConfig) []platformadmin.BackendSummary {
	refs := []tenant.BackendRef{config.Session, config.Memory, config.Knowledge, config.Artifact}
	values := make([]platformadmin.BackendSummary, 0, len(refs))
	for _, ref := range refs {
		if ref.IsZero() {
			continue
		}
		values = append(values, platformadmin.BackendSummary{
			Kind: ref.Kind, Provider: ref.Provider, Name: ref.Name,
			Status: "CONFIGURED",
		})
	}
	return values
}

// ListAgentApps returns application metadata from one tenant partition.
func (s *Store) ListAgentApps(ctx context.Context, options platformadmin.ListOptions) ([]platformadmin.AgentAppView, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateAdminListOptions(options, false); err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	args := []any{options.TenantID, limit}
	where := "a.tenant_id = $1"
	if strings.TrimSpace(options.AppID) != "" {
		args = []any{options.TenantID, options.AppID, limit}
		where = "a.tenant_id = $1 AND a.app_id = $2"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT a.tenant_id, a.app_id, a.name, a.active_config_version,
       COALESCE(a.canary_config_version, ''), a.canary_percentage, a.canary_status, a.status,
       a.updated_at, c.backend_config,
       COALESCE((
           SELECT jsonb_agg(jsonb_build_object(
               'provider', b.channel,
               'binding_id', b.binding_id,
               'status', b.status,
               'connection_status', b.connection_status
           ) ORDER BY b.binding_id)
           FROM platform.channel_binding AS b
           WHERE b.tenant_id = a.tenant_id AND b.app_id = a.app_id
       ), '[]'::jsonb)
FROM platform.agent_app AS a
JOIN platform.app_config_version AS c
  ON c.tenant_id = a.tenant_id
 AND c.app_id = a.app_id
 AND c.version = a.active_config_version
WHERE %s
ORDER BY a.app_id
LIMIT $%d`, where, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list agent apps: %w", err)
	}
	defer rows.Close()
	values := make([]platformadmin.AgentAppView, 0)
	for rows.Next() {
		var value platformadmin.AgentAppView
		var encodedBackend, encodedChannels []byte
		if err := rows.Scan(
			&value.TenantID, &value.AppID, &value.Name, &value.ActiveConfigVersion,
			&value.CanaryConfigVersion, &value.CanaryPercentage, &value.CanaryStatus,
			&value.Status, &value.UpdatedAt, &encodedBackend, &encodedChannels,
		); err != nil {
			return nil, fmt.Errorf("scan agent app: %w", err)
		}
		var backend tenant.BackendConfig
		if err := json.Unmarshal(encodedBackend, &backend); err != nil {
			return nil, fmt.Errorf("decode active app backend: %w", err)
		}
		if err := (tenant.AgentApp{
			TenantID: value.TenantID, AppID: value.AppID, Name: value.Name,
			ActiveConfigVersion: value.ActiveConfigVersion,
			CanaryConfigVersion: value.CanaryConfigVersion,
			CanaryPercentage:    value.CanaryPercentage,
			CanaryStatus:        value.CanaryStatus, Status: value.Status,
		}).Validate(); err != nil {
			return nil, fmt.Errorf("stored agent app: %w", err)
		}
		value.Backends = backendSummaries(backend)
		if err := json.Unmarshal(encodedChannels, &value.Channels); err != nil {
			return nil, fmt.Errorf("decode app channel summary: %w", err)
		}
		value.UpdatedAt = value.UpdatedAt.UTC()
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent apps: %w", err)
	}
	return values, nil
}

// ListAppConfigs returns immutable configuration versions without resolving
// any SecretRef. Config payloads contain references only and are safe for the
// Admin API to display.
func (s *Store) ListAppConfigs(ctx context.Context, options platformadmin.ListOptions) ([]platformadmin.AppConfigView, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateAdminListOptions(options, true); err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
SELECT c.version, c.model_config, c.tool_policy, c.backend_config,
       c.audit_policy, c.secret_refs, c.channel_binding_ids,
       c.knowledge_base_ids, c.status, c.created_at,
       a.active_config_version
FROM platform.app_config_version AS c
JOIN platform.agent_app AS a
  ON a.tenant_id = c.tenant_id AND a.app_id = c.app_id
WHERE c.tenant_id = $1 AND c.app_id = $2
ORDER BY c.created_at DESC, c.version DESC
LIMIT $3`, options.TenantID, options.AppID, limit)
	if err != nil {
		return nil, fmt.Errorf("list app configs: %w", err)
	}
	defer rows.Close()
	values := make([]platformadmin.AppConfigView, 0)
	for rows.Next() {
		var version, status, activeVersion string
		var createdAt time.Time
		var encoded appConfigColumns
		if err := rows.Scan(
			&version,
			&encoded.modelConfig,
			&encoded.toolPolicy,
			&encoded.backendConfig,
			&encoded.auditPolicy,
			&encoded.secretRefs,
			&encoded.channelBindingIDs,
			&encoded.knowledgeBaseIDs,
			&status,
			&createdAt,
			&activeVersion,
		); err != nil {
			return nil, fmt.Errorf("scan app config: %w", err)
		}
		cfg, err := unmarshalAppConfig(options.TenantID, options.AppID, version, encoded)
		if err != nil {
			return nil, fmt.Errorf("decode app config %s: %w", version, err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("stored app config %s: %w", version, err)
		}
		values = append(values, platformadmin.AppConfigView{
			Config: cfg, Status: status, Active: version == activeVersion, CreatedAt: createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate app configs: %w", err)
	}
	return values, nil
}

// ListChannelBindings returns binding metadata and SecretRef only.
func (s *Store) ListChannelBindings(ctx context.Context, options platformadmin.ListOptions) ([]channels.Binding, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateAdminListOptions(options, false); err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	args := []any{options.TenantID, limit}
	where := "tenant_id = $1"
	if strings.TrimSpace(options.AppID) != "" {
		args = []any{options.TenantID, options.AppID, limit}
		where = "tenant_id = $1 AND app_id = $2"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`%s
WHERE %s
ORDER BY app_id, binding_id
LIMIT $%d`, channelBindingSelect, where, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list channel bindings: %w", err)
	}
	defer rows.Close()
	values := make([]channels.Binding, 0)
	for rows.Next() {
		binding, err := scanChannelBinding(rows)
		if err != nil {
			return nil, fmt.Errorf("scan channel binding: %w", err)
		}
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("stored channel binding: %w", err)
		}
		values = append(values, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel bindings: %w", err)
	}
	return values, nil
}

// ListExecutions returns operational metadata but never the command payload
// or the lease fencing token.
func (s *Store) ListExecutions(ctx context.Context, options platformadmin.ListOptions) ([]platformadmin.ExecutionView, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateAdminListOptions(options, false); err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	args := []any{options.TenantID, limit}
	where := "tenant_id = $1"
	if strings.TrimSpace(options.AppID) != "" {
		args = []any{options.TenantID, options.AppID, limit}
		where = "tenant_id = $1 AND app_id = $2"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT tenant_id, app_id, request_id, session_principal_id, session_id,
       user_id, turn_seq, config_version, status, attempt, next_attempt_at,
       COALESCE(lease_owner, ''), lease_until, last_error, trace_id, started_at, finished_at,
       created_at, updated_at,
       COALESCE((
           SELECT ae.error_type
           FROM platform.audit_event AS ae
           WHERE ae.tenant_id = execution.tenant_id
             AND ae.app_id = execution.app_id
             AND ae.request_id = execution.request_id
             AND ae.error_type <> ''
           ORDER BY ae.created_at DESC, ae.audit_event_id DESC
           LIMIT 1
       ), '') AS error_type,
       COALESCE((
           SELECT jsonb_agg(jsonb_build_object(
               'tool_name', tool_summary.tool_name,
               'event_type', tool_summary.event_type,
               'decision', tool_summary.decision,
               'count', tool_summary.count
           ) ORDER BY tool_summary.tool_name, tool_summary.event_type, tool_summary.decision)
           FROM (
               SELECT ae.tool_name, ae.event_type, ae.decision, count(*)::int AS count
               FROM platform.audit_event AS ae
               WHERE ae.tenant_id = execution.tenant_id
                 AND ae.app_id = execution.app_id
                 AND ae.request_id = execution.request_id
                 AND ae.tool_name <> ''
               GROUP BY ae.tool_name, ae.event_type, ae.decision
           ) AS tool_summary
       ), '[]'::jsonb) AS tool_calls
FROM platform.execution AS execution
WHERE %s
ORDER BY created_at DESC, request_id DESC
LIMIT $%d`, where, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()
	values := make([]platformadmin.ExecutionView, 0)
	for rows.Next() {
		var value platformadmin.ExecutionView
		var lastError string
		var encodedToolCalls []byte
		if err := rows.Scan(
			&value.TenantID, &value.AppID, &value.RequestID,
			&value.SessionPrincipalID, &value.SessionID, &value.UserID,
			&value.TurnSeq, &value.ConfigVersion, &value.Status, &value.Attempt,
			&value.NextAttemptAt, &value.LeaseOwner, &value.LeaseUntil,
			&lastError, &value.TraceID, &value.StartedAt, &value.FinishedAt,
			&value.CreatedAt, &value.UpdatedAt, &value.ErrorType, &encodedToolCalls,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		if lastError != "" {
			value.LastError = platformlog.SafeError(errors.New(lastError))
		}
		if err := json.Unmarshal(encodedToolCalls, &value.ToolCalls); err != nil {
			return nil, fmt.Errorf("decode execution tool summary: %w", err)
		}
		if value.StartedAt != nil {
			end := value.FinishedAt
			if end == nil {
				now := time.Now().UTC()
				end = &now
			}
			if duration := end.Sub(*value.StartedAt); duration > 0 {
				value.DurationMS = duration.Milliseconds()
			}
		}
		value.NextAttemptAt = value.NextAttemptAt.UTC()
		value.CreatedAt = value.CreatedAt.UTC()
		value.UpdatedAt = value.UpdatedAt.UTC()
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate executions: %w", err)
	}
	return values, nil
}

// ListDataMigrations returns progress and checkpoint metadata for one tenant.
func (s *Store) ListDataMigrations(ctx context.Context, options platformadmin.ListOptions) ([]migration.Record, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateAdminListOptions(options, false); err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	args := []any{options.TenantID, limit}
	where := "tenant_id = $1"
	if strings.TrimSpace(options.AppID) != "" {
		args = []any{options.TenantID, options.AppID, limit}
		where = "tenant_id = $1 AND app_id = $2"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
SELECT migration_id, tenant_id, app_id, domain, source_config_version,
       target_config_version, status, lease_owner, lease_until, run_token,
       drain_deadline, failure_reason, total_sessions, copy_progress,
       verify_progress, success_count, last_checkpoint_at, last_failure_stage,
       created_at, updated_at
FROM platform.data_migration
WHERE %s
ORDER BY created_at DESC, migration_id DESC
LIMIT $%d`, where, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list data migrations: %w", err)
	}
	defer rows.Close()
	values := make([]migration.Record, 0)
	for rows.Next() {
		record, err := scanDataMigrationRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("scan data migration: %w", err)
		}
		if record.FailureReason != "" {
			record.FailureReason = platformlog.SafeError(errors.New(record.FailureReason))
		}
		values = append(values, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate data migrations: %w", err)
	}
	return values, nil
}

func validateAdminListOptions(options platformadmin.ListOptions, requireApp bool) error {
	if strings.TrimSpace(options.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if requireApp && strings.TrimSpace(options.AppID) == "" {
		return errors.New("app_id is required")
	}
	if options.Limit < 0 || options.Limit > 1000 {
		return errors.New("list limit is invalid")
	}
	return nil
}
