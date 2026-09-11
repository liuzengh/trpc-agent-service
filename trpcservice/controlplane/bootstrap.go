package controlplane

import (
	"context"
	"database/sql"
	"fmt"
)

// SeedBootstrap inserts development bootstrap objects without overwriting
// existing control-plane state.
func SeedBootstrap(ctx context.Context, db *sql.DB, data BootstrapData) error {
	if db == nil {
		return fmt.Errorf("bootstrap database is nil")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, tenant := range data.Tenants {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant(
    tenant_id, display_name, status, region, quota_config, audit_policy,
    secret_namespace, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10)
ON CONFLICT (tenant_id) DO NOTHING`,
			tenant.ID,
			tenant.DisplayName,
			tenant.Status,
			tenant.Region,
			string(tenant.QuotaConfig),
			string(tenant.AuditPolicy),
			tenant.SecretNamespace,
			tenant.Version,
			tenant.CreatedAt,
			tenant.UpdatedAt,
		); err != nil {
			return fmt.Errorf("seed tenant %q: %w", tenant.ID, err)
		}
	}
	for _, app := range data.Apps {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_app(
    app_id, tenant_id, name, description, status, rollout_policy,
    version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
ON CONFLICT (app_id) DO NOTHING`,
			app.ID,
			app.TenantID,
			app.Name,
			app.Description,
			app.Status,
			string(app.RolloutPolicy),
			app.Version,
			app.CreatedAt,
			app.UpdatedAt,
		); err != nil {
			return fmt.Errorf("seed agent app %q: %w", app.ID, err)
		}
	}
	for _, revision := range data.Revisions {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_revision(
    revision_id, tenant_id, app_id, revision_no, agent_type,
    agent_config, model_config, tool_policy, knowledge_config,
    memory_config, guardrail_config, checksum, created_by, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb,
    $10::jsonb, $11::jsonb, $12, $13, $14
)
ON CONFLICT (revision_id) DO NOTHING`,
			revision.ID,
			revision.TenantID,
			revision.AppID,
			revision.RevisionNo,
			revision.AgentType,
			string(revision.AgentConfig),
			string(revision.ModelConfig),
			string(revision.ToolPolicy),
			string(revision.KnowledgeConfig),
			string(revision.MemoryConfig),
			string(revision.GuardrailConfig),
			revision.Checksum,
			revision.CreatedBy,
			revision.CreatedAt,
		); err != nil {
			return fmt.Errorf("seed agent revision %q: %w", revision.ID, err)
		}
	}
	for _, app := range data.Apps {
		if app.StableRevisionID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE agent_app
SET stable_revision_id = COALESCE(stable_revision_id, $3), updated_at = updated_at
WHERE tenant_id = $1 AND app_id = $2`, app.TenantID, app.ID, app.StableRevisionID); err != nil {
			return fmt.Errorf("pin bootstrap revision for app %q: %w", app.ID, err)
		}
	}
	for _, binding := range data.ChannelBindings {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_binding(
    channel_binding_id, tenant_id, app_id, channel_type, account_id,
    callback_key, config, secret_ref, status, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12)
ON CONFLICT (channel_binding_id) DO NOTHING`,
			binding.ID,
			binding.TenantID,
			binding.AppID,
			binding.ChannelType,
			binding.AccountID,
			binding.CallbackKey,
			string(binding.Config),
			binding.SecretRef,
			binding.Status,
			binding.Version,
			binding.CreatedAt,
			binding.UpdatedAt,
		); err != nil {
			return fmt.Errorf("seed channel binding %q: %w", binding.ID, err)
		}
	}
	for _, binding := range data.BackendBindings {
		var appID any
		if binding.AppID != "" {
			appID = binding.AppID
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO backend_binding(
    binding_id, tenant_id, app_id, resource_type, backend_type, config,
    secret_ref, isolation_level, migration_state, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, NULLIF($7, ''), $8, $9, $10, $11, $12)
ON CONFLICT (binding_id) DO NOTHING`,
			binding.ID,
			binding.TenantID,
			appID,
			binding.ResourceType,
			binding.BackendType,
			string(binding.Config),
			binding.SecretRef,
			binding.IsolationLevel,
			binding.MigrationState,
			binding.Version,
			binding.CreatedAt,
			binding.UpdatedAt,
		); err != nil {
			return fmt.Errorf("seed backend binding %q: %w", binding.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap transaction: %w", err)
	}
	return nil
}
