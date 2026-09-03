package controlplane

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgresRepository reads control-plane snapshots from PostgreSQL.
type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository transfers ownership of db to the repository.
func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("control-plane database is nil")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) GetTenant(
	ctx context.Context,
	tenantID string,
) (Tenant, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT tenant_id, display_name, status, region, quota_config, audit_policy,
       secret_namespace, version, created_at, updated_at
FROM tenant
WHERE tenant_id = $1`, tenantID)
	var result Tenant
	var quotaConfig []byte
	var auditPolicy []byte
	if err := row.Scan(
		&result.ID,
		&result.DisplayName,
		&result.Status,
		&result.Region,
		&quotaConfig,
		&auditPolicy,
		&result.SecretNamespace,
		&result.Version,
		&result.CreatedAt,
		&result.UpdatedAt,
	); err != nil {
		return Tenant{}, mapNotFound("tenant", err)
	}
	result.QuotaConfig = json.RawMessage(quotaConfig)
	result.AuditPolicy = json.RawMessage(auditPolicy)
	return result, nil
}

func (r *PostgresRepository) GetAgentApp(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentApp, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT app_id, tenant_id, name, description, status, stable_revision_id,
       rollout_policy, version, created_at, updated_at
FROM agent_app
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
	return scanAgentApp(row)
}

func (r *PostgresRepository) GetRevision(
	ctx context.Context,
	tenantID string,
	revisionID string,
) (AgentRevision, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT revision_id, tenant_id, app_id, revision_no, agent_type,
       agent_config, model_config, tool_policy, knowledge_config,
       memory_config, guardrail_config, checksum, created_by, created_at
FROM agent_revision
WHERE tenant_id = $1 AND revision_id = $2`, tenantID, revisionID)
	return scanRevision(row)
}

func (r *PostgresRepository) GetStableRevision(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentRevision, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT r.revision_id, r.tenant_id, r.app_id, r.revision_no, r.agent_type,
       r.agent_config, r.model_config, r.tool_policy, r.knowledge_config,
       r.memory_config, r.guardrail_config, r.checksum, r.created_by, r.created_at
FROM agent_app a
JOIN agent_revision r
  ON r.tenant_id = a.tenant_id AND r.revision_id = a.stable_revision_id
WHERE a.tenant_id = $1 AND a.app_id = $2`, tenantID, appID)
	return scanRevision(row)
}

func (r *PostgresRepository) GetChannelBindingByCallbackKey(
	ctx context.Context,
	callbackKey string,
) (ChannelBinding, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT channel_binding_id, tenant_id, app_id, channel_type, account_id,
       callback_key, config, secret_ref, status, version, created_at, updated_at
FROM channel_binding
WHERE callback_key = $1`, callbackKey)
	var result ChannelBinding
	var configJSON []byte
	if err := row.Scan(
		&result.ID,
		&result.TenantID,
		&result.AppID,
		&result.ChannelType,
		&result.AccountID,
		&result.CallbackKey,
		&configJSON,
		&result.SecretRef,
		&result.Status,
		&result.Version,
		&result.CreatedAt,
		&result.UpdatedAt,
	); err != nil {
		return ChannelBinding{}, mapNotFound("channel binding", err)
	}
	result.Config = json.RawMessage(configJSON)
	return result, nil
}

func (r *PostgresRepository) GetChannelBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
) (ChannelBinding, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT channel_binding_id, tenant_id, app_id, channel_type, account_id,
       callback_key, config, secret_ref, status, version, created_at, updated_at
FROM channel_binding
WHERE tenant_id = $1 AND channel_binding_id = $2`, tenantID, bindingID)
	var result ChannelBinding
	var configJSON []byte
	if err := row.Scan(
		&result.ID,
		&result.TenantID,
		&result.AppID,
		&result.ChannelType,
		&result.AccountID,
		&result.CallbackKey,
		&configJSON,
		&result.SecretRef,
		&result.Status,
		&result.Version,
		&result.CreatedAt,
		&result.UpdatedAt,
	); err != nil {
		return ChannelBinding{}, mapNotFound("channel binding", err)
	}
	result.Config = json.RawMessage(configJSON)
	return result, nil
}

func (r *PostgresRepository) ListBackendBindings(
	ctx context.Context,
	tenantID string,
	appID string,
) ([]BackendBinding, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT binding_id, tenant_id, COALESCE(app_id, ''), resource_type,
       backend_type, config, COALESCE(secret_ref, ''), isolation_level,
       migration_state, version, created_at, updated_at
FROM backend_binding
WHERE tenant_id = $1 AND (app_id = $2 OR app_id IS NULL)
ORDER BY (app_id IS NOT NULL) DESC, resource_type, binding_id`, tenantID, appID)
	if err != nil {
		return nil, fmt.Errorf("list backend bindings: %w", err)
	}
	defer rows.Close()
	result := make([]BackendBinding, 0)
	for rows.Next() {
		var binding BackendBinding
		var configJSON []byte
		if err := rows.Scan(
			&binding.ID,
			&binding.TenantID,
			&binding.AppID,
			&binding.ResourceType,
			&binding.BackendType,
			&configJSON,
			&binding.SecretRef,
			&binding.IsolationLevel,
			&binding.MigrationState,
			&binding.Version,
			&binding.CreatedAt,
			&binding.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan backend binding: %w", err)
		}
		binding.Config = json.RawMessage(configJSON)
		result = append(result, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate backend bindings: %w", err)
	}
	return result, nil
}

func (r *PostgresRepository) GetBackendBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
) (BackendBinding, error) {
	return scanBackendBinding(r.db.QueryRowContext(ctx, `
SELECT binding_id, tenant_id, COALESCE(app_id, ''), resource_type,
       backend_type, config, COALESCE(secret_ref, ''), isolation_level,
       migration_state, version, created_at, updated_at
FROM backend_binding WHERE tenant_id=$1 AND binding_id=$2`, tenantID, bindingID))
}

func (r *PostgresRepository) GetBackendMigration(
	ctx context.Context,
	tenantID string,
	migrationID string,
) (BackendMigration, error) {
	return scanBackendMigration(r.db.QueryRowContext(ctx, backendMigrationSelect+
		` WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID))
}

func (r *PostgresRepository) GetActiveBackendMigration(
	ctx context.Context,
	tenantID string,
	appID string,
	resourceType string,
) (BackendMigration, error) {
	return scanBackendMigration(r.db.QueryRowContext(ctx, backendMigrationSelect+`
WHERE tenant_id=$1 AND app_id IS NOT DISTINCT FROM NULLIF($2,'') AND resource_type=$3
  AND state NOT IN ('completed','rolled_back','failed')
ORDER BY created_at DESC LIMIT 1`, tenantID, appID, resourceType))
}

func (r *PostgresRepository) AdjustBackendMigrationRepair(
	ctx context.Context,
	tenantID string,
	migrationID string,
	delta int64,
) error {
	result, err := r.db.ExecContext(ctx, `
UPDATE backend_migration
SET repair_backlog=GREATEST(0,repair_backlog+$3),updated_at=now()
WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID, delta)
	if err != nil {
		return fmt.Errorf("adjust backend migration repair backlog: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) Ready(ctx context.Context) error {
	if r == nil || r.db == nil {
		return ErrRepositoryClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping control-plane PostgreSQL: %w", err)
	}
	return nil
}

func (r *PostgresRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// SQLDB exposes the shared control-plane pool to repositories that participate
// in the same transactional message journal. The PostgresRepository remains
// the lifecycle owner.
func (r *PostgresRepository) SQLDB() *sql.DB {
	if r == nil {
		return nil
	}
	return r.db
}

func (r *PostgresRepository) CreateTenant(ctx context.Context, tenant Tenant) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO tenant(
    tenant_id, display_name, status, region, quota_config, audit_policy,
    secret_namespace, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10)`,
		tenant.ID, tenant.DisplayName, tenant.Status, tenant.Region,
		string(tenant.QuotaConfig), string(tenant.AuditPolicy), tenant.SecretNamespace,
		tenant.Version, tenant.CreatedAt, tenant.UpdatedAt)
	return mapMutationError("create tenant", err)
}

func (r *PostgresRepository) CreateAgentApp(ctx context.Context, app AgentApp) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO agent_app(
    app_id, tenant_id, name, description, status, stable_revision_id,
    rollout_policy, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7::jsonb, $8, $9, $10)`,
		app.ID, app.TenantID, app.Name, app.Description, app.Status,
		app.StableRevisionID, string(app.RolloutPolicy), app.Version,
		app.CreatedAt, app.UpdatedAt)
	return mapMutationError("create Agent app", err)
}

func (r *PostgresRepository) CreateRevision(
	ctx context.Context,
	revision AgentRevision,
) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO agent_revision(
    revision_id, tenant_id, app_id, revision_no, agent_type,
    agent_config, model_config, tool_policy, knowledge_config,
    memory_config, guardrail_config, checksum, created_by, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb,
    $10::jsonb, $11::jsonb, $12, $13, $14
)`, revision.ID, revision.TenantID, revision.AppID, revision.RevisionNo,
		revision.AgentType, string(revision.AgentConfig), string(revision.ModelConfig),
		string(revision.ToolPolicy), string(revision.KnowledgeConfig),
		string(revision.MemoryConfig), string(revision.GuardrailConfig),
		revision.Checksum, revision.CreatedBy, revision.CreatedAt)
	return mapMutationError("create Agent revision", err)
}

func (r *PostgresRepository) PublishRevision(
	ctx context.Context,
	tenantID string,
	appID string,
	revisionID string,
	expectedVersion int64,
) (AgentApp, error) {
	row := r.db.QueryRowContext(ctx, `
UPDATE agent_app a
SET stable_revision_id = r.revision_id,
    version = a.version + 1,
    updated_at = now()
FROM agent_revision r
WHERE a.tenant_id = $1 AND a.app_id = $2 AND a.version = $4
  AND r.tenant_id = a.tenant_id AND r.app_id = a.app_id AND r.revision_id = $3
RETURNING a.app_id, a.tenant_id, a.name, a.description, a.status,
          a.stable_revision_id, a.rollout_policy, a.version,
          a.created_at, a.updated_at`, tenantID, appID, revisionID, expectedVersion)
	app, err := scanAgentApp(row)
	if errors.Is(err, ErrNotFound) {
		return AgentApp{}, ErrConflict
	}
	return app, err
}

func (r *PostgresRepository) CreateChannelBinding(
	ctx context.Context,
	binding ChannelBinding,
) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO channel_binding(
    channel_binding_id, tenant_id, app_id, channel_type, account_id,
    callback_key, config, secret_ref, status, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12)`,
		binding.ID, binding.TenantID, binding.AppID, binding.ChannelType,
		binding.AccountID, binding.CallbackKey, string(binding.Config),
		binding.SecretRef, binding.Status, binding.Version,
		binding.CreatedAt, binding.UpdatedAt)
	return mapMutationError("create channel binding", err)
}

func (r *PostgresRepository) CreateBackendBinding(
	ctx context.Context,
	binding BackendBinding,
) error {
	var appID any
	if binding.AppID != "" {
		appID = binding.AppID
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO backend_binding(
    binding_id, tenant_id, app_id, resource_type, backend_type, config,
    secret_ref, isolation_level, migration_state, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, NULLIF($7, ''), $8, $9, $10, $11, $12)`,
		binding.ID, binding.TenantID, appID, binding.ResourceType,
		binding.BackendType, string(binding.Config), binding.SecretRef,
		binding.IsolationLevel, binding.MigrationState, binding.Version,
		binding.CreatedAt, binding.UpdatedAt)
	return mapMutationError("create backend binding", err)
}

func (r *PostgresRepository) CreateBackendMigration(
	ctx context.Context,
	migration BackendMigration,
) error {
	var appID any
	if migration.AppID != "" {
		appID = migration.AppID
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO backend_migration(
    migration_id,tenant_id,app_id,resource_type,source_binding_id,
    target_binding_id,state,checkpoint,verification,repair_backlog,
    version,created_at,updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10,$11,$12,$13)`,
		migration.ID, migration.TenantID, appID, migration.ResourceType,
		migration.SourceBindingID, migration.TargetBindingID, migration.State,
		string(migration.Checkpoint), string(migration.Verification), migration.RepairBacklog,
		migration.Version, migration.CreatedAt, migration.UpdatedAt,
	)
	return mapMutationError("create backend migration", err)
}

func (r *PostgresRepository) TransitionBackendMigration(
	ctx context.Context,
	tenantID string,
	migrationID string,
	nextState string,
	expectedVersion int64,
	checkpoint []byte,
	verification []byte,
) (BackendMigration, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return BackendMigration{}, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE backend_migration
SET state=$3,
    checkpoint=COALESCE(NULLIF($4,'')::jsonb,checkpoint),
    verification=COALESCE(NULLIF($5,'')::jsonb,verification),
    version=version+1,updated_at=now()
WHERE tenant_id=$1 AND migration_id=$2 AND version=$6`,
		tenantID, migrationID, nextState, string(checkpoint), string(verification), expectedVersion,
	)
	if err != nil {
		return BackendMigration{}, fmt.Errorf("transition backend migration: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return BackendMigration{}, err
	}
	if rows != 1 {
		return BackendMigration{}, ErrConflict
	}
	var sourceBindingID, targetBindingID string
	if err := tx.QueryRowContext(ctx, `
SELECT source_binding_id,target_binding_id FROM backend_migration
WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID).Scan(
		&sourceBindingID, &targetBindingID,
	); err != nil {
		return BackendMigration{}, err
	}
	if nextState == MigrationCompleted {
		if _, err := tx.ExecContext(ctx, `
UPDATE backend_binding SET migration_state='retired',version=version+1,updated_at=now()
WHERE tenant_id=$1 AND binding_id=$2`, tenantID, sourceBindingID); err != nil {
			return BackendMigration{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE backend_binding SET migration_state='active',version=version+1,updated_at=now()
WHERE tenant_id=$1 AND binding_id=$2`, tenantID, targetBindingID); err != nil {
			return BackendMigration{}, err
		}
	}
	if nextState == MigrationRolledBack {
		if _, err := tx.ExecContext(ctx, `
UPDATE backend_binding SET migration_state='active',version=version+1,updated_at=now()
WHERE tenant_id=$1 AND binding_id=$2`, tenantID, sourceBindingID); err != nil {
			return BackendMigration{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE backend_binding SET migration_state='migration_target',version=version+1,updated_at=now()
WHERE tenant_id=$1 AND binding_id=$2`, tenantID, targetBindingID); err != nil {
			return BackendMigration{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return BackendMigration{}, err
	}
	return r.GetBackendMigration(ctx, tenantID, migrationID)
}

func mapMutationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23505":
			return fmt.Errorf("%s: %w", operation, ErrConflict)
		case "23503":
			return fmt.Errorf("%s: %w", operation, ErrNotFound)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAgentApp(row scanner) (AgentApp, error) {
	var result AgentApp
	var stableRevision sql.NullString
	var rolloutPolicy []byte
	if err := row.Scan(
		&result.ID,
		&result.TenantID,
		&result.Name,
		&result.Description,
		&result.Status,
		&stableRevision,
		&rolloutPolicy,
		&result.Version,
		&result.CreatedAt,
		&result.UpdatedAt,
	); err != nil {
		return AgentApp{}, mapNotFound("agent app", err)
	}
	result.StableRevisionID = stableRevision.String
	result.RolloutPolicy = json.RawMessage(rolloutPolicy)
	return result, nil
}

func scanRevision(row scanner) (AgentRevision, error) {
	var result AgentRevision
	var agentConfig []byte
	var modelConfig []byte
	var toolPolicy []byte
	var knowledgeConfig []byte
	var memoryConfig []byte
	var guardrailConfig []byte
	if err := row.Scan(
		&result.ID,
		&result.TenantID,
		&result.AppID,
		&result.RevisionNo,
		&result.AgentType,
		&agentConfig,
		&modelConfig,
		&toolPolicy,
		&knowledgeConfig,
		&memoryConfig,
		&guardrailConfig,
		&result.Checksum,
		&result.CreatedBy,
		&result.CreatedAt,
	); err != nil {
		return AgentRevision{}, mapNotFound("agent revision", err)
	}
	result.AgentConfig = json.RawMessage(agentConfig)
	result.ModelConfig = json.RawMessage(modelConfig)
	result.ToolPolicy = json.RawMessage(toolPolicy)
	result.KnowledgeConfig = json.RawMessage(knowledgeConfig)
	result.MemoryConfig = json.RawMessage(memoryConfig)
	result.GuardrailConfig = json.RawMessage(guardrailConfig)
	return result, nil
}

func scanBackendBinding(row scanner) (BackendBinding, error) {
	var binding BackendBinding
	var configJSON []byte
	if err := row.Scan(
		&binding.ID, &binding.TenantID, &binding.AppID, &binding.ResourceType,
		&binding.BackendType, &configJSON, &binding.SecretRef, &binding.IsolationLevel,
		&binding.MigrationState, &binding.Version, &binding.CreatedAt, &binding.UpdatedAt,
	); err != nil {
		return BackendBinding{}, mapNotFound("backend binding", err)
	}
	binding.Config = json.RawMessage(configJSON)
	return binding, nil
}

const backendMigrationSelect = `SELECT migration_id,tenant_id,COALESCE(app_id,''),
resource_type,source_binding_id,target_binding_id,state,checkpoint,verification,
repair_backlog,version,created_at,updated_at FROM backend_migration`

func scanBackendMigration(row scanner) (BackendMigration, error) {
	var migration BackendMigration
	var checkpoint, verification []byte
	if err := row.Scan(
		&migration.ID, &migration.TenantID, &migration.AppID, &migration.ResourceType,
		&migration.SourceBindingID, &migration.TargetBindingID, &migration.State,
		&checkpoint, &verification, &migration.RepairBacklog, &migration.Version,
		&migration.CreatedAt, &migration.UpdatedAt,
	); err != nil {
		return BackendMigration{}, mapNotFound("backend migration", err)
	}
	migration.Checkpoint = json.RawMessage(checkpoint)
	migration.Verification = json.RawMessage(verification)
	return migration, nil
}

func mapNotFound(object string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", object, ErrNotFound)
	}
	return fmt.Errorf("read %s: %w", object, err)
}

var _ Repository = (*PostgresRepository)(nil)
