package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// SQLStore persists control-plane configuration in PostgreSQL.
type SQLStore struct{ db *sql.DB }

// NewSQLStore creates a PostgreSQL-backed Store. The caller owns db.
func NewSQLStore(db *sql.DB) (*SQLStore, error) {
	if db == nil {
		return nil, errors.New("repository: nil database")
	}
	return &SQLStore{db: db}, nil
}

// PublishConfig publishes within one transaction and locks the tenant head.
func (store *SQLStore) PublishConfig(ctx context.Context, record ConfigRecord, expected tenant.ConfigVersion) (ConfigRecord, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: begin publish: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tenants (tenant_id, name, enabled, current_config_version) VALUES ($1,$2,$3,0) ON CONFLICT (tenant_id) DO NOTHING`, record.TenantID, record.TenantName, record.TenantEnabled)
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: ensure tenant: %w", err)
	}
	var current tenant.ConfigVersion
	if err = tx.QueryRowContext(ctx, `SELECT current_config_version FROM tenants WHERE tenant_id=$1 FOR UPDATE`, record.TenantID).Scan(&current); err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: lock tenant: %w", err)
	}
	if current != expected {
		return ConfigRecord{}, ErrVersionConflict
	}
	record.Version = current + 1
	var rollback any
	if record.RolledBackFrom != nil {
		rollback = *record.RolledBackFrom
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO config_versions (tenant_id, version, config_yaml, config_sha256, rolled_back_from, created_by, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, record.TenantID, record.Version, record.Payload, record.SHA256, rollback, record.CreatedBy, record.CreatedAt)
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: insert config: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE agent_apps SET enabled=FALSE WHERE tenant_id=$1`, record.TenantID); err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: disable stale apps: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE channel_bindings SET enabled=FALSE WHERE tenant_id=$1`, record.TenantID); err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: disable stale bindings: %w", err)
	}
	for _, app := range record.Apps {
		_, err = tx.ExecContext(ctx, `INSERT INTO agent_apps (tenant_id, app_id, name, enabled, config_version) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,app_id) DO UPDATE SET name=EXCLUDED.name, enabled=EXCLUDED.enabled, config_version=EXCLUDED.config_version`, record.TenantID, app.ID, app.Name, app.Enabled, record.Version)
		if err != nil {
			return ConfigRecord{}, fmt.Errorf("repository: materialize app %q: %w", app.ID, err)
		}
		for _, binding := range app.Channels {
			_, err = tx.ExecContext(ctx, `INSERT INTO channel_bindings (tenant_id, app_id, binding_id, channel_type, provider_account_id, enabled, config_version) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,binding_id) DO UPDATE SET app_id=EXCLUDED.app_id, channel_type=EXCLUDED.channel_type, provider_account_id=EXCLUDED.provider_account_id, enabled=EXCLUDED.enabled, config_version=EXCLUDED.config_version`, record.TenantID, app.ID, binding.ID, binding.Type, binding.ProviderAccountID, binding.Enabled, record.Version)
			if err != nil {
				return ConfigRecord{}, fmt.Errorf("repository: materialize binding %q: %w", binding.ID, err)
			}
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE tenants SET name=$2, enabled=$3, current_config_version=$4, updated_at=NOW() WHERE tenant_id=$1 AND current_config_version=$5`, record.TenantID, record.TenantName, record.TenantEnabled, record.Version, expected)
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: advance tenant: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: inspect publish: %w", err)
	}
	if affected != 1 {
		return ConfigRecord{}, ErrVersionConflict
	}
	if err = tx.Commit(); err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: commit publish: %w", err)
	}
	return cloneRecord(record), nil
}

// ListConfigVersions lists only the requested tenant's versions.
func (store *SQLStore) ListConfigVersions(ctx context.Context, tenantID string) ([]ConfigRecord, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT tenant_id, version, config_yaml, config_sha256, rolled_back_from, created_by, created_at FROM config_versions WHERE tenant_id=$1 ORDER BY version DESC`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("repository: list configs: %w", err)
	}
	defer rows.Close()
	var records []ConfigRecord
	for rows.Next() {
		var record ConfigRecord
		var rollback sql.NullInt64
		var createdBy sql.NullString
		if err := rows.Scan(&record.TenantID, &record.Version, &record.Payload, &record.SHA256, &rollback, &createdBy, &record.CreatedAt); err != nil {
			return nil, fmt.Errorf("repository: scan config: %w", err)
		}
		if rollback.Valid {
			value := tenant.ConfigVersion(rollback.Int64)
			record.RolledBackFrom = &value
		}
		record.CreatedBy = createdBy.String
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: list configs: %w", err)
	}
	return records, nil
}

// GetConfigVersion returns one version with tenant scope in the predicate.
func (store *SQLStore) GetConfigVersion(ctx context.Context, tenantID string, version tenant.ConfigVersion) (ConfigRecord, error) {
	var record ConfigRecord
	var rollback sql.NullInt64
	var createdBy sql.NullString
	err := store.db.QueryRowContext(ctx, `SELECT tenant_id, version, config_yaml, config_sha256, rolled_back_from, created_by, created_at FROM config_versions WHERE tenant_id=$1 AND version=$2`, tenantID, version).Scan(&record.TenantID, &record.Version, &record.Payload, &record.SHA256, &rollback, &createdBy, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ConfigRecord{}, ErrNotFound
	}
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: get config: %w", err)
	}
	if rollback.Valid {
		value := tenant.ConfigVersion(rollback.Int64)
		record.RolledBackFrom = &value
	}
	record.CreatedBy = createdBy.String
	return record, nil
}

// GetCurrentConfig returns the tenant head with tenant scope in the predicate.
func (store *SQLStore) GetCurrentConfig(ctx context.Context, tenantID string) (ConfigRecord, error) {
	var version tenant.ConfigVersion
	err := store.db.QueryRowContext(ctx, `SELECT current_config_version FROM tenants WHERE tenant_id=$1`, tenantID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return ConfigRecord{}, ErrNotFound
	}
	if err != nil {
		return ConfigRecord{}, fmt.Errorf("repository: read tenant head: %w", err)
	}
	if version == 0 {
		return ConfigRecord{}, ErrNotFound
	}
	return store.GetConfigVersion(ctx, tenantID, version)
}
