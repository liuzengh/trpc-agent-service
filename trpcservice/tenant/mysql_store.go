package tenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLStore persists authoritative tenant configuration.
type MySQLStore struct {
	db *sql.DB
}

// OpenMySQLStore opens and verifies a MySQL control-plane connection.
func OpenMySQLStore(ctx context.Context, dsn string) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open MySQL config store: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping MySQL config store: %w", err)
	}
	return NewMySQLStore(db), nil
}

// NewMySQLStore uses an existing database handle.
func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

// DB exposes the shared handle for migrations and later runtime components.
func (s *MySQLStore) DB() *sql.DB {
	return s.db
}

// Close closes the underlying database handle.
func (s *MySQLStore) Close() error {
	return s.db.Close()
}

// UpsertTenant creates or replaces the mutable fields of a tenant.
func (s *MySQLStore) UpsertTenant(ctx context.Context, value Tenant) error {
	if err := value.Validate(); err != nil {
		return fmt.Errorf("validate tenant: %w", err)
	}
	quota, err := json.Marshal(value.Quota)
	if err != nil {
		return fmt.Errorf("encode tenant quota: %w", err)
	}
	policy, err := json.Marshal(value.Policy)
	if err != nil {
		return fmt.Errorf("encode tenant policy: %w", err)
	}
	const query = `INSERT INTO tenant
		(tenant_id, name, is_active, quota_json, policy_json)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		name = VALUES(name), is_active = VALUES(is_active),
		quota_json = VALUES(quota_json), policy_json = VALUES(policy_json)`
	if _, err := s.db.ExecContext(ctx, query, value.ID, value.Name, value.IsActive, quota, policy); err != nil {
		return fmt.Errorf("upsert tenant %q: %w", value.ID, err)
	}
	return nil
}

// GetTenant returns one tenant by identifier.
func (s *MySQLStore) GetTenant(ctx context.Context, id string) (Tenant, error) {
	const query = `SELECT tenant_id, name, is_active, quota_json, policy_json
		FROM tenant WHERE tenant_id = ?`
	var value Tenant
	var quota, policy []byte
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&value.ID, &value.Name, &value.IsActive, &quota, &policy,
	)
	if err != nil {
		return Tenant{}, wrapNotFound(fmt.Sprintf("get tenant %q", id), err)
	}
	if err := json.Unmarshal(quota, &value.Quota); err != nil {
		return Tenant{}, fmt.Errorf("decode tenant %q quota: %w", id, err)
	}
	if err := json.Unmarshal(policy, &value.Policy); err != nil {
		return Tenant{}, fmt.Errorf("decode tenant %q policy: %w", id, err)
	}
	return value, nil
}

// ListTenants returns all tenants in stable identifier order.
func (s *MySQLStore) ListTenants(ctx context.Context) ([]Tenant, error) {
	const query = `SELECT tenant_id, name, is_active, quota_json, policy_json
		FROM tenant ORDER BY tenant_id`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	var result []Tenant
	for rows.Next() {
		var value Tenant
		var quota, policy []byte
		if err := rows.Scan(&value.ID, &value.Name, &value.IsActive, &quota, &policy); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		if err := json.Unmarshal(quota, &value.Quota); err != nil {
			return nil, fmt.Errorf("decode tenant %q quota: %w", value.ID, err)
		}
		if err := json.Unmarshal(policy, &value.Policy); err != nil {
			return nil, fmt.Errorf("decode tenant %q policy: %w", value.ID, err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}
	return result, nil
}

// DeactivateTenant prevents new data-plane requests for one tenant.
func (s *MySQLStore) DeactivateTenant(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE tenant SET is_active = FALSE WHERE tenant_id = ?`, id)
	if err != nil {
		return fmt.Errorf("deactivate tenant %q: %w", id, err)
	}
	return requireAffected(result, fmt.Sprintf("tenant %q", id))
}

// UpsertApp inserts a new immutable application version.
func (s *MySQLStore) UpsertApp(ctx context.Context, value AgentApp) (version int, err error) {
	if err := value.Validate(); err != nil {
		return 0, fmt.Errorf("validate app: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin app version transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const nextVersionQuery = `SELECT COALESCE(MAX(version), 0) + 1
		FROM agent_app WHERE app_id = ? FOR UPDATE`
	if err = tx.QueryRowContext(ctx, nextVersionQuery, value.ID).Scan(&version); err != nil {
		return 0, fmt.Errorf("allocate app %q version: %w", value.ID, err)
	}
	model, marshalErr := json.Marshal(value.Model)
	if marshalErr != nil {
		return 0, fmt.Errorf("encode app model: %w", marshalErr)
	}
	tools, marshalErr := json.Marshal(value.Tools)
	if marshalErr != nil {
		return 0, fmt.Errorf("encode app tools: %w", marshalErr)
	}
	backends, marshalErr := json.Marshal(value.Backends)
	if marshalErr != nil {
		return 0, fmt.Errorf("encode app backends: %w", marshalErr)
	}
	isCurrent := version == 1
	const insert = `INSERT INTO agent_app
		(app_id, version, tenant_id, app_name, model_cfg_json,
		 tools_allowlist_json, backends_json, is_current)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err = tx.ExecContext(ctx, insert, value.ID, version, value.TenantID, value.AppName,
		model, tools, backends, isCurrent); err != nil {
		return 0, fmt.Errorf("insert app %q version %d: %w", value.ID, version, err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit app %q version %d: %w", value.ID, version, err)
	}
	return version, nil
}

// GetCurrentApp returns the active version for an application.
func (s *MySQLStore) GetCurrentApp(ctx context.Context, id string) (AgentApp, error) {
	const query = `SELECT app_id, tenant_id, app_name, model_cfg_json,
		tools_allowlist_json, backends_json, version, is_current
		FROM agent_app WHERE app_id = ? AND is_current = TRUE`
	var value AgentApp
	var model, tools, backends []byte
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&value.ID, &value.TenantID, &value.AppName, &model,
		&tools, &backends, &value.Version, &value.IsCurrent,
	)
	if err != nil {
		return AgentApp{}, wrapNotFound(fmt.Sprintf("get current app %q", id), err)
	}
	if err := json.Unmarshal(model, &value.Model); err != nil {
		return AgentApp{}, fmt.Errorf("decode app %q model: %w", id, err)
	}
	if err := json.Unmarshal(tools, &value.Tools); err != nil {
		return AgentApp{}, fmt.Errorf("decode app %q tools: %w", id, err)
	}
	if err := json.Unmarshal(backends, &value.Backends); err != nil {
		return AgentApp{}, fmt.Errorf("decode app %q backends: %w", id, err)
	}
	return value, nil
}

// ListApps returns current application versions for one tenant.
func (s *MySQLStore) ListApps(ctx context.Context, tenantID string) ([]AgentApp, error) {
	const query = `SELECT app_id, tenant_id, app_name, model_cfg_json,
		tools_allowlist_json, backends_json, version, is_current
		FROM agent_app WHERE tenant_id = ? AND is_current = TRUE ORDER BY app_id`
	rows, err := s.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list apps for tenant %q: %w", tenantID, err)
	}
	defer rows.Close()

	var result []AgentApp
	for rows.Next() {
		var value AgentApp
		var model, tools, backends []byte
		if err := rows.Scan(&value.ID, &value.TenantID, &value.AppName, &model,
			&tools, &backends, &value.Version, &value.IsCurrent); err != nil {
			return nil, fmt.Errorf("scan app: %w", err)
		}
		if err := json.Unmarshal(model, &value.Model); err != nil {
			return nil, fmt.Errorf("decode app %q model: %w", value.ID, err)
		}
		if err := json.Unmarshal(tools, &value.Tools); err != nil {
			return nil, fmt.Errorf("decode app %q tools: %w", value.ID, err)
		}
		if err := json.Unmarshal(backends, &value.Backends); err != nil {
			return nil, fmt.Errorf("decode app %q backends: %w", value.ID, err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate apps: %w", err)
	}
	return result, nil
}

// BindAppVersion atomically activates one existing application version.
func (s *MySQLStore) BindAppVersion(ctx context.Context, appID string, version int) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin app activation transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `UPDATE agent_app SET is_current = FALSE WHERE app_id = ?`, appID); err != nil {
		return fmt.Errorf("clear current app %q: %w", appID, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE agent_app SET is_current = TRUE WHERE app_id = ? AND version = ?`,
		appID, version,
	)
	if err != nil {
		return fmt.Errorf("activate app %q version %d: %w", appID, version, err)
	}
	if err = requireAffected(result, fmt.Sprintf("app %q version %d", appID, version)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit app %q activation: %w", appID, err)
	}
	return nil
}

// UpsertBinding creates or updates a channel route.
func (s *MySQLStore) UpsertBinding(ctx context.Context, value ChannelBinding) error {
	if err := value.Validate(); err != nil {
		return fmt.Errorf("validate binding: %w", err)
	}
	config, err := json.Marshal(value.Config)
	if err != nil {
		return fmt.Errorf("encode binding config: %w", err)
	}
	const query = `INSERT INTO channel_binding
		(binding_id, tenant_id, app_id, channel, route_key, config_json, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE tenant_id = VALUES(tenant_id), app_id = VALUES(app_id),
		channel = VALUES(channel), route_key = VALUES(route_key),
		config_json = VALUES(config_json), is_active = VALUES(is_active)`
	if _, err := s.db.ExecContext(ctx, query, value.ID, value.TenantID, value.AppID,
		value.Channel, value.RouteKey, config, value.IsActive); err != nil {
		return fmt.Errorf("upsert binding %q: %w", value.ID, err)
	}
	return nil
}

// GetBindingByRoute resolves a platform route without dereferencing secrets.
func (s *MySQLStore) GetBindingByRoute(ctx context.Context, channel, routeKey string) (ChannelBinding, error) {
	const query = `SELECT binding_id, tenant_id, app_id, channel, route_key, config_json, is_active
		FROM channel_binding WHERE channel = ? AND route_key = ?`
	var value ChannelBinding
	var config []byte
	err := s.db.QueryRowContext(ctx, query, channel, routeKey).Scan(
		&value.ID, &value.TenantID, &value.AppID, &value.Channel,
		&value.RouteKey, &config, &value.IsActive,
	)
	if err != nil {
		return ChannelBinding{}, wrapNotFound(
			fmt.Sprintf("get %s binding route %q", channel, routeKey), err,
		)
	}
	if err := json.Unmarshal(config, &value.Config); err != nil {
		return ChannelBinding{}, fmt.Errorf("decode binding %q config: %w", value.ID, err)
	}
	return value, nil
}

// ListBindings returns all bindings for one Agent application.
func (s *MySQLStore) ListBindings(ctx context.Context, appID string) ([]ChannelBinding, error) {
	const query = `SELECT binding_id, tenant_id, app_id, channel, route_key, config_json, is_active
		FROM channel_binding WHERE app_id = ? ORDER BY binding_id`
	rows, err := s.db.QueryContext(ctx, query, appID)
	if err != nil {
		return nil, fmt.Errorf("list bindings for app %q: %w", appID, err)
	}
	defer rows.Close()

	var result []ChannelBinding
	for rows.Next() {
		var value ChannelBinding
		var config []byte
		if err := rows.Scan(&value.ID, &value.TenantID, &value.AppID, &value.Channel,
			&value.RouteKey, &config, &value.IsActive); err != nil {
			return nil, fmt.Errorf("scan binding: %w", err)
		}
		if err := json.Unmarshal(config, &value.Config); err != nil {
			return nil, fmt.Errorf("decode binding %q config: %w", value.ID, err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bindings: %w", err)
	}
	return result, nil
}

func wrapNotFound(operation string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", operation, ErrNotFound)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func requireAffected(result sql.Result, object string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows for %s: %w", object, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", object, ErrNotFound)
	}
	return nil
}
