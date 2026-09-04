// Package tenantstore persists tenants in MySQL. It implements
// domain/tenant.Store; shared SQL helpers come from sqlutil.
package tenantstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// mysqlStore persists tenants in the tenants table (soft delete via
// is_deleted). The JSON policy columns (data_backend/quota/audit_policy) are
// marshaled/unmarshaled around the row.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a tenant manager persisting to MySQL.
func NewMySQLManager(db *sql.DB) *tenant.Manager {
	return tenant.NewManagerWithStore(&mysqlStore{db: db})
}

func (s *mysqlStore) Create(ctx context.Context, t *tenant.Tenant) error {
	backend, err := marshalBackend(t.DataBackend)
	if err != nil {
		return err
	}
	quota, err := marshalQuota(t.Quota)
	if err != nil {
		return err
	}
	policy, err := marshalAuditPolicy(t.AuditPolicy)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenants (tenant_id, name, status, data_backend, quota, audit_policy) VALUES (?, ?, ?, ?, ?, ?)`,
		t.ID, t.Name, t.Status, backend, quota, policy)
	if sqlutil.IsDuplicate(err) {
		return fmt.Errorf("tenant: %q already exists", t.ID)
	}
	return err
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*tenant.Tenant, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, name, status, data_backend, quota, audit_policy FROM tenants WHERE tenant_id = ? AND is_deleted = 0`, id)
	return scanTenant(row)
}

func (s *mysqlStore) List(ctx context.Context) ([]*tenant.Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, name, status, data_backend, quota, audit_policy FROM tenants WHERE is_deleted = 0 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*tenant.Tenant, 0)
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Update(ctx context.Context, t *tenant.Tenant) error {
	backend, err := marshalBackend(t.DataBackend)
	if err != nil {
		return err
	}
	quota, err := marshalQuota(t.Quota)
	if err != nil {
		return err
	}
	policy, err := marshalAuditPolicy(t.AuditPolicy)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET name = ?, status = ?, data_backend = ?, quota = ?, audit_policy = ? WHERE tenant_id = ? AND is_deleted = 0`,
		t.Name, t.Status, backend, quota, policy, t.ID)
	if err != nil {
		return err
	}
	return sqlutil.RowsAffected(res, tenant.ErrNotFound, t.ID)
}

// Delete soft-deletes the tenant and, in one transaction, cascades the
// deletion to its owned configuration rows so a deleted tenant leaves no
// visible orphans in the management domain. Cascade policy:
//   - soft delete (is_deleted=1): agents / model_endpoints(scope=tenant) /
//     tools(scope=tenant) / skills(scope=tenant) / knowledge_bases
//   - physical delete: channel_bindings (its uk_channel_account unique key has
//     no is_deleted component, so a soft delete would block re-binding the
//     same account) and chat_sessions + their chat_messages (no soft column)
//   - kept (audit/metering/history must survive the tenant): audit_logs,
//     usage_records, tenant_config_versions, outbox/idempotency, secrets
func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Root tenant row.
	res, err := tx.ExecContext(ctx,
		`UPDATE tenants SET is_deleted = 1 WHERE tenant_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	if err := sqlutil.RowsAffected(res, tenant.ErrNotFound, id); err != nil {
		return err
	}

	// Tenant-scoped children, soft delete where the column exists.
	soft := []struct {
		table string
		where string
	}{
		{"agents", "tenant_id = ?"},
		{"model_endpoints", "scope = 'tenant' AND tenant_id = ?"},
		{"tools", "scope = 'tenant' AND tenant_id = ?"},
		{"skills", "scope = 'tenant' AND owner_tenant_id = ?"},
		{"knowledge_bases", "tenant_id = ?"},
	}
	for _, c := range soft {
		if _, err := tx.ExecContext(ctx,
			`UPDATE `+c.table+` SET is_deleted = 1 WHERE `+c.where, id); err != nil {
			return fmt.Errorf("tenant cascade %s: %w", c.table, err)
		}
	}

	// Channel bindings are deleted physically, not soft-deleted: their
	// uk_channel_account unique key has no is_deleted component, so a soft
	// delete would block re-binding the same account to another tenant. This
	// matches bindingstore.Delete's physical-delete semantics.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM channel_bindings WHERE tenant_id = ?`, id); err != nil {
		return fmt.Errorf("tenant cascade channel_bindings: %w", err)
	}

	// Business conversation ledger has no soft column: physical delete of the
	// session rows and their messages.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM chat_messages WHERE session_id IN (SELECT session_id FROM chat_sessions WHERE tenant_id = ?)`, id); err != nil {
		return fmt.Errorf("tenant cascade chat_messages: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM chat_sessions WHERE tenant_id = ?`, id); err != nil {
		return fmt.Errorf("tenant cascade chat_sessions: %w", err)
	}

	return tx.Commit()
}

// SnapshotConfig records t in tenant_config_versions with the next per-tenant
// version. The version is assigned MAX+1 and retried on the unlikely
// concurrent duplicate, keeping snapshots atomic under concurrent updates.
func (s *mysqlStore) SnapshotConfig(ctx context.Context, t *tenant.Tenant) (int, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return 0, fmt.Errorf("tenant: encode config version: %w", err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		var v int
		if err := s.db.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(version), 0) + 1 FROM tenant_config_versions WHERE tenant_id = ?`, t.ID).Scan(&v); err != nil {
			return 0, fmt.Errorf("tenant: next config version: %w", err)
		}
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO tenant_config_versions (tenant_id, version, config_json) VALUES (?, ?, ?)`,
			t.ID, v, string(b))
		if err == nil {
			return v, nil
		}
		if !sqlutil.IsDuplicate(err) {
			return 0, err
		}
		// concurrent snapshot grabbed the same version; retry with a new MAX.
	}
	return 0, fmt.Errorf("tenant: config version contention for %q", t.ID)
}

func (s *mysqlStore) ListConfigVersions(ctx context.Context, tenantID string) ([]*tenant.ConfigVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT version, config_json, created_at FROM tenant_config_versions WHERE tenant_id = ? ORDER BY version DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*tenant.ConfigVersion, 0)
	for rows.Next() {
		cv, err := scanConfigVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cv)
	}
	return out, rows.Err()
}

func (s *mysqlStore) GetConfigVersion(ctx context.Context, tenantID string, version int) (*tenant.ConfigVersion, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, config_json, created_at FROM tenant_config_versions WHERE tenant_id = ? AND version = ?`, tenantID, version)
	cv, err := scanConfigVersion(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, tenant.ErrConfigVersionNotFound
		}
		return nil, err
	}
	return cv, nil
}

func scanConfigVersion(sc sqlutil.RowScanner) (*tenant.ConfigVersion, error) {
	var (
		cv      tenant.ConfigVersion
		raw     []byte
		created time.Time
	)
	if err := sc.Scan(&cv.Version, &raw, &created); err != nil {
		return nil, err
	}
	cv.CreatedAt = created
	var cfg tenant.Tenant
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("tenant: decode config version %d: %w", cv.Version, err)
	}
	cv.Config = &cfg
	return &cv, nil
}

func scanTenant(sc sqlutil.RowScanner) (*tenant.Tenant, error) {
	var (
		t       tenant.Tenant
		backend sql.NullString
		quota   sql.NullString
		policy  sql.NullString
	)
	if err := sc.Scan(&t.ID, &t.Name, &t.Status, &backend, &quota, &policy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, tenant.ErrNotFound
		}
		return nil, err
	}
	if backend.Valid && backend.String != "" {
		if err := json.Unmarshal([]byte(backend.String), &t.DataBackend); err != nil {
			return nil, fmt.Errorf("tenant: decode data_backend: %w", err)
		}
	}
	if quota.Valid && quota.String != "" {
		if err := json.Unmarshal([]byte(quota.String), &t.Quota); err != nil {
			return nil, fmt.Errorf("tenant: decode quota: %w", err)
		}
	}
	if policy.Valid && policy.String != "" {
		if err := json.Unmarshal([]byte(policy.String), &t.AuditPolicy); err != nil {
			return nil, fmt.Errorf("tenant: decode audit_policy: %w", err)
		}
	}
	return &t, nil
}

func marshalBackend(m map[string]string) (any, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("tenant: encode data_backend: %w", err)
	}
	return string(b), nil
}

func marshalQuota(q *tenant.Quota) (any, error) {
	if q == nil {
		return nil, nil
	}
	b, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("tenant: encode quota: %w", err)
	}
	return string(b), nil
}

func marshalAuditPolicy(p *tenant.AuditPolicy) (any, error) {
	if p == nil {
		return nil, nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("tenant: encode audit_policy: %w", err)
	}
	return string(b), nil
}
