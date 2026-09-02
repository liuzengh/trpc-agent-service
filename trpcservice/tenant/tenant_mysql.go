package tenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"
)

// mysqlStore persists tenants in the tenants table (soft delete via
// is_deleted). Only the fields the platform reads today are mapped; the JSON
// policy columns (model_config/audit_policy/quota) land with their features.
type mysqlStore struct {
	db *sql.DB
}

// NewMySQLManager returns a tenant manager persisting to MySQL.
func NewMySQLManager(db *sql.DB) *Manager {
	return &Manager{store: &mysqlStore{db: db}}
}

func (s *mysqlStore) Create(ctx context.Context, t *Tenant) error {
	backend, err := marshalBackend(t.DataBackend)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenants (tenant_id, name, status, data_backend) VALUES (?, ?, ?, ?)`,
		t.ID, t.Name, t.Status, backend)
	if isDuplicate(err) {
		return fmt.Errorf("tenant: %q already exists", t.ID)
	}
	return err
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*Tenant, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, name, status, data_backend FROM tenants WHERE tenant_id = ? AND is_deleted = 0`, id)
	return scanTenant(row)
}

func (s *mysqlStore) List(ctx context.Context) ([]*Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, name, status, data_backend FROM tenants WHERE is_deleted = 0 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]*Tenant, 0)
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *mysqlStore) Update(ctx context.Context, t *Tenant) error {
	backend, err := marshalBackend(t.DataBackend)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET name = ?, status = ?, data_backend = ? WHERE tenant_id = ? AND is_deleted = 0`,
		t.Name, t.Status, backend, t.ID)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, t.ID)
}

func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET is_deleted = 1 WHERE tenant_id = ? AND is_deleted = 0`, id)
	if err != nil {
		return err
	}
	return rowsAffectedNotFound(res, id)
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanTenant(sc scanner) (*Tenant, error) {
	var (
		t       Tenant
		backend sql.NullString
	)
	if err := sc.Scan(&t.ID, &t.Name, &t.Status, &backend); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if backend.Valid && backend.String != "" {
		if err := json.Unmarshal([]byte(backend.String), &t.DataBackend); err != nil {
			return nil, fmt.Errorf("tenant: decode data_backend: %w", err)
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

func isDuplicate(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}

func rowsAffectedNotFound(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return nil
}
