package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotFound is returned when a scoped lookup matches no row. It is distinct
// from a database error on purpose: a missing tenant and a failed query need
// different answers from the caller (404 versus 500).
var ErrNotFound = errors.New("controlplane: not found")

// Tenant is one row of the tenants table.
type Tenant struct {
	ID     string
	Name   string
	Status string
}

// CreateTenant inserts a tenant. The caller is by definition outside every
// tenant's Scope — creating the first row for a tenant cannot itself be
// tenant-scoped — so this is one of the package's two deliberate exceptions
// and it takes the raw *sql.DB, not a Scope.
func (d *DB) CreateTenant(ctx context.Context, id, name string) error {
	if id == "" {
		return ErrNoScope
	}
	if _, err := d.db.ExecContext(ctx,
		"INSERT INTO tenants (tenant_id, name) VALUES (?, ?)", id, name); err != nil {
		return fmt.Errorf("controlplane: create tenant %q: %w", id, err)
	}
	return nil
}

// GetTenant reads a tenant by id. Still an unscoped exception: the lookup is
// for the tenant row itself, not for anything inside it.
func (d *DB) GetTenant(ctx context.Context, id string) (Tenant, error) {
	var t Tenant
	err := d.db.QueryRowContext(ctx,
		"SELECT tenant_id, name, status FROM tenants WHERE tenant_id = ?", id).
		Scan(&t.ID, &t.Name, &t.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("controlplane: get tenant: %w", err)
	}
	return t, nil
}

// ListActiveTenants returns every active tenant id.
//
// This is the third deliberate unscoped exception, after CreateTenant and
// CreatePrincipalWithMembership (see principal.go): the roles that serve
// *all* tenants — worker, delivery, jobs — have no tenant scope yet when
// they decide what work exists, so the enumeration cannot be tenant-scoped
// by construction. Everything those roles do afterwards goes through a
// Scope built from one of these ids.
func (d *DB) ListActiveTenants(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx,
		"SELECT tenant_id FROM tenants WHERE status = 'active' ORDER BY tenant_id")
	if err != nil {
		return nil, fmt.Errorf("controlplane: list active tenants: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("controlplane: scan tenant id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetTenantStatus activates or suspends a tenant. Suspending does not delete
// anything: a suspended tenant's rows stay put and its credentials stop
// resolving (auth.Resolver's PrincipalForToken joins on this status), which
// is what lets an operator reverse the action without a restore.
func (d *DB) SetTenantStatus(ctx context.Context, id, status string) error {
	switch status {
	case "active", "suspended":
	default:
		return fmt.Errorf("controlplane: unknown tenant status %q", status)
	}
	res, err := d.db.ExecContext(ctx, "UPDATE tenants SET status = ? WHERE tenant_id = ?", status, id)
	if err != nil {
		return fmt.Errorf("controlplane: set tenant status: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// Get reads the caller's own tenant row through its Scope, which is the only
// way a request-driven code path should reach this table.
func (s Scope) Get(ctx context.Context) (Tenant, error) {
	row, err := s.QueryRow(ctx,
		"SELECT tenant_id, name, status FROM tenants WHERE tenant_id = ?", s.tenantID)
	if err != nil {
		return Tenant{}, err
	}
	var t Tenant
	switch err := row.Scan(&t.ID, &t.Name, &t.Status); {
	case errors.Is(err, sql.ErrNoRows):
		return Tenant{}, ErrNotFound
	case err != nil:
		return Tenant{}, fmt.Errorf("controlplane: get scoped tenant: %w", err)
	}
	return t, nil
}
