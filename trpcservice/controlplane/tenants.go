package controlplane

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

func (r *MemoryRepository) ListTenants(ctx context.Context, after string, limit int) ([]Tenant, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := []string{}
	for id := range r.tenants {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]Tenant, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneTenant(r.tenants[id]))
	}
	return out, nil
}

// TenantPolicyRepository is deliberately separate from routing's read model.
type TenantPolicyRepository interface {
	UpdateTenantPolicies(context.Context, string, int64, []byte, []byte, string, string) error
}

func (r *PostgresRepository) UpdateTenantPolicies(ctx context.Context, id string, version int64, quota, policy []byte, actor, trace string) error {
	var updated bool
	if err := r.dbFor(ctx).QueryRowContext(ctx, "SELECT platform_tenant_policy_update($1,$2,$3::jsonb,$4::jsonb,$5,$6)", id, version, string(quota), string(policy), actor, trace).Scan(&updated); err != nil {
		return err
	}
	if !updated {
		return ErrConflict
	}
	return nil
}
func (r *MemoryRepository) UpdateTenantPolicies(ctx context.Context, id string, version int64, quota, policy []byte, _, _ string) error {
	if err := r.check(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tenants[id]
	if !ok {
		return ErrNotFound
	}
	if t.Version != version {
		return ErrConflict
	}
	t.QuotaConfig = cloneJSON(quota)
	t.AuditPolicy = cloneJSON(policy)
	t.Version++
	t.UpdatedAt = time.Now().UTC()
	r.tenants[id] = t
	return nil
}
func (r *PostgresRepository) ListTenants(ctx context.Context, after string, limit int) ([]Tenant, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := r.dbFor(ctx).QueryContext(ctx, `SELECT tenant_id,audit_policy FROM tenant WHERE tenant_id>$1 ORDER BY tenant_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	result := []Tenant{}
	for rows.Next() {
		var item Tenant
		var policy []byte
		if err := rows.Scan(&item.ID, &policy); err != nil {
			return nil, err
		}
		item.AuditPolicy = json.RawMessage(policy)
		result = append(result, item)
	}
	return result, rows.Err()
}
