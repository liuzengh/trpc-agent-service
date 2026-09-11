package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

// CatalogQuery is passed by the authenticated Admin layer, not a public SQL API.
type CatalogQuery struct {
	Kind         string
	TenantIDs    []string
	AllTenants   bool
	AppID, After string
	Limit        int
}
type CatalogPage struct {
	Items []json.RawMessage `json:"items"`
	Next  string            `json:"next,omitempty"`
}

// CatalogRepository is an optional Admin-only listing capability.
type CatalogRepository interface {
	ListCatalog(context.Context, CatalogQuery) (CatalogPage, error)
}

func catalogTable(kind string) (string, string) {
	switch kind {
	case "tenants":
		return "tenant", "tenant_id"
	case "apps":
		return "agent_app", "app_id"
	case "revisions":
		return "agent_revision", "revision_id"
	case "channels":
		return "channel_binding", "channel_binding_id"
	case "backends":
		return "backend_binding", "binding_id"
	}
	return "", ""
}
func validateCatalog(q CatalogQuery) error {
	table, _ := catalogTable(q.Kind)
	if table == "" || q.Limit < 1 || q.Limit > 100 || len(q.After) > 256 || len(q.AppID) > 128 || len(q.TenantIDs) > 128 {
		return errors.New("invalid catalog query")
	}
	if !q.AllTenants && len(q.TenantIDs) == 0 {
		return errors.New("catalog requires tenant scope")
	}
	if q.Kind == "tenants" && q.AppID != "" {
		return errors.New("tenant list cannot filter apps")
	}
	return nil
}
func tenantIncluded(q CatalogQuery, id string) bool {
	if q.AllTenants {
		return true
	}
	for _, v := range q.TenantIDs {
		if id == v {
			return true
		}
	}
	return false
}

func (r *MemoryRepository) ListCatalog(ctx context.Context, q CatalogQuery) (CatalogPage, error) {
	p := CatalogPage{Items: []json.RawMessage{}}
	if err := validateCatalog(q); err != nil {
		return p, err
	}
	if err := r.check(ctx); err != nil {
		return p, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	type item struct {
		id  string
		raw json.RawMessage
	}
	items := []item{}
	add := func(tenant, app, id string, v any) {
		cursor := tenant + "/" + id
		if !tenantIncluded(q, tenant) || (q.AppID != "" && app != q.AppID) || cursor <= q.After {
			return
		}
		raw, _ := json.Marshal(v)
		items = append(items, item{cursor, raw})
	}
	switch q.Kind {
	case "tenants":
		for _, v := range r.tenants {
			add(v.ID, "", v.ID, v)
		}
	case "apps":
		for _, v := range r.apps {
			add(v.TenantID, v.ID, v.ID, v)
		}
	case "revisions":
		for _, v := range r.revisions {
			add(v.TenantID, v.AppID, v.ID, v)
		}
	case "channels":
		for _, v := range r.channelIDs {
			add(v.TenantID, v.AppID, v.ID, v)
		}
	case "backends":
		for _, v := range r.backends {
			add(v.TenantID, v.AppID, v.ID, v)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].id < items[j].id })
	for i, v := range items {
		if i == q.Limit {
			p.Next = items[i-1].id
			break
		}
		p.Items = append(p.Items, v.raw)
	}
	return p, nil
}

func (r *PostgresRepository) ListCatalog(ctx context.Context, q CatalogQuery) (CatalogPage, error) {
	p := CatalogPage{Items: []json.RawMessage{}}
	if err := validateCatalog(q); err != nil {
		return p, err
	}
	table, id := catalogTable(q.Kind)
	// Names come exclusively from catalogTable; all caller values are parameters.
	appPredicate := "TRUE"
	if q.Kind != "tenants" {
		appPredicate = "($4 = '' OR app_id = $4)"
	} else if q.AppID != "" {
		return p, errors.New("tenant list cannot filter apps")
	}
	query := "SELECT tenant_id || '/' || " + id + ",to_jsonb(t) FROM " + table + " t WHERE ($1 OR tenant_id = ANY($2)) AND tenant_id || '/' || " + id + " > $3 AND " + appPredicate + " ORDER BY tenant_id || '/' || " + id + " LIMIT $5"
	// Keep parameter 4 typed for the tenant query as well.
	if q.Kind == "tenants" {
		query = strings.Replace(query, "AND TRUE", "AND ($4::text = '')", 1)
	}
	rows, err := r.dbFor(ctx).QueryContext(ctx, query, q.AllTenants, q.TenantIDs, q.After, q.AppID, q.Limit+1)
	if err != nil {
		return p, err
	}
	defer func() { _ = rows.Close() }()
	last := ""
	for rows.Next() {
		var cursor string
		var raw []byte
		if err := rows.Scan(&cursor, &raw); err != nil {
			return p, err
		}
		if len(p.Items) == q.Limit {
			p.Next = last
			break
		}
		p.Items = append(p.Items, json.RawMessage(raw))
		last = cursor
	}
	return p, rows.Err()
}
