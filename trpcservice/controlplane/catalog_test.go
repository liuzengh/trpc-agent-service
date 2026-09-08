package controlplane

import (
	"context"
	"encoding/json"
	"testing"
)

func TestCatalogPagingAndTenantScope(t *testing.T) {
	data := DefaultBootstrapData()
	other := data.Tenants[0]
	other.ID = "other"
	data.Tenants = append(data.Tenants, other)
	r := NewMemoryRepository(data)
	defer func() { _ = r.Close() }()
	ctx := context.Background()
	page, err := r.ListCatalog(ctx, CatalogQuery{Kind: "tenants", AllTenants: true, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Next == "" {
		t.Fatal("missing first page", err)
	}
	next, err := r.ListCatalog(ctx, CatalogQuery{Kind: "tenants", AllTenants: true, Limit: 1, After: page.Next})
	if err != nil || len(next.Items) != 1 || next.Next != "" {
		t.Fatal("invalid second page", err)
	}
	for _, kind := range []string{"tenants", "apps", "revisions", "channels", "backends"} {
		p, err := r.ListCatalog(ctx, CatalogQuery{Kind: kind, TenantIDs: []string{"tutorial-tenant"}, Limit: 100})
		if err != nil || len(p.Items) == 0 {
			t.Fatalf("catalog %s: %v", kind, err)
		}
		for _, raw := range p.Items {
			var row map[string]any
			_ = json.Unmarshal(raw, &row)
			if row["tenant_id"] != "tutorial-tenant" {
				t.Fatal("cross tenant row")
			}
		}
	}
	if _, err := r.ListCatalog(ctx, CatalogQuery{Kind: "tenants", Limit: 1}); err == nil {
		t.Fatal("unscoped list allowed")
	}
}
