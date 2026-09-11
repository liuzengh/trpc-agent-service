package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
)

func newTenantServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewTenantAPI(tenant.NewManager()).Register(mux)
	return httptest.NewServer(asClaims(mux))
}

func TestTenantAPICRUD(t *testing.T) {
	srv := newTenantServer(t)
	defer srv.Close()

	// Tenant management is owner-only, so these hit the API as the owner.
	c := clientAs(ownerClaims())

	// create
	resp := postAs(t, c, srv.URL+"/tenants", `{"id":"t1","name":"acme","status":"active"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	// get
	resp = getAs(t, c, srv.URL+"/tenants/t1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got tenant.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if got.Name != "acme" {
		t.Errorf("name = %q, want %q", got.Name, "acme")
	}

	// list
	resp = getAs(t, c, srv.URL+"/tenants")
	var all []tenant.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(all) != 1 {
		t.Errorf("list len = %d, want 1", len(all))
	}

	// update
	resp = doAs(t, c, http.MethodPut, srv.URL+"/tenants/t1", `{"name":"acme-corp","status":"active"}`)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("update status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()

	// delete
	resp = doAs(t, c, http.MethodDelete, srv.URL+"/tenants/t1", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	resp.Body.Close()

	// get after delete -> not found
	resp = getAs(t, c, srv.URL+"/tenants/t1")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("get after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()
}

// TestTenantRoutesAreScopedToTheCallersTenant covers the tenant-level tenant
// isolation: an admin of tenant-a may read its own tenant, but another tenant
// is reported as missing and can neither be mutated nor rolled back.
func TestTenantRoutesAreScopedToTheCallersTenant(t *testing.T) {
	srv := newTenantServer(t)
	defer srv.Close()

	owner := clientAs(ownerClaims())
	for _, id := range []string{"tenant-a", "tenant-b"} {
		resp := postAs(t, owner, srv.URL+"/tenants", `{"id":"`+id+`","name":"`+id+`","status":"active"}`)
		resp.Body.Close()
	}

	admin := clientAs(adminClaims("tenant-a"))

	// Own tenant is readable.
	resp := getAs(t, admin, srv.URL+"/tenants/tenant-a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("own tenant status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// A foreign tenant is not even confirmed to exist.
	resp = getAs(t, admin, srv.URL+"/tenants/tenant-b")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("foreign tenant status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doAs(t, admin, http.MethodPut, srv.URL+"/tenants/tenant-b", `{"name":"hijacked","status":"active"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("foreign tenant update status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doAs(t, admin, http.MethodDelete, srv.URL+"/tenants/tenant-b", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("foreign tenant delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// The directory is narrowed to the caller's tenant.
	resp = getAs(t, admin, srv.URL+"/tenants")
	var list []tenant.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != "tenant-a" {
		t.Errorf("admin tenant list = %+v, want only tenant-a", list)
	}

	// The foreign tenant survived untouched.
	resp = getAs(t, owner, srv.URL+"/tenants/tenant-b")
	var b tenant.Tenant
	_ = json.NewDecoder(resp.Body).Decode(&b)
	resp.Body.Close()
	if b.Name != "tenant-b" {
		t.Errorf("tenant-b name = %q, want untouched", b.Name)
	}
}

// TestTenantConfigVersionsAndRollback covers the tenant-level configuration
// history + rollback endpoints.
func TestTenantConfigVersionsAndRollback(t *testing.T) {
	srv := newTenantServer(t)
	defer srv.Close()

	c := clientAs(ownerClaims())

	resp := postAs(t, c, srv.URL+"/tenants", `{"id":"t1","name":"acme","status":"active"}`)
	resp.Body.Close()

	put := func(body string) {
		t.Helper()
		resp := doAs(t, c, http.MethodPut, srv.URL+"/tenants/t1", body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("put status = %d, want 200 (body %s)", resp.StatusCode, body)
		}
	}
	// Two updates produce versions 1 and 2.
	put(`{"name":"acme","status":"active","quota":{"token_quota":1000}}`)
	put(`{"name":"acme","status":"active","quota":{"token_quota":5000}}`)

	// History lists newest first.
	resp = getAs(t, c, srv.URL+"/tenants/t1/config-versions")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config-versions status = %d", resp.StatusCode)
	}
	var vs []tenant.ConfigVersion
	if err := json.NewDecoder(resp.Body).Decode(&vs); err != nil {
		t.Fatalf("decode versions: %v", err)
	}
	resp.Body.Close()
	if len(vs) != 2 || vs[0].Version != 2 || vs[1].Version != 1 {
		t.Fatalf("versions = %+v, want v2,v1", vs)
	}

	// Rollback to version 1 restores quota 1000 and records a new head v3.
	resp = doAs(t, c, http.MethodPost, srv.URL+"/tenants/t1/config-rollback", `{"version":1}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rollback status = %d", resp.StatusCode)
	}
	var rb tenant.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&rb); err != nil {
		t.Fatalf("decode rollback: %v", err)
	}
	resp.Body.Close()
	if rb.Quota == nil || rb.Quota.TokenQuota != 1000 {
		t.Errorf("rolled-back quota = %+v, want 1000", rb.Quota)
	}

	// Unknown version -> 404.
	resp = doAs(t, c, http.MethodPost, srv.URL+"/tenants/t1/config-rollback", `{"version":99}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("rollback unknown version status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Zero version rejected.
	resp = doAs(t, c, http.MethodPost, srv.URL+"/tenants/t1/config-rollback", `{"version":0}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("rollback zero version status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}
