package web

import (
	"bytes"
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
	return httptest.NewServer(mux)
}

func TestTenantAPICRUD(t *testing.T) {
	srv := newTenantServer(t)
	defer srv.Close()

	// create
	resp, err := http.Post(srv.URL+"/tenants", "application/json",
		bytes.NewBufferString(`{"id":"t1","name":"acme","status":"active"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	// get
	resp, err = http.Get(srv.URL + "/tenants/t1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
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
	resp, err = http.Get(srv.URL + "/tenants")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var all []tenant.Tenant
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(all) != 1 {
		t.Errorf("list len = %d, want 1", len(all))
	}

	// update
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/tenants/t1",
		bytes.NewBufferString(`{"name":"acme-corp","status":"active"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("update status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()

	// delete
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/tenants/t1", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	resp.Body.Close()

	// get after delete -> not found
	resp, err = http.Get(srv.URL + "/tenants/t1")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("get after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()
}

// TestTenantConfigVersionsAndRollback covers the tenant-level configuration
// history + rollback endpoints.
func TestTenantConfigVersionsAndRollback(t *testing.T) {
	srv := newTenantServer(t)
	defer srv.Close()

	if resp, err := http.Post(srv.URL+"/tenants", "application/json",
		bytes.NewBufferString(`{"id":"t1","name":"acme","status":"active"}`)); err != nil {
		t.Fatalf("create: %v", err)
	} else {
		resp.Body.Close()
	}

	put := func(body string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/tenants/t1", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("put status = %d, want 200 (body %s)", resp.StatusCode, body)
		}
	}
	// Two updates produce versions 1 and 2.
	put(`{"name":"acme","status":"active","quota":{"token_quota":1000}}`)
	put(`{"name":"acme","status":"active","quota":{"token_quota":5000}}`)

	// History lists newest first.
	resp, err := http.Get(srv.URL + "/tenants/t1/config-versions")
	if err != nil {
		t.Fatalf("config-versions: %v", err)
	}
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
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/tenants/t1/config-rollback",
		bytes.NewBufferString(`{"version":1}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
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
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/tenants/t1/config-rollback",
		bytes.NewBufferString(`{"version":99}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rollback unknown: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("rollback unknown version status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Zero version rejected.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/tenants/t1/config-rollback",
		bytes.NewBufferString(`{"version":0}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rollback zero: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("rollback zero version status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}
