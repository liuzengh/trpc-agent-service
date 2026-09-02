package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
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
