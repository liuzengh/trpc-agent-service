package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
)

func newAgentServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewAgentAPI(agent.NewManager(llm.NewRegistry(nil))).Register(mux)
	return httptest.NewServer(asClaims(mux))
}

func createAgent(t *testing.T, c *http.Client, url, id, tenant, name string) {
	t.Helper()
	body := `{"id":"` + id + `","tenant_id":"` + tenant + `","name":"` + name + `"}`
	resp := postAs(t, c, url+"/agents", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create agent %s status = %d", id, resp.StatusCode)
	}
	resp.Body.Close()
}

// TestAgentRoutesAreTenantScoped is the regression guard for the cross-tenant
// hole this phase closed: before it, any admin could read, rewrite, publish,
// roll back or delete another tenant's agent simply by knowing its id.
func TestAgentRoutesAreTenantScoped(t *testing.T) {
	srv := newAgentServer(t)
	defer srv.Close()

	owner := clientAs(ownerClaims())
	acme := clientAs(adminClaims("acme"))
	globex := clientAs(adminClaims("globex"))

	createAgent(t, owner, srv.URL, "a-acme", "acme", "Acme bot")
	createAgent(t, owner, srv.URL, "a-globex", "globex", "Globex bot")

	// Every route that names the foreign agent reports it as missing.
	for _, tc := range []struct{ method, suffix, body string }{
		{http.MethodGet, "", ""},
		{http.MethodGet, "/profile", ""},
		{http.MethodGet, "/versions", ""},
		{http.MethodPut, "", `{"name":"hijacked"}`},
		{http.MethodDelete, "", ""},
		{http.MethodPost, "/publish", `{"endpoint_id":"e1","model":"m"}`},
		{http.MethodPost, "/rollback", `{"version":1}`},
	} {
		resp := doAs(t, globex, tc.method, srv.URL+"/agents/a-acme"+tc.suffix, tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s /agents/a-acme%s = %d, want 404", tc.method, tc.suffix, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// The victim agent is untouched and still visible to its own tenant.
	resp := getAs(t, acme, srv.URL+"/agents/a-acme")
	var got agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode acme agent: %v", err)
	}
	resp.Body.Close()
	if got.Name != "Acme bot" {
		t.Errorf("agent name = %q, want untouched", got.Name)
	}

	// Lists never leak across tenants, whatever filter is requested.
	resp = getAs(t, globex, srv.URL+"/agents?tenant_id=acme")
	var list []agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode globex list: %v", err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != "a-globex" {
		t.Errorf("globex agent list = %+v, want only its own", list)
	}

	// The owner is platform-wide and sees (and may target) every tenant.
	resp = getAs(t, owner, srv.URL+"/agents")
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode owner list: %v", err)
	}
	resp.Body.Close()
	if len(list) != 2 {
		t.Errorf("owner agent list = %d, want 2", len(list))
	}
	resp = getAs(t, owner, srv.URL+"/agents/a-acme")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("owner get foreign agent = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestAgentCreatePinsTenantToCaller pins the write side: a tenant admin cannot
// plant an agent inside another tenant by writing tenant_id into the body.
func TestAgentCreatePinsTenantToCaller(t *testing.T) {
	srv := newAgentServer(t)
	defer srv.Close()

	acme := clientAs(adminClaims("acme"))
	resp := postAs(t, acme, srv.URL+"/agents", `{"id":"a1","tenant_id":"globex","name":"Sneaky"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	var created agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if created.TenantID != "acme" {
		t.Fatalf("created tenant = %q, want acme (pinned to the caller)", created.TenantID)
	}

	// globex cannot see it.
	resp = getAs(t, clientAs(adminClaims("globex")), srv.URL+"/agents")
	var list []agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(list) != 0 {
		t.Errorf("globex sees %+v, want nothing", list)
	}

	// An update cannot move the agent out of its tenant either.
	resp = doAs(t, acme, http.MethodPut, srv.URL+"/agents/a1", `{"name":"Moved","tenant_id":"globex"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("update status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	resp = getAs(t, acme, srv.URL+"/agents/a1")
	var after agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&after); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if after.TenantID != "acme" {
		t.Errorf("tenant after update = %q, want acme (immutable)", after.TenantID)
	}
}

// TestEndpointRoutesAreTenantScoped covers the endpoint asset: tenant endpoints
// are isolated, global endpoints stay shared, and only the owner may mint one.
func TestEndpointRoutesAreTenantScoped(t *testing.T) {
	mux := http.NewServeMux()
	NewEndpointAPI(llm.NewRegistry(nil)).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	owner := clientAs(ownerClaims())
	acme := clientAs(adminClaims("acme"))
	globex := clientAs(adminClaims("globex"))

	// acme tries to plant an endpoint into globex; it lands in acme.
	resp := postAs(t, acme, srv.URL+"/endpoints",
		`{"id":"ep-acme","tenant_id":"globex","name":"m","provider":"openai","base_url":"http://x","model_name":"m"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("acme create = %d, want 201", resp.StatusCode)
	}
	var ep llm.Endpoint
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if ep.TenantID != "acme" {
		t.Fatalf("endpoint tenant = %q, want acme", ep.TenantID)
	}

	// A tenant admin may not mint a global endpoint.
	resp = postAs(t, acme, srv.URL+"/endpoints",
		`{"id":"ep-global","scope":"global","name":"g","provider":"openai","base_url":"http://x","model_name":"m"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("tenant admin global create = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()

	// The owner may, and everyone can then use it.
	resp = postAs(t, owner, srv.URL+"/endpoints",
		`{"id":"ep-shared","scope":"global","name":"g","provider":"openai","base_url":"http://x","model_name":"m"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("owner global create = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	// globex sees only the shared global endpoint.
	resp = getAs(t, globex, srv.URL+"/endpoints")
	var list []llm.Endpoint
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != "ep-shared" {
		t.Errorf("globex endpoints = %+v, want only the global one", list)
	}

	// A foreign tenant endpoint is missing on read, write and delete.
	for _, tc := range []struct{ method, body string }{
		{http.MethodGet, ""},
		{http.MethodPut, `{"name":"hijacked"}`},
		{http.MethodDelete, ""},
	} {
		resp = doAs(t, globex, tc.method, srv.URL+"/endpoints/ep-acme", tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s foreign endpoint = %d, want 404", tc.method, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// The shared global endpoint is reachable by every tenant.
	resp = getAs(t, globex, srv.URL+"/endpoints/ep-shared")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("globex get global endpoint = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}
