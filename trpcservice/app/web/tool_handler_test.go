package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
)

func newToolServer(t *testing.T) (*httptest.Server, *tool.Registry) {
	t.Helper()
	reg := tool.NewRegistry()
	_ = reg.Register(context.Background(), tool.Definition{ID: "code-exec", Name: "execute_code", Description: "x", RiskLevel: tool.RiskHigh})
	mux := http.NewServeMux()
	NewToolAPI(reg).Register(mux)
	return httptest.NewServer(asClaims(mux)), reg
}

// TestToolGrantRejectsForeignTenantTool covers the tool-grant boundary: a
// builtin (unscoped) tool is platform-wide, but a tenant tool must not be
// grantable by another tenant's admin.
func TestToolGrantRejectsForeignTenantTool(t *testing.T) {
	reg := tool.NewRegistry()
	ctx := context.Background()
	if err := reg.Register(ctx, tool.Definition{
		ID: "acme-crm", Name: "crm", Scope: tool.ScopeTenant, TenantID: "acme",
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ctx, tool.Definition{ID: "code-exec", Name: "execute_code"}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewToolAPI(reg).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	globex := clientAs(adminClaims("globex"))
	acme := clientAs(adminClaims("acme"))

	// A foreign tenant tool is invisible and un-grantable.
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/tools/acme-crm/grants"},
		{http.MethodPut, "/tools/acme-crm/grants/agent-x"},
		{http.MethodDelete, "/tools/acme-crm/grants/agent-x"},
	} {
		resp := doAs(t, globex, tc.method, srv.URL+tc.path, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", tc.method, tc.path, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// The owning tenant may grant it.
	resp := doAs(t, acme, http.MethodPut, srv.URL+"/tools/acme-crm/grants/agent-x", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("owning tenant grant = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	// A platform-wide builtin stays grantable from any tenant.
	resp = doAs(t, globex, http.MethodPut, srv.URL+"/tools/code-exec/grants/agent-y", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("builtin grant = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestToolGrantRejectsForeignTenantAgent covers the other half of the grant
// boundary: even a legitimate tool must not be attached to another tenant's
// agent. Without the agent source an admin could silently widen the toolset of
// an agent they do not own.
func TestToolGrantRejectsForeignTenantAgent(t *testing.T) {
	ctx := context.Background()
	reg := tool.NewRegistry()
	if err := reg.Register(ctx, tool.Definition{
		ID: "acme-crm", Name: "crm", Scope: tool.ScopeTenant, TenantID: "acme",
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ctx, tool.Definition{ID: "code-exec", Name: "execute_code"}); err != nil {
		t.Fatal(err)
	}

	agents := agent.NewManager(llm.NewRegistry(nil))
	for _, a := range []agent.Agent{
		{ID: "agent-acme", TenantID: "acme", Name: "acme agent"},
		{ID: "agent-globex", TenantID: "globex", Name: "globex agent"},
	} {
		if err := agents.Create(ctx, a); err != nil {
			t.Fatal(err)
		}
	}

	api := NewToolAPI(reg)
	api.SetAgentSource(agents)
	mux := http.NewServeMux()
	api.Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	acme := clientAs(adminClaims("acme"))
	owner := clientAs(ownerClaims())

	// acme may grant its own tool to its own agent.
	resp := doAs(t, acme, http.MethodPut, srv.URL+"/tools/acme-crm/grants/agent-acme", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("same-tenant grant = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	// acme must not touch globex's agent, not even with the platform tool.
	for _, path := range []string{
		"/tools/acme-crm/grants/agent-globex",
		"/tools/code-exec/grants/agent-globex",
	} {
		resp = doAs(t, acme, http.MethodPut, srv.URL+path, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("PUT %s = %d, want 404 (another tenant's agent)", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
	if allowed, _ := reg.IsAllowed(ctx, "agent-globex", "code-exec"); allowed {
		t.Error("cross-tenant grant leaked through")
	}

	// Even the owner cannot make a tenant tool reachable by another tenant.
	resp = doAs(t, owner, http.MethodPut, srv.URL+"/tools/acme-crm/grants/agent-globex", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("owner cross-tenant grant = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
	if allowed, _ := reg.IsAllowed(ctx, "agent-globex", "acme-crm"); allowed {
		t.Error("a tenant tool must stay inside its tenant even for the owner")
	}

	// The grant list only shows agents the caller can see.
	resp = doAs(t, acme, http.MethodGet, srv.URL+"/tools/acme-crm/grants", "")
	var listed []string
	_ = json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	if len(listed) != 1 || listed[0] != "agent-acme" {
		t.Errorf("visible grants = %v, want [agent-acme]", listed)
	}
}

func TestToolGrantsLifecycle(t *testing.T) {
	srv, reg := newToolServer(t)
	defer srv.Close()

	// initially no grants
	resp, err := http.Get(srv.URL + "/tools/code-exec/grants")
	if err != nil {
		t.Fatalf("list grants: %v", err)
	}
	var got []string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode grants: %v", err)
	}
	resp.Body.Close()
	if len(got) != 0 {
		t.Errorf("initial grants = %v, want empty", got)
	}

	// grant an agent
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/tools/code-exec/grants/agent-a", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("grant status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	// grant is idempotent
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/tools/code-exec/grants/agent-a", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("re-grant status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	// visible in the list + effective in RBAC
	resp, _ = http.Get(srv.URL + "/tools/code-exec/grants")
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if len(got) != 1 || got[0] != "agent-a" {
		t.Errorf("grants = %v, want [agent-a]", got)
	}
	allowed, _ := reg.IsAllowed(context.Background(), "agent-a", "code-exec")
	if !allowed {
		t.Error("granted agent should be allowed")
	}

	// unknown tool -> 404
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/tools/nope/grants/agent-a", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("grant to unknown tool status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// revoke
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/tools/code-exec/grants/agent-a", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("revoke status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
	allowed, _ = reg.IsAllowed(context.Background(), "agent-a", "code-exec")
	if allowed {
		t.Error("revoked agent should not be allowed")
	}
}
