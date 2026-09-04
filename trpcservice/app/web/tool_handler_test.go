package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
)

func newToolServer(t *testing.T) (*httptest.Server, *tool.Registry) {
	t.Helper()
	reg := tool.NewRegistry()
	_ = reg.Register(context.Background(), tool.Definition{ID: "code-exec", Name: "execute_code", Description: "x", RiskLevel: tool.RiskHigh})
	mux := http.NewServeMux()
	NewToolAPI(reg).Register(mux)
	return httptest.NewServer(mux), reg
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
