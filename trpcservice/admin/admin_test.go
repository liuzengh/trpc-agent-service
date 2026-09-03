package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

const (
	testKey      = "sk-secret-key-1234567890"
	testWecomKey = "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"
	initialYAML  = `default_tenant: demo
tenants:
  - id: demo
    name: Demo
    model:
      name: deepseek-chat
      api_key: ` + testKey + `
      base_url: https://api.deepseek.com
    channels:
      wecom:
        corp_id: wx5823bf96d3bd56c7
        corp_secret: super-secret
        agent_id: 218
        token: QDG6eK
        encoding_aes_key: ` + testWecomKey + `
  - id: second
    name: Second
    model:
      name: deepseek-chat
      api_key: sk-second-key-abcdefgh
`
)

func setupService(t *testing.T) (*Service, string, *agent.Registry) {
	t.Helper()
	// Defensive: a leftover MODEL_* export must not override test tenants.
	t.Setenv("MODEL_API_KEY", "")
	t.Setenv("MODEL_NAME", "")
	t.Setenv("MODEL_BASE_URL", "")

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(initialYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	reg, err := agent.NewRegistry(cfg)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return NewService(path, cfg, reg), path, reg
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, httptest.NewRequest(method, path, rdr))
	return rw
}

func reload(t *testing.T, path string) *config.Config {
	t.Helper()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config on disk must stay loadable: %v", err)
	}
	return cfg
}

func TestListMasksSecrets(t *testing.T) {
	s, _, _ := setupService(t)
	rw := do(t, s.Handler(), http.MethodGet, "/admin/tenants", "")
	if rw.Code != http.StatusOK {
		t.Fatalf("list = %d: %s", rw.Code, rw.Body.String())
	}
	body := rw.Body.String()
	for _, secret := range []string{testKey, "super-secret", "sk-second-key-abcdefgh", testWecomKey} {
		if strings.Contains(body, secret) {
			t.Fatalf("list leaked secret %q", secret)
		}
	}
	if !strings.Contains(body, "sk-****7890") || !strings.Contains(body, "****") {
		t.Fatalf("list missing masked secrets: %s", body)
	}
	var resp listResponse
	if err := json.Unmarshal(rw.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.DefaultTenant != "demo" || len(resp.Tenants) != 2 || resp.Tenants[0].ID != "demo" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestCreatePersistsAndHotApplies(t *testing.T) {
	s, path, reg := setupService(t)
	rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants",
		`{"id":"t3","name":"T3","model":{"name":"deepseek-chat","api_key":"sk-t3-key-123456"}}`)
	if rw.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rw.Code, rw.Body.String())
	}
	if _, ok := reg.Runner("t3"); !ok {
		t.Fatal("registry must hot-apply the new tenant")
	}
	if _, ok := reload(t, path).Tenants["t3"]; !ok {
		t.Fatal("new tenant must be persisted to disk")
	}

	// Duplicate and validation failures.
	if rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants",
		`{"id":"t3","model":{"name":"m","api_key":"sk-t3-key-123456"}}`); rw.Code != http.StatusConflict {
		t.Fatalf("duplicate create = %d, want 409", rw.Code)
	}
	if rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants",
		`{"id":"t4","model":{"name":"m"}}`); rw.Code != http.StatusBadRequest {
		t.Fatalf("missing api_key = %d, want 400", rw.Code)
	}
	if rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants",
		`{"id":"t5","model":{"name":"m","api_key":"sk-t5-key-123456"},"channels":{"wecom":{"corp_id":"wx1"}}}`); rw.Code != http.StatusBadRequest {
		t.Fatalf("incomplete wecom = %d, want 400: %s", rw.Code, rw.Body.String())
	}
	if _, ok := reg.Runner("t4"); ok {
		t.Fatal("rejected tenant must not reach the registry")
	}
}

func TestUpdateKeepsMaskedSecrets(t *testing.T) {
	s, path, _ := setupService(t)

	// Round-trip the GET response (masked secrets) with edited plain fields.
	rw := do(t, s.Handler(), http.MethodGet, "/admin/tenants/demo", "")
	if rw.Code != http.StatusOK {
		t.Fatalf("get = %d", rw.Code)
	}
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo2","model":{"name":"deepseek-v4-flash","api_key":"sk-****7890"},"channels":{"wecom":{"corp_id":"wxNew","corp_secret":"","agent_id":999,"token":"QDG6eK","encoding_aes_key":"`+testWecomKey+`"}}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rw.Code, rw.Body.String())
	}

	cfg := reload(t, path)
	demo := cfg.Tenants["demo"]
	if demo.Name != "Demo2" || demo.Model.Name != "deepseek-v4-flash" {
		t.Fatalf("plain fields not updated: %+v", demo)
	}
	if demo.Model.APIKey != testKey {
		t.Fatalf("masked api_key must keep the stored secret, got %q", demo.Model.APIKey)
	}
	w := demo.Channels.WeCom
	if w == nil || w.CorpID != "wxNew" || w.AgentID != 999 {
		t.Fatalf("wecom not updated: %+v", w)
	}
	if w.CorpSecret != "super-secret" {
		t.Fatalf("empty corp_secret must keep the stored secret, got %q", w.CorpSecret)
	}

	// Explicit unbind when the wecom block is absent.
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo2","model":{"name":"deepseek-v4-flash","api_key":""}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("unbind update = %d: %s", rw.Code, rw.Body.String())
	}
	if cfg := reload(t, path); cfg.Tenants["demo"].Channels.WeCom != nil {
		t.Fatal("wecom binding must be removed")
	}
	if cfg := reload(t, path); cfg.Tenants["demo"].Model.APIKey != testKey {
		t.Fatal("empty api_key must keep the stored secret")
	}

	// A failed commit (empty model name) leaves everything untouched.
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo", `{"name":"X","model":{"name":"","api_key":""}}`)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("bad update = %d, want 400", rw.Code)
	}
	if cfg := reload(t, path); cfg.Tenants["demo"].Name != "Demo2" {
		t.Fatal("failed commit must not mutate the persisted config")
	}
	if rw := do(t, s.Handler(), http.MethodPut, "/admin/tenants/nope", `{}`); rw.Code != http.StatusNotFound {
		t.Fatalf("update unknown = %d, want 404", rw.Code)
	}
}

func TestDelete(t *testing.T) {
	s, path, reg := setupService(t)
	if rw := do(t, s.Handler(), http.MethodDelete, "/admin/tenants/demo", ""); rw.Code != http.StatusConflict {
		t.Fatalf("delete default = %d, want 409", rw.Code)
	}
	rw := do(t, s.Handler(), http.MethodDelete, "/admin/tenants/second", "")
	if rw.Code != http.StatusNoContent {
		t.Fatalf("delete = %d: %s", rw.Code, rw.Body.String())
	}
	if _, ok := reg.Runner("second"); ok {
		t.Fatal("registry must drop the deleted tenant")
	}
	if _, ok := reload(t, path).Tenants["second"]; ok {
		t.Fatal("deleted tenant must be gone from disk")
	}
	if rw := do(t, s.Handler(), http.MethodDelete, "/admin/tenants/demo", ""); rw.Code != http.StatusConflict {
		t.Fatalf("delete last tenant = %d, want 409", rw.Code)
	}
	if rw := do(t, s.Handler(), http.MethodDelete, "/admin/tenants/second", ""); rw.Code != http.StatusNotFound {
		t.Fatalf("delete unknown = %d, want 404", rw.Code)
	}
}

func TestWeComBindingFollowsHotUpdates(t *testing.T) {
	s, _, _ := setupService(t)
	if _, ok := s.WeComBinding("demo"); !ok {
		t.Fatal("demo should start bound")
	}
	rw := do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"deepseek-chat","api_key":""}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rw.Code, rw.Body.String())
	}
	if _, ok := s.WeComBinding("demo"); ok {
		t.Fatal("binding must disappear after unbind")
	}
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"deepseek-chat","api_key":""},"channels":{"wecom":{"corp_id":"wx1","corp_secret":"sec","agent_id":1,"token":"tk","encoding_aes_key":"`+testWecomKey+`"}}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("rebind = %d: %s", rw.Code, rw.Body.String())
	}
	b, ok := s.WeComBinding("demo")
	if !ok || b.CorpID != "wx1" {
		t.Fatalf("binding after rebind = %+v, %v", b, ok)
	}
}

func TestMaskSecret(t *testing.T) {
	if got := maskSecret(testKey); got != "sk-****7890" {
		t.Fatalf("mask = %q", got)
	}
	if got := maskSecret("short"); got != "****" {
		t.Fatalf("short mask = %q", got)
	}
	if got := maskSecret(""); got != "" {
		t.Fatalf("empty mask = %q", got)
	}
	if !isMasked("sk-****7890") || isMasked(testKey) {
		t.Fatal("isMasked mismatch")
	}
}
