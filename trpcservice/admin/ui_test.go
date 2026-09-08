package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestAdminCatalogRBACAndUIShell(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	other := data.Tenants[0]
	other.ID = "private-other-tenant"
	data.Tenants = append(data.Tenants, other)
	repo := controlplane.NewMemoryRepository(data)
	defer func() { _ = repo.Close() }()
	service, _ := New(repo)
	const scopedToken = "scoped-ui-fixture-token-1234567890"
	h, err := NewHandlerWithPrincipals(service, []Principal{{Name: "reader", Token: scopedToken, Role: RoleAuditor, TenantIDs: []string{"tutorial-tenant"}}})
	if err != nil {
		t.Fatal(err)
	}
	call := func(path, body, token, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, bytes.NewBufferString(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("/admin/catalog/list", `{"kind":"tenants"}`, "", ""); w.Code != 401 {
		t.Fatal("anonymous data access")
	}
	w := call("/admin/catalog/list", `{"kind":"tenants"}`, scopedToken, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "private-other-tenant") {
		t.Fatalf("tenant enumeration leak %s", w.Body.String())
	}
	if w := call("/admin/catalog/list", `{"kind":"apps","tenant_id":"private-other-tenant"}`, scopedToken, ""); w.Code != 403 {
		t.Fatal("cross-tenant catalog allowed")
	}
	if w := call("/admin/apps", `{"tenant_id":"tutorial-tenant","app_id":"deny","name":"deny"}`, scopedToken, ""); w.Code != 403 {
		t.Fatal("read-only UI principal wrote")
	}
	if w := call("/admin/me", `{}`, scopedToken, "https://attacker.invalid"); w.Code != 403 {
		t.Fatal("cross-origin admin accepted")
	}
	if w := call("/admin/me", `{}`, scopedToken, ""); strings.Contains(w.Body.String(), scopedToken) {
		t.Fatal("identity endpoint returned token")
	}
	for _, path := range []string{"/admin/ui/", "/admin/ui/app.js", "/admin/ui/style.css"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
			t.Fatalf("UI shell/security %s: %d", path, w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/admin/ui/../../.env", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatal("arbitrary static path exposed")
	}
}

func TestCatalogRedactionRetainsQuotaAndReferences(t *testing.T) {
	value := map[string]any{"api_key": "private-canary", "bot_token": "private-canary", "url": "https://host.invalid/path?apikey=private-canary", "api_key_ref": "env://MODEL_KEY", "max_prompt_tokens": 1000}
	raw, _ := json.Marshal(redactCatalog(value))
	if strings.Contains(string(raw), "private-canary") || !strings.Contains(string(raw), "MODEL_KEY") || !strings.Contains(string(raw), `"max_prompt_tokens":1000`) {
		t.Fatal("catalog redaction incorrect", string(raw))
	}
}

func TestPublishAndCanaryRejectUnavailableSkill(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	revision := data.Revisions[0]
	revision.ID = "unavailable-skill-revision"
	revision.RevisionNo = 2
	revision.AgentConfig = json.RawMessage(`{"name":"agent","instruction":"test","skills":[{"name":"missing","version":"1","checksum":"` + strings.Repeat("a", 64) + `"}]}`)
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_run"]}`)
	data.Revisions = append(data.Revisions, revision)
	repo := controlplane.NewMemoryRepository(data)
	defer func() { _ = repo.Close() }()
	service, _ := New(repo)
	h, _ := NewHandler(service, testAdminToken)
	for path, body := range map[string]string{
		"/admin/revisions/publish": `{"tenant_id":"tutorial-tenant","app_id":"tutorial-app","revision_id":"unavailable-skill-revision","expected_version":1}`,
		"/admin/apps/rollout":      `{"tenant_id":"tutorial-tenant","app_id":"tutorial-app","expected_version":1,"rollout_policy":{"canary_revision_id":"unavailable-skill-revision","canary_percent":10}}`,
	} {
		r := httptest.NewRequest("POST", path, bytes.NewBufferString(body))
		r.Header.Set("Authorization", "Bearer "+testAdminToken)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("unavailable skill publication accepted: %s %d", path, w.Code)
		}
	}
}
