package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
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

func setupService(t *testing.T, aud ...*audit.Logger) (*Service, string, *agent.Registry) {
	t.Helper()
	// Defensive: a leftover MODEL_* export must not override test tenants,
	// and STORAGE_SESSION_* must not flip tests onto a redis backend.
	t.Setenv("MODEL_API_KEY", "")
	t.Setenv("MODEL_NAME", "")
	t.Setenv("MODEL_BASE_URL", "")
	t.Setenv("STORAGE_SESSION_BACKEND", "")
	t.Setenv("STORAGE_SESSION_REDIS_URL", "")

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(initialYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	reg, err := agent.NewRegistry(cfg, inmemory.NewSessionService())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	var auditor *audit.Logger
	if len(aud) > 0 {
		auditor = aud[0]
	}
	return NewService(path, cfg, reg, auditor), path, reg
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

// TestWeChatKfAdminLifecycle covers the kf binding through the admin API:
// create with clear secrets, masked echoes keep stored secrets, responses
// never leak them, and an absent block explicitly unbinds.
func TestWeChatKfAdminLifecycle(t *testing.T) {
	s, path, _ := setupService(t)

	create := `{"id":"kf1","name":"KF","model":{"name":"m","api_key":"sk-kf1-key-123456"},"channels":{"wechat_kf":{"corp_id":"wx1","secret":"kf-secret-value","token":"tk","encoding_aes_key":"` + testWecomKey + `"}}}`
	rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants", create)
	if rw.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rw.Code, rw.Body.String())
	}
	if strings.Contains(rw.Body.String(), "kf-secret-value") {
		t.Fatalf("create response leaked secret: %s", rw.Body.String())
	}
	if _, ok := s.WeChatKfBinding("kf1"); !ok {
		t.Fatal("accessor must resolve the new binding")
	}
	if b := reload(t, path).Tenants["kf1"].Channels.WeChatKf; b == nil || b.Secret != "kf-secret-value" {
		t.Fatalf("persisted binding = %+v", b)
	}

	// Incomplete kf secrets on create are rejected.
	if rw := do(t, s.Handler(), http.MethodPost, "/admin/tenants",
		`{"id":"kf2","model":{"name":"m","api_key":"sk-kf2-key-123456"},"channels":{"wechat_kf":{"corp_id":"wx1"}}}`); rw.Code != http.StatusBadRequest {
		t.Fatalf("incomplete kf = %d, want 400: %s", rw.Code, rw.Body.String())
	}

	// PUT with masked echoes: corp_id updates, secrets keep stored values.
	put := `{"name":"KF","model":{"name":"m","api_key":"sk-****3456"},"channels":{"wechat_kf":{"corp_id":"wx2","secret":"****","token":"****","encoding_aes_key":"****"}}}`
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/kf1", put)
	if rw.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rw.Code, rw.Body.String())
	}
	b := reload(t, path).Tenants["kf1"].Channels.WeChatKf
	if b.CorpID != "wx2" || b.Secret != "kf-secret-value" || b.EncodingAESKey != testWecomKey {
		t.Fatalf("binding after masked update = %+v", b)
	}

	// PUT without the wechat_kf block explicitly unbinds.
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/kf1",
		`{"name":"KF","model":{"name":"m","api_key":""},"channels":{}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("unbind = %d: %s", rw.Code, rw.Body.String())
	}
	if reload(t, path).Tenants["kf1"].Channels.WeChatKf != nil {
		t.Fatal("kf binding must be removed")
	}
	if _, ok := s.WeChatKfBinding("kf1"); ok {
		t.Fatal("accessor must follow the unbind")
	}
}

// TestGuardrailsAndAuditTrail covers the governance slice of the Admin API:
// policies round-trip through the DTO, hot-resolve via Guardrails, and every
// mutation lands on the audit trail.
func TestGuardrailsAndAuditTrail(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	aud, err := audit.New(auditPath)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	s, path, _ := setupService(t, aud)

	// PUT installs a policy; the masked api_key echo keeps the stored key.
	rw := do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"m","api_key":"sk-****7890"},`+
			`"guardrails":{"max_input_bytes":100,"blocked_keywords":["bad","secret word"]}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rw.Code, rw.Body.String())
	}
	got := s.Guardrails("demo")
	if got.MaxInputBytes != 100 || len(got.BlockedKeywords) != 2 || got.BlockedKeywords[1] != "secret word" {
		t.Fatalf("policy after update = %+v", got)
	}
	if !strings.Contains(readFile(t, path), "blocked_keywords") {
		t.Fatal("policy must persist to the config file")
	}
	if reload(t, path).Tenants["demo"].Model.APIKey != testKey {
		t.Fatal("masked api_key echo must keep the stored key")
	}

	// The GET response echoes the policy back.
	rw = do(t, s.Handler(), http.MethodGet, "/admin/tenants/demo", "")
	if !strings.Contains(rw.Body.String(), `"max_input_bytes":100`) {
		t.Fatalf("policy missing from GET: %s", rw.Body.String())
	}

	// PUT without the guardrails block clears it (channels semantics).
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"m","api_key":""}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("clear = %d: %s", rw.Code, rw.Body.String())
	}
	if cleared := s.Guardrails("demo"); cleared.MaxInputBytes != 0 || len(cleared.BlockedKeywords) != 0 {
		t.Fatalf("policy must be cleared, got %+v", cleared)
	}

	// Invalid policies are rejected at validation and leave config intact.
	rw = do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"m","api_key":""},"guardrails":{"max_input_bytes":-1}}`)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("negative limit must be rejected, got %d", rw.Code)
	}

	aud.Close()
	trail := readFile(t, auditPath)
	if lines := strings.Count(trail, `"event":"admin"`); lines != 3 { // 2 ok + 1 rejected
		t.Fatalf("audit trail has %d admin records, want 3", lines)
	}
	if !strings.Contains(trail, `"error_type":"commit"`) {
		t.Fatal("failed commit must be audited with an error type")
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestAdminAuditCarriesTraceID proves the admin.request span reaches the
// trail, so a mutation correlates with the spans and log lines it produced.
// Declared last on purpose: the otel global provider delegates permanently
// once a real one is installed.
func TestAdminAuditCarriesTraceID(t *testing.T) {
	otel.SetTracerProvider(tracesdk.NewTracerProvider())

	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	aud, err := audit.New(auditPath)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	s, _, _ := setupService(t, aud)

	rw := do(t, s.Handler(), http.MethodPut, "/admin/tenants/demo",
		`{"name":"Demo","model":{"name":"m","api_key":""},"guardrails":{"max_input_bytes":32}}`)
	if rw.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rw.Code, rw.Body.String())
	}
	aud.Close()

	trail := readFile(t, auditPath)
	m := regexp.MustCompile(`"trace_id":"([0-9a-f]{32})"`).FindStringSubmatch(trail)
	if m == nil {
		t.Fatalf("admin record must carry a 32 hex char trace id:\n%s", trail)
	}
	if !strings.Contains(trail, `"detail":"update"`) {
		t.Fatalf("update op missing from trail:\n%s", trail)
	}
}
