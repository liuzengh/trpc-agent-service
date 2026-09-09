package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// adminTestAPI builds the API over the compose PG; skips when unreachable.
// Rows created by the test are cleaned up.
func adminTestAPI(t *testing.T) (*http.ServeMux, *pgxpool.Pool) {
	t.Helper()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })

	mux := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, adminTestToken).RegisterRoutes(mux)
	return mux, pool
}

func doJSON(t *testing.T, mux *http.ServeMux, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Authorization", "Bearer "+adminTestToken)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: decode response: %v (body %q)", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

// TestAdminModelAllowlist enforces the platform model-endpoint allowlist on
// every config write path. The API is built with a nil pool: the
// allowlist check precedes any query, so a rejected request proves the gate
// without needing PG.
func TestAdminModelAllowlist(t *testing.T) {
	mux := http.NewServeMux()
	web.NewAdminAPI(nil, nil, nil, adminTestToken).RegisterRoutes(mux)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"createTenant", "POST", "/admin/tenants",
			`{"name":"t","model_config":{"base_url":"http://attacker.example.com"}}`},
		{"updateTenant", "PATCH", "/admin/tenants/00000000-0000-0000-0000-000000000001",
			`{"model_config":{"base_url":"https://attacker.example.com"}}`},
		{"createApp", "POST", "/admin/tenants/00000000-0000-0000-0000-000000000001/apps",
			`{"name":"a","agent_type":"llmagent","config":{"model":{"base_url":"https://attacker.example.com"}}}`},
		{"updateApp", "PATCH", "/admin/tenants/00000000-0000-0000-0000-000000000001/apps/00000000-0000-0000-0000-000000000002",
			`{"config":{"model":{"base_url":"http://attacker.example.com"}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := doJSON(t, mux, tc.method, tc.path, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %v)", code, out)
			}
			if msg, _ := out["error"].(string); !strings.Contains(msg, "attacker.example.com") {
				t.Fatalf("error %q must name the offending host", msg)
			}
		})
	}
}

// adminTestToken is the bearer token the test APIs are constructed with;
// doJSON/doJSONList present it on every request.
const adminTestToken = "test-admin-token"

func doJSONList(t *testing.T, mux *http.ServeMux, path string) []map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+adminTestToken)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d (%s)", path, rec.Code, rec.Body.String())
	}
	var out []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("GET %s: decode: %v", path, err)
	}
	return out
}

func TestAdminLifecycle(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	var tenantID, appV1, appV2, bindingID string
	t.Cleanup(func() {
		if tenantID != "" {
			_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE app_id IN
				(SELECT id FROM agent_app WHERE tenant_id = $1)`, tenantID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
			_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
		}
	})

	// Create tenant.
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants",
		`{"name":"admin-test-tenant","tool_policy":{"rate_limit":100}}`)
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d %v", code, out)
	}
	tenantID, _ = out["id"].(string)
	if tenantID == "" {
		t.Fatal("no tenant id returned")
	}

	// Detail carries the jsonb config.
	code, out = doJSON(t, mux, http.MethodGet, "/admin/tenants/"+tenantID, "")
	if code != http.StatusOK {
		t.Fatalf("get tenant: %d", code)
	}
	if pol, ok := out["tool_policy"].(map[string]any); !ok || pol["rate_limit"] != float64(100) {
		t.Fatalf("tool_policy round trip failed: %v", out["tool_policy"])
	}

	// storage_config is migration-flow only: direct change rejected.
	code, _ = doJSON(t, mux, http.MethodPatch, "/admin/tenants/"+tenantID,
		`{"storage_config":{"session":{"type":"pg"}}}`)
	if code != http.StatusConflict {
		t.Fatalf("storage_config patch must be 409, got %d", code)
	}

	// Create two versions of one app; both start as draft.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v1"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app v1: %d %v", code, out)
	}
	appV1, _ = out["id"].(string)
	if out["version"] != float64(1) || out["status"] != "draft" {
		t.Fatalf("want draft v1, got %v", out)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v2"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app v2: %d %v", code, out)
	}
	appV2, _ = out["id"].(string)
	if out["version"] != float64(2) {
		t.Fatalf("want v2, got %v", out)
	}

	// Draft config is editable; published versions are not.
	code, _ = doJSON(t, mux, http.MethodPatch,
		fmt.Sprintf("/admin/tenants/%s/apps/%s", tenantID, appV2), `{"config":{"prompt":"v2.1"}}`)
	if code != http.StatusOK {
		t.Fatalf("edit draft: %d", code)
	}

	// Publish v2, then v1: the published flag switches atomically.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("publish v2: %d %v", code, out)
	}
	code, _ = doJSON(t, mux, http.MethodPatch,
		fmt.Sprintf("/admin/tenants/%s/apps/%s", tenantID, appV2), `{"config":{"prompt":"x"}}`)
	if code != http.StatusConflict {
		t.Fatalf("published app must be immutable, got %d", code)
	}
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("publish v1 (switch): %d", code)
	}
	var published int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_app WHERE tenant_id=$1 AND name='bot' AND status='published'`,
		tenantID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Fatalf("exactly one published version expected, got %d", published)
	}

	// Rollback with no body: from v1 there is no earlier version, so publish
	// v2 again and roll back to v1.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/rollback", "")
	if code != http.StatusConflict {
		t.Fatalf("rollback from v1 must have no earlier version, got %d", code)
	}
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("re-publish v2: %d", code)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/rollback", "")
	if code != http.StatusOK || out["version"] != float64(1) {
		t.Fatalf("rollback to v1: %d %v", code, out)
	}

	// Bindings: create, list, delete. The supplied path is the channel's
	// env-global mount — the only caller-supplied webhook_path the platform
	// serves; the wecom and mock mounts are already taken.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"wxkf","webhook_path":"/wxkf/callback"}`)
	if code != http.StatusCreated {
		t.Fatalf("create binding: %d %v", code, out)
	}
	bindingID, _ = out["id"].(string)
	// The env-configured mount verifies under the env-global credentials, so a
	// binding naming its own refs there would store credentials inbound
	// never consults.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"wecom","webhook_path":"/wecom/callback","token_ref":"wecom-token"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("own refs on a legacy path must be rejected, got %d", code)
	}
	// Empty webhook_path: the canonical binding-scoped callback path is
	// derived from the DB-generated id in the same statement, so the route
	// the dispatcher resolves is self-consistent.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"wecom","token_ref":"wecom-token"}`)
	if code != http.StatusCreated {
		t.Fatalf("create auto-path binding: %d %v", code, out)
	}
	autoID, _ := out["id"].(string)
	if got, _ := out["webhook_path"].(string); got != "/callback/wecom/"+autoID {
		t.Fatalf("auto webhook_path = %q, want /callback/wecom/%s", got, autoID)
	}
	// Duplicate webhook_path is rejected by the unique constraint.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"wxkf","webhook_path":"/wxkf/callback"}`)
	if code != http.StatusConflict {
		t.Fatalf("duplicate webhook_path must be 409, got %d", code)
	}
	list := doJSONList(t, mux, "/admin/apps/"+appV1+"/bindings")
	if len(list) != 2 || list[0]["webhook_path"] != "/wxkf/callback" {
		t.Fatalf("list bindings: %+v", list)
	}
	if list[0]["token_ref"] != nil {
		t.Fatalf("the legacy-path row must carry no refs, got %v", list[0]["token_ref"])
	}
	if list[1]["token_ref"] != "wecom-token" {
		t.Fatalf("token_ref must be a reference, got %v", list[1]["token_ref"])
	}

	// Publishing must repoint bindings at the new version in the same tx —
	// otherwise callbacks keep routing to the version that just left
	// "published" (which is now disabled) and the gateway rejects everything.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("re-publish v2: %d", code)
	}
	var boundApp string
	if err := pool.QueryRow(ctx,
		`SELECT app_id FROM channel_binding WHERE id = $1`, bindingID).Scan(&boundApp); err != nil {
		t.Fatal(err)
	}
	if boundApp != appV2 {
		t.Fatalf("binding must follow the published version: want %s, got %s", appV2, boundApp)
	}
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/rollback", "")
	if code != http.StatusOK {
		t.Fatalf("rollback to v1: %d", code)
	}
	if err := pool.QueryRow(ctx,
		`SELECT app_id FROM channel_binding WHERE id = $1`, bindingID).Scan(&boundApp); err != nil {
		t.Fatal(err)
	}
	if boundApp != appV1 {
		t.Fatalf("rollback must repoint bindings too: want %s, got %s", appV1, boundApp)
	}
	code, _ = doJSON(t, mux, http.MethodDelete, "/admin/apps/"+appV1+"/bindings/"+bindingID, "")
	if code != http.StatusOK {
		t.Fatalf("delete binding: %d", code)
	}
}

func TestAdminKnowledgeIngestion(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })

	// Fixture app to ingest into.
	appID := "00000000-0000-0000-0000-0000000001aa"
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ('00000000-0000-0000-0000-0000000000aa', 'kb-test', 'active') ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, '00000000-0000-0000-0000-0000000000aa', 'kb-test', 'llm', '{}', 1, 'published') ON CONFLICT DO NOTHING`, appID); err != nil {
		t.Fatal(err)
	}

	// Disabled knowledge → 503.
	mux := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, adminTestToken).RegisterRoutes(mux)
	code, _ := doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/knowledge/documents",
		`{"name":"x","content":"y"}`)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("knowledge disabled must be 503, got %d", code)
	}

	// Enabled with the fake embedder: the document lands with tenant/app
	// metadata in the pgvector table.
	const table = "knowledge_test_admin_ingest"
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+table) })
	kb, _, err := agent.NewKnowledgeBase(
		"postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable", table, 64, fakeEmbedder{dim: 64})
	if err != nil {
		t.Fatal(err)
	}
	mux2 := http.NewServeMux()
	api := web.NewAdminAPI(pool, nil, nil, adminTestToken)
	api.Knowledge = kb
	api.RegisterRoutes(mux2)

	code, out := doJSON(t, mux2, http.MethodPost, "/admin/apps/"+appID+"/knowledge/documents",
		`{"name":"退款政策","content":"签收后七天内支持无理由退款。"}`)
	if code != http.StatusCreated {
		t.Fatalf("ingest: %d %v", code, out)
	}
	var hits int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM `+table+` WHERE metadata->>'tenant_id' = '00000000-0000-0000-0000-0000000000aa'
		 AND metadata->>'app_id' = $1`, appID).Scan(&hits); err != nil {
		t.Fatal(err)
	}
	if hits == 0 {
		t.Fatal("document not stored with tenant/app metadata")
	}
}

// fakeEmbedder produces deterministic bag-of-runes vectors for tests.
type fakeEmbedder struct{ dim int }

func (f fakeEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, f.dim)
	for _, r := range text {
		h := fnv.New32a()
		_, _ = h.Write([]byte(string(r)))
		v[int(h.Sum32())%f.dim]++
	}
	return v, nil
}

func (f fakeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := f.GetEmbedding(ctx, text)
	return v, nil, err
}

func (f fakeEmbedder) GetDimensions() int { return f.dim }

func TestAdminAuthAndAuditQuery(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	// Cleanup is LIFO: registered first, runs last — after row deletions.
	t.Cleanup(func() { pool.Close() })

	// Auth: an empty token disables every route (runtime backstop below the
	// startup gate) instead of serving unauthenticated.
	noAuth := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, "").RegisterRoutes(noAuth)
	req0 := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	rec0 := httptest.NewRecorder()
	noAuth.ServeHTTP(rec0, req0)
	if rec0.Code != http.StatusServiceUnavailable {
		t.Fatalf("empty token must 503, got %d", rec0.Code)
	}

	// Auth: with a token set, requests without it get 401.
	mux := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, "secret-token").RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 with token, got %d", rec.Code)
	}

	// Audit query: seed one row and filter by trace_id.
	if _, err := pool.Exec(ctx,
		`INSERT INTO audit_log (tenant_id, channel, decision, trace_id)
		 VALUES ('00000000-0000-0000-0000-000000000001', 'admin', 'allow', 'admin-test-trace')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM audit_log WHERE trace_id = 'admin-test-trace'`)
	})
	mux2 := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, adminTestToken).RegisterRoutes(mux2)
	rows := doJSONList(t, mux2, "/admin/audit?trace_id=admin-test-trace")
	if len(rows) != 1 || rows[0]["decision"] != "allow" {
		t.Fatalf("audit query: %+v", rows)
	}
	rows = doJSONList(t, mux2, "/admin/audit?trace_id=admin-test-trace&decision=deny")
	if len(rows) != 0 {
		t.Fatalf("decision filter must exclude the row: %+v", rows)
	}
}

// A write operation must be audited with its before/after content: creating a
// tenant leaves an audit row whose detail carries the creation payload.
func TestAdminAuditDetail(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })

	auditor := storage.NewAuditor(pool)
	mux := http.NewServeMux()
	web.NewAdminAPI(pool, auditor, nil, adminTestToken).RegisterRoutes(mux)

	name := fmt.Sprintf("audit-detail-%d", time.Now().UnixNano())
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants",
		fmt.Sprintf(`{"name":%q,"model_config":{"model":"m1"}}`, name))
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d %v", code, out)
	}
	tenantID, _ := out["id"].(string)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM audit_log WHERE tenant_id = $1`, tenantID)
	})

	var detail []byte
	if err := pool.QueryRow(ctx,
		`SELECT detail FROM audit_log WHERE tenant_id = $1 AND tool_name = 'create_tenant'`,
		tenantID).Scan(&detail); err != nil {
		t.Fatalf("audit row for create_tenant: %v", err)
	}
	var parsed struct {
		After struct {
			Name string `json:"name"`
		} `json:"after"`
	}
	if err := json.Unmarshal(detail, &parsed); err != nil {
		t.Fatalf("audit detail not parseable: %v (%s)", err, detail)
	}
	if parsed.After.Name != name {
		t.Fatalf("audit detail must carry the creation payload, got %s", detail)
	}

	// And the update path records the before image.
	code, _ = doJSON(t, mux, http.MethodPatch, "/admin/tenants/"+tenantID,
		`{"name":"renamed-tenant"}`)
	if code != http.StatusOK {
		t.Fatalf("update tenant: %d", code)
	}
	if err := pool.QueryRow(ctx,
		`SELECT detail FROM audit_log WHERE tenant_id = $1 AND tool_name = 'update_tenant'`,
		tenantID).Scan(&detail); err != nil {
		t.Fatalf("audit row for update_tenant: %v", err)
	}
	var upd struct {
		Before struct {
			Name string `json:"name"`
		} `json:"before"`
		After struct {
			Name string `json:"name"`
		} `json:"after"`
	}
	if err := json.Unmarshal(detail, &upd); err != nil {
		t.Fatalf("update audit detail not parseable: %v (%s)", err, detail)
	}
	if upd.Before.Name != name || upd.After.Name != "renamed-tenant" {
		t.Fatalf("want before/after names, got %s", detail)
	}
}

// Storage-migration endpoints: create validates the backend pair and the
// one-active-per-tenant constraint; get reports the phase.
func TestAdminStorageMigration(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants", `{"name":"migration-test"}`)
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d", code)
	}
	tenantID, _ := out["id"].(string)
	var migID string
	t.Cleanup(func() {
		if migID != "" {
			_, _ = pool.Exec(ctx, `DELETE FROM storage_migration WHERE id = $1`, migID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	// to_backend must be a menu member.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"session","to_backend":"etcd"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("unknown backend must be 400, got %d", code)
	}
	// Default backend is redis: migrating to redis is a no-op conflict.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"session","to_backend":"redis"}`)
	if code != http.StatusConflict {
		t.Fatalf("same-backend migration must be 409, got %d", code)
	}

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"session","to_backend":"postgres"}`)
	if code != http.StatusCreated {
		t.Fatalf("create migration: %d %v", code, out)
	}
	migID, _ = out["id"].(string)
	if out["phase"] != "dual_write" {
		t.Fatalf("new migration starts in dual_write, got %v", out["phase"])
	}

	// One active migration per tenant/resource.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"session","to_backend":"postgres"}`)
	if code != http.StatusConflict {
		t.Fatalf("concurrent migration must be 409, got %d", code)
	}

	// Status read-back.
	code, out = doJSON(t, mux, http.MethodGet, "/admin/storage-migrations/"+migID, "")
	if code != http.StatusOK || out["from_backend"] != "redis" || out["to_backend"] != "postgres" {
		t.Fatalf("get migration: %d %v", code, out)
	}
}

// wecomws bindings are validated up front (path shape + config credentials +
// no webhook-channel refs): WS inbound has no IM redelivery, so a typo'd
// binding would silently blackhole the bot's messages instead of erroring.
func TestValidateWecomwsBinding(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants", `{"name":"wecomws-validate"}`)
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d", code)
	}
	tenantID, _ := out["id"].(string)
	var appID string
	t.Cleanup(func() {
		if appID != "" {
			_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE app_id = $1`, appID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE id = $1`, appID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"p"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app: %d %v", code, out)
	}
	appID, _ = out["id"].(string)
	if code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", ""); code != http.StatusOK {
		t.Fatalf("publish app: %d", code)
	}
	bindPath := "/admin/apps/" + appID + "/bindings"

	cases := []struct {
		name string
		body string
	}{
		{"missing bot_id", `{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":{"secret_ref":"s"}}`},
		{"missing secret_ref", `{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":{"bot_id":"bot1"}}`},
		{"empty config", `{"channel":"wecomws","webhook_path":"/wecomws/bot1"}`},
		{"config not an object", `{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":"not-json"}`},
		{"bad path", `{"channel":"wecomws","webhook_path":"/callback/wecomws/bot1","config":{"bot_id":"bot1","secret_ref":"s"}}`},
		{"empty path", `{"channel":"wecomws","config":{"bot_id":"bot1","secret_ref":"s"}}`},
		{"token_ref set", `{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":{"bot_id":"bot1","secret_ref":"s"},"token_ref":"wecom-token"}`},
		{"aeskey_ref set", `{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":{"bot_id":"bot1","secret_ref":"s"},"aeskey_ref":"wecom-aes"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := doJSON(t, mux, http.MethodPost, bindPath, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %v)", code, out)
			}
		})
	}

	// A valid wecomws binding is accepted, config and all.
	code, out = doJSON(t, mux, http.MethodPost, bindPath,
		`{"channel":"wecomws","webhook_path":"/wecomws/bot1","config":{"bot_id":"bot1","secret_ref":"wecomws/bot1/secret"}}`)
	if code != http.StatusCreated {
		t.Fatalf("valid wecomws binding: %d %v", code, out)
	}
	if got, _ := out["webhook_path"].(string); got != "/wecomws/bot1" {
		t.Fatalf("webhook_path = %q, want /wecomws/bot1", got)
	}
	var stored []byte
	if err := pool.QueryRow(ctx,
		`SELECT config FROM channel_binding WHERE id = $1`, out["id"]).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(stored, &cfg); err != nil ||
		cfg["bot_id"] != "bot1" || cfg["secret_ref"] != "wecomws/bot1/secret" {
		t.Fatalf("config jsonb round trip failed: %s (%v)", stored, err)
	}

	// Other channels keep their own rules: the same payload shape without the
	// wecomws channel is not subject to the /wecomws/ path contract, and an
	// empty webhook_path is filled with the binding-scoped route the
	// dispatcher serves.
	code, out = doJSON(t, mux, http.MethodPost, bindPath,
		`{"channel":"mock","config":{"bot_id":"ignored"}}`)
	if code != http.StatusCreated {
		t.Fatalf("non-wecomws binding must skip the wecomws validation: %d %v", code, out)
	}
	if got, _ := out["webhook_path"].(string); !strings.HasPrefix(got, "/callback/mock/") {
		t.Fatalf("auto webhook_path = %q, want /callback/mock/{binding_id}", got)
	}
}

// A webhook_path no route is mounted on is invisible from the Admin API: the
// create answers 201 and listBindings shows an active row, while every
// callback the IM posts to that path 404s at the gateway mux. Only two shapes
// are served — the adapter's own env-configured mount, and the binding-scoped
// /callback/{channel}/{binding_id} whose last segment BindingDispatcher looks
// up as a binding id, so it has to be the DB-generated one.
func TestCreateBindingRejectsUnmountedWebhookPath(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants", `{"name":"webhook-path"}`)
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d", code)
	}
	tenantID, _ := out["id"].(string)
	var appID string
	t.Cleanup(func() {
		if appID != "" {
			_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE app_id = $1`, appID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE id = $1`, appID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"p"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app: %d %v", code, out)
	}
	appID, _ = out["id"].(string)
	if code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", ""); code != http.StatusOK {
		t.Fatalf("publish app: %d", code)
	}
	bindPath := "/admin/apps/" + appID + "/bindings"

	cases := []struct {
		name string
		body string
	}{
		// The shape this rule exists for: plausible, pretty, unroutable.
		{"pretty path", `{"channel":"wecom","webhook_path":"/wecom/tenant-a"}`},
		// An env-configured mount belongs to the channel that mounts it; the seeded
		// wecom row would make this a 409 if the check ran after the insert.
		{"another channel's mount", `{"channel":"mock","webhook_path":"/wecom/callback"}`},
		// Mounted, but the dispatcher resolves the last segment as a binding
		// id, and this row's id is generated by the insert.
		{"binding-scoped with a chosen id", `{"channel":"wecom","webhook_path":"/callback/wecom/tenant-a"}`},
		{"wecomws shape on a webhook channel", `{"channel":"wxkf","webhook_path":"/wxkf/bot1"}`},
		{"channel with no mount", `{"channel":"testweb","webhook_path":"/testweb/callback"}`},
		{"trailing slash", `{"channel":"wxkf","webhook_path":"/wxkf/callback/"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := doJSON(t, mux, http.MethodPost, bindPath, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %v)", code, out)
			}
			if msg, _ := out["error"].(string); !strings.Contains(msg, "webhook_path") {
				t.Fatalf("the refusal must name the offending field, got %q", msg)
			}
		})
	}

	// The adapter's own mount stays acceptable: the rule is about
	// routability, not about forcing every binding onto a generated path.
	code, out = doJSON(t, mux, http.MethodPost, bindPath,
		`{"channel":"wxkf","webhook_path":"/wxkf/callback"}`)
	if code != http.StatusCreated {
		t.Fatalf("the adapter's own mount must be accepted: %d %v", code, out)
	}
	if got, _ := out["webhook_path"].(string); got != "/wxkf/callback" {
		t.Fatalf("webhook_path = %q, want /wxkf/callback", got)
	}
}
