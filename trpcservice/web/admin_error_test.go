package web_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// wantCode fails the test unless the status matches.
func wantCode(t *testing.T, code, want int, out map[string]any) {
	t.Helper()
	if code != want {
		t.Fatalf("status = %d, want %d (body %v)", code, want, out)
	}
}

// uniqueName builds a collision-free fixture name: parallel test runs share
// one database, so every row this file creates is namespaced per test.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// createTenantFor creates a tenant through the API; returns the id. Row
// cleanup is the caller's job (it usually deletes dependent rows first).
func createTenantFor(t *testing.T, mux *http.ServeMux, name string) string {
	t.Helper()
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants", fmt.Sprintf(`{"name":%q}`, name))
	if code != http.StatusCreated {
		t.Fatalf("create tenant %q: %d %v", name, code, out)
	}
	id, _ := out["id"].(string)
	if id == "" {
		t.Fatalf("no tenant id for %q: %v", name, out)
	}
	return id
}

const missingUUID = "00000000-0000-0000-0000-00000000e001"

// Tenant create validation: bad JSON, missing name, and a DB-rejected write
// (name overflows varchar(128) → 500, never a panic or a silent drop).
func TestAdminCreateTenantValidation(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants", `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); !strings.HasPrefix(msg, "invalid json") {
		t.Fatalf("decode failure must be reported as invalid json, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants", `{"name":""}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "name is required" {
		t.Fatalf("empty name must be rejected, got %q", msg)
	}

	// A write the database refuses (name overflows varchar(128)) is a 500 that
	// names the operation and nothing else: never a panic, never a silent drop,
	// and never the driver's own text.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants",
		fmt.Sprintf(`{"name":%q}`, strings.Repeat("x", 129)))
	wantCode(t, code, http.StatusInternalServerError, out)
	if msg, _ := out["error"].(string); msg != "create_tenant failed" {
		t.Fatalf("a DB-refused write must name the operation only, got %q", msg)
	}
}

// Tenant reads: unknown id is 404, malformed id is a 500 (uuid cast error).
func TestAdminGetTenantNotFound(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodGet, "/admin/tenants/"+missingUUID, "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "tenant not found" {
		t.Fatalf("want tenant not found, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodGet, "/admin/tenants/not-a-uuid", "")
	wantCode(t, code, http.StatusInternalServerError, out)
}

// Tenant update: decode failures, the status menu, every policy column,
// the nothing-to-update guard, DB-rejected writes and the 404 for a missing
// row.
func TestAdminUpdateTenantValidation(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()
	tenantID := createTenantFor(t, mux, uniqueName("upd-validation"))
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID) })

	base := "/admin/tenants/" + tenantID

	code, out := doJSON(t, mux, http.MethodPatch, base, `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)

	code, out = doJSON(t, mux, http.MethodPatch, base, `{"status":"paused"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "status must be active or disabled" {
		t.Fatalf("status menu must be enforced, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodPatch, base, `{}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "nothing to update" {
		t.Fatalf("empty patch must be rejected, got %q", msg)
	}

	// Every nullable policy column is patchable in one request; the model
	// endpoint must sit on the platform allowlist.
	code, out = doJSON(t, mux, http.MethodPatch, base,
		`{"status":"disabled","model_config":{"base_url":"https://api.deepseek.com"},`+
			`"tool_policy":{"rate_limit":5},"audit_policy":{"sample":0.5},`+
			`"guardrail_policy":{"max_tokens_per_day":9},"rate_policy":{"qps":7,"burst":14}}`)
	wantCode(t, code, http.StatusOK, out)
	code, out = doJSON(t, mux, http.MethodGet, base, "")
	wantCode(t, code, http.StatusOK, out)
	if out["status"] != "disabled" {
		t.Fatalf("status not updated: %v", out["status"])
	}
	for _, col := range []string{"tool_policy", "audit_policy", "guardrail_policy", "rate_policy"} {
		if _, ok := out[col].(map[string]any); !ok {
			t.Fatalf("%s did not round trip: %v", col, out[col])
		}
	}

	// A DB-rejected write (varchar overflow) is a 500, and a patch against a
	// missing row is a 404 — not a silent no-op.
	code, out = doJSON(t, mux, http.MethodPatch, base,
		fmt.Sprintf(`{"name":%q}`, strings.Repeat("y", 129)))
	wantCode(t, code, http.StatusInternalServerError, out)

	code, out = doJSON(t, mux, http.MethodPatch, "/admin/tenants/"+missingUUID, `{"name":"ghost"}`)
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "tenant not found" {
		t.Fatalf("missing row must 404, got %q", msg)
	}
}

// App create: decode failure, missing fields, and a tenant id that fails the
// uuid cast (500 from the DB error branch).
func TestAdminCreateAppValidation(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+missingUUID+"/apps", `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+missingUUID+"/apps", `{"name":"a"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "name, agent_type and config are required" {
		t.Fatalf("missing fields must be rejected, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/not-a-uuid/apps",
		`{"name":"a","agent_type":"llm","config":{}}`)
	wantCode(t, code, http.StatusInternalServerError, out)
}

// App listing: two versions of one app come back newest-first; an appless
// tenant yields an empty list.
func TestAdminListApps(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()
	tenantID := createTenantFor(t, mux, uniqueName("list-apps"))
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID) })

	for _, v := range []string{"v1", "v2"} {
		code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
			fmt.Sprintf(`{"name":"bot","agent_type":"llm","config":{"prompt":%q}}`, v))
		wantCode(t, code, http.StatusCreated, out)
	}

	list := doJSONList(t, mux, "/admin/tenants/"+tenantID+"/apps")
	if len(list) != 2 {
		t.Fatalf("want 2 app versions, got %+v", list)
	}
	if list[0]["version"] != float64(2) || list[1]["version"] != float64(1) {
		t.Fatalf("versions must be ordered newest first: %+v", list)
	}
	if list[0]["name"] != "bot" || list[0]["status"] != "draft" || list[0]["agent_type"] != "llm" {
		t.Fatalf("app fields missing from listing: %+v", list[0])
	}

	empty := doJSONList(t, mux, "/admin/tenants/"+missingUUID+"/apps")
	if len(empty) != 0 {
		t.Fatalf("tenant without apps must list empty, got %+v", empty)
	}
}

// App update: decode failure, missing config, and an app id that fails the
// uuid cast (the before-image read logs and yields nil, the update 500s).
func TestAdminUpdateAppValidation(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodPatch,
		"/admin/tenants/"+missingUUID+"/apps/"+missingUUID, `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)

	code, out = doJSON(t, mux, http.MethodPatch,
		"/admin/tenants/"+missingUUID+"/apps/"+missingUUID, `{"prompt":"x"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "config is required" {
		t.Fatalf("missing config must be rejected, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodPatch,
		"/admin/tenants/"+missingUUID+"/apps/not-a-uuid", `{"config":{}}`)
	wantCode(t, code, http.StatusInternalServerError, out)
}

// Publish failure modes: unknown app (404 via errNotFound), malformed id
// (500), republish of the live version (409 via errConflict), and the
// publish-time allowlist gate (400 via errBadRequest) — a draft stored
// before a policy change must not become the serving version.
func TestAdminPublishErrors(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })

	api := web.NewAdminAPI(pool, nil, nil, adminTestToken)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	// Unknown / malformed app ids.
	code, out := doJSON(t, mux, http.MethodPost, "/admin/apps/"+missingUUID+"/publish", "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "app not found" {
		t.Fatalf("publish of unknown app must 404, got %q", msg)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/not-a-uuid/publish", "")
	wantCode(t, code, http.StatusInternalServerError, out)

	// A draft whose model endpoint was allowlisted at write time is refused
	// at publish time once the platform allowlist shrinks.
	tenantID := createTenantFor(t, mux, uniqueName("publish-gate"))
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"model":{"base_url":"https://api.deepseek.com"}}}`)
	wantCode(t, code, http.StatusCreated, out)
	appID, _ := out["id"].(string)

	api.ModelHosts = []string{"models.other.example"}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", "")
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); !strings.Contains(msg, "api.deepseek.com") {
		t.Fatalf("publish gate must name the offending host, got %q", msg)
	}

	// Republishing the already-published version is a conflict.
	api.ModelHosts = nil
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", "")
	wantCode(t, code, http.StatusOK, out)
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appID+"/publish", "")
	wantCode(t, code, http.StatusConflict, out)
	if msg, _ := out["error"].(string); msg != "app version is already published" {
		t.Fatalf("republish must conflict, got %q", msg)
	}
}

// Rollback failure modes: unknown app (404), malformed id (500), an explicit
// version that does not exist (404), and a rollback target that is currently
// the published version (409 — publish refuses inside the same tx).
func TestAdminRollbackErrors(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	tenantID := createTenantFor(t, mux, uniqueName("rollback-err"))
	var appV1, appV2 string
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v1"}}`)
	wantCode(t, code, http.StatusCreated, out)
	appV1, _ = out["id"].(string)
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v2"}}`)
	wantCode(t, code, http.StatusCreated, out)
	appV2, _ = out["id"].(string)

	// Unknown and malformed app ids.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+missingUUID+"/rollback", "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "app not found" {
		t.Fatalf("rollback of unknown app must 404, got %q", msg)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/not-a-uuid/rollback", "")
	wantCode(t, code, http.StatusInternalServerError, out)

	// Explicit version that does not exist.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/rollback", `{"version":99}`)
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "version 99 not found" {
		t.Fatalf("unknown rollback target must 404, got %q", msg)
	}

	// Rollback target == currently published version: publish refuses in-tx.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/publish", "")
	wantCode(t, code, http.StatusOK, out)
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/rollback", `{"version":1}`)
	wantCode(t, code, http.StatusConflict, out)
	if msg, _ := out["error"].(string); msg != "app version is already published" {
		t.Fatalf("rollback to the published version must conflict, got %q", msg)
	}
}

// Binding create: decode failure, missing channel, draft/unpublished target
// (404), and a malformed app id (500).
func TestAdminCreateBindingValidation(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	tenantID := createTenantFor(t, mux, uniqueName("binding-err"))
	var draftID string
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"p"}}`)
	wantCode(t, code, http.StatusCreated, out)
	draftID, _ = out["id"].(string)

	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+draftID+"/bindings", `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)

	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+draftID+"/bindings", `{"webhook_path":"/x"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "channel is required" {
		t.Fatalf("missing channel must be rejected, got %q", msg)
	}

	// Only a published app is bindable: a draft must 404.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+draftID+"/bindings",
		`{"channel":"wecom"}`)
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "app not found or not published" {
		t.Fatalf("binding a draft must 404, got %q", msg)
	}

	// Malformed app id: the insert fails on the uuid cast.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/not-a-uuid/bindings",
		`{"channel":"wecom"}`)
	wantCode(t, code, http.StatusInternalServerError, out)
}

// Binding delete: malformed binding id (500) and a missing row (404).
func TestAdminDeleteBindingNotFound(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodDelete,
		"/admin/apps/"+missingUUID+"/bindings/not-a-uuid", "")
	wantCode(t, code, http.StatusInternalServerError, out)

	code, out = doJSON(t, mux, http.MethodDelete,
		"/admin/apps/"+missingUUID+"/bindings/"+missingUUID, "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "binding not found" {
		t.Fatalf("missing binding must 404, got %q", msg)
	}
}

// erroringEmbedder fails every embedding call, forcing the ingestion
// endpoint's AddSource error branch.
type erroringEmbedder struct{ dim int }

func (e erroringEmbedder) GetEmbedding(_ context.Context, _ string) ([]float64, error) {
	return nil, errors.New("embedder down")
}

func (e erroringEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return nil, nil, errors.New("embedder down")
}

func (e erroringEmbedder) GetDimensions() int { return e.dim }

// Knowledge ingestion: decode failure, missing fields, unknown app (404),
// malformed app id (500), and an embedder outage (500 — the document is
// refused, not silently dropped).
func TestAdminKnowledgeValidation(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })

	// Unique fixture ids: parallel test runs share the database.
	nano := time.Now().UnixNano() & ((1 << 48) - 1)
	tenantID := fmt.Sprintf("00000000-0000-0000-0000-%012x", nano)
	appID := fmt.Sprintf("00000000-0000-0000-0000-%012x", nano+1)
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, $2, 'active')`, tenantID, uniqueName("kb-err")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'kb-app', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	// Knowledge is enabled here (validation runs before any embedding): the
	// table is dropped again at cleanup.
	table := "knowledge_test_admin_val_" + fmt.Sprint(nano)
	kb, _, err := agent.NewKnowledgeBase(testenv.PGDSN(), table, 8, fakeEmbedder{dim: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+table) })

	mux := http.NewServeMux()
	api := web.NewAdminAPI(pool, nil, nil, adminTestToken)
	api.Knowledge = kb
	api.RegisterRoutes(mux)
	path := "/admin/apps/" + appID + "/knowledge/documents"

	code, out := doJSON(t, mux, http.MethodPost, path, `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)
	code, out = doJSON(t, mux, http.MethodPost, path, `{"name":"x"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); msg != "name and content are required" {
		t.Fatalf("missing content must be rejected, got %q", msg)
	}
	code, out = doJSON(t, mux, http.MethodPost,
		"/admin/apps/"+missingUUID+"/knowledge/documents", `{"name":"x","content":"y"}`)
	wantCode(t, code, http.StatusNotFound, out)
	code, out = doJSON(t, mux, http.MethodPost,
		"/admin/apps/not-a-uuid/knowledge/documents", `{"name":"x","content":"y"}`)
	wantCode(t, code, http.StatusInternalServerError, out)

	// Enabled knowledge with a broken embedder: ingestion fails with 500.
	deadKB, _, err := agent.NewKnowledgeBase(testenv.PGDSN(),
		"knowledge_test_admin_err_"+fmt.Sprint(nano), 8, erroringEmbedder{dim: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS knowledge_test_admin_err_`+fmt.Sprint(nano))
	})
	api2 := web.NewAdminAPI(pool, nil, nil, adminTestToken)
	api2.Knowledge = deadKB
	mux2 := http.NewServeMux()
	api2.RegisterRoutes(mux2)
	code, out = doJSON(t, mux2, http.MethodPost, path, `{"name":"doc","content":"text"}`)
	wantCode(t, code, http.StatusInternalServerError, out)
	if msg, _ := out["error"].(string); !strings.Contains(msg, "ingest document") {
		t.Fatalf("embedder outage must surface as ingest failure, got %q", msg)
	}
}

// Storage-migration create: decode failure, resource menu, unknown tenant
// (404), malformed tenant id (500), and a tenant whose storage_config pins
// the session backend (the override becomes from_backend — migrating to it
// is a no-op conflict).
func TestAdminCreateMigrationValidation(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	tenantID := createTenantFor(t, mux, uniqueName("migration-err"))
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID) })

	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations", `{invalid`)
	wantCode(t, code, http.StatusBadRequest, out)

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"knowledge","to_backend":"redis"}`)
	wantCode(t, code, http.StatusBadRequest, out)
	if msg, _ := out["error"].(string); !strings.Contains(msg, `resource must be "session"`) {
		t.Fatalf("resource menu must be enforced, got %q", msg)
	}

	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+missingUUID+"/storage-migrations",
		`{"resource":"session","to_backend":"redis"}`)
	wantCode(t, code, http.StatusNotFound, out)
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/not-a-uuid/storage-migrations",
		`{"resource":"session","to_backend":"redis"}`)
	wantCode(t, code, http.StatusInternalServerError, out)

	// storage_config.session.type overrides the platform default backend.
	if _, err := pool.Exec(ctx,
		`UPDATE tenant SET storage_config = '{"session":{"type":"postgres"}}' WHERE id = $1`, tenantID); err != nil {
		t.Fatal(err)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/storage-migrations",
		`{"resource":"session","to_backend":"postgres"}`)
	wantCode(t, code, http.StatusConflict, out)
	if msg, _ := out["error"].(string); msg != "tenant already on postgres" {
		t.Fatalf("migration to the overridden backend must conflict, got %q", msg)
	}
}

// Migration reads: unknown id (404) and malformed id (500).
func TestAdminGetMigrationValidation(t *testing.T) {
	mux, _ := adminTestAPI(t)

	code, out := doJSON(t, mux, http.MethodGet, "/admin/storage-migrations/"+missingUUID, "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "migration not found" {
		t.Fatalf("unknown migration must 404, got %q", msg)
	}
	code, out = doJSON(t, mux, http.MethodGet, "/admin/storage-migrations/not-a-uuid", "")
	wantCode(t, code, http.StatusInternalServerError, out)
}

// With the database gone every DB-backed handler must answer a clean 500
// (never a panic and never a cached empty result): the list endpoints, the
// audit query and the publish transaction all fail at their first query.
func TestAdminDeadPool(t *testing.T) {
	deadPool := testenv.PG(t)
	deadPool.Close()

	mux := http.NewServeMux()
	web.NewAdminAPI(deadPool, nil, nil, adminTestToken).RegisterRoutes(mux)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"listTenants", http.MethodGet, "/admin/tenants"},
		{"getTenant", http.MethodGet, "/admin/tenants/" + missingUUID},
		{"listApps", http.MethodGet, "/admin/tenants/" + missingUUID + "/apps"},
		{"listBindings", http.MethodGet, "/admin/apps/" + missingUUID + "/bindings"},
		{"queryAudit", http.MethodGet, "/admin/audit"},
		{"publish", http.MethodPost, "/admin/apps/" + missingUUID + "/publish"},
		{"rollback", http.MethodPost, "/admin/apps/" + missingUUID + "/rollback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out := doJSON(t, mux, tc.method, tc.path, "")
			wantCode(t, code, http.StatusInternalServerError, out)
			if _, ok := out["error"].(string); !ok {
				t.Fatalf("error body must name the failure, got %v", out)
			}
		})
	}
}

// A failure that came from the database must answer with the operation that
// failed and nothing else. pgx and pgconn text names tables, columns and
// constraints, quotes the statement and its SQLSTATE, and on a connection
// failure recites the DSN's host, port, user and database — and an Admin API
// response body is precisely the text that gets pasted into a ticket. The
// operator still gets the whole error, from the log.
func TestAdminInternalErrorHidesDriverText(t *testing.T) {
	mux, _ := adminTestAPI(t)
	stop := testenv.CaptureLogs(t, "error")

	// Each of these makes the id column's uuid cast fail, so the handler
	// answers from its driver-error branch; the real Postgres error must not
	// reach the caller.
	cases := []struct{ method, path, op string }{
		{http.MethodGet, "/admin/tenants/not-a-uuid", "get_tenant"},
		{http.MethodPost, "/admin/apps/not-a-uuid/publish", "publish_app"},
		{http.MethodPost, "/admin/apps/not-a-uuid/rollback", "rollback_app"},
		{http.MethodGet, "/admin/storage-migrations/not-a-uuid", "get_storage_migration"},
	}
	for _, tc := range cases {
		code, out := doJSON(t, mux, tc.method, tc.path, "")
		wantCode(t, code, http.StatusInternalServerError, out)
		msg, _ := out["error"].(string)
		if want := tc.op + " failed"; msg != want {
			t.Fatalf("%s %s: body = %q, want %q", tc.method, tc.path, msg, want)
		}
	}

	// The genericizing must not swallow the errors this service wrote for the
	// caller: a judgement keeps its own message and status.
	code, out := doJSON(t, mux, http.MethodPost, "/admin/apps/"+missingUUID+"/publish", "")
	wantCode(t, code, http.StatusNotFound, out)
	if msg, _ := out["error"].(string); msg != "app not found" {
		t.Fatalf("a 404 judgement must keep its message, got %q", msg)
	}

	logged := stop()
	if logged == "" {
		t.Fatal("hiding an error from the caller must not hide it from the log")
	}
	for _, want := range []string{"invalid input syntax", "22P02",
		"admin get_tenant", "admin publish_app", "admin rollback_app"} {
		if !strings.Contains(logged, want) {
			t.Errorf("log must carry the hidden error; no %q in:\n%s", want, logged)
		}
	}
}

// A list query that fails must not answer 200 with an empty array. pgx defers
// a bind- or execute-time failure past Query, which returns a nil error and a
// Rows that simply ends, so the failure is only visible on rows.Err() — an
// empty list is indistinguishable from a genuinely empty table.
//
// listTenants carries the same check but has no parameter to break, so its
// branch is only reachable through a connection dropping mid-iteration.
func TestAdminListFailureIsNotAnEmptyList(t *testing.T) {
	mux, _ := adminTestAPI(t)

	cases := []struct {
		path string
		op   string
	}{
		{"/admin/tenants/not-a-uuid/apps", "list_apps"},
		{"/admin/apps/not-a-uuid/bindings", "list_bindings"},
		{"/admin/audit?tenant_id=not-a-uuid", "query_audit"},
	}
	for _, tc := range cases {
		// A failed list must answer 500: an empty array is not the error shape
		// doJSON decodes into.
		code, out := doJSON(t, mux, http.MethodGet, tc.path, "")
		wantCode(t, code, http.StatusInternalServerError, out)
		msg, _ := out["error"].(string)
		if want := tc.op + " failed"; msg != want {
			t.Fatalf("GET %s: body = %q, want %q", tc.path, msg, want)
		}
	}

	// A list that succeeds still lists: the check must not turn healthy reads
	// into failures.
	if list := doJSONList(t, mux, "/admin/tenants/"+missingUUID+"/apps"); len(list) != 0 {
		t.Fatalf("a tenant without apps must list empty, got %+v", list)
	}
}

// Invalidation broadcast: a healthy Redis receives the publish on every write;
// a dead Redis must not fail the write (TTL is the fallback). The audit row
// carries the X-Admin-User operator, and an auditor whose pool is gone must
// not fail the write either — audit detail never breaks the operation.
func TestAdminAfterWriteResilience(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
	t.Cleanup(func() { pool.Close() })
	rdb := testenv.Redis(t)

	auditor := storage.NewAuditor(pool)

	// Healthy Redis: write succeeds and the audit row records the operator.
	mux := http.NewServeMux()
	web.NewAdminAPI(pool, auditor, rdb, adminTestToken).RegisterRoutes(mux)
	name := uniqueName("afterwrite-ok")
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants",
		strings.NewReader(fmt.Sprintf(`{"name":%q}`, name)))
	req.Header.Set("Authorization", "Bearer "+adminTestToken)
	req.Header.Set("X-Admin-User", "alice")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create tenant with rdb: %d %s", rec.Code, rec.Body.String())
	}
	var tenantID string
	if err := pool.QueryRow(ctx, `SELECT id FROM tenant WHERE name = $1`, name).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
		_, _ = pool.Exec(ctx, `DELETE FROM audit_log WHERE tenant_id = $1`, tenantID)
	})
	var operator string
	if err := pool.QueryRow(ctx,
		`SELECT user_id FROM audit_log WHERE tenant_id = $1 AND tool_name = 'create_tenant'`,
		tenantID).Scan(&operator); err != nil {
		t.Fatalf("write must be audited: %v", err)
	}
	if operator != "alice" {
		t.Fatalf("X-Admin-User must reach the audit row, got %q", operator)
	}

	// Dead Redis: the broadcast fails, the write still succeeds.
	_ = rdb.Close()
	mux2 := http.NewServeMux()
	web.NewAdminAPI(pool, nil, rdb, adminTestToken).RegisterRoutes(mux2)
	code, out := doJSON(t, mux2, http.MethodPost, "/admin/tenants",
		fmt.Sprintf(`{"name":%q}`, uniqueName("afterwrite-dead")))
	if code != http.StatusCreated {
		t.Fatalf("write must survive a dead Redis: %d %v", code, out)
	}
	if id, _ := out["id"].(string); id != "" {
		t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, id) })
	}

	// Auditor over a closed pool: the LogSync error is logged, the write
	// still succeeds, and no audit row appears.
	deadPool := testenv.PG(t)
	deadPool.Close()
	mux3 := http.NewServeMux()
	web.NewAdminAPI(pool, storage.NewAuditor(deadPool), nil, adminTestToken).RegisterRoutes(mux3)
	name3 := uniqueName("afterwrite-noaudit")
	code, out = doJSON(t, mux3, http.MethodPost, "/admin/tenants", fmt.Sprintf(`{"name":%q}`, name3))
	wantCode(t, code, http.StatusCreated, out)
	if id, _ := out["id"].(string); id != "" {
		t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, id) })
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_log WHERE detail->'after'->>'name' = $1`, name3).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("failed audit must not write a row, got %d", n)
	}
}

// The change audit's operator comes from the verified mTLS client certificate
// when the listener has one: a header anyone holding the shared token could
// set is not attributable. The header stays the plain-token fallback, and a
// CN-less certificate falls back too rather than writing an empty operator.
func TestOperatorIDPrefersClientCertCN(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants", nil)
	req.Header.Set("X-Admin-User", "spoofed")
	if got := web.OperatorID(req); got != "spoofed" {
		t.Fatalf("plain-token deployment keeps the header operator, got %q", got)
	}
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{
		{Subject: pkix.Name{CommonName: "admin-client"}},
	}}
	if got := web.OperatorID(req); got != "admin-client" {
		t.Fatalf("a verified client cert must name the operator, got %q", got)
	}
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{}}}
	if got := web.OperatorID(req); got != "spoofed" {
		t.Fatalf("a CN-less certificate must fall back to the header, got %q", got)
	}
}
