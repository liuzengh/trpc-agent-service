package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// setupControlPlaneService wires a real MySQL control plane onto the admin
// service, over and above the YAML-backed one setupService already builds. It
// keeps both: the legacy file routes still work, and the new ones share the
// same Service value, exactly the way a real deployment in
// control_plane.mode=mysql will.
func setupControlPlaneService(t *testing.T) (*Service, *controlplane.DB, string) {
	t.Helper()
	dsn := os.Getenv("ADMINCP_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("ADMINCP_MYSQL_TEST_DSN not set; skipping real-mysql control-plane API test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetControlPlaneAdminSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	if err := cdp.CreateTenant(ctx, "demo", "Demo"); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

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

	token := "control-plane-admin-token"
	resolver := auth.NewResolver(cdp)
	if _, err := resolver.CreateBootstrapAdmin(ctx, "demo", "owner@demo", token); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}

	s := NewService(path, cfg, reg, nil).WithControlPlane(cdp, resolver)
	return s, cdp, token
}

func resetControlPlaneAdminSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"delivery_attempts", "reply_outbox", "session_events", "execution_attempts",
		"executions", "inbox_messages", "channel_reply_routes", "channel_checkpoints",
		"channel_notifications", "outbox_events", "audit_events", "sessions",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

func doAs(t *testing.T, h http.Handler, method, path, body, bearer string) *httpResponse {
	t.Helper()
	return doRaw(t, h, method, path, body, "Bearer "+bearer)
}

type httpResponse struct {
	Code int
	Body string
}

func doRaw(t *testing.T, h http.Handler, method, path, body, authorization string) *httpResponse {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	rw := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, rdr)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	h.ServeHTTP(rw, req)
	return &httpResponse{Code: rw.Code, Body: rw.Body.String()}
}

func seedProfileIDs(t *testing.T, scope controlplane.Scope) (modelID, backendID int64) {
	t.Helper()
	ctx := context.Background()
	var err error
	modelID, err = scope.CreateModelProfile(ctx, "default-model", "gpt-4o-mini", "", "env:MODEL_API_KEY")
	if err != nil {
		t.Fatalf("model profile: %v", err)
	}
	backendID, err = scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatalf("backend profile: %v", err)
	}
	return modelID, backendID
}

func TestControlPlaneAPIPublishesAndRollsBack(t *testing.T) {
	s, cdp, token := setupControlPlaneService(t)
	ctx := context.Background()
	scope := cdp.MustScope("demo")

	if _, err := scope.CreateApp(ctx, "assistant", "Assistant"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	modelID, backendID := seedProfileIDs(t, scope)

	// Publishing without a token is refused, and not with a 500: the route
	// exists, the caller is simply not who it claims to be.
	if got := doRaw(t, s.Handler(), http.MethodGet, "/admin/v2/apps/assistant/revisions/current", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET = %d: %s", got.Code, got.Body)
	}

	// The legacy /admin/tenants route must still work with the old static
	// token, proving the two auth systems coexist rather than replace.
	legacy := do(t, s.Handler(), http.MethodGet, "/admin/tenants", "")
	if legacy.Code != http.StatusOK {
		t.Fatalf("legacy tenants GET = %d: %s", legacy.Code, legacy.Body.String())
	}

	// Publish through the new API.
	spec := map[string]any{
		"instruction":        "be helpful",
		"model_profile_id":   modelID,
		"backend_profile_id": backendID,
		"max_llm_calls":      8,
		"message_timeout_ms": 120000,
	}
	body, _ := json.Marshal(spec)
	pub := doAs(t, s.Handler(), http.MethodPost, "/admin/v2/apps/assistant/revisions", string(body), token)
	if pub.Code != http.StatusCreated {
		t.Fatalf("publish = %d: %s", pub.Code, pub.Body)
	}
	var rev revisionDTO
	if err := json.Unmarshal([]byte(pub.Body), &rev); err != nil {
		t.Fatalf("decode publish response: %v\n%s", err, pub.Body)
	}
	if rev.RevisionNo != 1 {
		t.Fatalf("first published revision number = %d, want 1", rev.RevisionNo)
	}

	// GET current must see it.
	cur := doAs(t, s.Handler(), http.MethodGet, "/admin/v2/apps/assistant/revisions/current", "", token)
	if cur.Code != http.StatusOK {
		t.Fatalf("current = %d: %s", cur.Code, cur.Body)
	}
	var current revisionDTO
	if err := json.Unmarshal([]byte(cur.Body), &current); err != nil {
		t.Fatalf("decode current: %v", err)
	}
	if current.RevisionID != rev.RevisionID {
		t.Fatalf("current revision %d != just-published %d", current.RevisionID, rev.RevisionID)
	}

	// Publish a second revision, then roll back to the first, with a stale
	// expectation rejected and the correct one accepted.
	spec["instruction"] = "be more helpful"
	body, _ = json.Marshal(spec)
	pub2 := doAs(t, s.Handler(), http.MethodPost, "/admin/v2/apps/assistant/revisions", string(body), token)
	if pub2.Code != http.StatusCreated {
		t.Fatalf("second publish = %d: %s", pub2.Code, pub2.Body)
	}
	var rev2 revisionDTO
	if err := json.Unmarshal([]byte(pub2.Body), &rev2); err != nil {
		t.Fatalf("decode second publish: %v", err)
	}

	rollbackBody, _ := json.Marshal(rollbackDTO{TargetRevisionID: rev.RevisionID, ExpectCurrentRevisionID: rev.RevisionID})
	stale := doAs(t, s.Handler(), http.MethodPost, "/admin/v2/apps/assistant/revisions/rollback", string(rollbackBody), token)
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale rollback = %d: %s, want 409", stale.Code, stale.Body)
	}

	rollbackBody, _ = json.Marshal(rollbackDTO{TargetRevisionID: rev.RevisionID, ExpectCurrentRevisionID: rev2.RevisionID})
	good := doAs(t, s.Handler(), http.MethodPost, "/admin/v2/apps/assistant/revisions/rollback", string(rollbackBody), token)
	if good.Code != http.StatusOK {
		t.Fatalf("rollback = %d: %s", good.Code, good.Body)
	}

	cur2 := doAs(t, s.Handler(), http.MethodGet, "/admin/v2/apps/assistant/revisions/current", "", token)
	var current2 revisionDTO
	if err := json.Unmarshal([]byte(cur2.Body), &current2); err != nil {
		t.Fatalf("decode current after rollback: %v", err)
	}
	if current2.Instruction != "be helpful" {
		t.Fatalf("rolled-back instruction = %q, want the original", current2.Instruction)
	}
}

func TestControlPlaneAPIRejectsCrossTenantProfile(t *testing.T) {
	s, cdp, token := setupControlPlaneService(t)
	ctx := context.Background()
	scope := cdp.MustScope("demo")

	// A second tenant's profiles exist but must not be usable from demo's
	// published revision, even though demo created the app it's publishing to.
	if err := cdp.CreateTenant(ctx, "other", "Other"); err != nil {
		t.Fatal(err)
	}
	otherScope := cdp.MustScope("other")
	otherModelID, _ := seedProfileIDs(t, otherScope)

	if _, err := scope.CreateApp(ctx, "assistant", "Assistant"); err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, demoBackendID := seedProfileIDs(t, scope)

	spec := map[string]any{
		"instruction":        "stolen",
		"model_profile_id":   otherModelID,
		"backend_profile_id": demoBackendID,
	}
	body, _ := json.Marshal(spec)
	pub := doAs(t, s.Handler(), http.MethodPost, "/admin/v2/apps/assistant/revisions", string(body), token)
	if pub.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant publish = %d: %s, want 403", pub.Code, pub.Body)
	}
}
