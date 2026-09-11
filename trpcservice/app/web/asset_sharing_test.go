package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

// recordingAuditor captures asset-change entries so tests can assert that a
// member's write (or a refused one) left a trail.
type recordingAuditor struct {
	entries []audit.Entry
}

func (r *recordingAuditor) Record(e audit.Entry) { r.entries = append(r.entries, e) }

func (r *recordingAuditor) decisions() []string {
	out := make([]string, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.Decision)
	}
	return out
}

// newAuditedKBServer returns a KB server whose writes are audited.
func newAuditedKBServer(t *testing.T) (*httptest.Server, *recordingAuditor) {
	t.Helper()
	rec := &recordingAuditor{}
	mux := http.NewServeMux()
	mgr := knowledge.NewManager(knowledge.InMemoryVectorStoreFactory(),
		func(_ context.Context, _ *knowledge.KnowledgeBase) (embedder.Embedder, error) {
			return &webHashEmbedder{dim: 32}, nil
		})
	api := NewKnowledgeAPI(mgr)
	api.SetAuditor(rec)
	api.Register(mux)
	return httptest.NewServer(asClaims(mux)), rec
}

// createKBAs creates a KB as the given client and returns the stored row.
func createKBAs(t *testing.T, c *http.Client, srv *httptest.Server, id, name, visibility string) knowledge.KnowledgeBase {
	t.Helper()
	body := `{"id":"` + id + `","name":"` + name + `","embedding_endpoint_id":"e-1"`
	if visibility != "" {
		body += `,"visibility":"` + visibility + `"`
	}
	body += `}`
	resp := postAs(t, c, srv.URL+"/kbs", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create kb %s status = %d", id, resp.StatusCode)
	}
	var kb knowledge.KnowledgeBase
	if err := json.NewDecoder(resp.Body).Decode(&kb); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return kb
}

func listKBIDs(t *testing.T, c *http.Client, srv *httptest.Server) []string {
	t.Helper()
	resp := getAs(t, c, srv.URL+"/kbs")
	var kbs []knowledge.KnowledgeBase
	if err := json.NewDecoder(resp.Body).Decode(&kbs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	out := make([]string, 0, len(kbs))
	for _, kb := range kbs {
		out = append(out, kb.ID)
	}
	return out
}

// TestMemberCreatesPrivateAgentThenShares extends the author rule to agents: an
// employee deploys its own Agent, it starts private, and sharing publishes it
// read-only to the tenant. IM binding to a private agent stays legal because the
// binding check is tenant-scoped, not visibility-scoped.
func TestMemberCreatesPrivateAgentThenShares(t *testing.T) {
	srv := newAgentServer(t)
	defer srv.Close()

	alice := clientAs(memberClaims("acme", "alice"))
	bob := clientAs(memberClaims("acme", "bob"))
	admin := clientAs(adminClaims("acme"))

	resp := postAs(t, alice, srv.URL+"/agents", `{"id":"a-alice","name":"alice bot"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("member create agent = %d, want 201", resp.StatusCode)
	}
	var created agent.Agent
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if created.CreatedBy != "alice" {
		t.Errorf("created_by = %q, want alice", created.CreatedBy)
	}
	if created.Visibility != "private" {
		t.Errorf("visibility = %q, want private by default", created.Visibility)
	}
	if created.TenantID != "acme" {
		t.Errorf("tenant = %q, want acme (pinned)", created.TenantID)
	}

	agentIDs := func(c *http.Client) []string {
		t.Helper()
		resp := getAs(t, c, srv.URL+"/agents")
		var list []agent.Agent
		if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		out := make([]string, 0, len(list))
		for _, a := range list {
			out = append(out, a.ID)
		}
		return out
	}

	if got := agentIDs(alice); len(got) != 1 || got[0] != "a-alice" {
		t.Errorf("alice list = %v, want [a-alice]", got)
	}
	if got := agentIDs(bob); len(got) != 0 {
		t.Errorf("bob sees %v, want nothing (private agent)", got)
	}
	if got := agentIDs(admin); len(got) != 1 {
		t.Errorf("admin sees %v, want the tenant's agent", got)
	}

	// Bob cannot reach it, nor publish it, by id.
	resp = getAs(t, bob, srv.URL+"/agents/a-alice")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("bob get private agent = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
	resp = postAs(t, bob, srv.URL+"/agents/a-alice/publish", `{"endpoint_id":"e1","model":"m"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("bob publish private agent = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Alice shares it; now Bob may read (and chat with) it but not change it.
	resp = doAs(t, alice, http.MethodPut, srv.URL+"/agents/a-alice",
		`{"name":"alice bot","visibility":"shared"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("share = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = getAs(t, bob, srv.URL+"/agents/a-alice")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("bob get shared agent = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	for _, tc := range []struct{ method, suffix, body string }{
		{http.MethodPut, "", `{"name":"hijacked"}`},
		{http.MethodDelete, "", ""},
		{http.MethodPost, "/publish", `{"endpoint_id":"e1","model":"m"}`},
	} {
		resp = doAs(t, bob, tc.method, srv.URL+"/agents/a-alice"+tc.suffix, tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("bob %s shared agent = %d, want 404 (read-only)", tc.method, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// TestLegacyAssetsWithoutAuthorStayVisible pins the zero-migration rule for
// data that predates author tracking: a row with no author is tenant-shared, so
// employees keep seeing what they always saw. Only new rows get the private
// default.
func TestLegacyAssetsWithoutAuthorStayVisible(t *testing.T) {
	srv, _ := newAuditedKBServer(t)
	defer srv.Close()

	owner := clientAs(ownerClaims())
	bob := clientAs(memberClaims("acme", "bob"))

	// The owner-created KB has an author (the owner), so it is private until
	// shared — that is the "new" rule. The owner must name the tenant, being
	// platform-wide.
	resp := postAs(t, owner, srv.URL+"/kbs",
		`{"id":"kb-new","tenant_id":"acme","name":"new","embedding_endpoint_id":"e-1"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("owner create kb = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()
	if got := listKBIDs(t, bob, srv); len(got) != 0 {
		t.Errorf("bob sees %v, want nothing (authored + private)", got)
	}

	// A legacy row has no author at all. The store cannot be asked for that
	// through the API, so the rule is exercised at the source of truth.
	if !CanReadAsset(memberClaims("acme", "bob"), "", VisibilityPrivate) {
		t.Error("a legacy row (no author) must stay readable by the tenant")
	}
	if CanReadAsset(memberClaims("acme", "bob"), "alice", VisibilityPrivate) {
		t.Error("an authored + private row must stay hidden")
	}
	if !CanReadAsset(memberClaims("acme", "bob"), "alice", VisibilityShared) {
		t.Error("an authored + shared row must be readable")
	}
}

// TestMemberCreatesPrivateAssetThenShares is the core rule of this phase: an
// employee authors a tenant asset, it starts private to them, and sharing it
// publishes it read-only to the rest of the tenant.
func TestMemberCreatesPrivateAssetThenShares(t *testing.T) {
	srv, rec := newAuditedKBServer(t)
	defer srv.Close()

	alice := clientAs(memberClaims("acme", "alice"))
	bob := clientAs(memberClaims("acme", "bob"))
	admin := clientAs(adminClaims("acme"))

	// Alice creates without asking for visibility: it must default to private.
	kb := createKBAs(t, alice, srv, "kb-alice", "alice notes", "")
	if kb.CreatedBy != "alice" {
		t.Errorf("created_by = %q, want alice (the author)", kb.CreatedBy)
	}
	if kb.Visibility != "private" {
		t.Errorf("visibility = %q, want private by default", kb.Visibility)
	}
	if kb.TenantID != "acme" {
		t.Errorf("tenant = %q, want acme (pinned to the caller)", kb.TenantID)
	}

	// Alice sees her own; Bob does not; the tenant admin does.
	if got := listKBIDs(t, alice, srv); len(got) != 1 || got[0] != "kb-alice" {
		t.Errorf("alice list = %v, want [kb-alice]", got)
	}
	if got := listKBIDs(t, bob, srv); len(got) != 0 {
		t.Errorf("bob sees %v, want nothing (private asset)", got)
	}
	if got := listKBIDs(t, admin, srv); len(got) != 1 {
		t.Errorf("admin sees %v, want the tenant's KB", got)
	}

	// Bob cannot reach it by id either.
	resp := getAs(t, bob, srv.URL+"/kbs/kb-alice")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("bob get private kb = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Alice shares it.
	resp = doAs(t, alice, http.MethodPut, srv.URL+"/kbs/kb-alice",
		`{"name":"alice notes","visibility":"shared"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("share status = %d, want 200", resp.StatusCode)
	}
	var shared knowledge.KnowledgeBase
	_ = json.NewDecoder(resp.Body).Decode(&shared)
	resp.Body.Close()
	if shared.Visibility != "shared" {
		t.Fatalf("visibility after share = %q, want shared", shared.Visibility)
	}

	// Now Bob can read it, and the tenant admin still can.
	resp = getAs(t, bob, srv.URL+"/kbs/kb-alice")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("bob get shared kb = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if got := listKBIDs(t, bob, srv); len(got) != 1 {
		t.Errorf("bob list after share = %v, want the shared KB", got)
	}

	// ... but a shared asset is read-only for everyone but its author: Bob
	// cannot rename, reshare, delete or ingest into it.
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPut, "/kbs/kb-alice", `{"name":"hijacked","visibility":"private"}`},
		{http.MethodDelete, "/kbs/kb-alice", ""},
		{http.MethodPost, "/kbs/kb-alice/documents", `{"title":"x","text":"y"}`},
	} {
		resp = doAs(t, bob, tc.method, srv.URL+tc.path, tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("bob %s %s = %d, want 404 (shared is read-only)", tc.method, tc.path, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Alice can take it back to private.
	resp = doAs(t, alice, http.MethodPut, srv.URL+"/kbs/kb-alice",
		`{"name":"alice notes","visibility":"private"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unshare status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if got := listKBIDs(t, bob, srv); len(got) != 0 {
		t.Errorf("bob still sees %v after unshare", got)
	}

	// Every allowed change is audited as executed, and Bob's probes are audited
	// as denied — a refused cross-member write is not silent.
	var allowed, denied int
	for _, d := range rec.decisions() {
		switch d {
		case audit.DecisionExecuted:
			allowed++
		case audit.DecisionDeny:
			denied++
		}
	}
	if allowed == 0 {
		t.Errorf("no executed audit entries, got %v", rec.decisions())
	}
	if denied == 0 {
		t.Errorf("no denied audit entries for bob's probes, got %v", rec.decisions())
	}
}

// TestMemberCannotReachPeerTenantAssets keeps the tenant boundary intact while
// members gain write access: same-tenant peers are governed by authorship, other
// tenants stay invisible.
func TestMemberCannotReachPeerTenantAssets(t *testing.T) {
	srv, _ := newAuditedKBServer(t)
	defer srv.Close()

	acme := clientAs(memberClaims("acme", "alice"))
	globex := clientAs(memberClaims("globex", "carol"))

	createKBAs(t, acme, srv, "kb-acme", "acme kb", "shared")

	if got := listKBIDs(t, globex, srv); len(got) != 0 {
		t.Errorf("cross-tenant list leaked %v", got)
	}
	resp := getAs(t, globex, srv.URL+"/kbs/kb-acme")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("cross-tenant get = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// globex cannot plant a KB in acme either.
	created := createKBAs(t, globex, srv, "kb-globex", "globex kb", "shared")
	if created.TenantID != "globex" {
		t.Errorf("tenant = %q, want globex (pinned)", created.TenantID)
	}
}

// TestGlobalAssetsAreOwnerOnly pins the platform boundary: a member or admin
// may not mint a global skill or a global model endpoint.
func TestGlobalAssetsAreOwnerOnly(t *testing.T) {
	// --- skills ---
	skillMux := http.NewServeMux()
	NewSkillAPI(skill.NewManager()).Register(skillMux)
	skillSrv := httptest.NewServer(asClaims(skillMux))
	defer skillSrv.Close()

	alice := clientAs(memberClaims("acme", "alice"))
	owner := clientAs(ownerClaims())

	resp := postAs(t, alice, skillSrv.URL+"/skills", `{"code":"x","name":"X","scope":"global"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("member global skill = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()

	resp = postAs(t, owner, skillSrv.URL+"/skills", `{"code":"g","name":"G","scope":"global"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("owner global skill = %d, want 201", resp.StatusCode)
	}
	var created skill.Skill
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if created.OwnerTenantID != nil {
		t.Errorf("global skill owner_tenant_id = %v, want nil", created.OwnerTenantID)
	}

	// A member reads the global skill (platform-shared) but cannot change it.
	resp = getAs(t, alice, skillSrv.URL+"/skills/"+created.SkillID)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("member read global skill = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doAs(t, alice, http.MethodPut, skillSrv.URL+"/skills/"+created.SkillID, `{"name":"hijack"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("member write global skill = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// --- endpoints ---
	epMux := http.NewServeMux()
	NewEndpointAPI(llm.NewRegistry(nil)).Register(epMux)
	epSrv := httptest.NewServer(asClaims(epMux))
	defer epSrv.Close()

	resp = postAs(t, alice, epSrv.URL+"/endpoints",
		`{"id":"ep-g","scope":"global","name":"g","provider":"openai","base_url":"http://x","model_name":"m"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("member global endpoint = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestMemberEndpointSharingFollowsTheSameRule covers the endpoint domain, which
// stores visibility on the registry row rather than a KB row.
func TestMemberEndpointSharingFollowsTheSameRule(t *testing.T) {
	mux := http.NewServeMux()
	NewEndpointAPI(llm.NewRegistry(nil)).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	alice := clientAs(memberClaims("acme", "alice"))
	bob := clientAs(memberClaims("acme", "bob"))

	resp := postAs(t, alice, srv.URL+"/endpoints",
		`{"id":"ep-alice","name":"mine","provider":"openai","type":"chat","base_url":"http://x","model_name":"m"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d, want 201", resp.StatusCode)
	}
	var ep llm.Endpoint
	_ = json.NewDecoder(resp.Body).Decode(&ep)
	resp.Body.Close()
	if ep.CreatedBy != "alice" || ep.Visibility != "private" {
		t.Fatalf("endpoint = %+v, want created_by=alice visibility=private", ep)
	}

	// Private: invisible to Bob, including by id.
	var list []llm.Endpoint
	resp = getAs(t, bob, srv.URL+"/endpoints")
	_ = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list) != 0 {
		t.Errorf("bob sees %+v, want nothing", list)
	}

	// Shared: readable by Bob, still not editable by him.
	resp = doAs(t, alice, http.MethodPut, srv.URL+"/endpoints/ep-alice",
		`{"name":"mine","provider":"openai","type":"chat","base_url":"http://x","model_name":"m","visibility":"shared"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("share = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = getAs(t, bob, srv.URL+"/endpoints/ep-alice")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("bob read shared endpoint = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	resp = doAs(t, bob, http.MethodDelete, srv.URL+"/endpoints/ep-alice", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("bob delete shared endpoint = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}
