package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
)

func newSkillServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewSkillAPI(skill.NewManager()).Register(mux)
	return httptest.NewServer(asClaims(mux))
}

func TestSkillAPILifecycle(t *testing.T) {
	srv := newSkillServer(t)
	defer srv.Close()

	// acme's admin manages its own tenant's skills.
	acme := clientAs(adminClaims("acme"))

	// create a tenant-scoped skill
	createBody := `{"code":"triage","name":"分诊","description":"priority-first dispatch","scope":"tenant","owner_tenant_id":"acme"}`
	resp := postAs(t, acme, srv.URL+"/skills", createBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var created skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	resp.Body.Close()
	if created.SkillID == "" || created.Status != skill.StatusDraft {
		t.Errorf("created = %+v, want assigned id + draft status", created)
	}
	id := created.SkillID

	// duplicate code -> conflict
	resp = postAs(t, acme, srv.URL+"/skills", createBody)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("dup create status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	resp.Body.Close()

	// a tenant skill with no owner_tenant_id lands in the caller's tenant
	pinnedBody := `{"code":"pinned","name":"P","scope":"tenant"}`
	resp = postAs(t, acme, srv.URL+"/skills", pinnedBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("tenant-default create status = %d, want 201", resp.StatusCode)
	}
	var pinned skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&pinned); err != nil {
		t.Fatalf("decode pinned: %v", err)
	}
	resp.Body.Close()
	if pinned.OwnerTenantID == nil || *pinned.OwnerTenantID != "acme" {
		t.Errorf("owner_tenant_id = %v, want acme (pinned to the caller)", pinned.OwnerTenantID)
	}

	// add a version and publish it
	versionBody := `{"version":1,"content_md":"# 工单分诊\n先看优先级再分派"}`
	resp = postAs(t, acme, srv.URL+"/skills/"+id+"/versions", versionBody)
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("create version status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	resp = postAs(t, acme, srv.URL+"/skills/"+id+"/versions/1/publish", "")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("publish status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var pub map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&pub); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	resp.Body.Close()
	if pub["version"] != 1 {
		t.Errorf("published version = %d, want 1", pub["version"])
	}

	// publish a missing version -> not found
	resp = postAs(t, acme, srv.URL+"/skills/"+id+"/versions/99/publish", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("publish missing status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()

	// list versions -> one, published
	resp = getAs(t, acme, srv.URL+"/skills/"+id+"/versions")
	var versions []skill.SkillVersion
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		t.Fatalf("decode versions: %v", err)
	}
	resp.Body.Close()
	if len(versions) != 1 || versions[0].Status != skill.StatusPublished {
		t.Errorf("versions = %+v, want one published", versions)
	}

	// the caller's tenant list sees its own skills, whatever filter is asked for
	resp = getAs(t, acme, srv.URL+"/skills?tenant_id=globex")
	var all []skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(all) != 2 {
		t.Errorf("list = %+v, want the two acme skills (foreign filter ignored)", all)
	}

	// update name
	resp = doAs(t, acme, http.MethodPut, srv.URL+"/skills/"+id,
		`{"name":"智能分诊","description":"updated","status":"published"}`)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("update status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()

	// delete then not found
	resp = doAs(t, acme, http.MethodDelete, srv.URL+"/skills/"+id, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	resp.Body.Close()

	resp = getAs(t, acme, srv.URL+"/skills/"+id)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("get after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()
}

// TestSkillAPIMemberCreatesTenantSkill pins the member contribution rule: an
// employee may author a tenant skill, it lands in their own tenant, defaults to
// private, and carries them as the author.
func TestSkillAPIMemberCreatesTenantSkill(t *testing.T) {
	srv := newSkillServer(t)
	defer srv.Close()

	alice := clientAs(memberClaims("acme", "alice"))
	bob := clientAs(memberClaims("acme", "bob"))

	resp := postAs(t, alice, srv.URL+"/skills", `{"code":"alice-skill","name":"A","scope":"tenant"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("member create status = %d, want 201", resp.StatusCode)
	}
	var created skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if created.OwnerTenantID == nil || *created.OwnerTenantID != "acme" {
		t.Errorf("owner_tenant_id = %v, want acme", created.OwnerTenantID)
	}
	if created.CreatedBy != "alice" {
		t.Errorf("created_by = %q, want alice", created.CreatedBy)
	}
	if created.Visibility != "private" {
		t.Errorf("visibility = %q, want private by default", created.Visibility)
	}

	// A peer cannot see it until alice shares.
	resp = getAs(t, bob, srv.URL+"/skills")
	var bobList []skill.Skill
	_ = json.NewDecoder(resp.Body).Decode(&bobList)
	resp.Body.Close()
	if len(bobList) != 0 {
		t.Errorf("bob sees %+v, want nothing", bobList)
	}

	resp = doAs(t, alice, http.MethodPut, srv.URL+"/skills/"+created.SkillID,
		`{"name":"A","visibility":"shared"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("share status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = getAs(t, bob, srv.URL+"/skills")
	_ = json.NewDecoder(resp.Body).Decode(&bobList)
	resp.Body.Close()
	if len(bobList) != 1 {
		t.Fatalf("bob sees %+v after share, want the shared skill", bobList)
	}
	// Shared is read-only for the peer.
	resp = doAs(t, bob, http.MethodDelete, srv.URL+"/skills/"+created.SkillID, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("bob delete shared skill = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestSkillAPIGlobalScopeAndTenantIsolation covers the cross-tenant rules: a
// global skill is owner-only, and a tenant admin can never see or touch another
// tenant's skill even by guessing its id.
func TestSkillAPIGlobalScopeAndTenantIsolation(t *testing.T) {
	srv := newSkillServer(t)
	defer srv.Close()

	owner := clientAs(ownerClaims())
	acme := clientAs(adminClaims("acme"))
	globex := clientAs(adminClaims("globex"))

	global := `{"code":"sys-prompts","name":"Sys","scope":"global"}`
	resp := postAs(t, owner, srv.URL+"/skills", global)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create global status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	// A tenant admin may not mint a platform-wide skill.
	resp = postAs(t, acme, srv.URL+"/skills", `{"code":"sneaky","name":"S","scope":"global"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("tenant admin global create = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()

	// acme's own skill
	acmeBody := `{"code":"acme-triage","name":"AT","scope":"tenant","owner_tenant_id":"acme"}`
	resp = postAs(t, acme, srv.URL+"/skills", acmeBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create acme status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var acmeSkill skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&acmeSkill); err != nil {
		t.Fatalf("decode acme skill: %v", err)
	}
	resp.Body.Close()

	// acme asking for globex's tenant still only ever sees acme + global.
	resp = getAs(t, acme, srv.URL+"/skills?tenant_id=globex")
	var acmeList []skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&acmeList); err != nil {
		t.Fatalf("decode acme list: %v", err)
	}
	resp.Body.Close()
	for _, s := range acmeList {
		if s.Code == "acme-triage" {
			continue
		}
		if s.Scope != skill.ScopeGlobal {
			t.Errorf("acme list leaked %+v", s)
		}
	}

	// globex only sees the global skill.
	resp = getAs(t, globex, srv.URL+"/skills")
	var other []skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&other); err != nil {
		t.Fatalf("decode other: %v", err)
	}
	resp.Body.Close()
	if len(other) != 1 || other[0].Code != "sys-prompts" {
		t.Errorf("globex sees %+v, want only global skill", other)
	}

	// A foreign skill id is reported as missing on read, write and delete.
	for _, tc := range []struct{ method, body string }{
		{http.MethodGet, ""},
		{http.MethodPut, `{"name":"hijack"}`},
		{http.MethodDelete, ""},
		{http.MethodPost, ""}, // create version
	} {
		path := srv.URL + "/skills/" + acmeSkill.SkillID
		if tc.method == http.MethodPost {
			path += "/versions"
		}
		resp = doAs(t, globex, tc.method, path, tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s foreign skill = %d, want 404", tc.method, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// The foreign skill is still intact for its owner tenant.
	resp = getAs(t, acme, srv.URL+"/skills/"+acmeSkill.SkillID)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("acme get own skill = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}
