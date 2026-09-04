package web

import (
	"bytes"
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
	return httptest.NewServer(mux)
}

func TestSkillAPILifecycle(t *testing.T) {
	srv := newSkillServer(t)
	defer srv.Close()

	// create a tenant-scoped skill
	createBody := `{"code":"triage","name":"分诊","description":"priority-first dispatch","scope":"tenant","owner_tenant_id":"acme"}`
	resp, err := http.Post(srv.URL+"/skills", "application/json", bytes.NewBufferString(createBody))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
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
	resp, err = http.Post(srv.URL+"/skills", "application/json", bytes.NewBufferString(createBody))
	if err != nil {
		t.Fatalf("dup create: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("dup create status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	resp.Body.Close()

	// invalid tenant-scoped skill without owner -> bad request
	badBody := `{"code":"orphan","name":"O","scope":"tenant"}`
	resp, err = http.Post(srv.URL+"/skills", "application/json", bytes.NewBufferString(badBody))
	if err != nil {
		t.Fatalf("bad create: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad create status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	resp.Body.Close()

	// add a version and publish it
	versionBody := `{"version":1,"content_md":"# 工单分诊\n先看优先级再分派"}`
	resp, err = http.Post(srv.URL+"/skills/"+id+"/versions", "application/json", bytes.NewBufferString(versionBody))
	if err != nil {
		t.Fatalf("create version: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("create version status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	resp, err = http.Post(srv.URL+"/skills/"+id+"/versions/1/publish", "application/json", nil)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
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
	resp, err = http.Post(srv.URL+"/skills/"+id+"/versions/99/publish", "application/json", nil)
	if err != nil {
		t.Fatalf("publish missing: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("publish missing status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()

	// list versions -> one, published
	resp, err = http.Get(srv.URL + "/skills/" + id + "/versions")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	var versions []skill.SkillVersion
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		t.Fatalf("decode versions: %v", err)
	}
	resp.Body.Close()
	if len(versions) != 1 || versions[0].Status != skill.StatusPublished {
		t.Errorf("versions = %+v, want one published", versions)
	}

	// tenant-scoped list sees the skill
	resp, err = http.Get(srv.URL + "/skills?tenant_id=acme")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var all []skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(all) != 1 || all[0].Code != "triage" {
		t.Errorf("list = %+v, want one triage skill", all)
	}

	// update name
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/skills/"+id,
		bytes.NewBufferString(`{"name":"智能分诊","description":"updated","status":"published"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("update status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()

	// delete then not found
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/skills/"+id, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/skills/" + id)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("get after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	resp.Body.Close()
}

func TestSkillAPIGlobalScopeAndTenantIsolation(t *testing.T) {
	srv := newSkillServer(t)
	defer srv.Close()

	global := `{"code":"sys-prompts","name":"Sys","scope":"global"}`
	resp, err := http.Post(srv.URL+"/skills", "application/json", bytes.NewBufferString(global))
	if err != nil {
		t.Fatalf("create global: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create global status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	// acme's own skill
	acmeBody := `{"code":"acme-triage","name":"AT","scope":"tenant","owner_tenant_id":"acme"}`
	resp, _ = http.Post(srv.URL+"/skills", "application/json", bytes.NewBufferString(acmeBody))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create acme status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	resp.Body.Close()

	// a different tenant only sees global skills
	resp, err = http.Get(srv.URL + "/skills?tenant_id=other")
	if err != nil {
		t.Fatalf("list other: %v", err)
	}
	var other []skill.Skill
	if err := json.NewDecoder(resp.Body).Decode(&other); err != nil {
		t.Fatalf("decode other: %v", err)
	}
	resp.Body.Close()
	if len(other) != 1 || other[0].Code != "sys-prompts" {
		t.Errorf("other tenant sees %+v, want only global skill", other)
	}
}
