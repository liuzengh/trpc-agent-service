package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
)

// TestAgentAPISyncGrantsPersistsToolAndSkill is the regression guard for the
// IM-tool-unavailable bug: publishing an agent must persist its tool grants
// (agent_tool_grants) and skill bindings (agent_skills), otherwise the worker's
// IsAllowed RBAC check drops every mounted tool.
func TestAgentAPISyncGrantsPersistsToolAndSkill(t *testing.T) {
	ctx := context.Background()
	agentMgr := agent.NewManager(llm.NewRegistry(nil))
	toolReg := tool.NewRegistry()
	skillMgr := skill.NewManager()

	// Register a tool + create/publish a skill (current_version = 1).
	if err := toolReg.Register(ctx, tool.Definition{ID: "echo", Name: "echo", RiskLevel: tool.RiskLow}); err != nil {
		t.Fatal(err)
	}
	sk := &skill.Skill{Code: "sk1", Name: "S1", Scope: skill.ScopeGlobal}
	if err := skillMgr.Create(ctx, sk); err != nil {
		t.Fatal(err)
	}
	if err := skillMgr.CreateVersion(ctx, &skill.SkillVersion{SkillID: sk.SkillID, Version: 1, ContentMD: "# SKILL\nhello"}); err != nil {
		t.Fatal(err)
	}
	if err := skillMgr.PublishVersion(ctx, sk.SkillID, 1); err != nil {
		t.Fatal(err)
	}

	api := NewAgentAPI(agentMgr)
	api.SetGrants(toolReg, skillMgr)
	api.syncGrants(ctx, "a1", agent.RuntimeProfile{
		ToolIDs:  []string{"echo"},
		SkillIDs: []string{sk.SkillID},
	})

	// Tool grant persisted -> worker IsAllowed passes and the tool mounts.
	allowed, err := toolReg.IsAllowed(ctx, "a1", "echo")
	if err != nil || !allowed {
		t.Errorf("tool grant not persisted: allowed=%v err=%v", allowed, err)
	}

	// Skill binding persisted with the locked current version.
	bindings, err := skillMgr.ListAgentSkills(ctx, "a1")
	if err != nil || len(bindings) != 1 {
		t.Fatalf("skill bindings = %d (err=%v), want 1", len(bindings), err)
	}
	if bindings[0].Version != 1 {
		t.Errorf("skill binding version = %d, want 1", bindings[0].Version)
	}
}

// TestAgentGrayReleaseIsTenantScopedAndValidated covers the canary API: the
// release is an asset write (so it follows the same row-level permission as
// publish), and a bad target is rejected instead of silently doing nothing.
func TestAgentGrayReleaseIsTenantScopedAndValidated(t *testing.T) {
	ctx := context.Background()
	agentMgr := agent.NewManager(llm.NewRegistry(nil))
	for _, a := range []agent.Agent{
		{ID: "a-acme", TenantID: "acme", Name: "acme"},
		{ID: "a-globex", TenantID: "globex", Name: "globex"},
	} {
		if err := agentMgr.Create(ctx, a); err != nil {
			t.Fatal(err)
		}
		if _, err := agentMgr.Publish(ctx, a.ID, agent.RuntimeProfile{SystemPrompt: "v1"}); err != nil {
			t.Fatal(err)
		}
		if _, err := agentMgr.Publish(ctx, a.ID, agent.RuntimeProfile{SystemPrompt: "v2"}); err != nil {
			t.Fatal(err)
		}
	}

	mux := http.NewServeMux()
	NewAgentAPI(agentMgr).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	acme := clientAs(adminClaims("acme"))
	globex := clientAs(adminClaims("globex"))

	// A foreign tenant's agent is invisible (404, not a confirmation).
	resp := doAs(t, globex, http.MethodPut, srv.URL+"/agents/a-acme/gray", `{"version":1,"percent":10}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("cross-tenant gray = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Own agent, valid release.
	resp = doAs(t, acme, http.MethodPut, srv.URL+"/agents/a-acme/gray", `{"version":1,"percent":10}`)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("valid gray = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	ag, err := agentMgr.Get(ctx, "a-acme")
	if err != nil {
		t.Fatal(err)
	}
	if ag.Gray == nil || ag.Gray.Version != 1 || ag.Gray.Percent != 10 {
		t.Fatalf("stored gray = %+v, want {1 10}", ag.Gray)
	}

	// Unpublished target: rejected with a client error, not a silent no-op.
	resp = doAs(t, acme, http.MethodPut, srv.URL+"/agents/a-acme/gray", `{"version":7,"percent":10}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unpublished gray target = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Clearing is the rollback and works over the API.
	resp = doAs(t, acme, http.MethodDelete, srv.URL+"/agents/a-acme/gray", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("clear gray = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
	ag, _ = agentMgr.Get(ctx, "a-acme")
	if ag.Gray != nil {
		t.Errorf("gray = %+v after clearing, want nil", ag.Gray)
	}
}
