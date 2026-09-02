package web

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
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
