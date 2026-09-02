package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
)

func TestSkillInstructionSplicesPublishedSkills(t *testing.T) {
	ctx := context.Background()
	sm := skill.NewManager()

	acme := "acme"
	s := &skill.Skill{Code: "triage", Name: "分诊", Scope: skill.ScopeTenant, OwnerTenantID: &acme}
	if err := sm.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := sm.CreateVersion(ctx, &skill.SkillVersion{
		SkillID: s.SkillID, Version: 1,
		ContentMD: "# 工单分诊\n先看优先级再分派",
	}); err != nil {
		t.Fatal(err)
	}
	if err := sm.PublishVersion(ctx, s.SkillID, 1); err != nil {
		t.Fatal(err)
	}

	w := &Worker{skills: sm}
	prof := agent.RuntimeProfile{SystemPrompt: "base prompt", SkillIDs: []string{s.SkillID}}
	got := w.skillInstruction(ctx, prof)
	if !strings.Contains(got, "===== Skill: triage (v1) =====") {
		t.Errorf("missing skill header in %q", got)
	}
	if !strings.Contains(got, "先看优先级再分派") {
		t.Errorf("missing content_md in %q", got)
	}
	if !strings.Contains(got, "===== End Skill: triage =====") {
		t.Errorf("missing end marker in %q", got)
	}
}

func TestSkillInstructionEmptyWhenNilOrNone(t *testing.T) {
	ctx := context.Background()

	// no skills manager
	wNil := &Worker{}
	if got := wNil.skillInstruction(ctx, agent.RuntimeProfile{}); got != "" {
		t.Errorf("nil skills should return empty, got %q", got)
	}

	// manager but no skill ids
	sm := skill.NewManager()
	w := &Worker{skills: sm}
	if got := w.skillInstruction(ctx, agent.RuntimeProfile{SkillIDs: nil}); got != "" {
		t.Errorf("no skill ids should return empty, got %q", got)
	}

	// unpublished skill is skipped
	acme := "acme"
	s := &skill.Skill{Code: "draft", Name: "D", Scope: skill.ScopeTenant, OwnerTenantID: &acme}
	_ = sm.Create(ctx, s)
	if got := w.skillInstruction(ctx, agent.RuntimeProfile{SkillIDs: []string{s.SkillID}}); got != "" {
		t.Errorf("unpublished skill should be skipped, got %q", got)
	}
}

// keep llm import used for parity if future tests build agents
var _ = llm.ScopeTenant
