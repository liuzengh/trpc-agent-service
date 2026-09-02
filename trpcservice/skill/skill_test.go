package skill

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManagerCreateGetList(t *testing.T) {
	m := NewManager()
	ctx := context.Background()

	s := &Skill{
		Code:          "triage",
		Name:          "工单分诊",
		Description:   "简单分类工单",
		Scope:         ScopeTenant,
		OwnerTenantID: ptr("acme"),
	}
	if err := m.Create(ctx, s); err != nil {
		t.Fatalf("create: %v", err)
	}
	if s.SkillID == "" {
		t.Error("SkillID must be assigned")
	}
	if s.CurrentVersion != 0 || s.Status != StatusDraft {
		t.Errorf("new skill defaults wrong: current_version=%d status=%q", s.CurrentVersion, s.Status)
	}

	got, err := m.Get(ctx, s.SkillID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Code != "triage" || got.Name != "工单分诊" {
		t.Errorf("round-trip mismatch: %+v", got)
	}

	list, err := m.List(ctx, "acme")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("list size = %d, want 1", len(list))
	}

	// Empty tenant = all + global visible
	list, _ = m.List(ctx, "")
	if len(list) != 1 {
		t.Errorf("list(all) size = %d, want 1 (global+tenant)", len(list))
	}
}

func TestManagerCodeUnique(t *testing.T) {
	m := NewManager()
	ctx := context.Background()

	if err := m.Create(ctx, &Skill{Code: "foo", Name: "A", Scope: ScopeTenant, OwnerTenantID: ptr("acme")}); err != nil {
		t.Fatalf("first: %v", err)
	}
	err := m.Create(ctx, &Skill{Code: "foo", Name: "B", Scope: ScopeTenant, OwnerTenantID: ptr("acme")})
	if !errors.Is(err, ErrSkillCodeExists) {
		t.Errorf("dup code error = %v, want ErrSkillCodeExists", err)
	}
}

func TestManagerPublishVersionSwitchesCurrent(t *testing.T) {
	m := NewManager()
	ctx := context.Background()

	s := &Skill{Code: "sv", Name: "S", Scope: ScopeTenant, OwnerTenantID: ptr("acme")}
	if err := m.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateVersion(ctx, &SkillVersion{SkillID: s.SkillID, Version: 1, ContentMD: "v1 body"}); err != nil {
		t.Fatal(err)
	}
	if err := m.PublishVersion(ctx, s.SkillID, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cur, _ := m.Get(ctx, s.SkillID)
	if cur.Status != StatusPublished || cur.CurrentVersion != 1 {
		t.Errorf("after publish: status=%q current=%d", cur.Status, cur.CurrentVersion)
	}

	// Publishing same version twice is a no-op but not an error.
	if err := m.PublishVersion(ctx, s.SkillID, 1); err != nil {
		t.Errorf("re-publish should be idempotent, got %v", err)
	}
}

func TestManagerBindAgentSkillLoadForAgent(t *testing.T) {
	m := NewManager()
	ctx := context.Background()

	s1 := &Skill{Code: "a", Name: "A", Scope: ScopeGlobal}
	s2 := &Skill{Code: "b", Name: "B", Scope: ScopeTenant, OwnerTenantID: ptr("acme")}
	if err := m.Create(ctx, s1); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(ctx, s2); err != nil {
		t.Fatal(err)
	}
	// publish v1 for each
	if err := m.CreateVersion(ctx, &SkillVersion{SkillID: s1.SkillID, Version: 1, ContentMD: "A-v1"}); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateVersion(ctx, &SkillVersion{SkillID: s2.SkillID, Version: 1, ContentMD: "B-v1"}); err != nil {
		t.Fatal(err)
	}
	_ = m.PublishVersion(ctx, s1.SkillID, 1)
	_ = m.PublishVersion(ctx, s2.SkillID, 1)

	const agentID = "agent-1"
	// bind s2 first (sort=0), then s1 (sort=1) -> s2 first by sort
	if err := m.BindAgentSkill(ctx, agentID, s2.SkillID, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.BindAgentSkill(ctx, agentID, s1.SkillID, 1, 1); err != nil {
		t.Fatal(err)
	}
	// Re-binding same (agent, skill) updates version + sort, not duplicate.
	if err := m.BindAgentSkill(ctx, agentID, s1.SkillID, 1, 5); err != nil {
		t.Fatal(err)
	}

	loaded, err := m.LoadForAgent(ctx, agentID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded = %d, want 2", len(loaded))
	}
	// sort 0 (s2) first, then s1
	if loaded[0].Code != "b" || loaded[1].Code != "a" {
		t.Errorf("order = %s,%s, want b,a", loaded[0].Code, loaded[1].Code)
	}
	if loaded[0].ContentMD != "B-v1" || loaded[1].ContentMD != "A-v1" {
		t.Errorf("content round-trip failed")
	}
}

func TestManagerUpdateAndDelete(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	s := &Skill{Code: "ud", Name: "UD", Scope: ScopeTenant, OwnerTenantID: ptr("acme")}
	if err := m.Create(ctx, s); err != nil {
		t.Fatal(err)
	}

	// Update name + description (no code change).
	got, _ := m.Get(ctx, s.SkillID)
	got.Name = "新名字"
	got.Description = "new desc"
	if err := m.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	r, _ := m.Get(ctx, s.SkillID)
	if r.Name != "新名字" || r.Description != "new desc" {
		t.Errorf("update didn't stick: %+v", r)
	}

	if err := m.Delete(ctx, s.SkillID); err != nil {
		t.Fatal(err)
	}
	_, err := m.Get(ctx, s.SkillID)
	if !errors.Is(err, ErrSkillNotFound) {
		t.Errorf("after delete get = %v, want ErrSkillNotFound", err)
	}
}

func TestManagerTenantFilter(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	_ = m.Create(ctx, &Skill{Code: "g1", Name: "G", Scope: ScopeGlobal})
	_ = m.Create(ctx, &Skill{Code: "t1", Name: "T1", Scope: ScopeTenant, OwnerTenantID: ptr("acme")})
	_ = m.Create(ctx, &Skill{Code: "t2", Name: "T2", Scope: ScopeTenant, OwnerTenantID: ptr("globex")})

	acme, _ := m.List(ctx, "acme")
	// acme sees global + own tenant
	if got := len(acme); got != 2 {
		t.Errorf("acme list = %d, want 2", got)
	}
	globex, _ := m.List(ctx, "globex")
	if got := len(globex); got != 2 {
		t.Errorf("globex list = %d, want 2", got)
	}
}

func TestManagerCreateVersionUnique(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	s := &Skill{Code: "vv", Name: "V", Scope: ScopeTenant, OwnerTenantID: ptr("acme")}
	if err := m.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	_ = m.CreateVersion(ctx, &SkillVersion{SkillID: s.SkillID, Version: 1, ContentMD: "a"})
	err := m.CreateVersion(ctx, &SkillVersion{SkillID: s.SkillID, Version: 1, ContentMD: "b"})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("dup version error = %v", err)
	}
}

// keep time referenced to avoid unused import when extending
var _ = time.Now

func ptr(s string) *string { return &s }
