package agent

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
)

type fakeModel struct{}

func (f *fakeModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, nil
}

func (f *fakeModel) Info() model.Info { return model.Info{Name: "fake"} }

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	reg := llm.NewRegistry(func(_ context.Context, _ llm.Endpoint) (model.Model, error) {
		return &fakeModel{}, nil
	})
	reg.Upsert(context.Background(), llm.Endpoint{ID: "e1", ModelName: "m1", BaseURL: "http://x", APIKey: "k"})
	return NewManager(reg)
}

func TestManagerPublishRollbackResolve(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	if err := m.Create(ctx, Agent{ID: "a1", TenantID: "t1", Name: "first"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	v1, err := m.Publish(ctx, "a1", RuntimeProfile{SystemPrompt: "v1", EndpointID: "e1"})
	if err != nil {
		t.Fatalf("Publish v1: %v", err)
	}
	if _, err := m.Publish(ctx, "a1", RuntimeProfile{SystemPrompt: "v2", EndpointID: "e1"}); err != nil {
		t.Fatalf("Publish v2: %v", err)
	}

	p, err := m.Resolve(ctx, "a1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if p.SystemPrompt != "v2" {
		t.Errorf("current = %q, want v2", p.SystemPrompt)
	}

	if err := m.Rollback(ctx, "a1", v1); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	p, _ = m.Resolve(ctx, "a1")
	if p.SystemPrompt != "v1" {
		t.Errorf("after rollback = %q, want v1", p.SystemPrompt)
	}
}

func TestBuildAgent(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	if err := m.Create(ctx, Agent{ID: "a1", TenantID: "t1", Name: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Publish(ctx, "a1", RuntimeProfile{SystemPrompt: "hi", EndpointID: "e1"}); err != nil {
		t.Fatal(err)
	}

	a, err := m.BuildAgent(ctx, "a1", nil)
	if err != nil {
		t.Fatalf("BuildAgent: %v", err)
	}
	if a == nil {
		t.Fatal("agent should be non-nil")
	}
	if a.Info().Name != "a1" {
		t.Errorf("name = %q, want a1", a.Info().Name)
	}
}

func TestResolveWithoutPublish(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	if err := m.Create(ctx, Agent{ID: "a1", TenantID: "t1", Name: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Resolve(ctx, "a1"); err == nil {
		t.Error("Resolve without publish should error")
	}
}

func TestManagerListUpdateVersions(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)

	_ = m.Create(ctx, Agent{ID: "a1", TenantID: "t1", Name: "one"})
	_ = m.Create(ctx, Agent{ID: "a2", TenantID: "t2", Name: "two"})

	all, err := m.List(ctx, "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("List(all) = %d, want 2", len(all))
	}

	onlyT1, err := m.List(ctx, "t1")
	if err != nil {
		t.Fatalf("List t1: %v", err)
	}
	if len(onlyT1) != 1 || onlyT1[0].ID != "a1" {
		t.Errorf("List(t1) = %+v, want only a1", onlyT1)
	}

	if err := m.Update(ctx, Agent{ID: "a1", TenantID: "t1", Name: "one-renamed", Description: "d"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	a, _ := m.Get(ctx, "a1")
	if a.Name != "one-renamed" {
		t.Errorf("after update name = %q", a.Name)
	}

	_, _ = m.Publish(ctx, "a1", RuntimeProfile{SystemPrompt: "v1", EndpointID: "e1"})
	_, _ = m.Publish(ctx, "a1", RuntimeProfile{SystemPrompt: "v2", EndpointID: "e1"})
	vs, err := m.Versions(ctx, "a1")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(vs) != 2 {
		t.Errorf("Versions = %d, want 2", len(vs))
	}
	if vs[1].Version != 2 {
		t.Errorf("second version = %d, want 2", vs[1].Version)
	}
}
