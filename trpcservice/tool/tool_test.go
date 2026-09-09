package tool

import (
	"context"
	"testing"

	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestRegistry(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(Entry{Tool: testTool{name: "safe"}}); err != nil {
		t.Fatalf("Register(safe) error = %v", err)
	}
	if err := registry.Register(Entry{Tool: testTool{name: "danger"}, Dangerous: true}); err != nil {
		t.Fatalf("Register(danger) error = %v", err)
	}
	if err := registry.Register(Entry{Tool: testTool{name: "safe"}}); err == nil {
		t.Fatal("Register() accepted duplicate name")
	}
	tools, err := registry.Tools(context.Background(), tenant.Snapshot{})
	if err != nil {
		t.Fatalf("Tools() error = %v", err)
	}
	if len(tools) != 2 ||
		tools[0].Declaration().Name != "danger" ||
		tools[1].Declaration().Name != "safe" {
		t.Fatalf("Tools() order/content = %#v", tools)
	}
	if !registry.IsDangerous("danger") || registry.IsDangerous("safe") {
		t.Fatal("IsDangerous() returned wrong metadata")
	}
	if value, ok := registry.Lookup("danger"); !ok || value.Declaration().Name != "danger" {
		t.Fatalf("Lookup(danger) = %#v, %v", value, ok)
	}
}

type testTool struct {
	name string
}

func (t testTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{
		Name:        t.name,
		Description: t.name,
		InputSchema: &agenttool.Schema{Type: "object"},
	}
}
