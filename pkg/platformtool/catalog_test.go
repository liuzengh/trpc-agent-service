package platformtool

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type namedTool struct{ name string }

type nilNamedTool struct{}

func (*nilNamedTool) Declaration() *tool.Declaration { return nil }

type panicNamedTool struct{}

func (*panicNamedTool) Declaration() *tool.Declaration { panic("tool declaration secret") }

func (t *namedTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: t.name}
}

func TestCatalogResolveIsOrderedAndFailClosed(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.Register(&namedTool{name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(&namedTool{name: "b"}); err != nil {
		t.Fatal(err)
	}
	resolved, err := catalog.Resolve([]string{"b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved[0].Declaration().Name != "b" || resolved[1].Declaration().Name != "a" {
		t.Fatalf("tool order changed: %#v", resolved)
	}
	if _, err := catalog.Resolve([]string{"missing"}); err == nil {
		t.Fatal("unknown tool was accepted")
	}
	if _, err := catalog.Resolve([]string{"a", "a"}); err == nil {
		t.Fatal("duplicate tool was accepted")
	}
}

func TestCatalogRejectsTypedNilTool(t *testing.T) {
	var value *namedTool
	if err := NewCatalog().Register(value); err == nil {
		t.Fatal("typed-nil tool was accepted")
	}
}

func TestCatalogRejectsNilToolDeclaration(t *testing.T) {
	if err := NewCatalog().Register(&nilNamedTool{}); err == nil {
		t.Fatal("tool with a nil declaration was accepted")
	}
}

func TestCatalogRejectsPanickingToolDeclaration(t *testing.T) {
	if err := NewCatalog().Register(&panicNamedTool{}); err == nil {
		t.Fatal("tool with panicking declaration was accepted")
	}
}

func TestCurrentTimeToolRejectsUnknownTimezone(t *testing.T) {
	value := &currentTimeTool{}
	if _, err := value.Call(context.Background(), []byte(`{"timezone":"not/a-zone"}`)); err == nil {
		t.Fatal("invalid timezone was accepted")
	}
}

func TestFrameworkManagedToolClassification(t *testing.T) {
	for _, name := range []string{"memory_add", "memory_search", "knowledge_search"} {
		if !IsFrameworkManagedTool(name) {
			t.Fatalf("framework tool %q was not classified as managed", name)
		}
	}
	if IsFrameworkManagedTool("current_time") || IsFrameworkManagedTool("unknown") {
		t.Fatal("operator catalog tool was misclassified as framework-managed")
	}
}
