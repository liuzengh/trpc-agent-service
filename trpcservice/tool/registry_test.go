package tool

import (
	"context"
	"testing"

	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type safeTestTool struct{}

func (*safeTestTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{Name: "safe.test", Description: "safe test tool", InputSchema: &frameworktool.Schema{Type: "object"}}
}

func (*safeTestTool) Call(context.Context, []byte) (any, error) { return "ok", nil }

func TestRegistryNonDangerousNamesExcludesDangerousTools(t *testing.T) {
	registry := NewRegistry()
	if names := registry.NonDangerousNames(); names == nil || len(names) != 0 {
		t.Fatalf("initial non-dangerous names = %#v", names)
	}
	if err := registry.Register(&safeTestTool{}); err != nil {
		t.Fatal(err)
	}
	names := registry.NonDangerousNames()
	if len(names) != 1 || names[0] != "safe.test" {
		t.Fatalf("non-dangerous names = %#v", names)
	}
}
