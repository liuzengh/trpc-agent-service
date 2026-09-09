package tool

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// notCallableTool satisfies Tool but not CallableTool, to exercise the Call
// type-check branch.
type notCallableTool struct{ name string }

func (t notCallableTool) Declaration() *ttool.Declaration {
	return &ttool.Declaration{Name: t.name, Description: "not callable"}
}

func TestAllReturnsEveryTool(t *testing.T) {
	r := testRegistry()
	got := toolNames(r.All())
	if !equalNames(got, "alpha", "beta", "gamma") {
		t.Fatalf("want all tools, got %v", got)
	}
}

func TestIsDangerous(t *testing.T) {
	r := testRegistry()
	if !r.IsDangerous("gamma") {
		t.Fatal("gamma is registered dangerous")
	}
	if r.IsDangerous("alpha") {
		t.Fatal("alpha must not be dangerous")
	}
	if r.IsDangerous("nope") {
		t.Fatal("unknown tool must not be dangerous")
	}
}

func TestCallExecutesArguments(t *testing.T) {
	r := testRegistry()
	res, err := r.Call(context.Background(), "alpha", []byte(`{"x":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got["tool"] != "alpha" || got["x"] != "hi" {
		t.Fatalf("unexpected result: %v", got)
	}
}

func TestCallUnknownTool(t *testing.T) {
	r := testRegistry()
	if _, err := r.Call(context.Background(), "nope", []byte(`{}`)); err == nil {
		t.Fatal("unknown tool must error")
	} else if want := `unknown tool "nope"`; err.Error() != want {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallNotCallable(t *testing.T) {
	r := NewRegistry(Tool{Tool: notCallableTool{name: "opaque"}})
	if _, err := r.Call(context.Background(), "opaque", []byte(`{}`)); err == nil {
		t.Fatal("non-callable tool must error")
	} else if !strings.Contains(err.Error(), "not callable") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// DemoTools ships one safe and one dangerous demo tool, and both run.
func TestDemoTools(t *testing.T) {
	r := DemoTools()
	names := toolNames(r.All())
	sort.Strings(names)
	if !equalNames(names, "delete_user_data", "get_weather") {
		t.Fatalf("unexpected demo tools: %v", names)
	}
	if !r.IsDangerous("delete_user_data") {
		t.Fatal("delete_user_data must be marked dangerous")
	}
	if r.IsDangerous("get_weather") {
		t.Fatal("get_weather must be safe")
	}

	res, err := r.Call(context.Background(), "get_weather", []byte(`{"city":"上海"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(res)
	var w map[string]string
	if err := json.Unmarshal(data, &w); err != nil {
		t.Fatal(err)
	}
	if w["city"] != "上海" || w["weather"] == "" {
		t.Fatalf("unexpected weather result: %v", w)
	}

	res, err = r.Call(context.Background(), "delete_user_data", []byte(`{"user_id":"u9"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(res)
	var d map[string]any
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d["user_id"] != "u9" || d["deleted"] != true {
		t.Fatalf("unexpected delete result: %v", d)
	}
}
