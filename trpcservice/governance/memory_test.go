package governance

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDirectOnlyMemoryCannotBypassAudienceWithPreloadOrExtraction(t *testing.T) {
	for _, raw := range []string{
		`{"direct_only":true,"auto_extract":true,"every_turns":1}`,
		`{"auto_extract":true,"every_turns":0}`,
		`{"every_turns":-1}`, `{"direct_only":"true"}`, `{"unknown":true}`, `{} {}`,
	} {
		if _, err := ParseMemoryPolicy(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted invalid policy %s", raw)
		}
	}
	if err := ValidateMemoryPolicy(json.RawMessage(`{"preload_memory":5}`), json.RawMessage(`{"direct_only":true}`)); err == nil {
		t.Fatal("direct-only preload accepted")
	}
	if err := ValidateMemoryPolicy(json.RawMessage(`{}`), json.RawMessage(`{"direct_only":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestDirectOnlyMemoryKeepsOtherToolsAndDoesNotMutateRevision(t *testing.T) {
	allowed := []string{"echo", "memory_add", "memory_load", "read_attachment"}
	for _, chatType := range []string{"group", "", "unexpected"} {
		got := ScopeMemoryTools(allowed, MemoryPolicy{DirectOnly: true}, chatType)
		if !reflect.DeepEqual(got, []string{"echo", "read_attachment"}) {
			t.Fatalf("audience=%q tools=%v", chatType, got)
		}
	}
	if got := ScopeMemoryTools(allowed, MemoryPolicy{DirectOnly: true}, "direct"); !reflect.DeepEqual(got, allowed) {
		t.Fatal("direct tools filtered")
	}
	if len(allowed) != 4 || allowed[1] != "memory_add" {
		t.Fatal("original policy mutated")
	}
}
