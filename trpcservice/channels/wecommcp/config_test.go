package wecommcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBindingRequiresExplicitScopeAndNoInlineCredential(t *testing.T) {
	good := fixtureBinding()
	if _, err := ParseBinding(good); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(map[string]any){
		func(m map[string]any) { delete(m, "allowed_chat_ids") }, func(m map[string]any) { m["allowed_user_ids"] = []string{} }, func(m map[string]any) { m["allowed_chat_ids"] = []string{"*"} }, func(m map[string]any) { m["allowed_user_ids"] = []string{"human", "human"} }, func(m map[string]any) { m["dedupe_mode"] = "exactly-once" }, func(m map[string]any) { delete(m, "dedupe_mode") }, func(m map[string]any) { m["mention_prefix"] = "" }, func(m map[string]any) { m["timezone"] = "guess" }, func(m map[string]any) { m["start_at"] = "yesterday" }, func(m map[string]any) { m["mcp_url"] = "credential-canary" },
	} {
		var data map[string]any
		_ = json.Unmarshal(good.Config, &data)
		mutate(data)
		input := good
		input.Config, _ = json.Marshal(data)
		if _, err := ParseBinding(input); err == nil || strings.Contains(err.Error(), "credential-canary") {
			t.Fatal("unsafe config accepted or leaked")
		}
	}
	good.SecretRef = fixtureEndpoint
	if _, err := ParseBinding(good); err == nil {
		t.Fatal("raw endpoint accepted as secret reference")
	}
}
