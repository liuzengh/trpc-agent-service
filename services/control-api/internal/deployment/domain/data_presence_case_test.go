package domain

import (
	"encoding/json"
	"os"
	"testing"
)

func TestDataPresenceRejectsCaseAliasesOnValidLegacyManifest(t *testing.T) {
	raw, err := os.ReadFile("testdata/p0-legacy-canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Canonical string `json:"canonical"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeManifestContent([]byte(fixture.Canonical)); err != nil {
		t.Fatal("invalid baseline", err)
	}
	for _, key := range []string{"Memory", "ARTIFACT", "ADD_SESSION_SUMMARY", "MeMoRy", "Runtime", "RUNTIME"} {
		t.Run(key, func(t *testing.T) {
			var c map[string]any
			_ = json.Unmarshal([]byte(fixture.Canonical), &c)
			if key == "Runtime" || key == "RUNTIME" {
				c[key] = nil
			} else {
				for _, n := range c["agent_plan"].(map[string]any)["nodes"].(map[string]any) {
					node := n.(map[string]any)
					if node["kind"] == "llm" {
						node[key] = nil
						break
					}
				}
			}
			changed, _ := json.Marshal(c)
			if _, err := DecodeManifestContent(changed); err == nil {
				t.Fatal("noncanonical case alias accepted")
			}
		})
	}
}
