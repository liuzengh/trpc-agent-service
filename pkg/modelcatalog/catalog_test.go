package modelcatalog

import "testing"

func TestResolveUsesExactOperatorApprovedModelIDs(t *testing.T) {
	profile, ok := Resolve("openai", "gpt-4o-mini")
	if !ok || profile.ContextWindow != 128_000 || profile.MaxOutputTokens != 16_384 || profile.Revision != Revision {
		t.Fatalf("profile=%+v ok=%v", profile, ok)
	}

	for _, test := range []struct {
		provider string
		model    string
	}{
		{provider: "openai", model: "GPT-4"},
		{provider: "openai", model: " gpt-4"},
		{provider: "openai", model: "gpt-4 "},
		{provider: "openai", model: "gpt-4-evil"},
		{provider: "openai", model: "gpt-4o-mini-2026-01-01"},
		{provider: "openai", model: "unknown"},
		{provider: "other", model: "gpt-4"},
	} {
		if profile, ok := Resolve(test.provider, test.model); ok {
			t.Fatalf("unapproved model resolved: provider=%q model=%q profile=%+v", test.provider, test.model, profile)
		}
	}

	gpt4, ok := Resolve("openai", "gpt-4")
	if !ok || gpt4.ContextWindow != 8_192 || gpt4.MaxOutputTokens != 8_192 || gpt4.Revision != Revision {
		t.Fatalf("gpt-4 profile=%+v ok=%v", gpt4, ok)
	}
}
