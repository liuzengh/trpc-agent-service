package tool

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMCPRedactionPreservesStructuredAndEmbeddedJSON(t *testing.T) {
	cfg := MCPCredential{URL: "https://mcp.example.invalid/mcp", BearerToken: "bearer-secret-canary"}
	inner, _ := json.Marshal(map[string]any{"excerpt": "Authorization: Bearer hidden-value\nRedis Session survives \"quotes\".", "password": "inner-secret-canary"})
	payload := map[string]any{"content": []any{map[string]any{"type": "text", "text": string(inner)}}, "structured_content": map[string]any{"url": cfg.URL, "api_key": "key-secret-canary", "value": cfg.BearerToken, "safe": "Redis Session"}}
	clean, err := scrubMCPValue(payload, cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(clean)
	if err != nil || !json.Valid(raw) {
		t.Fatal("sanitizer broke JSON", err)
	}
	for _, forbidden := range []string{cfg.URL, cfg.BearerToken, "inner-secret-canary", "key-secret-canary", "hidden-value"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("MCP result leaked a canary")
		}
	}
	text := clean.(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	var decoded map[string]any
	if json.Unmarshal([]byte(text), &decoded) != nil || !strings.Contains(decoded["excerpt"].(string), "Redis Session") {
		t.Fatal("embedded JSON text was corrupted")
	}
}
func TestMCPRedactionRejectsExcessiveNesting(t *testing.T) {
	var nested any = "leaf"
	for range 70 {
		nested = []any{nested}
	}
	if _, err := scrubMCPValue(nested, MCPCredential{}, 0); err == nil {
		t.Fatal("unbounded nesting accepted")
	}
}
