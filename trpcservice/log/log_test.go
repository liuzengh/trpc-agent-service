package log

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedactorRemovesCanaryFromTextAndStructures(t *testing.T) {
	const canary = "canary-secret-123"
	redactor := NewRedactor([]string{"custom_secret"}, []string{canary})
	value := redactor.RedactString("Authorization: Bearer " + canary + " api_key=" + canary)
	if strings.Contains(value, canary) {
		t.Fatalf("text leaked: %s", value)
	}
	jsonText := redactor.RedactString(`{"token":"unregistered-canary","password":"another-canary"}`)
	if strings.Contains(jsonText, "unregistered-canary") || strings.Contains(jsonText, "another-canary") {
		t.Fatalf("JSON credential leaked: %s", jsonText)
	}
	mapped := redactor.RedactMap(map[string]any{"custom_secret": canary, "nested": map[string]any{"message": "password=" + canary}})
	payload, _ := json.Marshal(mapped)
	if strings.Contains(string(payload), canary) {
		t.Fatalf("map leaked: %s", payload)
	}
}
