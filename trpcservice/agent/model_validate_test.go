package agent

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestValidateModelSpec(t *testing.T) {
	cases := []struct {
		name    string
		baseURL string
		allowed []string
		wantErr bool
	}{
		{"empty inherits default", "", nil, false},
		{"default allowlist covers deepseek", "https://api.deepseek.com", nil, false},
		{"explicit allowlist match", "https://model.corp.internal/v1",
			[]string{"model.corp.internal"}, false},
		{"allowlist is exact host match", "https://evil.model.corp.internal",
			[]string{"model.corp.internal"}, true},
		{"off-allowlist host rejected", "https://attacker.example.com", nil, true},
		{"plain http rejected off loopback", "http://api.deepseek.com", nil, true},
		{"plain http rejected even on loopback", "http://127.0.0.1:8000", []string{"127.0.0.1"}, true},
		{"plain http rejected even on localhost", "http://localhost:8000", []string{"localhost"}, true},
		{"https loopback fine", "https://localhost:8000", []string{"localhost"}, false},
		{"missing host", "https://", nil, true},
		{"empty allowlist falls back to defaults", "https://api.deepseek.com", []string{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateModelSpec(ModelSpec{Name: "m", BaseURL: tc.baseURL}, tc.allowed)
			if tc.wantErr && err == nil {
				t.Fatalf("base_url %q must be rejected", tc.baseURL)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("base_url %q must be accepted: %v", tc.baseURL, err)
			}
		})
	}
}

func TestValidateModelConfigJSON(t *testing.T) {
	if err := ValidateModelConfig(nil, nil); err != nil {
		t.Fatalf("empty model_config must pass: %v", err)
	}
	if err := ValidateModelConfig(json.RawMessage(`{"base_url":"https://api.deepseek.com"}`), nil); err != nil {
		t.Fatalf("allowlisted endpoint must pass: %v", err)
	}
	err := ValidateModelConfig(json.RawMessage(`{"base_url":"https://attacker.example.com"}`), nil)
	if err == nil || !strings.Contains(err.Error(), "attacker.example.com") {
		t.Fatalf("off-allowlist endpoint must be rejected with the host named, got %v", err)
	}
	if err := ValidateModelConfig(json.RawMessage(`{not-json`), nil); err == nil {
		t.Fatal("unparseable model_config must be rejected, not silently defaulted")
	}
}

func TestValidateAppConfigJSON(t *testing.T) {
	ok := fmt.Sprintf(`{"prompt":"hi","model":{"name":"m","base_url":"https://%s"}}`, DefaultModelHosts()[0])
	if err := ValidateAppConfig(json.RawMessage(ok), nil); err != nil {
		t.Fatalf("allowlisted app config must pass: %v", err)
	}
	bad := `{"model":{"name":"m","base_url":"http://attacker.example.com"}}`
	if err := ValidateAppConfig(json.RawMessage(bad), nil); err == nil {
		t.Fatal("off-allowlist app config must be rejected")
	}
	if err := ValidateAppConfig(json.RawMessage(`{not-json`), nil); err == nil {
		t.Fatal("unparseable app config must be rejected")
	}
	if err := ValidateAppConfig(nil, nil); err != nil {
		t.Fatalf("empty app config must pass: %v", err)
	}
}
