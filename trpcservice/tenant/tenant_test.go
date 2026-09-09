package tenant_test

import (
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ParseGuardrailPolicy decodes tenant.guardrail_policy; empty or invalid
// input yields the zero value (platform defaults).
func TestParseGuardrailPolicy(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
		want tenant.GuardrailPolicy
	}{
		{"nil", nil, tenant.GuardrailPolicy{}},
		{"empty", json.RawMessage{}, tenant.GuardrailPolicy{}},
		{"valid", json.RawMessage(`{"input_allow_users":["u1","u2"],"input_deny_words":["spam"],"output_deny_words":["leak"],"max_tokens_per_day":1000}`),
			tenant.GuardrailPolicy{
				InputAllowUsers: []string{"u1", "u2"},
				InputDenyWords:  []string{"spam"},
				OutputDenyWords: []string{"leak"},
				MaxTokensPerDay: 1000,
			}},
		{"invalid json", json.RawMessage(`{not json`), tenant.GuardrailPolicy{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tenant.ParseGuardrailPolicy(tc.raw)
			if len(got.InputAllowUsers) != len(tc.want.InputAllowUsers) ||
				len(got.InputDenyWords) != len(tc.want.InputDenyWords) ||
				len(got.OutputDenyWords) != len(tc.want.OutputDenyWords) ||
				got.MaxTokensPerDay != tc.want.MaxTokensPerDay {
				t.Fatalf("ParseGuardrailPolicy(%s):\n got %+v\nwant %+v", tc.raw, got, tc.want)
			}
		})
	}
}

// ParseRateLimit decodes tenant.rate_policy; an empty or invalid policy means
// every limit stays at the platform default (zero value).
func TestParseRateLimit(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
		want tenant.RateLimit
	}{
		{"nil", nil, tenant.RateLimit{}},
		{"empty", json.RawMessage(``), tenant.RateLimit{}},
		{"valid", json.RawMessage(`{"qps":50,"burst":100,"send_qps":2.5,"send_burst":7}`),
			tenant.RateLimit{QPS: 50, Burst: 100, SendQPS: 2.5, SendBurst: 7}},
		{"invalid json", json.RawMessage(`{`), tenant.RateLimit{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tenant.ParseRateLimit(tc.raw); got != tc.want {
				t.Fatalf("ParseRateLimit(%s):\n got %+v\nwant %+v", tc.raw, got, tc.want)
			}
		})
	}
}
