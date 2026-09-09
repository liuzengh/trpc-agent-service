package governance

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestMaskingPreservesLargeIntegerBusinessID(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	input := struct {
		ID    int64  `json:"id"`
		Email string `json:"email"`
	}{ID: 9007199254740993, Email: "alice@example.com"}
	output, err := filter.AfterToolInvocation(context.Background(), "lookup", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"id":9007199254740993`) {
		t.Fatalf("masking changed an exact business ID: %s", encoded)
	}
	if strings.Contains(string(encoded), input.Email) {
		t.Fatalf("masking retained the sensitive email: %s", encoded)
	}
}

func TestMaskingPreservesNestedJSONNumbers(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	input := json.RawMessage(`{"records":[{"id":18446744073709551615,"balance":1234567890.123456789,"email":"alice@example.com"}],"exponent":1e400}`)
	output, err := filter.AfterToolInvocation(context.Background(), "lookup", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"id":18446744073709551615`, `"balance":1234567890.123456789`, `"exponent":1e400`, `"email":"a***@example.com"`} {
		if !strings.Contains(string(encoded), expected) {
			t.Fatalf("masked result lost %s: %s", expected, encoded)
		}
	}
}
