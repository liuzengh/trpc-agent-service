package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
)

func validTenant() Tenant {
	return Tenant{
		TenantID: "tenant-id", TenantKey: "tenant-a", DisplayName: "Tenant A", Status: StatusActive,
		BillingCurrency: "USD", AuditRetentionDays: 30, AuditPayloadMode: AuditRedacted, LogMaskingLevel: MaskingStrict,
		TraceSamplingRate: 0.25, DefaultAgentAppID: "app", DefaultBackendProfileID: "backend", ActiveConfigVersion: 1,
		Version: 1, CreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(1, 0).UTC(),
		RedactionRules: []redaction.Rule{{ID: "customer-ref", TextPattern: `CUST-[0-9]{6}`}},
	}
}

func TestTenantValidateFailsClosedOnIsolationAndPolicyViolations(t *testing.T) {
	if err := validTenant().Validate(); err != nil {
		t.Fatalf("valid tenant rejected: %v", err)
	}
	negativeTokens := int64(-1)
	negativeConcurrency := -1
	tests := []struct {
		name   string
		mutate func(*Tenant)
	}{
		{"missing id", func(value *Tenant) { value.TenantID = "" }},
		{"unsafe key", func(value *Tenant) { value.TenantKey = "Tenant_A" }},
		{"unknown status", func(value *Tenant) { value.Status = "deleting" }},
		{"negative budget", func(value *Tenant) { value.MonthlyTokenBudget = &negativeTokens }},
		{"negative concurrency", func(value *Tenant) { value.MaxConcurrentExecutions = &negativeConcurrency }},
		{"invalid currency", func(value *Tenant) { value.BillingCurrency = "usd" }},
		{"invalid retention", func(value *Tenant) { value.AuditRetentionDays = 0 }},
		{"invalid payload mode", func(value *Tenant) { value.AuditPayloadMode = "full_plaintext" }},
		{"invalid masking", func(value *Tenant) { value.LogMaskingLevel = "off" }},
		{"invalid sampling", func(value *Tenant) { value.TraceSamplingRate = 1.01 }},
		{"invalid custom redaction", func(value *Tenant) { value.RedactionRules = []redaction.Rule{{ID: "broken", TextPattern: "("}} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validTenant()
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestTenantContextBindsCompiledStrictRedactionProgram(t *testing.T) {
	value := validTenant()
	ctx, err := value.ContextWithRedaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	program, ok := redaction.ProgramFromContext(ctx)
	if !ok || !program.RedactKey("external_user_id") {
		t.Fatalf("program=%v ok=%t", program, ok)
	}
	if got := program.RedactText("customer CUST-123456 token=canary"); strings.Contains(got, "CUST-123456") || strings.Contains(got, "canary") {
		t.Fatalf("redaction leaked=%q", got)
	}
	value.RedactionRules = []redaction.Rule{{ID: "bad rule", TextPattern: "secret"}}
	if _, err := value.ContextWithRedaction(context.Background()); err == nil {
		t.Fatalf("invalid policy context err=%v", err)
	}
}

func TestChangeMetadataRequiresTraceableActorAndReason(t *testing.T) {
	valid := ChangeMetadata{ActorType: "operator", ActorID: "user-1", ReasonCode: "policy_change", CorrelationID: "request-1", TraceID: "trace-1"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, clear := range []func(*ChangeMetadata){
		func(value *ChangeMetadata) { value.ActorType = "" },
		func(value *ChangeMetadata) { value.ActorID = "" },
		func(value *ChangeMetadata) { value.ReasonCode = "" },
		func(value *ChangeMetadata) { value.CorrelationID = "" },
		func(value *ChangeMetadata) { value.TraceID = "" },
	} {
		value := valid
		clear(&value)
		if !errors.Is(value.Validate(), ErrInvalid) {
			t.Fatalf("metadata accepted: %#v", value)
		}
	}
}
