package audit

import "testing"

func validAudit() AuditLog {
	return AuditLog{TenantID: "tenant-a", AuditID: "audit-a", TraceID: "trace-a", RequestID: "request-a", ExecutionID: "execution-a", Channel: "web", Decision: "allow", Metadata: map[string]string{"component": "worker"}}
}

func TestAuditValidationAndRedaction(t *testing.T) {
	value := validAudit()
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	input := map[string]string{"api_key": "secret", "component": "worker"}
	output := RedactMetadata(input)
	if output["api_key"] != "[REDACTED]" || input["api_key"] != "secret" {
		t.Fatalf("redaction mutated input: input=%v output=%v", input, output)
	}
	value.Metadata = output
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAuditRejectsNegativeCostAndSensitiveMetadata(t *testing.T) {
	value := validAudit()
	value.CostCents = -1
	if err := value.Validate(); err == nil {
		t.Fatal("expected negative cost rejection")
	}
	value = validAudit()
	value.Metadata = map[string]string{"authorization": "Bearer abc"}
	if err := value.Validate(); err == nil {
		t.Fatal("expected sensitive metadata rejection")
	}
}
