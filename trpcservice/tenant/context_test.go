package tenant

import "testing"

func TestTenantContextRequiresTrustedSource(t *testing.T) {
	c := TenantContext{TenantID: "t", TenantVersion: 1, AgentAppID: "a", SubjectID: "u", Channel: "fake"}
	if c.Validate() == nil {
		t.Fatal("expected rejection")
	}
	c.TrustedSource = "binding:test"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestExecutionBindingRequiresEveryVersion(t *testing.T) {
	b := ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	b.PolicyVersion = 0
	if b.Validate() == nil {
		t.Fatal("expected rejection")
	}
}
