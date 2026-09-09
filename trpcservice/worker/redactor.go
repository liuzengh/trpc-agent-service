package worker

import (
	platformlog "github.com/Violet2314/trpc-agent-service/trpcservice/log"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const redactedValue = "${REDACTED}"

// PolicyRedactor applies built-in secret patterns plus tenant patterns.
type PolicyRedactor struct{}

// NewPolicyRedactor constructs the Worker output redactor.
func NewPolicyRedactor() *PolicyRedactor {
	return &PolicyRedactor{}
}

// Redact replaces matches without interpreting the replacement as a regexp
// capture reference.
func (r *PolicyRedactor) Redact(snapshot tenant.Snapshot, value string) string {
	return platformlog.NewRedactor(snapshot.Tenant.Policy.RedactPatterns...).Redact(value)
}
