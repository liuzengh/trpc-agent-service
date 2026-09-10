package cos

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateBackend(t *testing.T) {
	t.Parallel()
	ref := tenant.BackendRef{
		Kind:     tenant.BackendObject,
		Provider: providerName,
		Name:     "tenant-artifacts",
		SecretRef: tenant.SecretRef{
			Name: "cos-credentials",
		},
	}
	if err := ValidateBackend(ref); err != nil {
		t.Fatalf("validate backend: %v", err)
	}
}
