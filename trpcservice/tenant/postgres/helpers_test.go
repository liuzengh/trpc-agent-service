package postgres

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantPostgresBoundaryHelpers(t *testing.T) {
	if !validTenantTransition(tenant.StatusActive, tenant.StatusSuspended) || !validTenantTransition(tenant.StatusActive, tenant.StatusDisabled) ||
		!validTenantTransition(tenant.StatusSuspended, tenant.StatusActive) || !validTenantTransition(tenant.StatusSuspended, tenant.StatusDisabled) ||
		validTenantTransition(tenant.StatusActive, tenant.StatusActive) {
		t.Fatal("tenant transition helper is incorrect")
	}
	if nullableText("") != nil || nullableText("value") != "value" {
		t.Fatal("nullable text helper is incorrect")
	}
}
