// The behaviour assertions that used to live here are now the shared
// conformance suite in tenanttest, so the PostgreSQL implementation is held to
// exactly the same contract rather than to a copy of it.
//
// This file is an external test package (tenant_test) on purpose: tenanttest
// imports tenant, so an in-package test file importing tenanttest would be an
// import cycle.
package tenant_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/tenanttest"
)

// TestMemoryRepositoryConformance runs the whole Repository contract against
// the reference implementation. It needs no external service, so unlike the
// PostgreSQL run it is never gated.
func TestMemoryRepositoryConformance(t *testing.T) {
	tenanttest.RunRepositorySuite(t, func(t *testing.T) tenant.Repository {
		return tenant.NewMemoryRepository()
	})
}
