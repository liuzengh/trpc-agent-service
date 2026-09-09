package storagemigration

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNewJobAllowsPR23MigrationRoutes(t *testing.T) {
	cases := []struct {
		domain         Domain
		source, target tenant.BackendType
	}{
		{DomainSession, tenant.BackendPostgres, tenant.BackendRedis},
		{DomainSession, tenant.BackendRedis, tenant.BackendPostgres},
		{DomainArtifact, tenant.BackendPostgres, tenant.BackendS3},
		{DomainArtifact, tenant.BackendS3, tenant.BackendPostgres},
		{DomainKnowledge, tenant.BackendPostgres, tenant.BackendQdrant},
		{DomainKnowledge, tenant.BackendQdrant, tenant.BackendPostgres},
		{DomainMemory, tenant.BackendPostgres, tenant.BackendExternal},
	}
	for _, test := range cases {
		_, err := NewJob("tenant-a", "app-a", 1, test.domain, tenant.BackendConfig{Type: test.source}, tenant.BackendConfig{Type: test.target}, "operator")
		if err != nil {
			t.Fatalf("%s %s -> %s: %v", test.domain, test.source, test.target, err)
		}
	}
}

func TestNewJobRejectsUnsupportedCrossDomainRoute(t *testing.T) {
	_, err := NewJob("tenant-a", "app-a", 1, DomainKnowledge, tenant.BackendConfig{Type: tenant.BackendS3}, tenant.BackendConfig{Type: tenant.BackendPostgres}, "operator")
	if err == nil {
		t.Fatal("S3 is not a knowledge backend")
	}
}

func TestNewJobLedgerHashIncludesTenantAppAndDomain(t *testing.T) {
	route := tenant.BackendConfig{Type: tenant.BackendPostgres, Endpoint: "postgres://database/runtime"}
	first, _ := NewJob("tenant-a", "app", 1, DomainArtifact, route, route, "operator")
	second, _ := NewJob("tenant-b", "app", 1, DomainArtifact, route, route, "operator")
	third, _ := NewJob("tenant-a", "other", 1, DomainArtifact, route, route, "operator")
	if first.SourceRouteHash == second.SourceRouteHash || first.SourceRouteHash == third.SourceRouteHash {
		t.Fatal("migration ledger identity must include tenant and app scope")
	}
}
