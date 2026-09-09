package storage

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNewTestServicesRejectsEveryNonMemoryDomain(t *testing.T) {
	base := tenant.StorageProfile{
		Session: tenant.BackendConfig{Type: tenant.BackendInMemory}, Memory: tenant.BackendConfig{Type: tenant.BackendInMemory},
		Summary: tenant.BackendConfig{Type: tenant.BackendInMemory}, Artifact: tenant.BackendConfig{Type: tenant.BackendInMemory},
		Knowledge: tenant.BackendConfig{Type: tenant.BackendInMemory}, Audit: tenant.BackendConfig{Type: tenant.BackendInMemory},
	}
	tests := []struct {
		name   string
		mutate func(*tenant.StorageProfile)
	}{
		{"session", func(p *tenant.StorageProfile) { p.Session.Type = tenant.BackendRedis }},
		{"memory", func(p *tenant.StorageProfile) { p.Memory.Type = tenant.BackendRedis }},
		{"summary", func(p *tenant.StorageProfile) { p.Summary.Type = tenant.BackendRedis }},
		{"artifact", func(p *tenant.StorageProfile) { p.Artifact.Type = tenant.BackendLocal }},
		{"knowledge", func(p *tenant.StorageProfile) { p.Knowledge.Type = tenant.BackendQdrant }},
		{"audit", func(p *tenant.StorageProfile) { p.Audit.Type = tenant.BackendPostgres }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := base
			test.mutate(&profile)
			_, err := NewTestServices(profile)
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestValidatePostgresProfile(t *testing.T) {
	profile := tenant.StorageProfile{
		Session: tenant.BackendConfig{Type: tenant.BackendPostgres}, Memory: tenant.BackendConfig{Type: tenant.BackendPostgres},
		Summary: tenant.BackendConfig{Type: tenant.BackendPostgres}, Artifact: tenant.BackendConfig{Type: tenant.BackendPostgres},
		Knowledge: tenant.BackendConfig{Type: tenant.BackendPostgres}, Audit: tenant.BackendConfig{Type: tenant.BackendPostgres},
	}
	if err := ValidatePostgresProfile(profile); err != nil {
		t.Fatal(err)
	}
	profile.Knowledge.Type = tenant.BackendRedis
	if err := ValidatePostgresProfile(profile); err == nil || !strings.Contains(err.Error(), "knowledge") {
		t.Fatalf("error = %v", err)
	}
}
