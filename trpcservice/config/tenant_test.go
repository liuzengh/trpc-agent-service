package config

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func validBundle() TenantBundle {
	return TenantBundle{
		Tenant: tenant.Tenant{
			ID:             "tenant-a",
			Name:           "Tenant A",
			Status:         tenant.StatusActive,
			ConfigVersion:  1,
			DefaultAgentID: "agent-a",
			Backend:        tenant.BackendPolicy{Session: "redis", Memory: "postgres", Vector: "qdrant", Object: "s3"},
		},
		Agents: []tenant.AgentApp{{
			TenantID:       "tenant-a",
			ID:             "agent-a",
			Name:           "Agent A",
			Version:        1,
			Status:         "released",
			ModelConfigRef: "secret://models/a",
		}},
		Bindings: []tenant.ChannelBinding{{
			TenantID:      "tenant-a",
			ID:            "binding-a",
			Channel:       "telegram",
			ExternalAppID: "bot-a",
			SecretRef:     "secret://telegram/a",
			Enabled:       true,
		}},
	}
}

func TestTenantBundleValidate(t *testing.T) {
	if err := validBundle().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantBundleRejectsCrossTenantMembers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantBundle)
		want   string
	}{
		{name: "agent", mutate: func(b *TenantBundle) { b.Agents[0].TenantID = "tenant-b" }, want: "different tenant"},
		{name: "binding", mutate: func(b *TenantBundle) { b.Bindings[0].TenantID = "tenant-b" }, want: "different tenant"},
		{name: "missing default", mutate: func(b *TenantBundle) { b.Tenant.DefaultAgentID = "other-agent" }, want: "default_agent_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := validBundle()
			test.mutate(&bundle)
			err := bundle.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
}
