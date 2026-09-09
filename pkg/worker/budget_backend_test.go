package worker

import (
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestNewWorkerRejectsConfiguredBudgetWithoutRedis(t *testing.T) {
	for _, budget := range []tenant.BudgetConfig{
		{MaxTokensPerDay: 20_000, MaxTokensPerRequest: 8_192},
		{MaxConcurrentSessions: 1},
	} {
		tenantConfig := &tenant.Tenant{
			ID:         "budgeted-tenant",
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
			Budget:     budget,
		}
		if _, err := NewWorker(tenantConfig, nil, nil); err == nil || !strings.Contains(err.Error(), "redis is required") {
			t.Fatalf("budget=%+v was accepted without Redis: %v", budget, err)
		}
	}
}
