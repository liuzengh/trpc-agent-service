package agent

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestDefaultInvocationFactoryUsesResolvedActorRole(t *testing.T) {
	invocation, err := DefaultInvocationFactory(WithActorRole(context.Background(), "admin"), tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", ConfigVersion: 1, Tools: config.ToolPolicy{AllowedRoles: map[string][]string{"query_order": []string{"admin"}}}}}, "web-console", channels.InboundMessage{}, "tenant-a/support/session/session-1")
	if err != nil {
		t.Fatalf("DefaultInvocationFactory() error = %v", err)
	}
	if invocation.Execution.Role != "admin" {
		t.Fatalf("role = %q, want admin", invocation.Execution.Role)
	}
	if roles := invocation.Execution.ToolRoles["query_order"]; len(roles) != 1 || roles[0] != "admin" {
		t.Fatalf("tool roles = %#v, want query_order/admin", invocation.Execution.ToolRoles)
	}
}
