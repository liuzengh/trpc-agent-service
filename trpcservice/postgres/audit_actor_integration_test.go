//go:build integration

package postgres_test

import (
	"testing"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestControlPlaneMutationAuditUsesAuthenticatedActor(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	ctx := platformaudit.WithControlPlaneActor(p.ctx, "admin:operator", string("operator"))
	if _, err := p.store.SetChannelBindingStatus(ctx, p.scope.TenantID, p.scope.AppID, p.binding.BindingID, channels.BindingSuspended); err != nil {
		t.Fatalf("suspend binding: %v", err)
	}

	events, err := p.store.ListAuditEvents(ctx, p.scope.TenantID, p.scope.AppID, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	for _, event := range events {
		if event.EventType == platformaudit.ChannelSuspended {
			if event.ActorID != "admin:operator" || event.ActorRole != "operator" {
				t.Fatalf("mutation audit actor = %q/%q", event.ActorID, event.ActorRole)
			}
			return
		}
	}
	t.Fatal("channel suspension audit event not found")
}
