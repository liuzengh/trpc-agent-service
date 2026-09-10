package admin

import (
	"context"
	"testing"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

func TestWithPrincipalPropagatesStableAuditActorWithoutCredential(t *testing.T) {
	ctx := withPrincipal(context.Background(), AdminPrincipal{
		Role:    RoleOperator,
		ActorID: "admin:operator",
	})
	actorID, actorRole, ok := platformaudit.ControlPlaneActorFromContext(ctx)
	if !ok || actorID != "admin:operator" || actorRole != string(RoleOperator) {
		t.Fatalf("control-plane actor = %q/%q ok=%t", actorID, actorRole, ok)
	}
	if principal, ok := PrincipalFromContext(ctx); !ok || principal.ActorID != actorID {
		t.Fatalf("principal = %#v ok=%t", principal, ok)
	}
}
