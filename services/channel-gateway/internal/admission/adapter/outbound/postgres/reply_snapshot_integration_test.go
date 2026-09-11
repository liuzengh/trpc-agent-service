package postgresadapter_test

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	"testing"
)

func TestReplySnapshotKeepsOriginalTargetAfterRouteDisabled(t *testing.T) {
	s, _, routes := setup(t)
	ctx := context.Background()
	c := acceptance("reply-read")
	if _, err := s.Commit(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := applyRoute(ctx, routes, routeEvent(2, false)); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadReplySnapshot(ctx, c.Receipt.AdmissionID, c.Receipt.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConversationID != "100" || got.SourceMessageID != "42" || got.ManifestDigest != c.Route.ManifestDigest || got.TenantID != "tenant" || got.Origin != nil {
		t.Fatalf("reply snapshot drifted: %+v", got)
	}
	if _, err = s.ReadReplySnapshot(ctx, c.Receipt.AdmissionID, "other-run"); !errors.Is(err, domain.ErrReplyNotFound) {
		t.Fatalf("cross-run lookup succeeded: %v", err)
	}
}
