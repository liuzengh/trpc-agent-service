package application_test

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"testing"
)

type storeSpy struct {
	calls  int
	err    error
	route  domain.RouteSnapshot
	health domain.ProjectionHealth
}

func (s *storeSpy) BeginReplay(context.Context, domain.ReplaySource) error   { s.calls++; return s.err }
func (s *storeSpy) ObserveSource(context.Context, domain.ReplaySource) error { s.calls++; return s.err }
func (s *storeSpy) ApplyFromStream(context.Context, domain.StreamPosition, domain.RouteEvent) error {
	s.calls++
	return s.err
}
func (s *storeSpy) Quarantine(context.Context, domain.StreamPosition, domain.QuarantineReason, string) error {
	s.calls++
	return s.err
}
func (s *storeSpy) QueryProjectionHealth(context.Context) (domain.ProjectionHealth, error) {
	return s.health, s.err
}
func (s *storeSpy) Resolve(context.Context, string, string) (domain.RouteSnapshot, error) {
	s.calls++
	return s.route, s.err
}
func TestRejectUnsequencedWrites(t *testing.T) {
	store := &storeSpy{}
	service, err := application.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Apply(context.Background(), domain.RouteEvent{}); !errors.Is(err, domain.ErrStreamPositionRequired) {
		t.Fatal(err)
	}
	if err = service.ApplyFromStream(context.Background(), domain.StreamPosition{}, domain.RouteEvent{}); !errors.Is(err, domain.ErrStreamPositionRequired) {
		t.Fatal(err)
	}
	if _, err = service.Resolve(context.Background(), "telegram", ""); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal(err)
	}
	if store.calls != 0 {
		t.Fatal("invalid input reached persistence")
	}
	if _, err = application.NewService(nil); err == nil {
		t.Fatal("nil store accepted")
	}
}
func TestServicePreservesReadinessAndErrors(t *testing.T) {
	store := &storeSpy{health: domain.ProjectionHealth{Initialized: true}}
	service, _ := application.NewService(store)
	health, err := service.QueryProjectionHealth(context.Background())
	if err != nil || !health.Initialized {
		t.Fatalf("health=%#v %v", health, err)
	}
	store.err = domain.ErrProjectionBlocked
	p := domain.StreamPosition{StreamName: "CONTROL_ROUTES", StreamID: "2026-09-05T00:00:00Z", Sequence: 1}
	if err = service.ApplyFromStream(context.Background(), p, domain.RouteEvent{}); !errors.Is(err, domain.ErrProjectionBlocked) {
		t.Fatal(err)
	}
}

func TestResolveForSelectsBeforeReturningRoute(t *testing.T) {
	stable := domain.RouteSnapshot{
		Provider:             "telegram",
		AccountID:            "account-1",
		TenantID:             "tenant-1",
		BindingID:            "binding-1",
		Generation:           7,
		DeploymentRevisionID: "revision-stable",
		ManifestRef:          "manifest/revision-stable",
		ManifestDigest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Traffic: &domain.TrafficRollout{
			RolloutID: "rollout-1",
			Target: domain.PublishedTarget{
				TenantID:             "tenant-1",
				DeploymentID:         "deployment-canary",
				RevisionNumber:       2,
				DeploymentRevisionID: "revision-canary",
				ManifestRef:          "manifest/revision-canary",
				ManifestDigest:       "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			PercentageBasisPoints: 1,
			CanarySubjects:        []string{"user-1"},
		},
	}
	store := &storeSpy{route: stable}
	service, _ := application.NewService(store)
	selected, canary, err := service.ResolveFor(context.Background(), "telegram", "account-1", domain.Cohort{ConversationID: "conversation-1", SenderID: "user-1"})
	if err != nil || !canary || selected.DeploymentRevisionID != "revision-canary" || selected.Traffic != nil || selected.RolloutVariant != "canary" || store.calls != 1 {
		t.Fatalf("selected=%+v canary=%v calls=%d err=%v", selected, canary, store.calls, err)
	}
}
