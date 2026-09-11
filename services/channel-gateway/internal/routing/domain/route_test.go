package domain_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

func enabled() domain.RouteEvent {
	return domain.RouteEvent{EventID: "evt-1", SchemaVersion: 1, Enabled: true, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", TenantID: "tenant-1", BindingID: "binding-1", Generation: 7, DeploymentRevisionID: "revision-1", ManifestRef: "manifest/revision-1", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}}
}

func TestValidateTargetsAndTombstones(t *testing.T) {
	valid := enabled()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*domain.RouteEvent){
		"empty-event":         func(v *domain.RouteEvent) { v.EventID = "" },
		"event-too-long":      func(v *domain.RouteEvent) { v.EventID = strings.Repeat("e", 129) },
		"version":             func(v *domain.RouteEvent) { v.SchemaVersion = 2 },
		"provider":            func(v *domain.RouteEvent) { v.Route.Provider = "slack" },
		"account":             func(v *domain.RouteEvent) { v.Route.AccountID = "account with spaces" },
		"generation-zero":     func(v *domain.RouteEvent) { v.Route.Generation = 0 },
		"generation-overflow": func(v *domain.RouteEvent) { v.Route.Generation = domain.MaxGeneration + 1 },
		"tenant":              func(v *domain.RouteEvent) { v.Route.TenantID = "" },
		"binding":             func(v *domain.RouteEvent) { v.Route.BindingID = "" },
		"revision":            func(v *domain.RouteEvent) { v.Route.DeploymentRevisionID = "" },
		"manifest":            func(v *domain.RouteEvent) { v.Route.ManifestRef = "https://example.invalid/m?credential=x" },
		"manifest-too-long":   func(v *domain.RouteEvent) { v.Route.ManifestRef = strings.Repeat("m", 2049) },
		"digest":              func(v *domain.RouteEvent) { v.Route.ManifestDigest = "sha256:abc" },
		"tombstone-target":    func(v *domain.RouteEvent) { v.Enabled = false },
	} {
		t.Run(name, func(t *testing.T) {
			v := valid
			change(&v)
			if err := v.Validate(); !errors.Is(err, domain.ErrInvalidEvent) {
				t.Fatalf("got %v", err)
			}
		})
	}
	tombstone := domain.RouteEvent{EventID: "evt-disabled", SchemaVersion: 1, Route: domain.RouteSnapshot{Provider: "wecom", AccountID: "account-2", Generation: 8}}
	if err := tombstone.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonotonicProjectionAndStableDigests(t *testing.T) {
	current := enabled()
	digest := current.ProjectionDigest()
	replay := current
	replay.EventID = "evt-second-id"
	if replay.ProjectionDigest() != digest || replay.ReceiptDigest() == current.ReceiptDigest() {
		t.Fatal("event and projection identities conflated")
	}
	tests := []struct {
		name     string
		event    domain.RouteEvent
		want     domain.Change
		conflict bool
	}{
		{"first", current, domain.Replace, false},
		{"exact-replay", current, domain.Ignore, false},
		{"equivalent-new-event", replay, domain.Ignore, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			generation := current.Route.Generation
			if tc.name == "first" {
				generation = 0
			}
			got, err := domain.Compare(generation, digest, tc.event)
			if err != nil || got != tc.want {
				t.Fatalf("got %v %v", got, err)
			}
		})
	}
	changed := current
	changed.Route.BindingID = "binding-2"
	if _, err := domain.Compare(current.Route.Generation, digest, changed); !errors.Is(err, domain.ErrGenerationConflict) {
		t.Fatalf("same-generation update: %v", err)
	}
	disabled := domain.RouteEvent{EventID: "evt-disabled", SchemaVersion: 1, Route: domain.RouteSnapshot{Provider: "telegram", AccountID: "account-1", Generation: 8}}
	if change, err := domain.Compare(7, digest, disabled); err != nil || change != domain.Replace {
		t.Fatalf("disable: %v %v", change, err)
	}
	if change, err := domain.Compare(8, disabled.ProjectionDigest(), current); err != nil || change != domain.Ignore {
		t.Fatalf("stale: %v %v", change, err)
	}
	changed.Route.Generation = 9
	if change, err := domain.Compare(8, disabled.ProjectionDigest(), changed); err != nil || change != domain.Replace {
		t.Fatalf("reenable: %v %v", change, err)
	}
}

func rolloutRoute(percentage int64, subjects ...string) domain.RouteSnapshot {
	route := enabled().Route
	route.Traffic = &domain.TrafficRollout{
		RolloutID: "rollout-1",
		Target: domain.PublishedTarget{
			TenantID:             route.TenantID,
			DeploymentID:         "deployment-canary",
			RevisionNumber:       2,
			DeploymentRevisionID: "revision-canary",
			ManifestRef:          "manifest/revision-canary",
			ManifestDigest:       "sha256:" + strings.Repeat("c", 64),
		},
		PercentageBasisPoints: percentage,
		CanarySubjects:        append([]string{}, subjects...),
	}
	return route
}

func TestTrafficSelectionHonorsExplicitAndPercentageCohorts(t *testing.T) {
	cohort := domain.Cohort{ConversationID: "conversation-1", SenderID: "user-1"}

	stable, selected, err := rolloutRoute(0, "user-1").Select(cohort)
	if err != nil || selected || stable.DeploymentRevisionID != "revision-1" || stable.RolloutID != "rollout-1" || stable.RolloutVariant != "stable" || stable.Traffic != nil {
		t.Fatalf("paused rollout: %+v selected=%v err=%v", stable, selected, err)
	}

	explicit, selected, err := rolloutRoute(1, "user-1").Select(cohort)
	if err != nil || !selected || explicit.DeploymentRevisionID != "revision-canary" || explicit.ManifestRef != "manifest/revision-canary" || explicit.RolloutVariant != "canary" || explicit.Traffic != nil {
		t.Fatalf("explicit canary: %+v selected=%v err=%v", explicit, selected, err)
	}

	all, selected, err := rolloutRoute(10000).Select(domain.Cohort{ConversationID: "conversation-2", SenderID: "other-user"})
	if err != nil || !selected || all.DeploymentRevisionID != "revision-canary" {
		t.Fatalf("full rollout: %+v selected=%v err=%v", all, selected, err)
	}
}

func TestTrafficSelectionIsDeterministicAcrossRetriesAndConversations(t *testing.T) {
	route := rolloutRoute(3750)
	first, firstCanary, err := route.Select(domain.Cohort{ConversationID: "conversation-a", SenderID: "user-42"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		conversation := "conversation-a"
		if i%2 == 1 {
			conversation = "conversation-b"
		}
		got, canary, err := route.Select(domain.Cohort{ConversationID: conversation, SenderID: "user-42"})
		if err != nil || canary != firstCanary || got.DeploymentRevisionID != first.DeploymentRevisionID || got.ManifestRef != first.ManifestRef {
			t.Fatalf("selection moved at iteration %d: first=%+v/%v got=%+v/%v err=%v", i, first, firstCanary, got, canary, err)
		}
	}
}

func TestTrafficPolicyValidationRejectsAmbiguousInput(t *testing.T) {
	for name, change := range map[string]func(*domain.RouteSnapshot){
		"same revision": func(r *domain.RouteSnapshot) { r.Traffic.Target.DeploymentRevisionID = r.DeploymentRevisionID },
		"unsorted":      func(r *domain.RouteSnapshot) { r.Traffic.CanarySubjects = []string{"user-b", "user-a"} },
		"duplicate":     func(r *domain.RouteSnapshot) { r.Traffic.CanarySubjects = []string{"user-a", "user-a"} },
		"too high":      func(r *domain.RouteSnapshot) { r.Traffic.PercentageBasisPoints = 10001 },
		"other tenant":  func(r *domain.RouteSnapshot) { r.Traffic.Target.TenantID = "tenant-2" },
	} {
		t.Run(name, func(t *testing.T) {
			route := rolloutRoute(100, "user-a", "user-b")
			change(&route)
			if err := route.Validate(true); !errors.Is(err, domain.ErrInvalidEvent) {
				t.Fatalf("got %v", err)
			}
		})
	}
}
