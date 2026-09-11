package domain

import (
	"slices"
	"testing"
	"time"
)

func TestBindingTrafficLifecycle(t *testing.T) {
	a := fixtureAccount(t)
	b, err := NewBinding("chb_a", "usr_owner", a, target("stable"), testTime)
	if err != nil {
		t.Fatal(err)
	}

	inputSubjects := []string{"usr_b", "usr_a"}
	next, changed, err := b.SetTraffic(1, "rol_a", target("canary"), 1250, inputSubjects, testTime.Add(time.Second))
	if err != nil || !changed || next.Revision != 2 || next.Traffic == nil {
		t.Fatalf("set traffic: %+v changed=%v err=%v", next, changed, err)
	}
	if !slices.Equal(next.Traffic.CanarySubjects, []string{"usr_a", "usr_b"}) {
		t.Fatalf("subjects are not canonical: %v", next.Traffic.CanarySubjects)
	}
	inputSubjects[0] = "usr_mutated"
	if !slices.Equal(next.Traffic.CanarySubjects, []string{"usr_a", "usr_b"}) {
		t.Fatal("binding retained caller-owned subject storage")
	}

	// Updating the same candidate keeps the assignment salt, so percentage
	// changes do not reshuffle the whole cohort.
	updated, changed, err := next.SetTraffic(2, "rol_unused", target("canary"), 2500, []string{"usr_b", "usr_a"}, testTime.Add(2*time.Second))
	if err != nil || !changed || updated.Revision != 3 || updated.Traffic.ID != "rol_a" {
		t.Fatalf("update traffic: %+v changed=%v err=%v", updated, changed, err)
	}
	same, changed, err := updated.SetTraffic(3, "rol_other", target("canary"), 2500, []string{"usr_a", "usr_b"}, testTime.Add(3*time.Second))
	if err != nil || changed || same.Revision != 3 {
		t.Fatalf("semantic no-op: %+v changed=%v err=%v", same, changed, err)
	}

	stable, changed, err := updated.SetTarget(3, target("rollback"), testTime.Add(4*time.Second))
	if err != nil || !changed || stable.Revision != 4 || stable.Traffic != nil {
		t.Fatalf("stable retarget must clear rollout: %+v changed=%v err=%v", stable, changed, err)
	}
}

func TestBindingTrafficRejectsUnsafePolicies(t *testing.T) {
	a := fixtureAccount(t)
	b, _ := NewBinding("chb_a", "usr_owner", a, target("stable"), testTime)

	for name, tc := range map[string]struct {
		id         string
		candidate  PublishedTarget
		percentage int64
		subjects   []string
	}{
		"same-target":    {"rol_a", target("stable"), 100, []string{}},
		"bad-percentage": {"rol_a", target("canary"), 10001, []string{}},
		"duplicate":      {"rol_a", target("canary"), 100, []string{"usr_a", "usr_a"}},
		"nil-subjects":   {"rol_a", target("canary"), 100, nil},
		"bad-rollout-id": {"bad rollout", target("canary"), 100, []string{}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := b.SetTraffic(1, tc.id, tc.candidate, tc.percentage, tc.subjects, testTime)
			assertCode(t, err, InputInvalid)
		})
	}
}

func TestEnabledRouteCarriesCompleteTrafficSnapshot(t *testing.T) {
	a := fixtureAccount(t)
	a.Enabled = true
	b, _ := NewBinding("chb_a", "usr_owner", a, target("stable"), testTime)
	b.Enabled = true
	b, _, err := b.SetTraffic(1, "rol_a", target("canary"), 500, []string{"usr_a"}, testTime)
	if err != nil {
		t.Fatal(err)
	}

	state, changed, err := AdvanceRoute(RouteState{TenantID: a.TenantID, AccountID: a.ID}, a, &b, "evt_a")
	if err != nil || !changed || state.Projection.Route.Traffic == nil {
		t.Fatalf("project traffic: %+v changed=%v err=%v", state, changed, err)
	}
	if state.Projection.Route.Traffic.Target.ManifestID != "rmf_canary" || state.Projection.Route.Traffic.PercentageBasisPoints != 500 {
		t.Fatalf("incomplete traffic snapshot: %+v", state.Projection.Route.Traffic)
	}
	raw, _, err := state.Projection.Encode()
	if err != nil || len(raw) > MaxRouteEventBytes {
		t.Fatalf("route event size=%d err=%v", len(raw), err)
	}
}
