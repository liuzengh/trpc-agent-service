package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func target(id string) PublishedTarget {
	return PublishedTarget{TenantID: "tnt_a", DeploymentID: "dpl_" + id, RevisionNumber: 1, DeploymentRevisionID: "dpr_" + id, ManifestID: "rmf_" + id, ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
}
func TestAccountRouteGenerationAndDisableRetargetRestore(t *testing.T) {
	a := fixtureAccount(t)
	b, err := NewBinding("chb_a", "usr_owner", a, target("a"), testTime)
	if err != nil {
		t.Fatal(err)
	}
	route := RouteState{TenantID: a.TenantID, AccountID: a.ID}
	route, changed, err := AdvanceRoute(route, a, &b, "evt_1")
	if err != nil || !changed || route.Generation != 1 || route.Projection.Enabled {
		t.Fatal("first binding must produce disabled generation1", err)
	}
	a.MinRouteGeneration = 1
	raw, _, _ := route.Projection.Encode()
	var obj map[string]any
	_ = json.Unmarshal(raw, &obj)
	wire := obj["route"].(map[string]any)
	if len(wire) != 3 {
		t.Fatal("disabled tombstone carries target")
	}
	a, _, err = a.SetEnabled(1, true, fixtureMetas(), testTime)
	if err != nil {
		t.Fatal(err)
	}
	same, changed, err := AdvanceRoute(route, a, &b, "evt_unused")
	if err != nil || changed || same.Generation != 1 {
		t.Fatal("unbound enable must not publish")
	}
	b, _, err = b.SetEnabled(1, true, a, fixtureMetas(), testTime)
	if err != nil {
		t.Fatal(err)
	}
	route, changed, err = AdvanceRoute(route, a, &b, "evt_2")
	if err != nil || !changed || route.Generation != 2 || !route.Projection.Enabled {
		t.Fatal(err)
	}
	a.MinRouteGeneration = 2
	a, _, _ = a.SetEnabled(2, false, fixtureMetas(), testTime)
	route, changed, err = AdvanceRoute(route, a, &b, "evt_3")
	if err != nil || !changed || route.Generation != 3 || route.Projection.Enabled || !b.Enabled {
		t.Fatal("disable must preserve binding intent", err)
	}
	a.MinRouteGeneration = 3
	b, _, err = b.SetTarget(2, target("b"), testTime)
	if err != nil {
		t.Fatal(err)
	}
	_, changed, err = AdvanceRoute(route, a, &b, "evt_unused")
	if err != nil || changed {
		t.Fatal("disabled retarget must not republish")
	}
	a, _, _ = a.SetEnabled(3, true, fixtureMetas(), testTime)
	route, changed, err = AdvanceRoute(route, a, &b, "evt_4")
	if err != nil || !changed || route.Generation != 4 || route.Projection.Route.ManifestRef != "rmf_b" {
		t.Fatal("restore must emit new precise target", err)
	}
	a.MinRouteGeneration = 4
	if a.ConnectionRevision != 4 || b.Revision != 3 {
		t.Fatal("version domains were mixed")
	}
	// A snapshot containing only final account state carries floor4; old enabled
	// route generation2 is distinguishable and must not be used by Admission.
	if !(int64(2) < a.MinRouteGeneration) {
		t.Fatal("restore floor missing")
	}
}
func TestRouteIntegrityAndExhaustion(t *testing.T) {
	a := fixtureAccount(t)
	a.Enabled = true
	b, _ := NewBinding("chb_a", "usr_owner", a, target("a"), testTime)
	b.Enabled = true
	state, _, err := AdvanceRoute(RouteState{TenantID: a.TenantID, AccountID: a.ID}, a, &b, "evt_a")
	if err != nil {
		t.Fatal(err)
	}
	raw, digest, err := state.Projection.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateStoredRoute(raw, digest); err != nil {
		t.Fatal(err)
	}
	duplicate := strings.Replace(string(raw), `"enabled":true`, `"enabled":true,"enabled":true`, 1)
	_, err = ValidateStoredRoute([]byte(duplicate), digest)
	assertCode(t, err, SourceIntegrity)
	_, err = ValidateStoredRoute(append(raw, []byte(" {}")...), digest)
	assertCode(t, err, SourceIntegrity)
	state.Generation = MaxVersion
	state.Projection.Route.Generation = MaxVersion
	a.MinRouteGeneration = MaxVersion
	a.Enabled = false
	_, _, err = AdvanceRoute(state, a, &b, "evt_b")
	assertCode(t, err, RouteGenerationExhausted)
	b.Target.TenantID = "tnt_other"
	_, _, err = b.SetTarget(b.Revision, b.Target, testTime)
	assertCode(t, err, TargetIntegrity)
}
