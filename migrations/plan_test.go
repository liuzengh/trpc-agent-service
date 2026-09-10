package migrations

import (
	"strings"
	"testing"
)

func TestBuildPlanRequiresExactContiguousHistory(t *testing.T) {
	all := []Migration{
		{Version: "000001", Up: "BEGIN; SELECT 1; COMMIT;"},
		{Version: "000002", Up: "BEGIN; SELECT 2; COMMIT;"},
		{Version: "000003", Up: "BEGIN; SELECT 3; COMMIT;"},
	}
	applied := map[string]string{"000001": migrationChecksum(all[0].Up)}
	plan, err := buildPlan(all, applied, "000003")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Current != "000001" || plan.Target != "000003" || strings.Join(plan.Pending, ",") != "000002,000003" {
		t.Fatalf("plan=%+v", plan)
	}

	for name, state := range map[string]map[string]string{
		"gap":     {"000002": migrationChecksum(all[1].Up)},
		"unknown": {"000999": "digest"},
		"drift":   {"000001": "wrong"},
	} {
		if _, err := buildPlan(all, state, "000003"); err == nil {
			t.Fatalf("%s history must fail", name)
		}
	}
}

func TestBuildPlanIsForwardOnlyAndIdempotent(t *testing.T) {
	all := []Migration{
		{Version: "000001", Up: "BEGIN; SELECT 1; COMMIT;"},
		{Version: "000002", Up: "BEGIN; SELECT 2; COMMIT;"},
	}
	applied := map[string]string{
		"000001": migrationChecksum(all[0].Up),
		"000002": migrationChecksum(all[1].Up),
	}
	plan, err := buildPlan(all, applied, "000002")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Current != "000002" || len(plan.Pending) != 0 {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := buildPlan(all, applied, "000001"); err == nil || !strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("expected downgrade rejection, got %v", err)
	}
	if _, err := buildPlan(all, nil, "missing"); err == nil {
		t.Fatal("expected unknown target rejection")
	}
}

func TestBuildPlanRejectsChecksumDriftAfterBaselineCompression(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) == 0 {
		t.Fatal("expected embedded baseline")
	}
	// This models a released, compressed baseline: its recorded checksum is
	// immutable, so a later edit to the embedded SQL must be rejected.
	edited := append([]Migration(nil), all...)
	edited[0].Up += "\n-- accidental baseline rewrite"
	applied := map[string]string{all[0].Version: migrationChecksum(all[0].Up)}
	if _, err := buildPlan(edited, applied, edited[len(edited)-1].Version); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected baseline checksum drift rejection, got %v", err)
	}
}
