package rebuild

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

func testProjection() vector.ProjectionConfig {
	return vector.ProjectionConfig{Model: "test-model", ModelVersion: "v1", SchemaVersion: "schema-v1", Dimension: 8}
}

func TestProjectionFingerprintStableAndBound(t *testing.T) {
	first, err := ProjectionFingerprint(testProjection())
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProjectionFingerprint(testProjection())
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("unstable fingerprint: %s vs %s", first, second)
	}
	changed := testProjection()
	changed.Dimension = 4
	other, err := ProjectionFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Fatal("dimension change did not move fingerprint")
	}
	if _, err := ProjectionFingerprint(vector.ProjectionConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config accepted: %v", err)
	}
}

func TestNewRunIDServerOwnedAndUnique(t *testing.T) {
	first := NewRunID("tenant-a", "abc", time.Unix(1, 100))
	second := NewRunID("tenant-a", "abc", time.Unix(1, 200))
	if first == second {
		t.Fatal("run ids collided")
	}
	if !validRunID(first) || !validRunID(second) {
		t.Fatalf("invalid run id format: %s", first)
	}
	if third := NewRunID("tenant-b", "abc", time.Unix(1, 100)); third == first {
		t.Fatal("tenant not bound into run id")
	}
}

func TestConfigDefaultsFailClosed(t *testing.T) {
	if _, err := (Config{}).WithDefaults(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config accepted: %v", err)
	}
	cfg := Config{Owner: "owner-a"}
	filled, err := cfg.WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if filled.BatchSize != 100 || filled.MaxBatches != 1000 || filled.MaxAttempts != 20 {
		t.Fatalf("unexpected defaults: %+v", filled)
	}
	tooBig := Config{Owner: "owner-a", BatchSize: 501}
	if _, err := tooBig.WithDefaults(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("oversized batch accepted: %v", err)
	}
}

func TestReconcileClassificationBuckets(t *testing.T) {
	report := Report{DryRun: true}
	report.add(ClassMissing, "doc-1")
	report.add(ClassStale, "doc-2")
	report.add(ClassOrphan, "doc-3")
	if report.Missing != 1 || report.Stale != 1 || report.Orphan != 1 {
		t.Fatalf("unexpected buckets: %+v", report)
	}
	if len(report.Classes[ClassMissing]) != 1 || report.Classes[ClassMissing][0] != "doc-1" {
		t.Fatalf("unexpected class members: %+v", report.Classes)
	}
}

func TestValidRunIDAndFingerprintRejectGarbage(t *testing.T) {
	if validRunID("not-a-run-id") || validRunID("vrr-v1-NOHEX") {
		t.Fatal("garbage run id accepted")
	}
	if validHex64("xyz") {
		t.Fatal("garbage fingerprint accepted")
	}
	tc := tenant.TenantContext{}
	_ = tc
}
