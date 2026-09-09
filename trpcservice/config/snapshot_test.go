package config

import (
	"strings"
	"testing"
)

func TestRuntimeSnapshotReturnsDefensiveCopies(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	snapshot, err := file.Snapshot("tenant-a", "support")
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.TenantID() != "tenant-a" || snapshot.AppID() != "support" {
		t.Fatalf("unexpected snapshot identity: %q/%q", snapshot.TenantID(), snapshot.AppID())
	}
	if snapshot.Version() != 3 {
		t.Fatalf("Version() = %d", snapshot.Version())
	}

	first := snapshot.App()
	*first.Model.Temperature = 1.8
	first.Tools.Allow[0] = "mutated"
	first.Channels[0].ID = "mutated"
	firstAudit := snapshot.Audit()
	firstAudit.RedactFields[0] = "mutated"

	second := snapshot.App()
	secondAudit := snapshot.Audit()
	if *second.Model.Temperature != 0.2 {
		t.Fatalf("snapshot temperature mutated: %v", *second.Model.Temperature)
	}
	if second.Tools.Allow[0] != "calculator" {
		t.Fatalf("snapshot tools mutated: %v", second.Tools.Allow)
	}
	if second.Channels[0].ID != "http-a" {
		t.Fatalf("snapshot channels mutated: %v", second.Channels)
	}
	if secondAudit.RedactFields[0] != "authorization" {
		t.Fatalf("snapshot audit mutated: %v", secondAudit.RedactFields)
	}
}

func TestRuntimeSnapshotRejectsMissingScope(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if _, err := file.Snapshot("missing", "support"); err == nil {
		t.Fatal("Snapshot() missing tenant error = nil")
	}
	if _, err := file.Snapshot("tenant-a", "missing"); err == nil {
		t.Fatal("Snapshot() missing app error = nil")
	}
}

func TestRuntimeSnapshotRejectsDisabledScope(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	file.Tenants[0].Enabled = false
	if _, err := file.Snapshot("tenant-a", "support"); err == nil ||
		!strings.Contains(err.Error(), "tenant \"tenant-a\" is disabled") {
		t.Fatalf("Snapshot() disabled tenant error = %v", err)
	}

	file.Tenants[0].Enabled = true
	file.Tenants[0].Apps[0].Enabled = false
	if _, err := file.Snapshot("tenant-a", "support"); err == nil ||
		!strings.Contains(err.Error(), "app \"support\" is disabled") {
		t.Fatalf("Snapshot() disabled app error = %v", err)
	}
}
