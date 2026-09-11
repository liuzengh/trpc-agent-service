package migrations

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
)

func TestConfigControlPlaneEmbeddedMigration(t *testing.T) {
	body, err := ConfigControlPlaneDDL()
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS config_tenant_revisions",
		"CREATE TABLE IF NOT EXISTS config_tenant_state",
		"CREATE TABLE IF NOT EXISTS config_releases",
		"CREATE TABLE IF NOT EXISTS config_release_nodes",
		"CREATE TABLE IF NOT EXISTS config_release_events",
		"CREATE TABLE IF NOT EXISTS config_node_heartbeats",
		"active_pending_ack",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded control-plane migration is missing %q", required)
		}
	}
	decoded, err := hex.DecodeString(ConfigControlPlaneChecksum())
	if err != nil || len(decoded) != 32 {
		t.Fatalf("control-plane checksum is not SHA-256: %v", err)
	}
}

func TestVerifyConfigControlPlaneAppliedAndMismatch(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: ConfigControlPlaneChecksum()}}
	if err := VerifyConfigControlPlane(context.Background(), q); err != nil {
		t.Fatalf("VerifyConfigControlPlane() error = %v", err)
	}
	if q.version != ConfigControlPlaneVersion {
		t.Fatalf("queried version = %q, want %q", q.version, ConfigControlPlaneVersion)
	}
	q.row = verifierRow{checksum: "bad"}
	if err := VerifyConfigControlPlane(context.Background(), q); err == nil || !strings.Contains(err.Error(), ConfigControlPlaneVersion) {
		t.Fatalf("checksum mismatch = %v", err)
	}
}
