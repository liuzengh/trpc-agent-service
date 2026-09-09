package migrations

import (
	"strings"
	"testing"
)

func TestArtifactVersionMigrationDefinesTenantScopedIntegrityMetadata(t *testing.T) {
	up, err := files.ReadFile("039_artifact_versions.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(up)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS artifact_versions",
		"PRIMARY KEY (tenant_id, app_name, user_id, session_id, filename, version)",
		"content_sha256 CHAR(64) NOT NULL",
		"object_key TEXT NOT NULL UNIQUE",
		"deleted_at TIMESTAMPTZ",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("039 migration missing %q", required)
		}
	}
}
