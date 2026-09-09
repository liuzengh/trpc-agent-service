package runtimeplane

import (
	"encoding/json"
	"testing"
)

func TestMigrationProfilesUsePhysicalIdentityAndEmbeddingCompatibility(t *testing.T) {
	var definitions []map[string]any
	if err := json.Unmarshal([]byte(validProfileManifest), &definitions); err != nil {
		t.Fatal(err)
	}
	clone := func(source map[string]any) map[string]any {
		out := map[string]any{}
		for key, value := range source {
			out[key] = value
		}
		return out
	}
	alias := clone(definitions[0])
	alias["id"] = "knowledge-alias"
	target := clone(definitions[0])
	target["id"] = "knowledge-target"
	target["collection"] = "different_collection"
	incompatible := clone(target)
	incompatible["id"] = "knowledge-incompatible"
	incompatible["dimension"] = 7
	definitions = append(definitions, alias, target, incompatible)
	body, err := json.Marshal(definitions)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadProfiles(string(body), func(string) (string, bool) { return "test-secret", true })
	if err != nil {
		t.Fatal(err)
	}
	source, _ := catalog.MigrationProfile("tenant-a", "knowledge-local", "qdrant")
	aliasInfo, _ := catalog.MigrationProfile("tenant-a", "knowledge-alias", "qdrant")
	targetInfo, _ := catalog.MigrationProfile("tenant-a", "knowledge-target", "qdrant")
	badInfo, _ := catalog.MigrationProfile("tenant-a", "knowledge-incompatible", "qdrant")
	if source.Identity != aliasInfo.Identity || source.Identity == targetInfo.Identity {
		t.Fatal("profile aliases changed physical identity")
	}
	if source.Compatibility != targetInfo.Compatibility || source.Compatibility == badInfo.Compatibility {
		t.Fatal("embedding compatibility omitted model dimensions")
	}
	if err := catalog.ValidateMigrationProfiles("tenant-a", "qdrant", "knowledge-local", "knowledge-target"); err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateMigrationProfiles("tenant-a", "qdrant", "knowledge-local", "knowledge-alias"); err == nil {
		t.Fatal("accepted same physical source and target")
	}
	if _, err := catalog.MigrationProfile("tenant-b", "knowledge-local", "qdrant"); err == nil {
		t.Fatal("migration resolver bypassed tenant allowlist")
	}
}
