package runtimeplane

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestArtifactMigrationAdmissionMatchesCompatibilityMetadata(t *testing.T) {
	for _, targetLimit := range []int{1 << 19, 1 << 20, 1 << 21} {
		t.Run(strconv.Itoa(targetLimit), func(t *testing.T) {
			var definitions []map[string]interface{}
			if err := json.Unmarshal([]byte(validProfileManifest), &definitions); err != nil {
				t.Fatal(err)
			}
			target := make(map[string]interface{}, len(definitions[1]))
			for key, value := range definitions[1] {
				target[key] = value
			}
			target["id"], target["bucket"], target["maxBytes"] = "artifact-target", "different-bucket", targetLimit
			definitions = append(definitions, target)
			manifest, err := json.Marshal(definitions)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := LoadProfiles(string(manifest), func(string) (string, bool) { return "test-only", true })
			if err != nil {
				t.Fatal(err)
			}
			sourceInfo, err := catalog.MigrationProfile("tenant-a", "artifact-local", "s3")
			if err != nil {
				t.Fatal(err)
			}
			targetInfo, err := catalog.MigrationProfile("tenant-a", "artifact-target", "s3")
			if err != nil {
				t.Fatal(err)
			}
			err = catalog.ValidateMigrationProfiles("tenant-a", "s3", "artifact-local", "artifact-target")
			if sourceInfo.Compatibility == targetInfo.Compatibility {
				if err != nil {
					t.Fatalf("compatible artifact profiles rejected: %v", err)
				}
			} else if err == nil {
				t.Fatal("admitted artifact profiles have incompatible metadata and will fail live migration creation")
			}
		})
	}
}
