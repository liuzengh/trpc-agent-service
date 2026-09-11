package deploymentv1

import (
	"encoding/json"
	"testing"
)

func TestResolvedDataCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		target    func() any
		valid     bool
	}{
		{"memory tools", `{"resource":"memory","tools":["memory_add","memory_search"]}`, func() any { return new(ManifestMemory) }, true},
		{"memory all", `{"resource":"memory","tools":[],"preload_limit":-1}`, func() any { return new(ManifestMemory) }, true},
		{"memory adaptive entries", `{"resource":"memory","tools":[],"preload_limit":9007199254740991}`, func() any { return new(ManifestMemory) }, true},
		{"memory disabled no closure", `{"resource":"memory","tools":[],"preload_limit":0}`, func() any { return new(ManifestMemory) }, false},
		{"memory empty", `{"resource":"memory","tools":[]}`, func() any { return new(ManifestMemory) }, false},
		{"memory explicit zero with tools", `{"resource":"memory","tools":["memory_load"],"preload_limit":0}`, func() any { return new(ManifestMemory) }, true},
		{"memory unsafe integer", `{"resource":"memory","tools":[],"preload_limit":9007199254740992}`, func() any { return new(ManifestMemory) }, false},
		{"memory invalid negative", `{"resource":"memory","tools":[],"preload_limit":-2}`, func() any { return new(ManifestMemory) }, false},
		{"memory duplicate tools", `{"resource":"memory","tools":["memory_add","memory_add"]}`, func() any { return new(ManifestMemory) }, false},
		{"memory arbitrary tool", `{"resource":"memory","tools":["execute"]}`, func() any { return new(ManifestMemory) }, false},
		{"memory secret", `{"resource":"memory","tools":["memory_add"],"password":"secret"}`, func() any { return new(ManifestMemory) }, false},
		{"artifact enabled", `{"resource":"artifact","enabled":true}`, func() any { return new(ManifestArtifact) }, true},
		{"artifact disabled no closure", `{"resource":"artifact","enabled":false}`, func() any { return new(ManifestArtifact) }, false},
		{"artifact no implicit tools", `{"resource":"artifact","enabled":true,"tools":["upload"]}`, func() any { return new(ManifestArtifact) }, false},
		{"summary", `{"enabled":true,"model_resource":"primary","event_threshold":5}`, func() any { return new(ManifestSummary) }, true},
		{"summary missing threshold", `{"enabled":true,"model_resource":"primary"}`, func() any { return new(ManifestSummary) }, false},
		{"summary disabled no closure", `{"enabled":false,"model_resource":"primary","event_threshold":5}`, func() any { return new(ManifestSummary) }, false},
		{"summary zero threshold", `{"enabled":true,"model_resource":"primary","event_threshold":0}`, func() any { return new(ManifestSummary) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.target()
			err := json.Unmarshal([]byte(tc.raw), out)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
			if !tc.valid {
				return
			}
			raw, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(raw, tc.target()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResolvedDataCapabilitiesProgrammaticValidation(t *testing.T) {
	if (ManifestMemory{Resource: "memory", Tools: []string{"memory_clear"}}).Validate() != nil {
		t.Fatal("valid memory")
	}
	if (ManifestMemory{}).Validate() == nil {
		t.Fatal("zero memory")
	}
	if (ManifestArtifact{Enabled: true, Resource: "artifact"}).Validate() != nil {
		t.Fatal("valid artifact")
	}
	if (ManifestArtifact{}).Validate() == nil {
		t.Fatal("zero artifact")
	}
	if (ManifestSummary{Enabled: true, ModelResource: "primary", EventThreshold: 1}).Validate() != nil {
		t.Fatal("valid summary")
	}
	if (ManifestSummary{}).Validate() == nil {
		t.Fatal("zero summary")
	}
	if ArtifactMetadataContract != "worker-artifact-metadata-v1" {
		t.Fatal("metadata contract changed")
	}
}
