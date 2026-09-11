package deploymentv1

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// These resolved components are not publication inputs. Their aggregate wire
// representation does not enable execution before compiler and consumer gates.
type ManifestMemory struct {
	Resource     string   `json:"resource"`
	Tools        []string `json:"tools"`
	PreloadLimit *int64   `json:"preload_limit,omitempty"`
}
type ManifestArtifact struct {
	Enabled  bool   `json:"enabled"`
	Resource string `json:"resource"`
}
type ManifestSummary struct {
	Enabled        bool   `json:"enabled"`
	ModelResource  string `json:"model_resource"`
	EventThreshold int64  `json:"event_threshold"`
}

const ArtifactMetadataContract = "worker-artifact-metadata-v1"

//go:embed data-capabilities.schema.json
var DataCapabilitiesSchema []byte

var dataCapabilityValidators = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(DataCapabilitiesSchema))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	const location = "https://jfsas.dev/schemas/deployment/v1/data-capabilities.schema.json"
	if err = c.AddResource(location, doc); err != nil {
		return nil, err
	}
	out := make(map[string]*jsonschema.Schema)
	for _, kind := range []string{"memory", "artifact", "summary"} {
		out[kind], err = c.Compile(location + "#/$defs/" + kind)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
})

func decodeDataCapability(raw []byte, kind string, target any) error {
	validators, err := dataCapabilityValidators()
	if err != nil {
		return fmt.Errorf("compile data capability schema: %w", err)
	}
	// Apply the same duplicate-key and integer handling as the Manifest decoder.
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return ErrInvalidManifest
	}
	if err = validators[kind].Validate(doc); err != nil {
		return ErrInvalidManifest
	}
	return strictDecodeJSON(raw, target)
}

func (m *ManifestMemory) UnmarshalJSON(raw []byte) error {
	type wire ManifestMemory
	var out wire
	if err := decodeDataCapability(raw, "memory", &out); err != nil {
		return err
	}
	*m = ManifestMemory(out)
	return nil
}
func (a *ManifestArtifact) UnmarshalJSON(raw []byte) error {
	type wire ManifestArtifact
	var out wire
	if err := decodeDataCapability(raw, "artifact", &out); err != nil {
		return err
	}
	*a = ManifestArtifact(out)
	return nil
}
func (s *ManifestSummary) UnmarshalJSON(raw []byte) error {
	type wire ManifestSummary
	var out wire
	if err := decodeDataCapability(raw, "summary", &out); err != nil {
		return err
	}
	*s = ManifestSummary(out)
	return nil
}

// Validate catches invalid programmatic construction as well as wire input.
func (m ManifestMemory) Validate() error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	var out ManifestMemory
	return json.Unmarshal(b, &out)
}
func (a ManifestArtifact) Validate() error {
	b, e := json.Marshal(a)
	if e != nil {
		return e
	}
	var out ManifestArtifact
	return json.Unmarshal(b, &out)
}
func (s ManifestSummary) Validate() error {
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	var out ManifestSummary
	return json.Unmarshal(b, &out)
}
