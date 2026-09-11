package domain

import (
	"encoding/json"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"strings"
)

// Share resolved component semantics rather than maintaining a second codec.
// Manifest integration follows compiler closure; pendingDataContractDiagnostics
// remains in force until that complete integration is delivered.
type ManifestWorkspace = deploymentv1.ManifestWorkspace
type ManifestExecutorResource = deploymentv1.ManifestExecutorResource
type ManifestMemory = deploymentv1.ManifestMemory
type ManifestArtifact = deploymentv1.ManifestArtifact
type ManifestSummary = deploymentv1.ManifestSummary

const ArtifactMetadataContract = deploymentv1.ArtifactMetadataContract

// Preserve source presence when pointer decoding would collapse explicit null
// into absence. The shared schema checks all new aggregate field shapes.
func validateDataCapabilityPresence(raw []byte) error {
	var wire struct {
		Runtime   json.RawMessage `json:"runtime"`
		AgentPlan struct {
			Nodes map[string]map[string]json.RawMessage `json:"nodes"`
		} `json:"agent_plan"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	present := len(wire.Runtime) > 0
	for _, n := range wire.AgentPlan.Nodes {
		for _, key := range []string{"memory", "artifact", "add_session_summary", "workspace"} {
			for candidate := range n {
				if strings.EqualFold(candidate, key) {
					present = true
				}
			}
		}
	}
	if present {
		_, err := deploymentv1.DecodeManifestContent(raw)
		return err
	}
	return nil
}
