package trpcagent

import (
	"fmt"
	"slices"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

// BuildManifestSummarizer constructs the SDK generator from one explicit model
// resource in an authenticated, verified Manifest. Models are borrowed and must
// already be initialized from the fixed resource and its authorized credential.
// No separate database, token budget, word limit or recent-event skip is added.
// The result belongs on the same Session service that stores conversation state.
func BuildManifestSummarizer(content protocol.ManifestContent, models map[string]model.Model) (summary.SessionSummarizer, error) {
	if content.Runtime == nil {
		return nil, nil
	}
	spec := content.Runtime.Summary
	if spec == nil || spec.Validate() != nil || int64(int(spec.EventThreshold)) != spec.EventThreshold {
		return nil, fmt.Errorf("%w: summary configuration", ErrManifestCapabilities)
	}
	resource, ok := content.Resources.Models[spec.ModelResource]
	if !ok || resource.Kind != "openai_compatible" || resource.AdapterVersion != "openai-compatible-v1" || !slices.Contains(resource.Capabilities, "chat") {
		return nil, fmt.Errorf("%w: summary model resource", ErrManifestCapabilities)
	}
	selected := models[spec.ModelResource]
	if nilCapabilityService(selected) || selected.Info().Name != resource.Model {
		return nil, fmt.Errorf("%w: initialized summary model binding", ErrManifestCapabilities)
	}
	key := content.StorageRoles["session"]
	storage, exists := content.Resources.Storage[key]
	if key == "" || !exists {
		return nil, fmt.Errorf("%w: summary session resource", ErrManifestCapabilities)
	}
	if storage.Kind == "managed_session" {
		if manifestCapabilityStorage(content, key, "session") != nil {
			return nil, fmt.Errorf("%w: summary session backend", ErrManifestCapabilities)
		}
	} else if storage.Kind != "postgres_state" || storage.AdapterVersion != "postgres-state-v1" || storage.Backend != nil {
		return nil, fmt.Errorf("%w: summary session backend", ErrManifestCapabilities)
	}
	return summary.NewSummarizer(selected, summary.WithEventThreshold(int(spec.EventThreshold))), nil
}
