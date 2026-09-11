package trpcagent

import (
	"errors"
	"fmt"
	"slices"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
)

var ErrManifestCapabilities = errors.New("invalid or unsupported manifest capabilities")

// BuildManifestCapabilityOptions bridges a previously authenticated, digest-
// verified immutable Manifest to SDK assembly. It does not open production gates
// or construct services. The caller must build each final service from the exact
// selected resource and enforce authorization/Attempt semantics there. Services
// cannot be introspected to prove that binding. Summary only configures SDK
// consumption; the caller must construct its Summarizer and summary-capable
// Session service. This function does not generate or store summaries.
// One selected Knowledge resource maps to one service; multiple resources need a future explicit composite API.
func BuildManifestCapabilityOptions(content protocol.ManifestContent, nodeID string, services CapabilityServices) (CapabilityOptions, error) {
	fail := func(reason string) (CapabilityOptions, error) {
		return CapabilityOptions{}, fmt.Errorf("%w: %s", ErrManifestCapabilities, reason)
	}
	node, ok := content.AgentPlan.Nodes[nodeID]
	if !ok || node.Kind != "llm" || nodeID == "" || len(node.Children) > 0 || node.Body != "" || node.MaxIterations != 0 {
		return fail("selected node must be llm")
	}
	cfg := CapabilityConfig{}
	if content.Runtime != nil {
		summary := content.Runtime.Summary
		if summary == nil || summary.Validate() != nil {
			return fail("summary configuration")
		}
		model, exists := content.Resources.Models[summary.ModelResource]
		if summary.ModelResource == "" || !exists || !slices.Contains(model.Capabilities, "chat") {
			return fail("summary model resource")
		}
		sessionKey := content.StorageRoles["session"]
		session, exists := content.Resources.Storage[sessionKey]
		if sessionKey == "" || !exists {
			return fail("summary session dependency")
		}
		if session.Backend != nil {
			if err := manifestCapabilityStorage(content, sessionKey, "session"); err != nil {
				return fail("summary session backend")
			}
		} else if session.Kind != "postgres_state" || session.AdapterVersion != "postgres-state-v1" {
			return fail("summary session backend")
		}
	}
	if node.AddSessionSummary != nil {
		if !*node.AddSessionSummary || content.Runtime == nil {
			return fail("summary consumption requires enabled runtime summary")
		}
		cfg.AddSessionSummary = true
	}
	if node.Memory != nil {
		m := node.Memory
		if m.Validate() != nil {
			return fail("memory component")
		}
		if err := manifestCapabilityStorage(content, m.Resource, "memory"); err != nil {
			return fail("memory resource binding")
		}
		if m.PreloadLimit != nil {
			limit := *m.PreloadLimit
			if limit < -1 || int64(int(limit)) != limit {
				return fail("memory preload range")
			}
			cfg.MemoryPreloadLimit = int(limit)
		}
		if len(m.Tools) == 0 && cfg.MemoryPreloadLimit == 0 {
			return fail("disabled memory component must be absent")
		}
		cfg.MemoryTools = append([]string(nil), m.Tools...)
	}
	if node.Artifact != nil {
		if node.Artifact.Validate() != nil {
			return fail("disabled artifact component must be absent")
		}
		if err := manifestCapabilityStorage(content, node.Artifact.Resource, "artifact"); err != nil {
			return fail("artifact resource binding")
		}
		cfg.Artifact = true
	}
	if len(node.KnowledgeResources) > 1 {
		return fail("multiple knowledge resources require an explicit composite service")
	}
	if len(node.KnowledgeResources) == 1 {
		key := node.KnowledgeResources[0]
		resource, exists := content.Resources.Knowledge[key]
		if key == "" || !exists {
			return fail("knowledge resource missing")
		}
		if resource.Kind == "managed_knowledge" {
			if resource.AdapterVersion != "managed-knowledge-v1" || resource.Backend == nil || resource.Backend.TenantID != content.TenantID || resource.Backend.ValidateForRole("knowledge") != nil {
				return fail("knowledge backend")
			}
		} else if resource.Kind != "qdrant_openai" || resource.AdapterVersion != "qdrant-openai-v1" || resource.Backend != nil {
			return fail("knowledge adapter")
		}
		if resource.Embedding.Model == "" || resource.Embedding.BaseURL == "" || resource.Embedding.Dimensions <= 0 {
			return fail("knowledge embedding")
		}
		if resource.Backend != nil && resource.Backend.Qdrant.Dimensions != resource.Embedding.Dimensions {
			return fail("knowledge embedding dimensions")
		}
		cfg.Knowledge = true
	}
	options, err := BuildCapabilityOptions(cfg, services)
	if err != nil {
		return CapabilityOptions{}, fmt.Errorf("%w: %v", ErrManifestCapabilities, err)
	}
	return options, nil
}

func manifestCapabilityStorage(content protocol.ManifestContent, key, role string) error {
	if key == "" || content.StorageRoles[role] != key {
		return ErrManifestCapabilities
	}
	resource, exists := content.Resources.Storage[key]
	if !exists || resource.Kind != "managed_"+role || resource.AdapterVersion != "managed-"+role+"-v1" || resource.Backend == nil || resource.Backend.TenantID != content.TenantID || resource.Backend.ValidateForRole(role) != nil {
		return ErrManifestCapabilities
	}
	if role == "artifact" {
		if resource.MetadataContract != protocol.ArtifactMetadataContract {
			return ErrManifestCapabilities
		}
	} else if resource.MetadataContract != "" {
		return ErrManifestCapabilities
	}
	return nil
}
