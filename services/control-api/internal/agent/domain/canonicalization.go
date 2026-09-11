package domain

import (
	"encoding/json"
	"sort"

	"github.com/gowebpki/jcs"
)

func normalizeSpec(spec Spec) Spec {
	normalized := Spec{
		SchemaVersion: spec.SchemaVersion,
		Root:          spec.Root,
		Runtime:       cloneRuntime(spec.Runtime),
		Requirements: Requirements{
			Executors: make(map[string]CapabilityRequirement, len(spec.Requirements.Executors)),
			Models:    make(map[string]ModelRequirement, len(spec.Requirements.Models)),
			Tools:     make(map[string]CapabilityRequirement, len(spec.Requirements.Tools)),
			Knowledge: make(map[string]CapabilityRequirement, len(spec.Requirements.Knowledge)),
		},
		Nodes: make(map[string]Node, len(spec.Nodes)),
	}
	for name, requirement := range spec.Requirements.Models {
		capabilities := append([]string{}, requirement.Capabilities...)
		sort.Strings(capabilities)
		normalized.Requirements.Models[name] = ModelRequirement{Capabilities: capabilities}
	}
	for name, requirement := range spec.Requirements.Tools {
		normalized.Requirements.Tools[name] = requirement
	}
	for name, requirement := range spec.Requirements.Knowledge {
		normalized.Requirements.Knowledge[name] = requirement
	}
	for name, r := range spec.Requirements.Executors {
		normalized.Requirements.Executors[name] = r
	}
	for id, node := range spec.Nodes {
		if node.Workspace != nil {
			w := *node.Workspace
			w.Tools = append([]string{}, w.Tools...)
			sort.Strings(w.Tools)
			node.Workspace = &w
		}
		node = cloneNodeData(node)
		node.ToolSlots = append([]string{}, node.ToolSlots...)
		node.KnowledgeSlots = append([]string{}, node.KnowledgeSlots...)
		node.Children = append([]string{}, node.Children...)
		sort.Strings(node.ToolSlots)
		sort.Strings(node.KnowledgeSlots)
		normalized.Nodes[id] = node
	}
	return normalized
}

func canonicalJSON(spec Spec) ([]byte, error) {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	return jcs.Transform(encoded)
}
