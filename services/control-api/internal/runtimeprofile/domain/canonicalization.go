package domain

import (
	"encoding/json"
	"sort"

	"github.com/gowebpki/jcs"
)

func normalizeSpec(spec Spec) Spec {
	normalized := Spec{
		Executors:                 make(map[string]ExecutorResource, len(spec.Executors)),
		SchemaVersion:             spec.SchemaVersion,
		CredentialProtocolVersion: spec.CredentialProtocolVersion,
		Models:                    make(map[string]ModelResource, len(spec.Models)),
		Tools:                     make(map[string]ToolResource, len(spec.Tools)),
		Knowledge:                 make(map[string]KnowledgeResource, len(spec.Knowledge)),
		Storage:                   make(map[string]StorageResource, len(spec.Storage)),
	}
	for key, resource := range spec.Executors {
		normalized.Executors[key] = resource
	}
	for key, resource := range spec.Models {
		resource.Capabilities = append([]string{}, resource.Capabilities...)
		sort.Strings(resource.Capabilities)
		normalized.Models[key] = resource
	}
	for key, resource := range spec.Tools {
		normalized.Tools[key] = resource
	}
	for key, resource := range spec.Knowledge {
		normalized.Knowledge[key] = resource
	}
	for key, resource := range spec.Storage {
		normalized.Storage[key] = resource
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
