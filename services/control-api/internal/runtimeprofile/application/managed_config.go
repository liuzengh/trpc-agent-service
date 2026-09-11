package application

import (
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (r StorageConfig) MarshalJSON() ([]byte, error) {
	if r.Kind.Managed() {
		if r.Destination != nil {
			return nil, domain.ErrCredentialInput
		}
		return json.Marshal(struct {
			Kind            domain.StorageKind `json:"kind"`
			BackendID       string             `json:"backend_id"`
			BackendRevision uint64             `json:"backend_revision"`
		}{r.Kind, r.BackendID, r.BackendRevision})
	}
	type plain StorageConfig
	return json.Marshal(plain(r))
}
func (r KnowledgeConfig) MarshalJSON() ([]byte, error) {
	if r.Kind == domain.KnowledgeKindManaged {
		if r.Host != "" || r.Port != 0 || r.TLS || r.Collection != "" {
			return nil, domain.ErrCredentialInput
		}
		return json.Marshal(struct {
			Kind            domain.KnowledgeKind `json:"kind"`
			BackendID       string               `json:"backend_id"`
			BackendRevision uint64               `json:"backend_revision"`
			Embedding       EmbeddingConfig      `json:"embedding"`
		}{r.Kind, r.BackendID, r.BackendRevision, r.Embedding})
	}
	type plain KnowledgeConfig
	return json.Marshal(plain(r))
}

// Reject inactive branch fields even when their values happen to be zero.
func managedWireFields(b []byte) bool {
	var w struct {
		Config map[string]map[string]map[string]json.RawMessage `json:"config"`
	}
	if json.Unmarshal(b, &w) != nil {
		return false
	}
	for _, category := range []string{"storage", "knowledge"} {
		for _, r := range w.Config[category] {
			var kind string
			if json.Unmarshal(r["kind"], &kind) != nil {
				continue
			}
			managed := domain.StorageKind(kind).Managed() || kind == string(domain.KnowledgeKindManaged)
			if !managed {
				continue
			}
			for k := range r {
				if k != "kind" && k != "backend_id" && k != "backend_revision" && !(category == "knowledge" && k == "embedding") {
					return false
				}
			}
		}
	}
	return true
}
