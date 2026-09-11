package application

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type credentialSlot struct {
	Category       string
	Name           string
	Purpose        string
	ID             string
	AudienceDigest string
}

func (s credentialSlot) key() string { return s.Category + "/" + s.Name + "/" + s.Purpose }
func audience(parts ...any) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func credentialSlots(spec domain.Spec) []credentialSlot {
	slots := make([]credentialSlot, 0)
	for n, r := range spec.Models {
		slots = append(slots, credentialSlot{"models", n, "api_key", r.APIKeyCredentialID, audience(r.Kind, r.BaseURL)})
	}
	for n, r := range spec.Tools {
		if r.Auth.Kind == domain.AuthKindBearer {
			slots = append(slots, credentialSlot{"tools", n, "bearer_token", r.Auth.CredentialID, audience(r.Kind, r.ServerURL, r.Auth.Kind)})
		}
	}
	for n, r := range spec.Knowledge {
		if r.Kind == domain.KnowledgeKindManaged {
			slots = append(slots, credentialSlot{"knowledge", n, "qdrant_api_key", r.QdrantAPIKeyCredentialID, r.CredentialAudienceDigest})
		} else {
			slots = append(slots, credentialSlot{"knowledge", n, "qdrant_api_key", r.QdrantAPIKeyCredentialID, audience(r.Kind, r.Host, r.Port, r.TLS)})
		}
		slots = append(slots, credentialSlot{"knowledge", n, "embedding_api_key", r.Embedding.APIKeyCredentialID, audience(r.Kind, r.Embedding.BaseURL)})
	}
	for n, r := range spec.Storage {
		if r.Kind == domain.StorageKindManagedArtifact {
			slots = append(slots, credentialSlot{"storage", n, "access_key_id", r.AccessKeyIDCredentialID, r.CredentialAudienceDigest}, credentialSlot{"storage", n, "secret_access_key", r.SecretAccessKeyCredentialID, r.CredentialAudienceDigest})
			continue
		}
		if r.Kind == domain.StorageKindManagedMemory || r.Kind == domain.StorageKindManagedSession {
			slots = append(slots, credentialSlot{"storage", n, "dsn_password", r.DSNCredentialID, r.CredentialAudienceDigest})
			continue
		}
		if r.Kind.Managed() {
			continue
		}
		slots = append(slots, credentialSlot{"storage", n, "dsn", r.DSNCredentialID, audience(r.Kind, r.Destination)})
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].key() < slots[j].key() })
	return slots
}
func setCredentialID(spec *domain.Spec, slot credentialSlot, id string) {
	switch slot.Category {
	case "models":
		r := spec.Models[slot.Name]
		r.APIKeyCredentialID = id
		spec.Models[slot.Name] = r
	case "tools":
		r := spec.Tools[slot.Name]
		r.Auth.CredentialID = id
		spec.Tools[slot.Name] = r
	case "knowledge":
		r := spec.Knowledge[slot.Name]
		if slot.Purpose == "qdrant_api_key" {
			r.QdrantAPIKeyCredentialID = id
		} else {
			r.Embedding.APIKeyCredentialID = id
		}
		spec.Knowledge[slot.Name] = r
	case "storage":
		r := spec.Storage[slot.Name]
		switch slot.Purpose {
		case "access_key_id":
			r.AccessKeyIDCredentialID = id
		case "secret_access_key":
			r.SecretAccessKeyCredentialID = id
		default:
			r.DSNCredentialID = id
		}
		spec.Storage[slot.Name] = r
	}
}
func specFromConfig(config ProfileConfig) (domain.Spec, error) {
	b, err := json.Marshal(config)
	if err != nil {
		return domain.Spec{}, domain.ErrCredentialInput
	}
	var spec domain.Spec
	if err = json.Unmarshal(b, &spec); err != nil {
		return domain.Spec{}, domain.ErrCredentialInput
	}
	spec.SchemaVersion = domain.SchemaVersionV1
	spec.CredentialProtocolVersion = domain.CredentialProtocolVersionV1
	return spec, nil
}

// configFromSpec uses a distinct typed representation: internal IDs have no
// destination field and therefore cannot escape into the public JSON view.
func configFromSpec(spec domain.Spec) ProfileConfig {
	b, _ := json.Marshal(spec)
	var config ProfileConfig
	_ = json.Unmarshal(b, &config)
	if config.Models == nil {
		config.Models = map[string]ModelConfig{}
	}
	if config.Tools == nil {
		config.Tools = map[string]ToolConfig{}
	}
	if config.Knowledge == nil {
		config.Knowledge = map[string]KnowledgeConfig{}
	}
	if config.Storage == nil {
		config.Storage = map[string]StorageConfig{}
	}
	for name, model := range config.Models {
		if model.Capabilities == nil {
			model.Capabilities = []string{}
			config.Models[name] = model
		}
	}
	return config
}
