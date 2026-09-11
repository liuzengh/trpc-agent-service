package deploymentv1

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"regexp"
)

func (r ManifestStorageResource) MarshalJSON() ([]byte, error) {
	if r.Credentials != nil && r.Kind != "managed_artifact" {
		return nil, ErrInvalidManifest
	}
	if (r.Kind == "managed_artifact" && r.MetadataContract != ArtifactMetadataContract) || (r.Kind != "managed_artifact" && r.MetadataContract != "") {
		return nil, ErrInvalidManifest
	}
	if r.Kind == "managed_session" || r.Kind == "managed_memory" || r.Kind == "managed_artifact" {
		if r.Backend == nil || r.Destination != (StorageDestination{}) {
			return nil, ErrInvalidManifest
		}
		var credential *CredentialUse
		if r.Credential != (CredentialUse{}) {
			if !((r.Kind == "managed_memory" && (r.Backend.Kind == datav1.PostgreSQL || r.Backend.Kind == datav1.Redis)) || (r.Kind == "managed_session" && r.Backend.Kind == datav1.Redis)) {
				return nil, ErrInvalidManifest
			}
			c := r.Credential
			credential = &c
		}
		return json.Marshal(struct {
			Credentials      *ArtifactCredentials `json:"credentials,omitempty"`
			Credential       *CredentialUse       `json:"credential,omitempty"`
			MetadataContract string               `json:"metadata_contract,omitempty"`
			Adapter          string               `json:"adapter_version"`
			Kind             string               `json:"kind"`
			Backend          *datav1.Snapshot     `json:"backend"`
		}{r.Credentials, credential, r.MetadataContract, r.AdapterVersion, r.Kind, r.Backend})
	}
	if r.Backend != nil {
		return nil, ErrInvalidManifest
	}
	type plain ManifestStorageResource
	return json.Marshal(plain(r))
}
func (r ManifestKnowledgeResource) MarshalJSON() ([]byte, error) {
	if r.Kind == "managed_knowledge" {
		if r.Backend == nil || r.Host != "" || r.Port != 0 || r.TLS || r.Collection != "" {
			return nil, ErrInvalidManifest
		}
		return json.Marshal(struct {
			Credential *CredentialUse            `json:"credential,omitempty"`
			Adapter    string                    `json:"adapter_version"`
			Kind       string                    `json:"kind"`
			Backend    *datav1.Snapshot          `json:"backend"`
			Embedding  ManifestEmbeddingResource `json:"embedding"`
			Capability string                    `json:"capability"`
		}{r.Credential, r.AdapterVersion, r.Kind, r.Backend, r.Embedding, r.Capability})
	}
	if r.Backend != nil {
		return nil, ErrInvalidManifest
	}
	type plain ManifestKnowledgeResource
	return json.Marshal(plain(r))
}

// The shared codec rejects cross-role descriptors before a runtime factory can
// consume them. Platform authorization and operational readiness are separate.
func validateManagedResourceRoles(c ManifestContent) error {
	for name, r := range c.Resources.Storage {
		if r.Credentials != nil {
			if r.Kind != "managed_artifact" || r.Backend == nil {
				return ErrInvalidManifest
			}
			d, err := r.Backend.Digest()
			if err != nil {
				return ErrInvalidManifest
			}
			for purpose, u := range map[string]CredentialUse{"access_key_id": r.Credentials.AccessKeyID, "secret_access_key": r.Credentials.SecretAccessKey} {
				if u.Purpose != purpose || u.AudienceDigest != d || !regexp.MustCompile(`^crd_[0-9a-f]{32}$`).MatchString(u.CredentialID) {
					return ErrInvalidManifest
				}
			}
		}
		role := ""
		switch r.Kind {
		case "managed_session":
			role = "session"
		case "managed_memory":
			role = "memory"
		case "managed_artifact":
			role = "artifact"
		default:
			continue
		}
		if name != role || r.Backend == nil || r.Backend.TenantID != c.TenantID || r.Backend.ValidateForRole(role) != nil {
			return ErrInvalidManifest
		}
		if (role == "memory" && r.Backend.Kind == datav1.PostgreSQL) || ((role == "memory" || role == "session") && r.Backend.Kind == datav1.Redis && r.Credential != (CredentialUse{})) {
			digest, err := r.Backend.Digest()
			if err != nil || r.Credential.Purpose != CredentialPurposeDSNPassword || !regexp.MustCompile(`^crd_[0-9a-f]{32}$`).MatchString(r.Credential.CredentialID) || r.Credential.AudienceDigest != digest {
				return ErrInvalidManifest
			}
		} else if r.Credential != (CredentialUse{}) {
			return ErrInvalidManifest
		}
	}
	for _, r := range c.Resources.Knowledge {
		if r.Kind != "managed_knowledge" {
			continue
		}
		if r.Credential != nil {
			if r.Backend == nil {
				return ErrInvalidManifest
			}
			d, err := r.Backend.Digest()
			u := r.Credential
			if err != nil || u.Purpose != "qdrant_api_key" || u.AudienceDigest != d || !regexp.MustCompile(`^crd_[0-9a-f]{32}$`).MatchString(u.CredentialID) {
				return ErrInvalidManifest
			}
		}
		if r.Backend == nil || r.Backend.TenantID != c.TenantID || r.Backend.ValidateForRole("knowledge") != nil || r.Backend.Qdrant.Dimensions != r.Embedding.Dimensions {
			return ErrInvalidManifest
		}
	}
	return nil
}
