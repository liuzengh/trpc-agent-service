package domain

import (
	"encoding/json"
	"net/url"
	"strings"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const (
	StorageAdapterManagedArtifactV1 = "managed-artifact-v1"
	StorageAdapterManagedSessionV1  = "managed-session-v1"
	StorageAdapterManagedMemoryV1   = "managed-memory-v1"
	KnowledgeAdapterManagedV1       = "managed-knowledge-v1"
)

type ManagedBackendView struct {
	BackendID       string      `json:"backend_id"`
	BackendRevision uint64      `json:"backend_revision"`
	Kind            datav1.Kind `json:"kind"`
}

func backendView(b datav1.Snapshot) *ManagedBackendView {
	return &ManagedBackendView{b.BackendID, b.BackendRevision, b.Kind}
}
func (r ManifestStorageResource) MarshalJSON() ([]byte, error) {
	if (r.Kind == profiledomain.StorageKindManagedArtifact && r.MetadataContract != ArtifactMetadataContract) || (r.Kind != profiledomain.StorageKindManagedArtifact && r.MetadataContract != "") {
		return nil, ErrInvalidManifestContent
	}
	if r.Credentials != nil && (r.Kind != profiledomain.StorageKindManagedArtifact || r.Backend == nil || r.Backend.Kind != datav1.S3) {
		return nil, ErrInvalidManifestContent
	}
	if r.Kind.Managed() {
		if r.Backend == nil || r.Destination != (profiledomain.StorageDestination{}) {
			return nil, ErrInvalidManifestContent
		}
		var credential *CredentialUse
		if r.Credential != (CredentialUse{}) {
			if !managedPasswordBackend(r.Kind, *r.Backend) {
				return nil, ErrInvalidManifestContent
			}
			c := r.Credential
			credential = &c
		}
		return json.Marshal(struct {
			Credentials      *ArtifactCredentials      `json:"credentials,omitempty"`
			Credential       *CredentialUse            `json:"credential,omitempty"`
			MetadataContract string                    `json:"metadata_contract,omitempty"`
			Adapter          string                    `json:"adapter_version"`
			Kind             profiledomain.StorageKind `json:"kind"`
			Backend          *datav1.Snapshot          `json:"backend"`
		}{r.Credentials, credential, r.MetadataContract, r.AdapterVersion, r.Kind, r.Backend})
	}
	if r.Backend != nil {
		return nil, ErrInvalidManifestContent
	}
	type plain ManifestStorageResource
	return json.Marshal(plain(r))
}
func (r ManifestKnowledgeResource) MarshalJSON() ([]byte, error) {
	if r.Kind == profiledomain.KnowledgeKindManaged {
		if r.Backend == nil || r.Host != "" || r.Port != 0 || r.TLS || r.Collection != "" {
			return nil, ErrInvalidManifestContent
		}
		return json.Marshal(struct {
			Credential *CredentialUse              `json:"credential,omitempty"`
			Adapter    string                      `json:"adapter_version"`
			Kind       profiledomain.KnowledgeKind `json:"kind"`
			Backend    *datav1.Snapshot            `json:"backend"`
			Embedding  ManifestEmbeddingResource   `json:"embedding"`
			Capability string                      `json:"capability"`
		}{r.Credential, r.AdapterVersion, r.Kind, r.Backend, r.Embedding, r.Capability})
	}
	if r.Backend != nil {
		return nil, ErrInvalidManifestContent
	}
	type plain ManifestKnowledgeResource
	return json.Marshal(plain(r))
}
func (r ManifestStorageResourceView) MarshalJSON() ([]byte, error) {
	if r.Backend != nil {
		var present *bool
		if (r.Kind == profiledomain.StorageKindManagedArtifact && r.CredentialPresent) || (r.Kind == profiledomain.StorageKindManagedMemory && r.Backend.Kind == datav1.PostgreSQL) || ((r.Kind == profiledomain.StorageKindManagedMemory || r.Kind == profiledomain.StorageKindManagedSession) && r.Backend.Kind == datav1.Redis && r.CredentialPresent) {
			v := r.CredentialPresent
			present = &v
		}
		return json.Marshal(struct {
			CredentialPresent *bool                     `json:"credential_present,omitempty"`
			MetadataContract  string                    `json:"metadata_contract,omitempty"`
			Adapter           string                    `json:"adapter_version"`
			Kind              profiledomain.StorageKind `json:"kind"`
			Backend           *ManagedBackendView       `json:"backend"`
		}{present, r.MetadataContract, r.AdapterVersion, r.Kind, r.Backend})
	}
	type plain ManifestStorageResourceView
	return json.Marshal(plain(r))
}
func (r ManifestKnowledgeResourceView) MarshalJSON() ([]byte, error) {
	if r.Backend != nil {
		var present *bool
		if r.CredentialPresent {
			v := true
			present = &v
		}
		return json.Marshal(struct {
			CredentialPresent *bool                       `json:"credential_present,omitempty"`
			Adapter           string                      `json:"adapter_version"`
			Kind              profiledomain.KnowledgeKind `json:"kind"`
			Backend           *ManagedBackendView         `json:"backend"`
			Embedding         ManifestEmbeddingView       `json:"embedding"`
			Capability        string                      `json:"capability"`
		}{present, r.AdapterVersion, r.Kind, r.Backend, r.Embedding, r.Capability})
	}
	type plain ManifestKnowledgeResourceView
	return json.Marshal(plain(r))
}
func publicEndpointHosts(c ManifestContent) []string {
	// Derive the public list from public user resource endpoints. Do not copy the
	// internal execution allowlist containing platform-only physical targets.
	hosts := map[string]bool{}
	add := func(raw string) {
		h := urlHost(raw)
		if h != "" {
			hosts[h] = true
		}
	}
	for _, r := range c.Resources.Models {
		add(r.BaseURL)
	}
	for _, r := range c.Resources.Tools {
		add(r.ServerURL)
	}
	for _, r := range c.Resources.Knowledge {
		add(r.Embedding.BaseURL)
		if r.Backend == nil {
			hosts[strings.ToLower(r.Host)] = true
		}
	}
	for _, r := range c.Resources.Storage {
		if r.Backend == nil {
			hosts[strings.ToLower(r.Destination.Host)] = true
		}
	}
	return sortedTrueKeys(hosts)
}
func validateManagedWire(raw []byte) error {
	var tree struct {
		Resources struct {
			Storage   map[string]json.RawMessage `json:"storage"`
			Knowledge map[string]json.RawMessage `json:"knowledge"`
		} `json:"resources"`
	}
	if json.Unmarshal(raw, &tree) != nil {
		return ErrInvalidManifestContent
	}
	for category, resources := range map[string]map[string]json.RawMessage{"storage": tree.Resources.Storage, "knowledge": tree.Resources.Knowledge} {
		for _, raw := range resources {
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &fields) != nil {
				return ErrInvalidManifestContent
			}
			var kind string
			_ = json.Unmarshal(fields["kind"], &kind)
			managed := profiledomain.StorageKind(kind).Managed() || kind == string(profiledomain.KnowledgeKindManaged)
			if !managed {
				if _, ok := fields["backend"]; ok {
					return ErrInvalidManifestContent
				}
				continue
			}
			allowed := map[string]bool{"kind": true, "adapter_version": true, "backend": true}
			if category == "storage" && (kind == string(profiledomain.StorageKindManagedMemory) || kind == string(profiledomain.StorageKindManagedSession)) {
				if _, ok := fields["credential"]; ok {
					allowed["credential"] = true
				}
			}
			if category == "storage" && kind == string(profiledomain.StorageKindManagedArtifact) {
				allowed["metadata_contract"] = true
				if _, ok := fields["credentials"]; ok {
					allowed["credentials"] = true
				}
			}
			if category == "knowledge" {
				allowed["embedding"] = true
				allowed["capability"] = true
				if _, ok := fields["credential"]; ok {
					allowed["credential"] = true
				}
			}
			if len(fields) != len(allowed) {
				return ErrInvalidManifestContent
			}
			for k := range fields {
				if !allowed[k] {
					return ErrInvalidManifestContent
				}
			}
			snapshot, err := datav1.Decode(fields["backend"])
			if err != nil {
				return ErrInvalidManifestContent
			}
			if category == "knowledge" {
				if v, ok := fields["credential"]; ok {
					var use CredentialUse
					digest, _ := snapshot.Digest()
					if strictDecodeJSON(v, &use) != nil || snapshot.Kind != datav1.Qdrant || !validCredentialUse(use, CredentialPurposeQdrantAPIKey) || use.AudienceDigest != digest {
						return ErrInvalidManifestContent
					}
				}
			}
			if category == "storage" && kind == string(profiledomain.StorageKindManagedArtifact) {
				if v, present := fields["credentials"]; present {
					var creds ArtifactCredentials
					if strictDecodeJSON(v, &creds) != nil || !validArtifactCredentials(&creds, snapshot) {
						return ErrInvalidManifestContent
					}
				}
			}
			if category == "storage" && (kind == string(profiledomain.StorageKindManagedMemory) || kind == string(profiledomain.StorageKindManagedSession)) {
				credentialRaw, present := fields["credential"]
				if (kind == string(profiledomain.StorageKindManagedMemory) && snapshot.Kind == datav1.PostgreSQL) || (snapshot.Kind == datav1.Redis && present) {
					var use CredentialUse
					if !present || strictDecodeJSON(credentialRaw, &use) != nil || !validCredentialUse(use, CredentialPurposeDSNPassword) {
						return ErrInvalidManifestContent
					}
					digest, _ := snapshot.Digest()
					if use.AudienceDigest != digest {
						return ErrInvalidManifestContent
					}
				} else if present {
					return ErrInvalidManifestContent
				}
			}
		}
	}
	return nil
}

func urlHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

func managedPasswordBackend(kind profiledomain.StorageKind, b datav1.Snapshot) bool {
	return (kind == profiledomain.StorageKindManagedMemory && (b.Kind == datav1.PostgreSQL || b.Kind == datav1.Redis)) || (kind == profiledomain.StorageKindManagedSession && b.Kind == datav1.Redis)
}

func validArtifactCredentials(c *ArtifactCredentials, b datav1.Snapshot) bool {
	if c == nil || b.Kind != datav1.S3 {
		return false
	}
	digest, err := b.Digest()
	return err == nil && validCredentialUse(c.AccessKeyID, "access_key_id") && validCredentialUse(c.SecretAccessKey, "secret_access_key") && c.AccessKeyID.AudienceDigest == digest && c.SecretAccessKey.AudienceDigest == digest
}
