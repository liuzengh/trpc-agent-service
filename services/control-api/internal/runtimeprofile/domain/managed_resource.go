package domain

import (
	"encoding/json"
	"regexp"
)

var managedBackendID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

const (
	StorageKindManagedSession  StorageKind   = "managed_session"
	StorageKindManagedMemory   StorageKind   = "managed_memory"
	StorageKindManagedArtifact StorageKind   = "managed_artifact"
	KnowledgeKindManaged       KnowledgeKind = "managed_knowledge"
)

func (k StorageKind) Managed() bool {
	return k == StorageKindManagedSession || k == StorageKindManagedMemory || k == StorageKindManagedArtifact
}
func (k StorageKind) Role() string {
	switch k {
	case StorageKindManagedSession:
		return "session"
	case StorageKindManagedMemory:
		return "memory"
	case StorageKindManagedArtifact:
		return "artifact"
	}
	return ""
}
func (r StorageResource) MarshalJSON() ([]byte, error) {
	if r.Kind == StorageKindManagedArtifact {
		if r.Destination != (StorageDestination{}) || r.DSNCredentialID != "" || ((r.AccessKeyIDCredentialID != "" || r.SecretAccessKeyCredentialID != "") != (r.CredentialAudienceDigest != "")) {
			return nil, ErrCredentialInput
		}
		return json.Marshal(struct {
			Kind            StorageKind `json:"kind"`
			BackendID       string      `json:"backend_id"`
			BackendRevision uint64      `json:"backend_revision"`
			AccessKeyID     string      `json:"access_key_id_credential_id,omitempty"`
			SecretAccessKey string      `json:"secret_access_key_credential_id,omitempty"`
			Audience        string      `json:"credential_audience_digest,omitempty"`
		}{r.Kind, r.BackendID, r.BackendRevision, r.AccessKeyIDCredentialID, r.SecretAccessKeyCredentialID, r.CredentialAudienceDigest})
	}
	if r.AccessKeyIDCredentialID != "" || r.SecretAccessKeyCredentialID != "" {
		return nil, ErrCredentialInput
	}
	if r.Kind.Managed() {
		if (r.DSNCredentialID == "") != (r.CredentialAudienceDigest == "") {
			return nil, ErrCredentialInput
		}
		if r.Destination != (StorageDestination{}) || (r.Kind != StorageKindManagedMemory && r.Kind != StorageKindManagedSession && (r.DSNCredentialID != "" || r.CredentialAudienceDigest != "")) {
			return nil, ErrCredentialInput
		}
		return json.Marshal(struct {
			Kind                     StorageKind `json:"kind"`
			BackendID                string      `json:"backend_id"`
			BackendRevision          uint64      `json:"backend_revision"`
			DSNCredentialID          string      `json:"dsn_credential_id,omitempty"`
			CredentialAudienceDigest string      `json:"credential_audience_digest,omitempty"`
		}{r.Kind, r.BackendID, r.BackendRevision, r.DSNCredentialID, r.CredentialAudienceDigest})
	}
	type plain StorageResource
	return json.Marshal(plain(r))
}
func (r KnowledgeResource) MarshalJSON() ([]byte, error) {
	if r.Kind == KnowledgeKindManaged {
		if r.Host != "" || r.Port != 0 || r.TLS || r.Collection != "" || ((r.QdrantAPIKeyCredentialID == "") != (r.CredentialAudienceDigest == "")) {
			return nil, ErrCredentialInput
		}
		return json.Marshal(struct {
			QdrantID        string            `json:"qdrant_api_key_credential_id,omitempty"`
			Audience        string            `json:"credential_audience_digest,omitempty"`
			Kind            KnowledgeKind     `json:"kind"`
			BackendID       string            `json:"backend_id"`
			BackendRevision uint64            `json:"backend_revision"`
			Embedding       EmbeddingResource `json:"embedding"`
		}{r.QdrantAPIKeyCredentialID, r.CredentialAudienceDigest, r.Kind, r.BackendID, r.BackendRevision, r.Embedding})
	}
	type plain KnowledgeResource
	return json.Marshal(plain(r))
}
func validateManagedSelection(pointer string, object map[string]any, fields []string, diagnostics *[]Diagnostic) {
	validateAllowedFields(object, pointer, fields, diagnostics)
	requireFields(object, pointer, fields, diagnostics)
	validateBoundedString(object, "backend_id", pointer, 1, 128, diagnostics)
	if v, ok := object["backend_id"].(string); ok && !managedBackendID.MatchString(v) {
		*diagnostics = append(*diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", pointer+"/backend_id", "backend identifier is invalid"))
	}
	validateIntegerField(object, "backend_revision", pointer, 1, 9007199254740991, diagnostics)
}
