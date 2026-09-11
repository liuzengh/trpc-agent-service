package domain

import "encoding/json"

const (
	SchemaVersionV1             = "v1"
	CredentialProtocolVersionV1 = "v1"

	MaxDocumentBytes      = 512 * 1024
	MaxModelResources     = 16
	MaxToolResources      = 64
	MaxKnowledgeResources = 32
	MaxStorageResources   = 16
	MaxCapabilities       = 2
)

type ModelKind string
type ToolKind string
type KnowledgeKind string
type StorageKind string
type AuthKind string

const (
	ModelKindOpenAICompatible ModelKind     = "openai_compatible"
	ToolKindMCPStreamableHTTP ToolKind      = "mcp_streamable_http"
	KnowledgeKindQdrantOpenAI KnowledgeKind = "qdrant_openai"
	StorageKindPostgresState  StorageKind   = "postgres_state"

	AuthKindNone   AuthKind = "none"
	AuthKindBearer AuthKind = "bearer"

	CapabilityChat            = "chat"
	CapabilityToolCall        = "tool_call"
	CapabilityWebSearch       = "web.search"
	CapabilityKnowledgeSearch = "knowledge.search"
	CapabilityStorageSession  = "storage.session"
	CapabilityStorageMemory   = "storage.memory"
)

// Spec is the typed, validated representation of RuntimeProfileSpec V1.
type ExecutorResource struct {
	Kind string `json:"kind"`
}

type Spec struct {
	Executors                 map[string]ExecutorResource  `json:"executors,omitempty"`
	SchemaVersion             string                       `json:"schema_version"`
	CredentialProtocolVersion string                       `json:"credential_protocol_version"`
	Models                    map[string]ModelResource     `json:"models"`
	Tools                     map[string]ToolResource      `json:"tools"`
	Knowledge                 map[string]KnowledgeResource `json:"knowledge"`
	Storage                   map[string]StorageResource   `json:"storage"`
}

type ModelResource struct {
	Kind               ModelKind `json:"kind"`
	Model              string    `json:"model"`
	BaseURL            string    `json:"base_url"`
	APIKeyCredentialID string    `json:"api_key_credential_id"`
	Capabilities       []string  `json:"capabilities"`
}

func (r ModelResource) ProvidedCapabilities() []string {
	return append([]string(nil), r.Capabilities...)
}

type ToolResource struct {
	Kind        ToolKind `json:"kind"`
	ServerURL   string   `json:"server_url"`
	ToolsetName string   `json:"toolset_name"`
	ToolName    string   `json:"tool_name"`
	Auth        ToolAuth `json:"auth"`
	Capability  string   `json:"capability"`
}

func (r ToolResource) ProvidedCapabilities() []string {
	return []string{r.Capability}
}

// ToolAuth is a closed discriminated union. The bearer branch contains one
// CredentialID; the none branch contains no inactive credential field.
type ToolAuth struct {
	Kind         AuthKind
	CredentialID string
}

func (a *ToolAuth) UnmarshalJSON(data []byte) error {
	var wire struct {
		Kind         AuthKind `json:"kind"`
		CredentialID string   `json:"credential_id"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*a = ToolAuth{Kind: wire.Kind, CredentialID: wire.CredentialID}
	return nil
}

func (a ToolAuth) MarshalJSON() ([]byte, error) {
	switch a.Kind {
	case AuthKindBearer:
		return json.Marshal(struct {
			Kind         AuthKind `json:"kind"`
			CredentialID string   `json:"credential_id"`
		}{a.Kind, a.CredentialID})
	default:
		return json.Marshal(struct {
			Kind AuthKind `json:"kind"`
		}{a.Kind})
	}
}

type KnowledgeResource struct {
	CredentialAudienceDigest string            `json:"credential_audience_digest,omitempty"`
	BackendID                string            `json:"backend_id,omitempty"`
	BackendRevision          uint64            `json:"backend_revision,omitempty"`
	Kind                     KnowledgeKind     `json:"kind"`
	Host                     string            `json:"host"`
	Port                     int64             `json:"port"`
	TLS                      bool              `json:"tls"`
	Collection               string            `json:"collection"`
	QdrantAPIKeyCredentialID string            `json:"qdrant_api_key_credential_id,omitempty"`
	Embedding                EmbeddingResource `json:"embedding"`
}

func (KnowledgeResource) ProvidedCapabilities() []string {
	return []string{CapabilityKnowledgeSearch}
}

type EmbeddingResource struct {
	Model              string `json:"model"`
	BaseURL            string `json:"base_url"`
	APIKeyCredentialID string `json:"api_key_credential_id"`
	Dimensions         int64  `json:"dimensions"`
}

type StorageResource struct {
	AccessKeyIDCredentialID     string             `json:"access_key_id_credential_id,omitempty"`
	SecretAccessKeyCredentialID string             `json:"secret_access_key_credential_id,omitempty"`
	CredentialAudienceDigest    string             `json:"credential_audience_digest,omitempty"`
	BackendID                   string             `json:"backend_id,omitempty"`
	BackendRevision             uint64             `json:"backend_revision,omitempty"`
	Kind                        StorageKind        `json:"kind"`
	DSNCredentialID             string             `json:"dsn_credential_id"`
	Destination                 StorageDestination `json:"destination"`
}

// StorageDestination declares the non-secret, fixed PostgreSQL connection target.
type StorageDestination struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	SSLMode  string `json:"sslmode"`
}

func (r StorageResource) ProvidedCapabilities() []string {
	switch r.Kind {
	case StorageKindManagedSession:
		return []string{CapabilityStorageSession}
	case StorageKindManagedMemory:
		return []string{CapabilityStorageMemory}
	case StorageKindManagedArtifact:
		return []string{"storage.artifact"}
	case StorageKindPostgresState:
		return []string{CapabilityStorageSession, CapabilityStorageMemory}
	default:
		return nil
	}
}

// CanonicalSpec is the immutable publishable representation.
type CanonicalSpec struct {
	SchemaVersion string
	Document      json.RawMessage
	Digest        string
}
