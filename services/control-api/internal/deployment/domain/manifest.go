package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const (
	CredentialPurposeAPIKey          = "api_key"
	CredentialPurposeBearerToken     = "bearer_token"
	CredentialPurposeQdrantAPIKey    = "qdrant_api_key"
	CredentialPurposeEmbeddingAPIKey = "embedding_api_key"
	CredentialPurposeDSN             = "dsn"
	CredentialPurposeDSNPassword     = "dsn_password"

	StorageRoleSession = "session"
	StorageRoleMemory  = "memory"
)

type CredentialUse struct {
	CredentialID   string `json:"credential_id"`
	Purpose        string `json:"purpose"`
	AudienceDigest string `json:"audience_digest"`
}

type PlatformContractReference struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

type ManifestRuntime struct {
	Summary *ManifestSummary `json:"summary"`
}

type ManifestContent struct {
	Runtime                *ManifestRuntime          `json:"runtime,omitempty"`
	SchemaVersion          string                    `json:"schema_version"`
	CompilerVersion        string                    `json:"compiler_version"`
	RuntimeContractVersion string                    `json:"runtime_contract_version"`
	PlatformContract       PlatformContractReference `json:"platform_contract"`
	TenantID               string                    `json:"tenant_id"`
	Sources                ManifestSources           `json:"sources"`
	AgentPlan              AgentPlan                 `json:"agent_plan"`
	Resources              ManifestResources         `json:"resources"`
	ResolvedRequirements   ResolvedRequirements      `json:"resolved_requirements"`
	StorageRoles           map[string]string         `json:"storage_roles"`
	Execution              ExecutionPolicy           `json:"execution"`
}

type ManifestSources struct {
	Agent   ManifestAgentSource   `json:"agent"`
	Profile ManifestProfileSource `json:"profile"`
}

type ManifestAgentSource struct {
	AgentID       string `json:"agent_id"`
	VersionNumber int64  `json:"version_number"`
	VersionID     string `json:"version_id"`
	SchemaVersion string `json:"schema_version"`
	Digest        string `json:"digest"`
}

type ManifestProfileSource struct {
	ProfileID      string `json:"profile_id"`
	RevisionNumber int64  `json:"revision_number"`
	RevisionID     string `json:"revision_id"`
	SchemaVersion  string `json:"schema_version"`
	Digest         string `json:"digest"`
}

type AgentPlan struct {
	Root  string                  `json:"root"`
	Nodes map[string]ManifestNode `json:"nodes"`
}

// ManifestNode is a closed union. MarshalJSON omits every inactive branch so
// zero values cannot silently become executable options.
type ManifestNode struct {
	Workspace         *ManifestWorkspace
	Memory            *ManifestMemory
	Artifact          *ManifestArtifact
	AddSessionSummary *bool
	Kind              agentdomain.NodeKind

	Name               string
	Instruction        string
	ModelResource      string
	ToolResources      []string
	KnowledgeResources []string
	CallableEntries    []string
	Generation         *agentdomain.Generation

	Children      []string
	Body          string
	MaxIterations int64
}

func (n ManifestNode) MarshalJSON() ([]byte, error) {
	switch n.Kind {
	case agentdomain.NodeKindLLM:
		return json.Marshal(struct {
			Kind               agentdomain.NodeKind    `json:"kind"`
			Name               string                  `json:"name,omitempty"`
			Instruction        string                  `json:"instruction"`
			ModelResource      string                  `json:"model_resource"`
			ToolResources      []string                `json:"tool_resources"`
			KnowledgeResources []string                `json:"knowledge_resources"`
			CallableEntries    []string                `json:"callable_entries"`
			Generation         *agentdomain.Generation `json:"generation,omitempty"`
			Memory             *ManifestMemory         `json:"memory,omitempty"`
			Artifact           *ManifestArtifact       `json:"artifact,omitempty"`
			AddSessionSummary  *bool                   `json:"add_session_summary,omitempty"`
			Workspace          *ManifestWorkspace      `json:"workspace,omitempty"`
		}{
			n.Kind, n.Name, n.Instruction, n.ModelResource,
			n.ToolResources, n.KnowledgeResources, n.CallableEntries, n.Generation, n.Memory, n.Artifact, n.AddSessionSummary, n.Workspace,
		})
	case agentdomain.NodeKindSequence, agentdomain.NodeKindParallel:
		return json.Marshal(struct {
			Kind     agentdomain.NodeKind `json:"kind"`
			Name     string               `json:"name,omitempty"`
			Children []string             `json:"children"`
		}{n.Kind, n.Name, n.Children})
	case agentdomain.NodeKindLoop:
		return json.Marshal(struct {
			Kind          agentdomain.NodeKind `json:"kind"`
			Name          string               `json:"name,omitempty"`
			Body          string               `json:"body"`
			MaxIterations int64                `json:"max_iterations"`
		}{n.Kind, n.Name, n.Body, n.MaxIterations})
	default:
		return json.Marshal(struct {
			Kind agentdomain.NodeKind `json:"kind"`
		}{n.Kind})
	}
}

func (n *ManifestNode) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		Kind agentdomain.NodeKind `json:"kind"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	switch discriminator.Kind {
	case agentdomain.NodeKindLLM:
		var wire struct {
			Kind               agentdomain.NodeKind    `json:"kind"`
			Name               string                  `json:"name,omitempty"`
			Instruction        string                  `json:"instruction"`
			ModelResource      string                  `json:"model_resource"`
			ToolResources      []string                `json:"tool_resources"`
			KnowledgeResources []string                `json:"knowledge_resources"`
			CallableEntries    []string                `json:"callable_entries"`
			Generation         *agentdomain.Generation `json:"generation,omitempty"`
			Memory             *ManifestMemory         `json:"memory,omitempty"`
			Artifact           *ManifestArtifact       `json:"artifact,omitempty"`
			AddSessionSummary  *bool                   `json:"add_session_summary,omitempty"`
			Workspace          *ManifestWorkspace      `json:"workspace,omitempty"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*n = ManifestNode{
			Kind: wire.Kind, Name: wire.Name, Instruction: wire.Instruction,
			ModelResource: wire.ModelResource, ToolResources: wire.ToolResources,
			KnowledgeResources: wire.KnowledgeResources,
			CallableEntries:    wire.CallableEntries, Generation: wire.Generation,
			Memory: wire.Memory, Artifact: wire.Artifact, AddSessionSummary: wire.AddSessionSummary, Workspace: wire.Workspace,
		}
		return nil
	case agentdomain.NodeKindSequence, agentdomain.NodeKindParallel:
		var wire struct {
			Kind     agentdomain.NodeKind `json:"kind"`
			Name     string               `json:"name,omitempty"`
			Children []string             `json:"children"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*n = ManifestNode{Kind: wire.Kind, Name: wire.Name, Children: wire.Children}
		return nil
	case agentdomain.NodeKindLoop:
		var wire struct {
			Kind          agentdomain.NodeKind `json:"kind"`
			Name          string               `json:"name,omitempty"`
			Body          string               `json:"body"`
			MaxIterations int64                `json:"max_iterations"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*n = ManifestNode{
			Kind: wire.Kind, Name: wire.Name, Body: wire.Body,
			MaxIterations: wire.MaxIterations,
		}
		return nil
	default:
		return ErrInvalidManifestContent
	}
}

type ManifestResources struct {
	Executors map[string]ManifestExecutorResource  `json:"executors,omitempty"`
	Models    map[string]ManifestModelResource     `json:"models"`
	Tools     map[string]ManifestToolResource      `json:"tools"`
	Knowledge map[string]ManifestKnowledgeResource `json:"knowledge"`
	Storage   map[string]ManifestStorageResource   `json:"storage"`
}

type ManifestModelResource struct {
	AdapterVersion string                  `json:"adapter_version"`
	Kind           profiledomain.ModelKind `json:"kind"`
	Model          string                  `json:"model"`
	BaseURL        string                  `json:"base_url"`
	Credential     CredentialUse           `json:"credential"`
	Capabilities   []string                `json:"capabilities"`
}

type ManifestToolResource struct {
	AdapterVersion string                 `json:"adapter_version"`
	Kind           profiledomain.ToolKind `json:"kind"`
	ServerURL      string                 `json:"server_url"`
	ToolsetName    string                 `json:"toolset_name"`
	ToolName       string                 `json:"tool_name"`
	Auth           ManifestToolAuth       `json:"auth"`
	Capability     string                 `json:"capability"`
}

type ManifestToolAuth struct {
	Kind       profiledomain.AuthKind
	Credential *CredentialUse
}

func (a ManifestToolAuth) MarshalJSON() ([]byte, error) {
	if a.Kind == profiledomain.AuthKindBearer {
		return json.Marshal(struct {
			Kind       profiledomain.AuthKind `json:"kind"`
			Credential *CredentialUse         `json:"credential"`
		}{a.Kind, a.Credential})
	}
	return json.Marshal(struct {
		Kind profiledomain.AuthKind `json:"kind"`
	}{a.Kind})
}

func (a *ManifestToolAuth) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		Kind profiledomain.AuthKind `json:"kind"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	switch discriminator.Kind {
	case profiledomain.AuthKindNone:
		var wire struct {
			Kind profiledomain.AuthKind `json:"kind"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*a = ManifestToolAuth{Kind: wire.Kind}
		return nil
	case profiledomain.AuthKindBearer:
		var wire struct {
			Kind       profiledomain.AuthKind `json:"kind"`
			Credential *CredentialUse         `json:"credential"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		if wire.Credential == nil {
			return ErrInvalidManifestContent
		}
		*a = ManifestToolAuth{Kind: wire.Kind, Credential: wire.Credential}
		return nil
	default:
		return ErrInvalidManifestContent
	}
}

type ManifestKnowledgeResource struct {
	Backend        *datav1.Snapshot            `json:"backend,omitempty"`
	AdapterVersion string                      `json:"adapter_version"`
	Kind           profiledomain.KnowledgeKind `json:"kind"`
	Host           string                      `json:"host"`
	Port           int64                       `json:"port"`
	TLS            bool                        `json:"tls"`
	Collection     string                      `json:"collection"`
	Credential     *CredentialUse              `json:"credential,omitempty"`
	Embedding      ManifestEmbeddingResource   `json:"embedding"`
	Capability     string                      `json:"capability"`
}

type ManifestEmbeddingResource struct {
	Model      string        `json:"model"`
	BaseURL    string        `json:"base_url"`
	Dimensions int64         `json:"dimensions"`
	Credential CredentialUse `json:"credential"`
}

type ArtifactCredentials struct {
	AccessKeyID     CredentialUse `json:"access_key_id"`
	SecretAccessKey CredentialUse `json:"secret_access_key"`
}
type ManifestStorageResource struct {
	Credentials      *ArtifactCredentials             `json:"credentials,omitempty"`
	MetadataContract string                           `json:"metadata_contract,omitempty"`
	Backend          *datav1.Snapshot                 `json:"backend,omitempty"`
	AdapterVersion   string                           `json:"adapter_version"`
	Kind             profiledomain.StorageKind        `json:"kind"`
	Destination      profiledomain.StorageDestination `json:"destination"`
	Credential       CredentialUse                    `json:"credential"`
}

type ResolvedRequirements struct {
	Executors map[string]string `json:"executors,omitempty"`
	Models    map[string]string `json:"models"`
	Tools     map[string]string `json:"tools"`
	Knowledge map[string]string `json:"knowledge"`
}

type CompiledManifest struct {
	Content          ManifestContent
	CanonicalContent json.RawMessage
	ContentDigest    string
	CredentialUses   []CredentialUse
}

// ManifestView is the stable public projection. Its types have no field that
// can hold an internal CredentialID, purpose, or audience digest.
type ManifestView struct {
	Runtime                *ManifestRuntime          `json:"runtime,omitempty"`
	SchemaVersion          string                    `json:"schema_version"`
	CompilerVersion        string                    `json:"compiler_version"`
	RuntimeContractVersion string                    `json:"runtime_contract_version"`
	PlatformContract       PlatformContractReference `json:"platform_contract"`
	TenantID               string                    `json:"tenant_id"`
	Sources                ManifestSources           `json:"sources"`
	AgentPlan              AgentPlan                 `json:"agent_plan"`
	Resources              ManifestResourceView      `json:"resources"`
	ResolvedRequirements   ResolvedRequirements      `json:"resolved_requirements"`
	StorageRoles           map[string]string         `json:"storage_roles"`
	Execution              ExecutionPolicy           `json:"execution"`
}

// PublicManifestView names the public, credential-identifier-free projection
// at the application boundary.
type PublicManifestView = ManifestView

type ManifestResourceView struct {
	Executors map[string]ManifestExecutorResource      `json:"executors,omitempty"`
	Models    map[string]ManifestModelResourceView     `json:"models"`
	Tools     map[string]ManifestToolResourceView      `json:"tools"`
	Knowledge map[string]ManifestKnowledgeResourceView `json:"knowledge"`
	Storage   map[string]ManifestStorageResourceView   `json:"storage"`
}

type ManifestModelResourceView struct {
	AdapterVersion    string                  `json:"adapter_version"`
	Kind              profiledomain.ModelKind `json:"kind"`
	Model             string                  `json:"model"`
	BaseURL           string                  `json:"base_url"`
	CredentialPresent bool                    `json:"credential_present"`
	Capabilities      []string                `json:"capabilities"`
}

type ManifestToolResourceView struct {
	AdapterVersion string                 `json:"adapter_version"`
	Kind           profiledomain.ToolKind `json:"kind"`
	ServerURL      string                 `json:"server_url"`
	ToolsetName    string                 `json:"toolset_name"`
	ToolName       string                 `json:"tool_name"`
	Auth           ManifestToolAuthView   `json:"auth"`
	Capability     string                 `json:"capability"`
}

type ManifestToolAuthView struct {
	Kind              profiledomain.AuthKind `json:"kind"`
	CredentialPresent bool                   `json:"credential_present,omitempty"`
}

type ManifestKnowledgeResourceView struct {
	Backend           *ManagedBackendView         `json:"backend,omitempty"`
	AdapterVersion    string                      `json:"adapter_version"`
	Kind              profiledomain.KnowledgeKind `json:"kind"`
	Host              string                      `json:"host"`
	Port              int64                       `json:"port"`
	TLS               bool                        `json:"tls"`
	Collection        string                      `json:"collection"`
	CredentialPresent bool                        `json:"credential_present"`
	Embedding         ManifestEmbeddingView       `json:"embedding"`
	Capability        string                      `json:"capability"`
}

type ManifestEmbeddingView struct {
	Model             string `json:"model"`
	BaseURL           string `json:"base_url"`
	Dimensions        int64  `json:"dimensions"`
	CredentialPresent bool   `json:"credential_present"`
}

type ManifestStorageResourceView struct {
	MetadataContract  string                           `json:"metadata_contract,omitempty"`
	Backend           *ManagedBackendView              `json:"backend,omitempty"`
	AdapterVersion    string                           `json:"adapter_version"`
	Kind              profiledomain.StorageKind        `json:"kind"`
	Destination       profiledomain.StorageDestination `json:"destination"`
	CredentialPresent bool                             `json:"credential_present"`
}

func NewManifestView(content ManifestContent) ManifestView {
	normalized := normalizeManifestContent(content)
	view := ManifestView{
		SchemaVersion: normalized.SchemaVersion, CompilerVersion: normalized.CompilerVersion,
		RuntimeContractVersion: normalized.RuntimeContractVersion,
		PlatformContract:       normalized.PlatformContract, TenantID: normalized.TenantID,
		Sources: normalized.Sources, AgentPlan: normalized.AgentPlan,
		ResolvedRequirements: normalized.ResolvedRequirements,
		StorageRoles:         cloneMap(normalized.StorageRoles), Execution: normalized.Execution,
		Resources: ManifestResourceView{
			Executors: cloneMap(normalized.Resources.Executors),
			Models:    make(map[string]ManifestModelResourceView, len(normalized.Resources.Models)),
			Tools:     make(map[string]ManifestToolResourceView, len(normalized.Resources.Tools)),
			Knowledge: make(map[string]ManifestKnowledgeResourceView, len(normalized.Resources.Knowledge)),
			Storage:   make(map[string]ManifestStorageResourceView, len(normalized.Resources.Storage)),
		},
	}
	for name, resource := range normalized.Resources.Models {
		view.Resources.Models[name] = ManifestModelResourceView{
			AdapterVersion: resource.AdapterVersion, Kind: resource.Kind,
			Model: resource.Model, BaseURL: resource.BaseURL, CredentialPresent: true,
			Capabilities: append([]string(nil), resource.Capabilities...),
		}
	}
	for name, resource := range normalized.Resources.Tools {
		view.Resources.Tools[name] = ManifestToolResourceView{
			AdapterVersion: resource.AdapterVersion, Kind: resource.Kind,
			ServerURL: resource.ServerURL, ToolsetName: resource.ToolsetName,
			ToolName: resource.ToolName, Capability: resource.Capability,
			Auth: ManifestToolAuthView{
				Kind:              resource.Auth.Kind,
				CredentialPresent: resource.Auth.Kind == profiledomain.AuthKindBearer && resource.Auth.Credential != nil,
			},
		}
	}
	for name, resource := range normalized.Resources.Knowledge {
		if resource.Backend != nil {
			view.Resources.Knowledge[name] = ManifestKnowledgeResourceView{CredentialPresent: resource.Credential != nil, Backend: backendView(*resource.Backend), Kind: resource.Kind, AdapterVersion: resource.AdapterVersion, Embedding: ManifestEmbeddingView{Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL, Dimensions: resource.Embedding.Dimensions, CredentialPresent: true}, Capability: resource.Capability}
			continue
		}
		view.Resources.Knowledge[name] = ManifestKnowledgeResourceView{
			AdapterVersion: resource.AdapterVersion, Kind: resource.Kind,
			Host: resource.Host, Port: resource.Port, TLS: resource.TLS,
			Collection: resource.Collection, CredentialPresent: resource.Credential != nil,
			Embedding: ManifestEmbeddingView{
				Model: resource.Embedding.Model, BaseURL: resource.Embedding.BaseURL,
				Dimensions: resource.Embedding.Dimensions, CredentialPresent: true,
			},
			Capability: resource.Capability,
		}
	}
	for name, resource := range normalized.Resources.Storage {
		if resource.Backend != nil {
			view.Resources.Storage[name] = ManifestStorageResourceView{CredentialPresent: resource.Credential.CredentialID != "" || resource.Credentials != nil, MetadataContract: resource.MetadataContract, Backend: backendView(*resource.Backend), Kind: resource.Kind, AdapterVersion: resource.AdapterVersion}
			continue
		}
		view.Resources.Storage[name] = ManifestStorageResourceView{
			AdapterVersion: resource.AdapterVersion, Kind: resource.Kind,
			Destination: resource.Destination, CredentialPresent: true,
		}
	}
	view.Runtime = normalized.Runtime
	view.Execution.AllowedEndpointHosts = publicEndpointHosts(normalized)
	return view
}

func NewPublicManifestView(content ManifestContent) PublicManifestView {
	return NewManifestView(content)
}

// PublicManifestViewFromJSON strictly decodes stored internal content before
// deriving its explicit public projection. PostgreSQL jsonb does not preserve
// canonical byte ordering, so this path validates and normalizes the typed
// content without requiring the retrieved representation itself to be JCS.
// Unknown fields and trailing JSON remain rejected; callers must separately
// verify the digest over a freshly canonicalized representation.
func PublicManifestViewFromJSON(raw json.RawMessage) (PublicManifestView, error) {
	content, err := DecodeManifestContent(raw)
	if err != nil {
		return PublicManifestView{}, err
	}
	if err := validateManifestCredentialShape(content); err != nil {
		return PublicManifestView{}, err
	}
	normalized, _, _, err := CanonicalizeManifest(content)
	if err != nil {
		return PublicManifestView{}, ErrInvalidManifestContent
	}
	return NewPublicManifestView(normalized), nil
}
