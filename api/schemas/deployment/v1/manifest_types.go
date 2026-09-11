package deploymentv1

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
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
	Kind              string

	Name               string
	Instruction        string
	ModelResource      string
	ToolResources      []string
	KnowledgeResources []string
	CallableEntries    []string
	Generation         *Generation

	Children      []string
	Body          string
	MaxIterations int64
}

func (n ManifestNode) MarshalJSON() ([]byte, error) {
	switch n.Kind {
	case "llm":
		return json.Marshal(struct {
			Kind               string             `json:"kind"`
			Name               string             `json:"name,omitempty"`
			Instruction        string             `json:"instruction"`
			ModelResource      string             `json:"model_resource"`
			ToolResources      []string           `json:"tool_resources"`
			KnowledgeResources []string           `json:"knowledge_resources"`
			CallableEntries    []string           `json:"callable_entries"`
			Generation         *Generation        `json:"generation,omitempty"`
			Memory             *ManifestMemory    `json:"memory,omitempty"`
			Artifact           *ManifestArtifact  `json:"artifact,omitempty"`
			AddSessionSummary  *bool              `json:"add_session_summary,omitempty"`
			Workspace          *ManifestWorkspace `json:"workspace,omitempty"`
		}{
			n.Kind, n.Name, n.Instruction, n.ModelResource,
			n.ToolResources, n.KnowledgeResources, n.CallableEntries, n.Generation, n.Memory, n.Artifact, n.AddSessionSummary, n.Workspace,
		})
	case "sequence", "parallel":
		return json.Marshal(struct {
			Kind     string   `json:"kind"`
			Name     string   `json:"name,omitempty"`
			Children []string `json:"children"`
		}{n.Kind, n.Name, n.Children})
	case "loop":
		return json.Marshal(struct {
			Kind          string `json:"kind"`
			Name          string `json:"name,omitempty"`
			Body          string `json:"body"`
			MaxIterations int64  `json:"max_iterations"`
		}{n.Kind, n.Name, n.Body, n.MaxIterations})
	default:
		return json.Marshal(struct {
			Kind string `json:"kind"`
		}{n.Kind})
	}
}

func (n *ManifestNode) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	switch discriminator.Kind {
	case "llm":
		var wire struct {
			Kind               string             `json:"kind"`
			Name               string             `json:"name,omitempty"`
			Instruction        string             `json:"instruction"`
			ModelResource      string             `json:"model_resource"`
			ToolResources      []string           `json:"tool_resources"`
			KnowledgeResources []string           `json:"knowledge_resources"`
			CallableEntries    []string           `json:"callable_entries"`
			Generation         *Generation        `json:"generation,omitempty"`
			Memory             *ManifestMemory    `json:"memory,omitempty"`
			Artifact           *ManifestArtifact  `json:"artifact,omitempty"`
			AddSessionSummary  *bool              `json:"add_session_summary,omitempty"`
			Workspace          *ManifestWorkspace `json:"workspace,omitempty"`
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
	case "sequence", "parallel":
		var wire struct {
			Kind     string   `json:"kind"`
			Name     string   `json:"name,omitempty"`
			Children []string `json:"children"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*n = ManifestNode{Kind: wire.Kind, Name: wire.Name, Children: wire.Children}
		return nil
	case "loop":
		var wire struct {
			Kind          string `json:"kind"`
			Name          string `json:"name,omitempty"`
			Body          string `json:"body"`
			MaxIterations int64  `json:"max_iterations"`
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
		return ErrInvalidManifest
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
	AdapterVersion string        `json:"adapter_version"`
	Kind           string        `json:"kind"`
	Model          string        `json:"model"`
	BaseURL        string        `json:"base_url"`
	Credential     CredentialUse `json:"credential"`
	Capabilities   []string      `json:"capabilities"`
}

type ManifestToolResource struct {
	AdapterVersion string           `json:"adapter_version"`
	Kind           string           `json:"kind"`
	ServerURL      string           `json:"server_url"`
	ToolsetName    string           `json:"toolset_name"`
	ToolName       string           `json:"tool_name"`
	Auth           ManifestToolAuth `json:"auth"`
	Capability     string           `json:"capability"`
}

type ManifestToolAuth struct {
	Kind       string
	Credential *CredentialUse
}

func (a ManifestToolAuth) MarshalJSON() ([]byte, error) {
	if a.Kind == "bearer" {
		return json.Marshal(struct {
			Kind       string         `json:"kind"`
			Credential *CredentialUse `json:"credential"`
		}{a.Kind, a.Credential})
	}
	return json.Marshal(struct {
		Kind string `json:"kind"`
	}{a.Kind})
}

func (a *ManifestToolAuth) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	switch discriminator.Kind {
	case "none":
		var wire struct {
			Kind string `json:"kind"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		*a = ManifestToolAuth{Kind: wire.Kind}
		return nil
	case "bearer":
		var wire struct {
			Kind       string         `json:"kind"`
			Credential *CredentialUse `json:"credential"`
		}
		if err := strictDecodeJSON(data, &wire); err != nil {
			return err
		}
		if wire.Credential == nil {
			return ErrInvalidManifest
		}
		*a = ManifestToolAuth{Kind: wire.Kind, Credential: wire.Credential}
		return nil
	default:
		return ErrInvalidManifest
	}
}

type ManifestKnowledgeResource struct {
	Backend        *datav1.Snapshot          `json:"backend,omitempty"`
	AdapterVersion string                    `json:"adapter_version"`
	Kind           string                    `json:"kind"`
	Host           string                    `json:"host"`
	Port           int64                     `json:"port"`
	TLS            bool                      `json:"tls"`
	Collection     string                    `json:"collection"`
	Credential     *CredentialUse            `json:"credential,omitempty"`
	Embedding      ManifestEmbeddingResource `json:"embedding"`
	Capability     string                    `json:"capability"`
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
	Credentials      *ArtifactCredentials `json:"credentials,omitempty"`
	MetadataContract string               `json:"metadata_contract,omitempty"`
	Backend          *datav1.Snapshot     `json:"backend,omitempty"`
	AdapterVersion   string               `json:"adapter_version"`
	Kind             string               `json:"kind"`
	Destination      StorageDestination   `json:"destination"`
	Credential       CredentialUse        `json:"credential"`
}

type ResolvedRequirements struct {
	Executors map[string]string `json:"executors,omitempty"`
	Models    map[string]string `json:"models"`
	Tools     map[string]string `json:"tools"`
	Knowledge map[string]string `json:"knowledge"`
}

type Generation struct {
	Temperature     *float64 `json:"temperature,omitempty"`
	MaxOutputTokens *int64   `json:"max_output_tokens,omitempty"`
}
type StorageDestination struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	SSLMode  string `json:"sslmode"`
}
type ExecutionPolicy struct {
	Backend              string   `json:"backend"`
	AllowedEndpointHosts []string `json:"allowed_endpoint_hosts"`
	MaxRunSeconds        int64    `json:"max_run_seconds"`
	MaxToolCalls         int64    `json:"max_tool_calls"`
	MaxOutputTokens      int64    `json:"max_output_tokens"`
}
