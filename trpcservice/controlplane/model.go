// Package controlplane stores tenant, Agent application and binding metadata.
package controlplane

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	StatusActive    = "active"
	StatusSuspended = "suspended"
	StatusDisabled  = "disabled"
)

// Tenant is the top-level isolation and billing boundary.
type Tenant struct {
	ID              string          `json:"tenant_id"`
	DisplayName     string          `json:"display_name"`
	Status          string          `json:"status"`
	Region          string          `json:"region"`
	QuotaConfig     json.RawMessage `json:"quota_config"`
	AuditPolicy     json.RawMessage `json:"audit_policy"`
	SecretNamespace string          `json:"secret_namespace"`
	Version         int64           `json:"version"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// AgentApp is a named Agent deployment owned by one tenant.
type AgentApp struct {
	ID               string          `json:"app_id"`
	TenantID         string          `json:"tenant_id"`
	Name             string          `json:"name"`
	Description      string          `json:"description"`
	Status           string          `json:"status"`
	StableRevisionID string          `json:"stable_revision_id,omitempty"`
	RolloutPolicy    json.RawMessage `json:"rollout_policy"`
	Version          int64           `json:"version"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// AgentRevision is an immutable, reproducible Agent configuration snapshot.
type AgentRevision struct {
	ID              string          `json:"revision_id"`
	TenantID        string          `json:"tenant_id"`
	AppID           string          `json:"app_id"`
	RevisionNo      int64           `json:"revision_no"`
	AgentType       string          `json:"agent_type"`
	AgentConfig     json.RawMessage `json:"agent_config"`
	ModelConfig     json.RawMessage `json:"model_config"`
	ToolPolicy      json.RawMessage `json:"tool_policy"`
	KnowledgeConfig json.RawMessage `json:"knowledge_config"`
	MemoryConfig    json.RawMessage `json:"memory_config"`
	GuardrailConfig json.RawMessage `json:"guardrail_config"`
	Checksum        string          `json:"checksum"`
	CreatedBy       string          `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
}

// ChannelBinding maps one external channel account to a tenant Agent app.
type ChannelBinding struct {
	ID          string          `json:"channel_binding_id"`
	TenantID    string          `json:"tenant_id"`
	AppID       string          `json:"app_id"`
	ChannelType string          `json:"channel_type"`
	AccountID   string          `json:"account_id"`
	CallbackKey string          `json:"callback_key"`
	Config      json.RawMessage `json:"config"`
	SecretRef   string          `json:"secret_ref"`
	Status      string          `json:"status"`
	Version     int64           `json:"version"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// BackendBinding chooses a physical backend for one tenant resource type.
type BackendBinding struct {
	ID             string          `json:"binding_id"`
	TenantID       string          `json:"tenant_id"`
	AppID          string          `json:"app_id,omitempty"`
	ResourceType   string          `json:"resource_type"`
	BackendType    string          `json:"backend_type"`
	Config         json.RawMessage `json:"config"`
	SecretRef      string          `json:"secret_ref,omitempty"`
	IsolationLevel string          `json:"isolation_level"`
	MigrationState string          `json:"migration_state"`
	Version        int64           `json:"version"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// BootstrapData is the initial control-plane snapshot for local development.
type BootstrapData struct {
	Tenants         []Tenant
	Apps            []AgentApp
	Revisions       []AgentRevision
	ChannelBindings []ChannelBinding
	BackendBindings []BackendBinding
}

// DefaultBootstrapData preserves the current tutorial as a real tenant/app
// pair while the HTTP API is migrated to explicit tenant routing.
func DefaultBootstrapData() BootstrapData {
	now := time.Now().UTC()
	revision := AgentRevision{
		ID:         "tutorial-revision-1",
		TenantID:   "tutorial-tenant",
		AppID:      "tutorial-app",
		RevisionNo: 1,
		AgentType:  "llm",
		AgentConfig: json.RawMessage(`{
            "name":"tutorial-agent",
            "description":"A minimal agent for learning tRPC-Agent-Go",
            "instruction":"Reply clearly and use the conversation history supplied by the session."
        }`),
		ModelConfig:     json.RawMessage(`{"source":"startup_env"}`),
		ToolPolicy:      json.RawMessage(`{"allowed_tools":[]}`),
		KnowledgeConfig: json.RawMessage(`{}`),
		MemoryConfig:    json.RawMessage(`{}`),
		GuardrailConfig: json.RawMessage(`{}`),
		CreatedBy:       "bootstrap",
		CreatedAt:       now,
	}
	revision.Checksum = RevisionChecksum(revision)
	return BootstrapData{
		Tenants: []Tenant{{
			ID:              "tutorial-tenant",
			DisplayName:     "Tutorial Tenant",
			Status:          StatusActive,
			Region:          "local",
			QuotaConfig:     json.RawMessage(`{}`),
			AuditPolicy:     json.RawMessage(`{"level":"basic"}`),
			SecretNamespace: "local/tutorial",
			Version:         1,
			CreatedAt:       now,
			UpdatedAt:       now,
		}},
		Apps: []AgentApp{{
			ID:               "tutorial-app",
			TenantID:         "tutorial-tenant",
			Name:             "tutorial-agent",
			Description:      "Getting-started Agent application",
			Status:           StatusActive,
			StableRevisionID: revision.ID,
			RolloutPolicy:    json.RawMessage(`{"mode":"stable"}`),
			Version:          1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}},
		Revisions: []AgentRevision{revision},
		ChannelBindings: []ChannelBinding{{
			ID:          "tutorial-http-binding",
			TenantID:    "tutorial-tenant",
			AppID:       "tutorial-app",
			ChannelType: "http",
			AccountID:   "local-http",
			CallbackKey: "tutorial-http",
			Config:      json.RawMessage(`{}`),
			SecretRef:   "local://none",
			Status:      StatusActive,
			Version:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}},
		BackendBindings: []BackendBinding{
			{
				ID:             "tutorial-session-backend",
				TenantID:       "tutorial-tenant",
				AppID:          "tutorial-app",
				ResourceType:   "session",
				BackendType:    "startup_config",
				Config:         json.RawMessage(`{}`),
				IsolationLevel: "shared",
				MigrationState: "active",
				Version:        1,
				CreatedAt:      now,
				UpdatedAt:      now,
			},
		},
	}
}

// RevisionChecksum returns a stable checksum for immutable Agent behavior.
func RevisionChecksum(revision AgentRevision) string {
	digest := sha256.New()
	for _, value := range [][]byte{
		[]byte(revision.AgentType),
		revision.AgentConfig,
		revision.ModelConfig,
		revision.ToolPolicy,
		revision.KnowledgeConfig,
		revision.MemoryConfig,
		revision.GuardrailConfig,
	} {
		canonical := canonicalJSON(value)
		_, _ = digest.Write(canonical)
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func canonicalJSON(value []byte) []byte {
	if len(value) == 0 {
		return value
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return value
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return value
	}
	return canonical
}
