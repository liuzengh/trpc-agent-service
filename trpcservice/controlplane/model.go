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
	ID              string
	DisplayName     string
	Status          string
	Region          string
	QuotaConfig     json.RawMessage
	AuditPolicy     json.RawMessage
	SecretNamespace string
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// AgentApp is a named Agent deployment owned by one tenant.
type AgentApp struct {
	ID               string
	TenantID         string
	Name             string
	Description      string
	Status           string
	StableRevisionID string
	RolloutPolicy    json.RawMessage
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// AgentRevision is an immutable, reproducible Agent configuration snapshot.
type AgentRevision struct {
	ID              string
	TenantID        string
	AppID           string
	RevisionNo      int64
	AgentType       string
	AgentConfig     json.RawMessage
	ModelConfig     json.RawMessage
	ToolPolicy      json.RawMessage
	KnowledgeConfig json.RawMessage
	MemoryConfig    json.RawMessage
	GuardrailConfig json.RawMessage
	Checksum        string
	CreatedBy       string
	CreatedAt       time.Time
}

// ChannelBinding maps one external channel account to a tenant Agent app.
type ChannelBinding struct {
	ID          string
	TenantID    string
	AppID       string
	ChannelType string
	AccountID   string
	CallbackKey string
	Config      json.RawMessage
	SecretRef   string
	Status      string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// BackendBinding chooses a physical backend for one tenant resource type.
type BackendBinding struct {
	ID             string
	TenantID       string
	AppID          string
	ResourceType   string
	BackendType    string
	Config         json.RawMessage
	SecretRef      string
	IsolationLevel string
	MigrationState string
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
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
		ToolPolicy:      json.RawMessage(`{"allowed":[]}`),
		KnowledgeConfig: json.RawMessage(`{}`),
		MemoryConfig:    json.RawMessage(`{}`),
		GuardrailConfig: json.RawMessage(`{}`),
		CreatedBy:       "bootstrap",
		CreatedAt:       now,
	}
	revision.Checksum = revisionChecksum(revision)
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

func revisionChecksum(revision AgentRevision) string {
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
		_, _ = digest.Write(value)
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}
