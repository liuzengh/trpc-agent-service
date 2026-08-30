package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StatusActive    = "active"
	StatusSuspended = "suspended"
	StatusDeleted   = "deleted"
)

type Tenant struct {
	ID             string        `json:"tenant_id"`
	Name           string        `json:"name"`
	Status         string        `json:"status"`
	ConfigVersion  int64         `json:"config_version"`
	DefaultAgentID string        `json:"default_agent_id"`
	Backend        BackendPolicy `json:"backend"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type AgentApp struct {
	TenantID       string `json:"tenant_id"`
	ID             string `json:"agent_id"`
	Name           string `json:"name"`
	Version        int64  `json:"version"`
	Status         string `json:"status"`
	ModelConfigRef string `json:"model_config_ref"`
	SystemPrompt   string `json:"system_prompt"`
	ToolPolicyID   string `json:"tool_policy_id"`
	GuardrailRef   string `json:"guardrail_ref"`
}

type ChannelBinding struct {
	TenantID       string `json:"tenant_id"`
	ID             string `json:"binding_id"`
	Channel        string `json:"channel"`
	ExternalAppID  string `json:"external_app_id"`
	SecretRef      string `json:"secret_ref"`
	VerifyTokenRef string `json:"verify_token_ref"`
	Enabled        bool   `json:"enabled"`
}

type BackendPolicy struct {
	Session string `json:"session"`
	Memory  string `json:"memory"`
	Vector  string `json:"vector"`
	Object  string `json:"object"`
}

func (p BackendPolicy) Validate() error {
	for name, value := range map[string]string{
		"session": p.Session,
		"memory":  p.Memory,
		"vector":  p.Vector,
		"object":  p.Object,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s backend is required", name)
		}
	}
	return nil
}

type TenantContext struct {
	TenantID         string
	AgentAppID       string
	BindingID        string
	Channel          string
	ExternalUser     string
	ExternalChat     string
	ExternalThreadID string
	InternalUser     string
	SessionID        string
	RequestID        string
	MessageID        string
	TraceID          string
	ConfigVersion    int64
	Permissions      []string
	BackendPolicy    BackendPolicy
}

func (t Tenant) Validate() error {
	if !validID(t.ID) {
		return errors.New("tenant_id is required and must be a valid identifier")
	}
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("tenant name is required")
	}
	if t.Status != StatusActive && t.Status != StatusSuspended && t.Status != StatusDeleted {
		return fmt.Errorf("invalid tenant status %q", t.Status)
	}
	if t.ConfigVersion < 1 {
		return errors.New("config_version must be positive")
	}
	if err := t.Backend.Validate(); err != nil {
		return fmt.Errorf("backend policy: %w", err)
	}
	return nil
}

func (a AgentApp) Validate() error {
	if !validID(a.TenantID) || !validID(a.ID) {
		return errors.New("tenant_id and agent_id are required")
	}
	if strings.TrimSpace(a.Name) == "" {
		return errors.New("agent name is required")
	}
	if a.Version < 1 {
		return errors.New("agent version must be positive")
	}
	if a.Status == "" {
		return errors.New("agent status is required")
	}
	return nil
}

func (b ChannelBinding) Validate() error {
	if !validID(b.TenantID) || !validID(b.ID) {
		return errors.New("tenant_id and binding_id are required")
	}
	if strings.TrimSpace(b.Channel) == "" || strings.TrimSpace(b.ExternalAppID) == "" {
		return errors.New("channel and external_app_id are required")
	}
	if strings.TrimSpace(b.SecretRef) == "" {
		return errors.New("secret_ref is required")
	}
	return nil
}

func (tc TenantContext) Validate() error {
	if !validID(tc.TenantID) || !validID(tc.AgentAppID) || !validID(tc.BindingID) {
		return errors.New("tenant context requires tenant, agent, and binding IDs")
	}
	if tc.ConfigVersion < 1 {
		return errors.New("tenant context requires a positive config version")
	}
	if err := tc.BackendPolicy.Validate(); err != nil {
		return fmt.Errorf("backend policy: %w", err)
	}
	if tc.Channel == "" || tc.RequestID == "" || tc.MessageID == "" || tc.TraceID == "" {
		return errors.New("tenant context requires channel and request identifiers")
	}
	return nil
}

func (tc TenantContext) AllowsTenant(id string) bool { return id != "" && tc.TenantID == id }
func (tc TenantContext) HasPermission(permission string) bool {
	for _, candidate := range tc.Permissions {
		if candidate == permission {
			return true
		}
	}
	return false
}

func WithContext(ctx context.Context, tc TenantContext) context.Context {
	tc.Permissions = append([]string(nil), tc.Permissions...)
	return context.WithValue(ctx, contextKey{}, tc)
}
func FromContext(ctx context.Context) (TenantContext, bool) {
	tc, ok := ctx.Value(contextKey{}).(TenantContext)
	if ok {
		tc.Permissions = append([]string(nil), tc.Permissions...)
	}
	return tc, ok
}

type contextKey struct{}

func validID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !validIDRune(r) {
			return false
		}
	}
	return true
}

func validIDRune(r rune) bool {
	return r == '-' || r == '_' || r == '.' ||
		r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}
