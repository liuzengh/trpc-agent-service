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
	TenantID           string        `json:"tenant_id"`
	ID                 string        `json:"binding_id"`
	Channel            string        `json:"channel"`
	ExternalAppID      string        `json:"external_app_id"`
	SecretRef          string        `json:"secret_ref"`
	VerifyTokenRef     string        `json:"verify_token_ref"`
	ExternalTargetType string        `json:"external_target_type,omitempty"`
	ExternalTargetID   string        `json:"external_target_id,omitempty"`
	Status             BindingStatus `json:"status"`
	Enabled            bool          `json:"enabled"`
	Version            int64         `json:"version"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
	ExpiresAt          time.Time     `json:"expires_at,omitempty"`
	DisabledAt         time.Time     `json:"disabled_at,omitempty"`
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
	ExternalChatType string
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
	if b.Channel != ChannelLark && b.Channel != ChannelTelegram {
		return ErrUnsupportedBindingChannel
	}
	if !validID(b.ExternalAppID) {
		return errors.New("channel and external_app_id are required")
	}
	if b.SecretRef != "" {
		if err := ValidateSecretRef(b.SecretRef); err != nil {
			return err
		}
	}
	if b.VerifyTokenRef != "" {
		if err := ValidateSecretRef(b.VerifyTokenRef); err != nil {
			return err
		}
	}
	status := b.EffectiveStatus()
	if !validBindingStatus(status) {
		return ErrInvalidBindingStatus
	}
	if b.Status != "" && ((status == BindingStatusActive) != b.Enabled) {
		return ErrBindingStateConflict
	}
	if status == BindingStatusActive && b.SecretRef == "" {
		return ErrBindingSecretRequired
	}
	if b.Version < 0 {
		return errors.New("binding version cannot be negative")
	}
	targetType := b.EffectiveTargetType()
	if targetType != BindingTargetMessage && targetType != BindingTargetUser && targetType != BindingTargetChat {
		return ErrInvalidBindingTarget
	}
	if targetType == BindingTargetMessage {
		if b.ExternalTargetID != "" {
			return ErrInvalidBindingTarget
		}
	} else if !validOpaqueID(b.ExternalTargetID, 256) {
		return ErrInvalidBindingTarget
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
	for _, value := range []string{tc.ExternalUser, tc.ExternalChat, tc.ExternalChatType, tc.ExternalThreadID, tc.InternalUser} {
		if value != "" && !validOpaqueID(value, 256) {
			return errors.New("tenant context contains an invalid external identity")
		}
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

func (b ChannelBinding) EffectiveStatus() BindingStatus {
	if b.Status != "" {
		return b.Status
	}
	if b.Enabled {
		return BindingStatusActive
	}
	return BindingStatusDisabled
}

func (b ChannelBinding) EffectiveTargetType() string {
	if b.ExternalTargetType == "" {
		return BindingTargetMessage
	}
	return b.ExternalTargetType
}

func (b ChannelBinding) IsUsableAt(now time.Time) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if b.EffectiveStatus() == BindingStatusExpired || (!b.ExpiresAt.IsZero() && !now.Before(b.ExpiresAt)) {
		return ErrBindingExpired
	}
	if b.EffectiveStatus() != BindingStatusActive || !b.Enabled {
		return ErrBindingDisabled
	}
	return nil
}

func (b ChannelBinding) Canonical() ChannelBinding {
	if b.Status == "" {
		b.Status = b.EffectiveStatus()
	}
	if b.Version == 0 {
		b.Version = 1
	}
	if b.ExternalTargetType == "" {
		b.ExternalTargetType = BindingTargetMessage
	}
	return b
}

func (b ChannelBinding) Enable(now time.Time) (ChannelBinding, error) {
	b = b.Canonical()
	if err := b.Validate(); err != nil {
		return ChannelBinding{}, err
	}
	if b.SecretRef == "" {
		return ChannelBinding{}, ErrBindingSecretRequired
	}
	b.Enabled = true
	b.Status = BindingStatusActive
	b.DisabledAt = time.Time{}
	b.Version++
	b.UpdatedAt = now.UTC()
	return b, nil
}

func (b ChannelBinding) Disable(now time.Time) (ChannelBinding, error) {
	b = b.Canonical()
	if err := b.Validate(); err != nil && !errors.Is(err, ErrBindingSecretRequired) {
		return ChannelBinding{}, err
	}
	b.Enabled = false
	b.Status = BindingStatusDisabled
	b.DisabledAt = now.UTC()
	b.Version++
	b.UpdatedAt = now.UTC()
	return b, nil
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

func validOpaqueID(value string, max int) bool {
	if value == "" || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validIDRune(r rune) bool {
	return r == '-' || r == '_' || r == '.' ||
		r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

type BindingStatus string

const (
	BindingStatusActive   BindingStatus = "active"
	BindingStatusDisabled BindingStatus = "disabled"
	BindingStatusExpired  BindingStatus = "expired"
	BindingStatusRetired  BindingStatus = "retired"
	ChannelLark                         = "lark"
	ChannelTelegram                     = "telegram"
	BindingTargetMessage                = "message"
	BindingTargetUser                   = "user"
	BindingTargetChat                   = "chat"
)

var (
	ErrUnsupportedBindingChannel = errors.New("unsupported channel binding channel")
	ErrInvalidBindingStatus      = errors.New("invalid channel binding status")
	ErrBindingStateConflict      = errors.New("channel binding status and enabled state conflict")
	ErrBindingSecretRequired     = errors.New("enabled channel binding requires a secret reference")
	ErrBindingDisabled           = errors.New("channel binding is disabled")
	ErrBindingExpired            = errors.New("channel binding is expired")
	ErrInvalidBindingTarget      = errors.New("invalid channel binding target")
)

func validBindingStatus(status BindingStatus) bool {
	switch status {
	case BindingStatusActive, BindingStatusDisabled, BindingStatusExpired, BindingStatusRetired:
		return true
	default:
		return false
	}
}
