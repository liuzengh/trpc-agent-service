// Package channels models tenant-owned IM channel bindings.
package channels

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	publicRouteEntropyBytes = 16
	// MaxPublicRouteIDLength bounds the opaque route value accepted at the
	// public IM ingress boundary.
	MaxPublicRouteIDLength = 256
)

var (
	// ErrBindingNotFound means an opaque public route does not identify a
	// channel binding.
	ErrBindingNotFound = errors.New("channel binding not found")
	// ErrBindingChannelMismatch means a public route belongs to another
	// channel than the URL requested.
	ErrBindingChannelMismatch = errors.New("channel binding channel mismatch")
	// ErrBindingInactive means a binding is not allowed to receive provider events.
	ErrBindingInactive = errors.New("channel binding is not active")
	// ErrIdentityInactive means a provider identity is not allowed to enter
	// admission. Suspended identities must not be reused by provider retries.
	ErrIdentityInactive = errors.New("channel identity is not active")
	// ErrChannelNotProvisionable means the control plane must not create or
	// activate a channel without a running adapter/ingress entry point.
	ErrChannelNotProvisionable = errors.New("channel has no active ingress")
)

// Channel identifies an external IM platform.
type Channel string

const (
	// ChannelWeCom identifies Enterprise WeChat.
	ChannelWeCom Channel = "wecom"
	// ChannelWeChatCustomer identifies WeChat customer service.
	ChannelWeChatCustomer Channel = "wechat_customer"
	// ChannelFeishu identifies Feishu.
	ChannelFeishu Channel = "feishu"
)

// Validate checks that the channel is one of the supported platform values.
func (c Channel) Validate() error {
	if !validChannel(c) {
		return errors.New("channel is invalid")
	}
	return nil
}

// ValidateProvisionable checks that a channel has a live ingress adapter.
// Validate intentionally remains broader so legacy persisted bindings can be
// read and suspended without making an unimplemented channel configurable.
func (c Channel) ValidateProvisionable() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c == ChannelWeChatCustomer {
		return ErrChannelNotProvisionable
	}
	return nil
}

// BindingStatus is the lifecycle state of an IM channel binding.
type BindingStatus string

const (
	// BindingActive accepts inbound events and outbound replies.
	BindingActive BindingStatus = "ACTIVE"
	// BindingSuspended rejects new inbound provider events.
	BindingSuspended BindingStatus = "SUSPENDED"
)

// ConnectionStatus describes the latest long-connection observation. It is
// operational metadata, distinct from the administrative BindingStatus.
type ConnectionStatus string

const (
	ConnectionReady    ConnectionStatus = "READY"
	ConnectionNotReady ConnectionStatus = "NOT_READY"
	ConnectionDegraded ConnectionStatus = "DEGRADED"
)

// Binding maps one tenant-owned external IM account to an application.
type Binding struct {
	TenantID        string  `json:"tenant_id"`
	AppID           string  `json:"app_id"`
	BindingID       string  `json:"binding_id"`
	Channel         Channel `json:"channel"`
	ExternalAccount string  `json:"external_account"`
	// Secret is the provider credential reference: WeCom Bot Secret or
	// Feishu App Secret. It is resolved only inside the binding scope.
	Secret tenant.SecretRef `json:"secret_ref"`
	// PublicRouteID is retained for the legacy route locator. Long-connection
	// adapters do not require or use it.
	PublicRouteID    string           `json:"public_route_id,omitempty"`
	BindingRevision  int64            `json:"binding_revision"`
	Status           BindingStatus    `json:"status"`
	ConnectionStatus ConnectionStatus `json:"connection_status"`
	LastConnectedAt  *time.Time       `json:"last_connected_at,omitempty"`
	LastError        string           `json:"last_error,omitempty"`
}

// BindingSnapshot is a value copy of one provider event. Its revision
// identifies the binding version observed before provider authentication and
// admission. HTTP channels may also carry a public route.
type BindingSnapshot struct {
	Binding
}

// Snapshot returns a value copy of the binding for one request.
func (b Binding) Snapshot() BindingSnapshot {
	return BindingSnapshot{Binding: b}
}

// NewPublicRouteID generates an unpredictable URL-safe route identifier for a
// channel binding.
func NewPublicRouteID() (string, error) {
	return newPublicRouteID(rand.Reader)
}

func newPublicRouteID(random io.Reader) (string, error) {
	if random == nil {
		return "", errors.New("public route random source is required")
	}
	value := make([]byte, publicRouteEntropyBytes)
	if _, err := io.ReadFull(random, value); err != nil {
		return "", fmt.Errorf("generate public route id: %w", err)
	}
	return publicRoutePrefix + base64.RawURLEncoding.EncodeToString(value), nil
}

const publicRoutePrefix = "r_"

// ValidatePublicRouteID checks the syntax and size of an opaque public route.
// Unpredictability is established by NewPublicRouteID or the control-plane
// caller that provisions a binding; this function only validates transport
// safety.
func ValidatePublicRouteID(publicRouteID string) error {
	if publicRouteID == "" {
		return errors.New("public_route_id is required")
	}
	if len(publicRouteID) > MaxPublicRouteIDLength {
		return errors.New("public_route_id is too long")
	}
	for i := 0; i < len(publicRouteID); i++ {
		value := publicRouteID[i]
		if (value >= 'a' && value <= 'z') ||
			(value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') ||
			value == '-' || value == '_' {
			continue
		}
		return errors.New("public_route_id contains invalid characters")
	}
	return nil
}

// Scope returns the tenant application scope that owns the binding.
func (b Binding) Scope() tenant.Scope {
	return tenant.Scope{TenantID: b.TenantID, AppID: b.AppID}
}

// Validate checks that the binding is structurally valid for persistence.
func (b Binding) Validate() error {
	if b.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if b.AppID == "" {
		return errors.New("app_id is required")
	}
	if b.BindingID == "" {
		return errors.New("binding_id is required")
	}
	if err := b.Channel.Validate(); err != nil {
		return err
	}
	if b.ExternalAccount == "" {
		return errors.New("external_account is required")
	}
	if b.Channel == ChannelWeCom || b.Channel == ChannelFeishu {
		if err := b.Secret.Validate(); err != nil {
			return fmt.Errorf("provider secret_ref: %w", err)
		}
	} else if b.Secret.Name != "" || b.Secret.Version != "" {
		if err := b.Secret.Validate(); err != nil {
			return fmt.Errorf("provider secret_ref: %w", err)
		}
	}
	if b.PublicRouteID != "" {
		if err := ValidatePublicRouteID(b.PublicRouteID); err != nil {
			return err
		}
	}
	if b.BindingRevision <= 0 {
		return errors.New("binding_revision must be positive")
	}
	if !validBindingStatus(b.Status) {
		return errors.New("binding status is invalid")
	}
	if b.ConnectionStatus != "" && !validConnectionStatus(b.ConnectionStatus) {
		return errors.New("connection status is invalid")
	}
	return nil
}

// Identity maps a platform user into the internal user model.
type Identity struct {
	TenantID            string
	AppID               string
	BindingID           string
	Channel             Channel
	ExternalUserKeyHash string
	UserID              string
	Status              string
	// KeyVersion is the HMAC key version used for ExternalUserKeyHash. It is
	// distinct from ProviderTargetEnvelope.KeyVersion.
	KeyVersion             string
	ProviderTargetEnvelope TargetEnvelope
}

// Conversation maps an external chat or thread into a session principal.
type Conversation struct {
	TenantID            string
	AppID               string
	BindingID           string
	Channel             Channel
	ExternalChatKeyHash string
	ThreadKeyHash       string
	ConversationID      string
	SessionPrincipalID  string
	Scope               string
	// KeyVersion is the HMAC key version used for ExternalChatKeyHash and
	// ThreadKeyHash. It is distinct from ProviderTargetEnvelope.KeyVersion.
	KeyVersion             string
	ProviderTargetEnvelope TargetEnvelope
}

func validChannel(channel Channel) bool {
	switch channel {
	case ChannelWeCom, ChannelWeChatCustomer, ChannelFeishu:
		return true
	default:
		return false
	}
}

func validBindingStatus(status BindingStatus) bool {
	return status == BindingActive || status == BindingSuspended
}

func validConnectionStatus(status ConnectionStatus) bool {
	return status == ConnectionReady || status == ConnectionNotReady || status == ConnectionDegraded
}
