// Package identity defines the login-surface seams: identity sources
// (IdentityProvider) and server-side login sessions (SessionStore).
// Decisions live in docs/adr/0001-enterprise-login-identity.md and the
// glossary in CONTEXT.md.
package identity

import (
	"context"
	"errors"
	"time"
)

// Role is a tenant-scoped role. Deployment-wide administration is carried by
// SessionUser.IsSystemAdmin instead of being modeled as another tenant role.
type Role string

const (
	// RoleAdmin is a tenant administrator.
	RoleAdmin Role = "admin"
	// RoleMember is an ordinary tenant member without management rights.
	RoleMember Role = "member"
)

type ProviderType string

const (
	ProviderLocal  ProviderType = "local"
	ProviderWeCom  ProviderType = "wecom"
	ProviderFeishu ProviderType = "feishu"
	ProviderOIDC   ProviderType = "oidc"
	// ProviderMock is an explicitly enabled development/test login source.
	// It must never be registered implicitly by production composition.
	ProviderMock ProviderType = "mock"
)

// ProviderDescriptor is safe to expose on the public login page. Provider
// secrets and protocol endpoints remain deployment-side configuration.
type ProviderDescriptor struct {
	ProviderID  string       `json:"provider_id"`
	Type        ProviderType `json:"type"`
	DisplayName string       `json:"display_name"`
}

// AuthRequest contains per-browser authorization values. PKCEVerifier never
// leaves the HttpOnly login transaction cookie; only its challenge is sent to
// providers that support PKCE.
type AuthRequest struct {
	State         string
	Nonce         string
	PKCEChallenge string
}

// AuthExchange contains the provider callback proof and the values bound to
// the browser authorization transaction.
type AuthExchange struct {
	Code         string
	Nonce        string
	PKCEVerifier string
}

// Identity is a provider-confirmed external principal. ProviderID+SubjectID is
// the stable login key. EnterpriseID is the provider-native enterprise key
// (WeCom CorpID, Feishu tenant_key, OIDC issuer) and is always verified against
// the configured provider before this value is returned.
type Identity struct {
	ProviderID   string
	ProviderType ProviderType
	EnterpriseID string
	SubjectID    string
	DisplayName  string
	Email        string
}

// IdentityProvider is one concrete enterprise login source. A deployment may
// register multiple provider instances simultaneously.
type IdentityProvider interface {
	Descriptor() ProviderDescriptor
	Begin(request AuthRequest) (authURL string, err error)
	Exchange(ctx context.Context, exchange AuthExchange) (Identity, error)
}

// IdentityProviderConfiguration exposes only non-secret identifiers that help
// administrators verify third-party OAuth/OIDC settings. It is intentionally
// separate from IdentityProvider so login providers never need to expose
// secrets or protocol internals on the public login surface.
type IdentityProviderConfiguration interface {
	ConfigurationMetadata() map[string]string
}

// TenantRole is one tenant membership carried by a login session: the tenant
// and the member's role inside it. Authorization (console visibility, write
// gates) is decided from the full set, never a single "first" membership.
type TenantRole struct {
	TenantID                 string `json:"tenant_id"`
	DisplayName              string `json:"display_name"`
	Role                     Role   `json:"role"`
	Status                   string `json:"status"`
	ConversationContentAudit bool   `json:"conversation_content_audit"`
}

// SessionUser is the authenticated principal stored in a login session.
type SessionUser struct {
	PlatformUserID     string
	DisplayName        string
	Email              string
	Role               Role
	IsSystemAdmin      bool
	MustChangePassword bool
	// Tenants is the complete authorization set captured at login. Role is only
	// a display convenience and is empty when multiple memberships exist.
	Tenants []TenantRole
}

var ErrPlatformUserSuspended = errors.New("platform user is suspended")

// SessionPrincipalResolver resolves current grants for an authenticated person.
// Login sessions carry identity only; authorization is refreshed per request.
type SessionPrincipalResolver interface {
	ResolveSessionUser(ctx context.Context, platformUserID string) (SessionUser, error)
}

// ErrSessionNotFound means no session exists for the ID.
var ErrSessionNotFound = errors.New("identity session not found")

// ErrSessionExpired means the session existed but outlived its TTL.
var ErrSessionExpired = errors.New("identity session expired")

// AuditRecorder keeps login-domain audit writes separate from IdentityStore.
type AuditRecorder interface {
	// RecordAudit writes one identity audit event (action/result/detail).
	RecordAudit(ctx context.Context, action, result, detail string) error
}

// SessionStore is the login-state seam. Backends: Redis (shared across nodes)
// and Memory (tests / local-only). Sessions carry an owner and TTL.
type SessionStore interface {
	// Create issues a new session ID for the user, valid for ttl.
	Create(ctx context.Context, user SessionUser, ttl time.Duration) (sessionID string, err error)
	// Get returns the user for a live session; ErrSessionNotFound or
	// ErrSessionExpired otherwise. Expired sessions are removed.
	Get(ctx context.Context, sessionID string) (SessionUser, error)
	// Delete invalidates a session.
	Delete(ctx context.Context, sessionID string) error
	// DeleteForUser immediately invalidates every browser session for a user.
	// Membership/role changes call this so stale authorization never survives
	// until the normal session TTL.
	DeleteForUser(ctx context.Context, platformUserID string) error
}
