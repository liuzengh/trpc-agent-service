// Package adminauth provides authenticated, route-scoped control-plane
// principals. It intentionally does not trust caller-supplied actor headers:
// the audit identity is derived from a bootstrap bearer credential or an
// operator-owned external PrincipalResolver.
package adminauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Permission is one control-plane capability.
type Permission string

const (
	PermissionTenantRead        Permission = "tenant.read"
	PermissionTenantCreate      Permission = "tenant.create"
	PermissionTenantWrite       Permission = "tenant.write"
	PermissionTenantDelete      Permission = "tenant.delete"
	PermissionAgentWrite        Permission = "agent.write"
	PermissionAgentPublish      Permission = "agent.publish"
	PermissionAgentDeploy       Permission = "agent.deploy"
	PermissionToolApprovalRead  Permission = "tool.approval.read"
	PermissionToolApprovalGrant Permission = "tool.approval.grant"
	// Reconciliation can authorize a retry after an uncertain external side
	// effect. Keep it platform-admin-only until an independently audited
	// operator workflow exists for narrower roles.
	PermissionExecutionReconcile Permission = "execution.reconcile"
	PermissionOutboxReplay       Permission = "outbox.replay"
)

// Role is an operator-owned permission bundle.
type Role string

const (
	RolePlatformAdmin  Role = "platform_admin"
	RoleTenantAdmin    Role = "tenant_admin"
	RoleReleaseManager Role = "release_manager"
	RoleAuditor        Role = "auditor"
)

var (
	ErrUnauthenticated = errors.New("admin principal is unauthenticated")
	ErrForbidden       = errors.New("admin principal is forbidden")
)

var rolePermissions = map[Role]map[Permission]struct{}{
	RolePlatformAdmin: {
		PermissionTenantRead: {}, PermissionTenantCreate: {}, PermissionTenantWrite: {},
		PermissionTenantDelete: {}, PermissionAgentWrite: {}, PermissionAgentPublish: {},
		PermissionAgentDeploy: {}, PermissionExecutionReconcile: {}, PermissionOutboxReplay: {},
		PermissionToolApprovalRead: {}, PermissionToolApprovalGrant: {},
	},
	RoleTenantAdmin: {
		PermissionTenantRead: {}, PermissionTenantWrite: {}, PermissionAgentWrite: {},
		PermissionToolApprovalRead: {}, PermissionToolApprovalGrant: {},
	},
	RoleReleaseManager: {
		PermissionTenantRead: {}, PermissionAgentWrite: {}, PermissionAgentPublish: {},
		PermissionAgentDeploy: {},
	},
	RoleAuditor: {PermissionTenantRead: {}},
}

// Principal is immutable authentication output carried in request context.
// An empty tenant set means all tenants only for platform_admin; it means no
// tenants for every other role.
type Principal struct {
	ID      string
	Role    Role
	tenants map[string]struct{}
}

func (p Principal) Has(permission Permission) bool {
	permissions := rolePermissions[p.Role]
	_, ok := permissions[permission]
	return ok
}

func (p Principal) AllowsTenant(tenantID string) bool {
	if tenantID == "" {
		return false
	}
	if p.Role == RolePlatformAdmin {
		return true
	}
	_, ok := p.tenants[tenantID]
	return ok
}

// TenantIDs returns a defensive copy for response filtering.
func (p Principal) TenantIDs() map[string]struct{} {
	result := make(map[string]struct{}, len(p.tenants))
	for id := range p.tenants {
		result[id] = struct{}{}
	}
	return result
}

type principalContextKey struct{}

func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, error) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok || strings.TrimSpace(principal.ID) == "" || rolePermissions[principal.Role] == nil {
		return Principal{}, ErrUnauthenticated
	}
	return principal, nil
}

// CredentialConfig is parsed from the operator-owned ADMIN_PRINCIPALS_JSON
// secret. Tokens are hashed during construction and never retained as strings.
type CredentialConfig struct {
	ID        string   `json:"id"`
	Token     string   `json:"token"`
	Role      Role     `json:"role"`
	TenantIDs []string `json:"tenantIds,omitempty"`
}

// NewPrincipal constructs a validated server-side Principal for an external
// identity provider adapter. Callers cannot set the internal tenant map
// directly; all role and scope invariants are applied here.
func NewPrincipal(id string, role Role, tenantIDs []string) (Principal, error) {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 256 || strings.ContainsAny(id, "\r\n\x00") {
		return Principal{}, fmt.Errorf("admin principal id is required and must be safe")
	}
	if rolePermissions[role] == nil {
		return Principal{}, fmt.Errorf("admin principal %q has unsupported role %q", id, role)
	}
	if role == RolePlatformAdmin && len(tenantIDs) != 0 {
		return Principal{}, fmt.Errorf("platform_admin %q must not carry a tenant allowlist", id)
	}
	tenants := make(map[string]struct{}, len(tenantIDs))
	for _, raw := range tenantIDs {
		tenantID := strings.TrimSpace(raw)
		if tenantID == "" || tenantID == "*" || len(tenantID) > 64 || strings.ContainsAny(tenantID, "/\\\r\n\x00") {
			return Principal{}, fmt.Errorf("admin principal %q has invalid tenant scope", id)
		}
		tenants[tenantID] = struct{}{}
	}
	return Principal{ID: id, Role: role, tenants: tenants}, nil
}

type credential struct {
	tokenHash [sha256.Size]byte
	principal Principal
}

// Authenticator authenticates bootstrap and optional scoped principals.
type Authenticator struct {
	credentials []credential
	// resolver is an operator-owned identity boundary for deployments that
	// terminate OIDC/IAP or mTLS before the Admin service. The resolver must
	// validate the external assertion and return a server-side Principal; the
	// Admin package never trusts identity headers by itself.
	resolver PrincipalResolver
}

// PrincipalResolver authenticates an externally asserted operator identity.
// Implementations must verify the provider signature, issuer/audience,
// expiry, and any mTLS identity before returning a Principal. Returning a
// Principal with an unknown role or empty ID is rejected by Middleware.
type PrincipalResolver interface {
	ResolvePrincipal(context.Context, *http.Request) (Principal, error)
}

// PrincipalResolverFunc adapts a function to PrincipalResolver.
type PrincipalResolverFunc func(context.Context, *http.Request) (Principal, error)

func (f PrincipalResolverFunc) ResolvePrincipal(ctx context.Context, request *http.Request) (Principal, error) {
	if f == nil {
		return Principal{}, ErrUnauthenticated
	}
	return f(ctx, request)
}

// NewExternalAuthenticator creates an Admin authenticator backed by an
// operator-owned OIDC/IAP/mTLS verifier. The verifier is intentionally
// injected instead of implemented here so the platform does not pretend to
// validate provider-specific tokens or client certificates itself.
func NewExternalAuthenticator(resolver PrincipalResolver) (*Authenticator, error) {
	if resolver == nil {
		return nil, fmt.Errorf("external admin principal resolver is required")
	}
	return &Authenticator{resolver: resolver}, nil
}

// NewAuthenticator maps bootstrapToken to an internal platform administrator
// and parses optionalJSON as a JSON array of additional scoped credentials.
func NewAuthenticator(bootstrapToken, optionalJSON string) (*Authenticator, error) {
	if len(bootstrapToken) < 32 {
		return nil, fmt.Errorf("bootstrap admin token must contain at least 32 characters")
	}
	configs := []CredentialConfig{{
		ID: "bootstrap-platform-admin", Token: bootstrapToken, Role: RolePlatformAdmin,
	}}
	if strings.TrimSpace(optionalJSON) != "" {
		decoder := json.NewDecoder(strings.NewReader(optionalJSON))
		decoder.DisallowUnknownFields()
		var additional []CredentialConfig
		if err := decoder.Decode(&additional); err != nil {
			return nil, fmt.Errorf("decode ADMIN_PRINCIPALS_JSON: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode ADMIN_PRINCIPALS_JSON: exactly one JSON value is required")
		}
		configs = append(configs, additional...)
	}

	authenticator := &Authenticator{credentials: make([]credential, 0, len(configs))}
	seenIDs := make(map[string]struct{}, len(configs))
	seenHashes := make(map[string]struct{}, len(configs))
	for _, config := range configs {
		principal, tokenHash, err := validateCredential(config)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seenIDs[principal.ID]; duplicate {
			return nil, fmt.Errorf("duplicate admin principal id %q", principal.ID)
		}
		hashKey := hex.EncodeToString(tokenHash[:])
		if _, duplicate := seenHashes[hashKey]; duplicate {
			return nil, fmt.Errorf("admin credentials must use distinct tokens")
		}
		seenIDs[principal.ID], seenHashes[hashKey] = struct{}{}, struct{}{}
		authenticator.credentials = append(authenticator.credentials, credential{tokenHash: tokenHash, principal: principal})
	}
	return authenticator, nil
}

func validateCredential(config CredentialConfig) (Principal, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	id := strings.TrimSpace(config.ID)
	if len(config.Token) < 32 {
		return Principal{}, zero, fmt.Errorf("admin principal %q token must contain at least 32 characters", id)
	}
	principal, err := NewPrincipal(id, config.Role, config.TenantIDs)
	if err != nil {
		return Principal{}, zero, err
	}
	return principal, sha256.Sum256([]byte(config.Token)), nil
}

func (a *Authenticator) authenticate(request *http.Request) (Principal, error) {
	if a == nil || request == nil {
		return Principal{}, ErrUnauthenticated
	}
	if a.resolver != nil {
		principal, err := a.resolver.ResolvePrincipal(request.Context(), request)
		if err != nil {
			return Principal{}, ErrUnauthenticated
		}
		// Normalize through the same constructor used for bearer credentials.
		// External resolvers are an authentication boundary, not an authority to
		// bypass ID, role, or tenant-scope invariants.
		tenantIDs := make([]string, 0, len(principal.tenants))
		for tenantID := range principal.tenants {
			tenantIDs = append(tenantIDs, tenantID)
		}
		validated, validationErr := NewPrincipal(principal.ID, principal.Role, tenantIDs)
		if validationErr != nil {
			return Principal{}, ErrUnauthenticated
		}
		return validated, nil
	}
	if len(a.credentials) == 0 {
		return Principal{}, ErrUnauthenticated
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	const prefix = "Bearer "
	header := values[0]
	if !strings.HasPrefix(header, prefix) || len(header) <= len(prefix) {
		return Principal{}, ErrUnauthenticated
	}
	presented := sha256.Sum256([]byte(header[len(prefix):]))
	match := -1
	for i := range a.credentials {
		if subtle.ConstantTimeCompare(presented[:], a.credentials[i].tokenHash[:]) == 1 {
			match = i
		}
	}
	if match < 0 {
		return Principal{}, ErrUnauthenticated
	}
	return a.credentials[match].principal, nil
}

// Middleware authenticates before invoking any downstream handler.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		principal, err := a.authenticate(request)
		if err != nil {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(writer, request.WithContext(ContextWithPrincipal(request.Context(), principal)))
	})
}

// RequireMethods applies the permission selected by HTTP method. Authorization
// happens before the downstream handler can read tenant or Agent data.
func RequireMethods(methods map[string]Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		permission, ok := methods[request.Method]
		if !ok {
			next.ServeHTTP(writer, request)
			return
		}
		principal, err := PrincipalFromContext(request.Context())
		if err != nil {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !principal.Has(permission) {
			http.Error(writer, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// RequireTenant enforces principal tenant scope inside a decoded/path-bound
// handler and returns the immutable authenticated principal.
func RequireTenant(ctx context.Context, tenantID string) (Principal, error) {
	principal, err := PrincipalFromContext(ctx)
	if err != nil {
		return Principal{}, err
	}
	if !principal.AllowsTenant(tenantID) {
		return Principal{}, ErrForbidden
	}
	return principal, nil
}
