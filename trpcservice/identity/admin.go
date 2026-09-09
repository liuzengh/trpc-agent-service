package identity

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// minAdminCredentialLength is the floor for a control-plane credential.
//
// It is twice the data-plane floor on purpose. A chat key can call one app of
// one tenant; an admin key creates tenants, publishes revisions and decides
// what the platform executes, so the two are not the same credential with a
// different label and must not share a strength bound. Existing chat keys keep
// minCredentialLength — raising that would refuse credentials already in use —
// and admin keys are new, so their floor is set where it belongs from the
// start.
const minAdminCredentialLength = 32

// AdminRole is what a control-plane credential is allowed to be. There are
// exactly two, and they are a closed set: a role is not a string a manifest can
// invent, because an unrecognized role would otherwise have to be given some
// default meaning.
type AdminRole string

const (
	// RolePlatformAdmin manages every tenant, and is the only role that may
	// create one. It belongs to no tenant.
	RolePlatformAdmin AdminRole = "platform_admin"
	// RoleTenantAdmin manages exactly one tenant: the one it is bound to.
	RoleTenantAdmin AdminRole = "tenant_admin"
)

// AdminIdentity is everything the platform knows about a control-plane caller
// after authentication. It is derived from a server-side credential only.
//
// It is a separate type from Identity rather than a flag on it. The two answer
// different questions — Identity says which app a caller may chat with, this
// says which tenants a caller may administer — and the fields do not overlap:
// a platform admin has no tenant and no app list, which is precisely the shape
// Identity.Validate refuses. Sharing one struct would mean either loosening
// that validation for every chat credential or storing a platform admin as a
// chat identity with impossible fields; both trade a compile-time distinction
// for a runtime one.
type AdminIdentity struct {
	Role        AdminRole
	PrincipalID string
	// TenantID is the tenant a tenant_admin is bound to, and is empty for a
	// platform_admin. It is not a scope hint: it is the whole of what a
	// tenant_admin may reach.
	TenantID string
}

// Validate rejects any admin identity that cannot fully scope a request.
func (i AdminIdentity) Validate() error {
	if err := tenant.ValidateResourceID("principal id", i.PrincipalID); err != nil {
		return err
	}
	switch i.Role {
	case RolePlatformAdmin:
		// A platform admin that carried a tenant would read as scoped while
		// being unscoped, and every comparison against it would be a false
		// negative waiting to be treated as a grant.
		if i.TenantID != "" {
			return fmt.Errorf(
				"%w: a %s identity must not carry a tenant", tenant.ErrInvalidArgument,
				RolePlatformAdmin)
		}
		return nil
	case RoleTenantAdmin:
		return tenant.ValidateResourceID("tenant id", i.TenantID)
	default:
		return fmt.Errorf(
			"%w: unsupported admin role %q", tenant.ErrInvalidArgument, i.Role)
	}
}

// IsPlatformAdmin reports whether this identity administers the whole platform.
// An identity that does not validate is nothing.
func (i AdminIdentity) IsPlatformAdmin() bool {
	return i.Validate() == nil && i.Role == RolePlatformAdmin
}

// AllowsTenant reports whether this identity may administer tenantID. The
// comparison is exact: a tenant_admin reaches its own tenant and nothing else,
// and a platform admin reaches any tenant whose id is well formed.
func (i AdminIdentity) AllowsTenant(tenantID string) bool {
	if i.Validate() != nil {
		return false
	}
	if tenant.ValidateResourceID("tenant id", tenantID) != nil {
		return false
	}
	if i.Role == RolePlatformAdmin {
		return true
	}
	return i.TenantID == tenantID
}

// AdminAuthenticator turns a bearer credential into a control-plane identity.
// It must fail closed: an unknown credential is an error, never an anonymous
// identity.
//
// The method is named AuthenticateAdmin rather than Authenticate so that no
// type can satisfy both this and Authenticator by accident. A single value
// serving as both authenticators would make an admin key a chat key, which is
// the exact confusion the two interfaces exist to prevent.
type AdminAuthenticator interface {
	AuthenticateAdmin(ctx context.Context, bearerToken string) (AdminIdentity, error)
}

// StaticAdminAPIKeyAuthenticator resolves admin identities from a fixed key
// set. Like its chat counterpart it retains only the SHA-256 digest of each
// key, so the long-lived map never holds a credential that a memory dump or an
// accidental log of the struct could disclose, and lookup by digest removes the
// prefix timing signal a raw string comparison would leak.
type StaticAdminAPIKeyAuthenticator struct {
	identities map[[sha256.Size]byte]AdminIdentity
}

// NewStaticAdminAPIKeyAuthenticator builds the control-plane authenticator.
// keys maps a plaintext API key to the role it carries; the plaintext is
// discarded once its digest is computed.
//
// No error here names a key or a digest. The role and, for a tenant admin, the
// tenant are enough to locate the offending manifest entry, and are not secret.
func NewStaticAdminAPIKeyAuthenticator(
	keys map[string]AdminIdentity,
) (*StaticAdminAPIKeyAuthenticator, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("identity: at least one admin API key is required")
	}
	identities := make(map[[sha256.Size]byte]AdminIdentity, len(keys))
	for key, granted := range keys {
		// Before the length floor: thirty-two spaces clears the floor and
		// authenticates nobody, and "your key is whitespace" is the more useful
		// half of that to be told.
		if err := validateBearerCredential(key); err != nil {
			return nil, fmt.Errorf(
				"identity: admin API key for role %q cannot be sent as a Bearer credential: %w",
				granted.Role, err)
		}
		if len(key) < minAdminCredentialLength {
			return nil, fmt.Errorf(
				"identity: admin API key for role %q must have at least %d characters",
				granted.Role,
				minAdminCredentialLength,
			)
		}
		if err := granted.Validate(); err != nil {
			return nil, fmt.Errorf("identity: invalid admin API key grant: %w", err)
		}
		identities[sha256.Sum256([]byte(key))] = granted
	}
	return &StaticAdminAPIKeyAuthenticator{identities: identities}, nil
}

// AuthenticateAdmin returns the grant of a known credential. The key set is
// read-only after construction, so concurrent callers need no lock.
func (a *StaticAdminAPIKeyAuthenticator) AuthenticateAdmin(
	ctx context.Context,
	bearerToken string,
) (AdminIdentity, error) {
	if a == nil || len(a.identities) == 0 {
		return AdminIdentity{}, ErrUnauthenticated
	}
	if ctx == nil {
		return AdminIdentity{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return AdminIdentity{}, err
	}
	if len(bearerToken) < minAdminCredentialLength {
		return AdminIdentity{}, ErrUnauthenticated
	}
	granted, ok := a.identities[sha256.Sum256([]byte(bearerToken))]
	if !ok {
		return AdminIdentity{}, ErrUnauthenticated
	}
	if err := granted.Validate(); err != nil {
		return AdminIdentity{}, ErrUnauthenticated
	}
	return granted, nil
}
