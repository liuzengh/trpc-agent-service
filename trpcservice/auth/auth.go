// Package auth resolves a request's bearer token to a tenant and a role.
//
// It is deliberately small: the approved plan's first milestone names two
// roles and an IM-user allowlist, not a permission system. What it does
// enforce is that a role comes from an authenticated identity — the token,
// verified against a hash stored by the control plane — never from a field a
// caller typed into a body or a query string.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// Roles the platform distinguishes. admin is scoped to one tenant: it can
// publish revisions and manage bindings for its own tenant and nothing else.
const (
	RoleAdmin = "admin"
	RoleUser  = "user"
)

// ErrUnauthenticated means no usable credential was presented.
var ErrUnauthenticated = errors.New("auth: no valid credential")

// ErrForbidden means the credential is real but may not do this.
var ErrForbidden = errors.New("auth: not permitted")

// Actor is an authenticated identity, narrowed to one tenant.
type Actor struct {
	TenantID    string
	PrincipalID int64
	Role        string
}

// CanAdmin reports whether this actor may change control-plane state. A
// plain user may talk to an agent; nothing else.
func (a Actor) CanAdmin() bool { return a.Role == RoleAdmin }

// RequireAdmin is the guard every write-side handler runs through, so the
// check is in one place and not re-derived per endpoint.
func (a Actor) RequireAdmin() error {
	if !a.CanAdmin() {
		return ErrForbidden
	}
	return nil
}

// Resolver turns a bearer token into an Actor.
type Resolver struct {
	db *controlplane.DB
}

// NewResolver wires the authenticator to the control plane's database.
func NewResolver(db *controlplane.DB) *Resolver { return &Resolver{db: db} }

// HashToken is the single place a management token is turned into what the
// database stores. It lives here rather than in controlplane so a token never
// travels further than its own hash.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// TokenMatches compares a candidate token against a stored hash without
// leaking the stored value or timing the comparison by its content.
func TokenMatches(token, storedHash string) bool {
	return subtle.ConstantTimeCompare([]byte(HashToken(token)), []byte(storedHash)) == 1
}

// Resolve turns an Authorization header value into an Actor.
func (r *Resolver) Resolve(ctx context.Context, authorizationHeader string) (Actor, error) {
	token := strings.TrimSpace(strings.TrimPrefix(authorizationHeader, "Bearer "))
	if token == "" {
		return Actor{}, ErrUnauthenticated
	}
	m, err := r.db.PrincipalForToken(ctx, HashToken(token))
	if err != nil {
		if errors.Is(err, controlplane.ErrNotFound) {
			return Actor{}, ErrUnauthenticated
		}
		return Actor{}, err
	}
	return Actor{TenantID: m.TenantID, PrincipalID: m.PrincipalID, Role: m.Role}, nil
}

// CreateBootstrapAdmin registers a principal that can sign in over the
// Admin API. It is called "bootstrap" rather than "create" because the very
// first one cannot itself be authorised — there is not yet an admin to ask —
// so this is the command an operator runs once, not something the Admin API
// exposes.
func (r *Resolver) CreateBootstrapAdmin(ctx context.Context, tenantID, subject, token string) (int64, error) {
	if token == "" {
		return 0, errors.New("auth: a bootstrap admin needs a token")
	}
	return r.db.CreatePrincipalWithMembership(ctx, tenantID, subject, RoleAdmin, HashToken(token))
}
