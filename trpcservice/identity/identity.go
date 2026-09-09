// Package identity derives and carries the caller identity of data-plane
// traffic. Nothing here reads a client-supplied identity field: a caller
// presents a credential and the platform decides who it is.
package identity

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrUnauthenticated reports a missing, malformed or unknown credential.
	ErrUnauthenticated = errors.New("identity: request is not authenticated")
	// ErrForbidden reports an authenticated caller acting outside its grant.
	ErrForbidden = errors.New("identity: caller is not allowed")
)

// minCredentialLength keeps trivially guessable static keys out of the
// authenticator at build time instead of failing open on every request.
const minCredentialLength = 16

// Identity is everything the platform knows about a caller after
// authentication. It is derived from a server-side credential only.
type Identity struct {
	TenantID      string
	PrincipalID   string
	AllowedAppIDs []string
}

// Validate rejects any identity that cannot fully scope a request.
func (i Identity) Validate() error {
	if err := tenant.ValidateResourceID("tenant id", i.TenantID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("principal id", i.PrincipalID); err != nil {
		return err
	}
	if len(i.AllowedAppIDs) == 0 {
		return fmt.Errorf("%w: identity grants no agent app", tenant.ErrInvalidArgument)
	}
	for _, appID := range i.AllowedAppIDs {
		if err := tenant.ValidateResourceID("allowed app id", appID); err != nil {
			return err
		}
	}
	return nil
}

// AllowsApp reports whether this identity may address appID. An identity that
// does not validate allows nothing.
func (i Identity) AllowsApp(appID string) bool {
	if i.Validate() != nil {
		return false
	}
	if tenant.ValidateResourceID("app id", appID) != nil {
		return false
	}
	for _, allowed := range i.AllowedAppIDs {
		if allowed == appID {
			return true
		}
	}
	return false
}

// Clone detaches AllowedAppIDs so a stored grant cannot be edited through a
// value the caller received or supplied.
func (i Identity) Clone() Identity {
	i.AllowedAppIDs = append([]string(nil), i.AllowedAppIDs...)
	return i
}

// Authenticator turns a bearer credential into a platform identity. It must
// fail closed: an unknown credential is an error, never an anonymous identity.
type Authenticator interface {
	Authenticate(ctx context.Context, bearerToken string) (Identity, error)
}

// StaticAPIKeyAuthenticator resolves identities from a fixed key set. Only the
// SHA-256 digest of each key is retained, so the long-lived map never holds a
// credential that a memory dump or an accidental log of the struct could
// disclose. Hashing before lookup also removes the prefix timing signal a raw
// string comparison would leak.
type StaticAPIKeyAuthenticator struct {
	identities map[[sha256.Size]byte]Identity
}

// NewStaticAPIKeyAuthenticator builds a local development authenticator. keys
// maps a plaintext API key to the grant it carries; the plaintext is discarded
// once its digest is computed.
func NewStaticAPIKeyAuthenticator(
	keys map[string]Identity,
) (*StaticAPIKeyAuthenticator, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("identity: at least one API key is required")
	}
	identities := make(map[[sha256.Size]byte]Identity, len(keys))
	for key, granted := range keys {
		// The data plane presents its credential the same way the control plane
		// does, so it is refused for the same reasons. This cannot turn away a
		// key that works today: every key it refuses is one the header parser
		// could never have reproduced.
		if err := validateBearerCredential(key); err != nil {
			return nil, fmt.Errorf(
				"identity: API key for tenant %q cannot be sent as a Bearer credential: %w",
				granted.TenantID, err)
		}
		if len(key) < minCredentialLength {
			return nil, fmt.Errorf(
				"identity: API key for tenant %q must have at least %d characters",
				granted.TenantID,
				minCredentialLength,
			)
		}
		if err := granted.Validate(); err != nil {
			return nil, fmt.Errorf("identity: invalid API key grant: %w", err)
		}
		identities[sha256.Sum256([]byte(key))] = granted.Clone()
	}
	return &StaticAPIKeyAuthenticator{identities: identities}, nil
}

// Authenticate returns the grant of a known credential. The key set is
// read-only after construction, so concurrent callers need no lock.
func (a *StaticAPIKeyAuthenticator) Authenticate(
	ctx context.Context,
	bearerToken string,
) (Identity, error) {
	if a == nil || len(a.identities) == 0 {
		return Identity{}, ErrUnauthenticated
	}
	if ctx == nil {
		return Identity{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return Identity{}, err
	}
	if len(bearerToken) < minCredentialLength {
		return Identity{}, ErrUnauthenticated
	}
	granted, ok := a.identities[sha256.Sum256([]byte(bearerToken))]
	if !ok {
		return Identity{}, ErrUnauthenticated
	}
	if err := granted.Validate(); err != nil {
		return Identity{}, ErrUnauthenticated
	}
	return granted.Clone(), nil
}
