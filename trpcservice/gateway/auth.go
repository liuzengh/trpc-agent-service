package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// APIIdentity is the fixed Tenant/App mapping configured by an API
// Authenticator. Request body and header fields must never be used as this
// value.
type APIIdentity struct {
	TenantID  string
	AppID     string
	SubjectID string
}

// AuthenticatedAPI is an opaque, proof-bearing authenticator result. Its
// private fields deliberately prevent request adapters from constructing one
// from tenant/app strings.
type AuthenticatedAPI struct {
	identity APIIdentity
	proof    *apiAuthProof
}

type apiAuthProof struct {
	identity APIIdentity
}

// APIAuthenticator maps a credential-bearing HTTP request to a fixed API
// identity. It never receives routing fields from the JSON body.
type APIAuthenticator interface {
	Authenticate(context.Context, *http.Request) (AuthenticatedAPI, error)
}

// APIAuthenticatorFunc adapts a proof-bearing authenticator function.
type APIAuthenticatorFunc func(context.Context, *http.Request) (AuthenticatedAPI, error)

// Authenticate implements APIAuthenticator.
func (f APIAuthenticatorFunc) Authenticate(ctx context.Context, request *http.Request) (AuthenticatedAPI, error) {
	if f == nil {
		return AuthenticatedAPI{}, ErrUnauthenticated
	}
	return f(ctx, request)
}

// Validate confirms that the result was issued by the Gateway authenticator
// boundary and was not modified after issuance.
func (result AuthenticatedAPI) Validate() error {
	if result.proof == nil {
		return fmt.Errorf("%w: API authenticator proof is missing", ErrUnauthenticated)
	}
	if result.identity != result.proof.identity {
		return fmt.Errorf("%w: API authenticator result was modified", ErrUnauthenticated)
	}
	return result.proof.validate()
}

// Identity returns the fixed authenticated mapping. It cannot mint a
// Principal; the Gateway performs that conversion only after Validate.
func (result AuthenticatedAPI) Identity() (APIIdentity, error) {
	if err := result.Validate(); err != nil {
		return APIIdentity{}, err
	}
	return result.identity, nil
}

func (proof *apiAuthProof) validate() error {
	if proof == nil {
		return fmt.Errorf("%w: API authenticator proof is missing", ErrUnauthenticated)
	}
	if err := validateScopedID(proof.identity.TenantID, "t_", "tenant"); err != nil {
		return err
	}
	if err := validateScopedID(proof.identity.AppID, "app_", "agent app"); err != nil {
		return err
	}
	if err := validateExternalID(proof.identity.SubjectID, "API subject"); err != nil {
		return err
	}
	return nil
}

func newAuthenticatedAPI(identity APIIdentity) (AuthenticatedAPI, error) {
	proof := &apiAuthProof{identity: identity}
	if err := proof.validate(); err != nil {
		return AuthenticatedAPI{}, err
	}
	return AuthenticatedAPI{identity: identity, proof: proof}, nil
}

// StaticAPIAuthenticator is a small offline authenticator for tests and local
// development. Credentials are exact Bearer tokens and map to fixed
// identities at construction time; request JSON cannot change that mapping.
type StaticAPIAuthenticator struct {
	credentials map[string]APIIdentity
}

// NewStaticAPIAuthenticator creates an offline credential-to-identity map.
func NewStaticAPIAuthenticator(credentials map[string]APIIdentity) (*StaticAPIAuthenticator, error) {
	copyCredentials := make(map[string]APIIdentity, len(credentials))
	for credential, identity := range credentials {
		if err := validateCredential(credential); err != nil {
			return nil, err
		}
		if _, err := newAuthenticatedAPI(identity); err != nil {
			return nil, err
		}
		copyCredentials[credential] = identity
	}
	return &StaticAPIAuthenticator{credentials: copyCredentials}, nil
}

// Authenticate maps the Authorization Bearer credential to its fixed result.
func (authenticator *StaticAPIAuthenticator) Authenticate(ctx context.Context, request *http.Request) (AuthenticatedAPI, error) {
	if ctx == nil {
		return AuthenticatedAPI{}, ErrUnauthenticated
	}
	select {
	case <-ctx.Done():
		return AuthenticatedAPI{}, ctx.Err()
	default:
	}
	if authenticator == nil || request == nil {
		return AuthenticatedAPI{}, ErrUnauthenticated
	}
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return AuthenticatedAPI{}, ErrUnauthenticated
	}
	credential := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	identity, ok := authenticator.credentials[credential]
	if !ok {
		return AuthenticatedAPI{}, ErrUnauthenticated
	}
	return newAuthenticatedAPI(identity)
}

func validateCredential(credential string) error {
	if strings.TrimSpace(credential) == "" || hasControl(credential) || len([]rune(credential)) > maxPrincipalIDRunes {
		return fmt.Errorf("%w: API credential is invalid", ErrInvalid)
	}
	return nil
}
