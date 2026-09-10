// Package auth authenticates ingress credentials and resolves trusted tenant routing.
package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrUnauthenticated means the request did not contain a recognized API key.
	ErrUnauthenticated = errors.New("api key authentication failed")
	// ErrCredentialNotFound means a credential store did not find the requested
	// API key digest. CredentialStore implementations must use it only for an
	// unknown credential, never for an unavailable store.
	ErrCredentialNotFound = errors.New("api credential not found")
	// ErrCredentialInactive means the API credential is not active.
	ErrCredentialInactive = errors.New("api credential is inactive")
	// ErrTenantInactive means the authenticated tenant is not active.
	ErrTenantInactive = errors.New("tenant is inactive")
	// ErrAppInactive means the authenticated agent application is not active.
	ErrAppInactive = errors.New("agent app is inactive")
)

// CredentialStatus is the lifecycle state of an API credential.
type CredentialStatus string

const (
	// CredentialActive allows the credential to authenticate requests.
	CredentialActive CredentialStatus = "ACTIVE"
	// CredentialSuspended rejects the credential without deleting its metadata.
	CredentialSuspended CredentialStatus = "SUSPENDED"
	// CredentialRevoked permanently rejects the credential.
	CredentialRevoked CredentialStatus = "REVOKED"
)

// Credential binds one API key to a tenant application.
// It contains metadata only and must never contain the raw API key.
type Credential struct {
	ID        string
	TenantID  string
	AppID     string
	KeyPrefix string
	Status    CredentialStatus
	// ExpiresAt is the exclusive credential expiry time. The zero value means
	// that the credential does not expire automatically.
	ExpiresAt time.Time
}

// Validate checks persisted API credential metadata. The zero value is invalid.
func (c Credential) Validate() error {
	if c.ID == "" {
		return errors.New("credential_id is required")
	}
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.AppID == "" {
		return errors.New("app_id is required")
	}
	if c.Status != CredentialActive &&
		c.Status != CredentialSuspended &&
		c.Status != CredentialRevoked {
		return errors.New("credential status is invalid")
	}
	return nil
}

// APIKeyDigest is the SHA-256 digest used to look up a high-entropy API key.
// Credential stores must persist only the digest, never the raw key.
type APIKeyDigest [sha256.Size]byte

// DigestAPIKey returns the lookup digest for a non-empty API key.
func DigestAPIKey(apiKey string) (APIKeyDigest, error) {
	if apiKey == "" {
		return APIKeyDigest{}, errors.New("api key is required")
	}
	return sha256.Sum256([]byte(apiKey)), nil
}

// CredentialStore resolves API credential metadata by an API key digest. It
// returns ErrCredentialNotFound for an unknown digest and preserves operational
// failures so ingress can distinguish them from invalid credentials.
type CredentialStore interface {
	ResolveAPIKey(ctx context.Context, digest APIKeyDigest) (Credential, error)
}

// Directory resolves tenant and application control-plane metadata.
type Directory interface {
	ResolveTenant(ctx context.Context, tenantID string) (tenant.Tenant, error)
	ResolveAgentApp(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error)
}

// RequestIdentity contains request routing fields supplied by an ingress entry.
// HTTPAPIKeyResolver accepts only SessionID and TraceID from this value. It
// derives the service user and session principal from the authenticated
// credential rather than trusting caller-provided identity fields.
type RequestIdentity struct {
	SessionID          string
	SessionPrincipalID string
	UserID             string
	TraceID            string
}

// HTTPAPIKeyResolver authenticates HTTP Bearer API keys and resolves trusted
// tenant routing. Each authenticated credential has one fixed service
// principal for user and session routing.
type HTTPAPIKeyResolver struct {
	Credentials CredentialStore
	Directory   Directory
}

// Resolve authenticates req and returns a resolver suitable for gateway.Request.
// TenantID, AppID, and ConfigVersion come only from authenticated control-plane records.
func (r HTTPAPIKeyResolver) Resolve(
	ctx context.Context,
	req *http.Request,
	identity RequestIdentity,
) (gateway.TenantResolver, error) {
	if req == nil {
		return nil, errors.New("http request is required")
	}
	if r.Credentials == nil {
		return nil, errors.New("credential store is required")
	}
	if r.Directory == nil {
		return nil, errors.New("tenant directory is required")
	}

	apiKey, err := bearerAPIKey(req.Header.Values("Authorization"))
	if err != nil {
		return nil, err
	}
	digest, err := DigestAPIKey(apiKey)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	credential, err := r.Credentials.ResolveAPIKey(ctx, digest)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, ErrCredentialNotFound) {
			return nil, ErrUnauthenticated
		}
		return nil, fmt.Errorf("resolve api credential: %w", err)
	}
	if err := credential.Validate(); err != nil {
		return nil, fmt.Errorf("api credential: %w", err)
	}
	if credential.Status != CredentialActive {
		return nil, ErrCredentialInactive
	}
	if !credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt) {
		return nil, ErrCredentialInactive
	}

	tnt, err := r.Directory.ResolveTenant(ctx, credential.TenantID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant: %w", err)
	}
	if err := tnt.Validate(); err != nil {
		return nil, fmt.Errorf("tenant: %w", err)
	}
	if tnt.ID != credential.TenantID {
		return nil, errors.New("resolved tenant does not match credential scope")
	}
	if tnt.Status != tenant.StatusActive {
		return nil, ErrTenantInactive
	}

	app, err := r.Directory.ResolveAgentApp(ctx, credential.TenantID, credential.AppID)
	if err != nil {
		return nil, fmt.Errorf("resolve agent app: %w", err)
	}
	if err := app.Validate(); err != nil {
		return nil, fmt.Errorf("agent app: %w", err)
	}
	if app.TenantID != credential.TenantID || app.AppID != credential.AppID {
		return nil, errors.New("resolved agent app does not match credential scope")
	}
	if app.Status != tenant.StatusActive {
		return nil, ErrAppInactive
	}

	servicePrincipal := servicePrincipalID(credential.ID)
	runtimeContext := tenant.RuntimeContext{
		TenantID:           credential.TenantID,
		AppID:              credential.AppID,
		ConfigVersion:      app.ActiveConfigVersion,
		SessionID:          identity.SessionID,
		SessionPrincipalID: servicePrincipal,
		UserID:             servicePrincipal,
		TraceID:            identity.TraceID,
	}
	if err := runtimeContext.Validate(); err != nil {
		return nil, fmt.Errorf("request identity: %w", err)
	}
	return &resolvedTenant{
		runtimeContext:   runtimeContext,
		credentialID:     credential.ID,
		credentialDigest: gateway.CredentialDigest(digest),
	}, nil
}

func servicePrincipalID(credentialID string) string {
	return "service:" + credentialID
}

type resolvedTenant struct {
	runtimeContext   tenant.RuntimeContext
	credentialID     string
	credentialDigest gateway.CredentialDigest
}

func (r *resolvedTenant) ResolveTenant(ctx context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	if err := ctx.Err(); err != nil {
		return tenant.RuntimeContext{}, "", err
	}
	if r == nil {
		return tenant.RuntimeContext{}, "", errors.New("tenant resolver is nil")
	}
	return r.runtimeContext, gateway.TenantSourceAuthenticatedClaims, nil
}

// ResolveAdmissionIdentity returns the authenticated credential reference and
// trusted tenant routing needed by the atomic admission transaction.
func (r *resolvedTenant) ResolveAdmissionIdentity(
	ctx context.Context,
) (gateway.AdmissionIdentity, error) {
	if err := ctx.Err(); err != nil {
		return gateway.AdmissionIdentity{}, err
	}
	if r == nil {
		return gateway.AdmissionIdentity{}, errors.New("tenant resolver is nil")
	}
	identity := gateway.AdmissionIdentity{
		Tenant:           r.runtimeContext,
		Source:           gateway.TenantSourceAuthenticatedClaims,
		SourceID:         r.credentialID,
		CredentialDigest: r.credentialDigest,
	}
	if err := identity.Validate(); err != nil {
		return gateway.AdmissionIdentity{}, fmt.Errorf("admission identity: %w", err)
	}
	return identity, nil
}

func bearerAPIKey(values []string) (string, error) {
	if len(values) != 1 {
		return "", ErrUnauthenticated
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", ErrUnauthenticated
	}
	return parts[1], nil
}
