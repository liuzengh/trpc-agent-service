package auth_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const testAPIKey = "test_api_key_with_at_least_32_bytes"

func TestHTTPAPIKeyResolverReturnsTrustedTenantRouting(t *testing.T) {
	digest, err := auth.DigestAPIKey(testAPIKey)
	if err != nil {
		t.Fatalf("digest API key: %v", err)
	}
	credentials := &recordingCredentialStore{
		credential: validCredential(),
	}
	directory := &staticDirectory{
		tenant: validTenant(),
		app:    validAgentApp(),
	}
	resolver := auth.HTTPAPIKeyResolver{
		Credentials: credentials,
		Directory:   directory,
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	identity := validRequestIdentity()
	identity.UserID = "caller-selected-user"
	identity.SessionPrincipalID = "caller-selected-principal"
	trusted, err := resolver.Resolve(context.Background(), req, identity)
	if err != nil {
		t.Fatalf("resolve HTTP API key: %v", err)
	}
	if credentials.digest != digest {
		t.Fatalf("credential digest = %x, want %x", credentials.digest, digest)
	}
	tc, source, err := trusted.ResolveTenant(context.Background())
	if err != nil {
		t.Fatalf("resolve trusted tenant: %v", err)
	}
	if source != gateway.TenantSourceAuthenticatedClaims {
		t.Fatalf("tenant source = %q, want authenticated claims", source)
	}
	if tc.TenantID != "tenant-a" || tc.AppID != "support" || tc.ConfigVersion != "v2" {
		t.Fatalf("trusted tenant routing = %#v", tc)
	}
	if tc.SessionPrincipalID != "service:credential-1" || tc.UserID != "service:credential-1" || tc.TraceID != "trace-1" {
		t.Fatalf("trusted request identity = %#v", tc)
	}
	identityResolver, ok := trusted.(gateway.AdmissionIdentityResolver)
	if !ok {
		t.Fatal("trusted resolver does not expose admission identity")
	}
	admissionIdentity, err := identityResolver.ResolveAdmissionIdentity(context.Background())
	if err != nil {
		t.Fatalf("resolve admission identity: %v", err)
	}
	if admissionIdentity.Source != gateway.TenantSourceAuthenticatedClaims ||
		admissionIdentity.SourceID != validCredential().ID ||
		admissionIdentity.CredentialDigest != gateway.CredentialDigest(digest) {
		t.Fatalf("admission identity = %#v", admissionIdentity)
	}
}

func TestHTTPAPIKeyResolverSeparatesCredentialSessions(t *testing.T) {
	identity := validRequestIdentity()
	var principals []string
	for _, credentialID := range []string{"credential-a", "credential-b"} {
		resolver := validHTTPResolver()
		credentials := resolver.Credentials.(*recordingCredentialStore)
		credentials.credential.ID = credentialID
		req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+testAPIKey)

		trusted, err := resolver.Resolve(context.Background(), req, identity)
		if err != nil {
			t.Fatalf("resolve credential %q: %v", credentialID, err)
		}
		runtimeContext, _, err := trusted.ResolveTenant(context.Background())
		if err != nil {
			t.Fatalf("resolve tenant for credential %q: %v", credentialID, err)
		}
		if runtimeContext.SessionID != identity.SessionID || runtimeContext.UserID != runtimeContext.SessionPrincipalID {
			t.Fatalf("credential %q context = %#v", credentialID, runtimeContext)
		}
		principals = append(principals, runtimeContext.SessionPrincipalID)
	}
	if principals[0] == principals[1] {
		t.Fatalf("credential principals must differ: %#v", principals)
	}
}

func TestCredentialValidateRejectsInvalidMetadata(t *testing.T) {
	tests := []struct {
		name string
		edit func(*auth.Credential)
	}{
		{name: "id", edit: func(c *auth.Credential) { c.ID = "" }},
		{name: "tenant", edit: func(c *auth.Credential) { c.TenantID = "" }},
		{name: "app", edit: func(c *auth.Credential) { c.AppID = "" }},
		{name: "status", edit: func(c *auth.Credential) { c.Status = "DELETED" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			credential := validCredential()
			tt.edit(&credential)
			if err := credential.Validate(); err == nil {
				t.Fatal("validate credential succeeded with invalid metadata")
			}
		})
	}
}

func TestDigestAPIKeyRejectsEmptyKey(t *testing.T) {
	if _, err := auth.DigestAPIKey(""); err == nil {
		t.Fatal("digest API key succeeded with an empty key")
	}
}

func TestHTTPAPIKeyResolverRejectsInvalidAuthorization(t *testing.T) {
	tests := []struct {
		name    string
		headers []string
	}{
		{name: "missing"},
		{name: "wrong scheme", headers: []string{"Basic abc"}},
		{name: "missing key", headers: []string{"Bearer"}},
		{name: "multiple", headers: []string{"Bearer first", "Bearer second"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := validHTTPResolver()
			req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			for _, value := range tt.headers {
				req.Header.Add("Authorization", value)
			}
			_, err = resolver.Resolve(context.Background(), req, validRequestIdentity())
			if !errors.Is(err, auth.ErrUnauthenticated) {
				t.Fatalf("resolve error = %v, want unauthenticated", err)
			}
		})
	}
}

func TestHTTPAPIKeyResolverDoesNotExposeRejectedAPIKey(t *testing.T) {
	resolver := validHTTPResolver()
	resolver.Credentials = &recordingCredentialStore{err: auth.ErrCredentialNotFound}
	req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	_, err = resolver.Resolve(context.Background(), req, validRequestIdentity())
	if !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("resolve error = %v, want unauthenticated", err)
	}
	if strings.Contains(err.Error(), testAPIKey) {
		t.Fatal("authentication error exposes the raw API key")
	}
}

func TestHTTPAPIKeyResolverPreservesCredentialStoreFailure(t *testing.T) {
	resolver := validHTTPResolver()
	unavailable := errors.New("credential store unavailable")
	resolver.Credentials = &recordingCredentialStore{err: unavailable}
	req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	_, err = resolver.Resolve(context.Background(), req, validRequestIdentity())
	if !errors.Is(err, unavailable) || errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("resolve error = %v, want preserved store failure", err)
	}
}

func TestHTTPAPIKeyResolverRejectsInactiveResources(t *testing.T) {
	tests := []struct {
		name string
		edit func(*auth.HTTPAPIKeyResolver)
		want error
	}{
		{
			name: "credential",
			edit: func(resolver *auth.HTTPAPIKeyResolver) {
				store := resolver.Credentials.(*recordingCredentialStore)
				store.credential.Status = auth.CredentialSuspended
			},
			want: auth.ErrCredentialInactive,
		},
		{
			name: "expired credential",
			edit: func(resolver *auth.HTTPAPIKeyResolver) {
				store := resolver.Credentials.(*recordingCredentialStore)
				store.credential.ExpiresAt = time.Now().Add(-time.Second)
			},
			want: auth.ErrCredentialInactive,
		},
		{
			name: "revoked credential",
			edit: func(resolver *auth.HTTPAPIKeyResolver) {
				store := resolver.Credentials.(*recordingCredentialStore)
				store.credential.Status = auth.CredentialRevoked
			},
			want: auth.ErrCredentialInactive,
		},
		{
			name: "tenant",
			edit: func(resolver *auth.HTTPAPIKeyResolver) {
				directory := resolver.Directory.(*staticDirectory)
				directory.tenant.Status = tenant.StatusSuspended
			},
			want: auth.ErrTenantInactive,
		},
		{
			name: "app",
			edit: func(resolver *auth.HTTPAPIKeyResolver) {
				directory := resolver.Directory.(*staticDirectory)
				directory.app.Status = tenant.StatusSuspended
			},
			want: auth.ErrAppInactive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := validHTTPResolver()
			tt.edit(&resolver)
			req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+testAPIKey)

			_, err = resolver.Resolve(context.Background(), req, validRequestIdentity())
			if !errors.Is(err, tt.want) {
				t.Fatalf("resolve error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestHTTPAPIKeyResolverRejectsMismatchedDirectoryScope(t *testing.T) {
	resolver := validHTTPResolver()
	directory := resolver.Directory.(*staticDirectory)
	directory.app.AppID = "other-app"
	req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)

	if _, err := resolver.Resolve(context.Background(), req, validRequestIdentity()); err == nil {
		t.Fatal("resolve succeeded with mismatched app scope")
	}
}

func TestHTTPAPIKeyResolverRejectsIncompleteRequestRouting(t *testing.T) {
	resolver := validHTTPResolver()
	req, err := http.NewRequest(http.MethodPost, "/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	identity := validRequestIdentity()
	identity.SessionID = ""

	if _, err := resolver.Resolve(context.Background(), req, identity); err == nil {
		t.Fatal("resolve succeeded with missing session_id")
	}
}

type recordingCredentialStore struct {
	digest     auth.APIKeyDigest
	credential auth.Credential
	err        error
}

func (s *recordingCredentialStore) ResolveAPIKey(
	_ context.Context,
	digest auth.APIKeyDigest,
) (auth.Credential, error) {
	s.digest = digest
	return s.credential, s.err
}

type staticDirectory struct {
	tenant tenant.Tenant
	app    tenant.AgentApp
}

func (d *staticDirectory) ResolveTenant(_ context.Context, _ string) (tenant.Tenant, error) {
	return d.tenant, nil
}

func (d *staticDirectory) ResolveAgentApp(
	_ context.Context,
	_, _ string,
) (tenant.AgentApp, error) {
	return d.app, nil
}

func validHTTPResolver() auth.HTTPAPIKeyResolver {
	return auth.HTTPAPIKeyResolver{
		Credentials: &recordingCredentialStore{credential: validCredential()},
		Directory: &staticDirectory{
			tenant: validTenant(),
			app:    validAgentApp(),
		},
	}
}

func validCredential() auth.Credential {
	return auth.Credential{
		ID:       "credential-1",
		TenantID: "tenant-a",
		AppID:    "support",
		Status:   auth.CredentialActive,
	}
}

func validTenant() tenant.Tenant {
	return tenant.Tenant{
		ID:     "tenant-a",
		Name:   "Tenant A",
		Status: tenant.StatusActive,
	}
}

func validAgentApp() tenant.AgentApp {
	return tenant.AgentApp{
		TenantID:            "tenant-a",
		AppID:               "support",
		Name:                "Support",
		ActiveConfigVersion: "v2",
		Status:              tenant.StatusActive,
	}
}

func validRequestIdentity() auth.RequestIdentity {
	return auth.RequestIdentity{
		SessionID: "session-1",
		TraceID:   "trace-1",
	}
}
