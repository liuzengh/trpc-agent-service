package adminauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testBootstrap = "bootstrap-token-that-is-at-least-32-bytes"
const testTenantToken = "tenant-admin-token-that-is-at-least-32-bytes"

func TestAuthenticatorBootstrapAndScopedPrincipal(t *testing.T) {
	authenticator, err := NewAuthenticator(testBootstrap, `[
		{"id":"alice","token":"`+testTenantToken+`","role":"tenant_admin","tenantIds":["tenant-a"]}
	]`)
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		principal, err := PrincipalFromContext(request.Context())
		if err != nil {
			t.Fatal(err)
		}
		if request.URL.Path == "/bootstrap" && principal.Role != RolePlatformAdmin {
			t.Fatalf("bootstrap role = %q", principal.Role)
		}
		if request.URL.Path == "/tenant" && (principal.ID != "alice" || !principal.AllowsTenant("tenant-a") || principal.AllowsTenant("tenant-b")) {
			t.Fatalf("scoped principal = %#v", principal)
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	for path, token := range map[string]string{"/bootstrap": testBootstrap, "/tenant": testTenantToken} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		authenticator.Middleware(next).ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("%s status = %d", path, response.Code)
		}
	}
}

func TestAuthenticatorRejectsMalformedAndDuplicateCredentials(t *testing.T) {
	if _, err := NewAuthenticator("short", ""); err == nil {
		t.Fatal("weak bootstrap token accepted")
	}
	if _, err := NewAuthenticator(testBootstrap, `[{"id":"x","token":"`+testBootstrap+`","role":"auditor","tenantIds":["t"]}]`); err == nil {
		t.Fatal("duplicate token accepted")
	}
	if _, err := NewAuthenticator(testBootstrap, `[{"id":"x","token":"`+testTenantToken+`","role":"unknown"}]`); err == nil {
		t.Fatal("unknown role accepted")
	}
	if _, err := NewAuthenticator(testBootstrap, `[{"id":"x","token":"`+testTenantToken+`","role":"tenant_admin","tenantIds":["*"]}]`); err == nil {
		t.Fatal("wildcard tenant scope accepted")
	}
}

func TestMiddlewareRejectsMultipleAuthorizationHeaders(t *testing.T) {
	authenticator, err := NewAuthenticator(testBootstrap, "")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Add("Authorization", "Bearer "+testBootstrap)
	request.Header.Add("Authorization", "Bearer "+testBootstrap)
	response := httptest.NewRecorder()
	authenticator.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestRequireMethodsAndTenantScope(t *testing.T) {
	principal := Principal{ID: "auditor", Role: RoleAuditor, tenants: map[string]struct{}{"tenant-a": {}}}
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(http.StatusNoContent) })
	handler := RequireMethods(map[string]Permission{
		http.MethodGet: PermissionTenantRead,
		http.MethodPut: PermissionTenantWrite,
	}, next)

	for method, want := range map[string]int{http.MethodGet: http.StatusNoContent, http.MethodPut: http.StatusForbidden} {
		request := httptest.NewRequest(method, "/", nil)
		request = request.WithContext(ContextWithPrincipal(request.Context(), principal))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s status = %d, want %d", method, response.Code, want)
		}
	}

	ctx := ContextWithPrincipal(httptest.NewRequest(http.MethodGet, "/", nil).Context(), principal)
	if _, err := RequireTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("allowed tenant rejected: %v", err)
	}
	if _, err := RequireTenant(ctx, "tenant-b"); err != ErrForbidden {
		t.Fatalf("cross-tenant error = %v", err)
	}
}

func TestExecutionReconcilePermissionIsPlatformAdminOnly(t *testing.T) {
	if !(Principal{ID: "platform", Role: RolePlatformAdmin}).Has(PermissionExecutionReconcile) {
		t.Fatal("platform admin must be allowed to reconcile executions")
	}
	for _, role := range []Role{RoleTenantAdmin, RoleReleaseManager, RoleAuditor} {
		if (Principal{ID: "operator", Role: role}).Has(PermissionExecutionReconcile) {
			t.Fatalf("role %q must not reconcile executions", role)
		}
	}
}

func TestExternalAuthenticatorUsesVerifiedPrincipalResolver(t *testing.T) {
	// The fixture header stands in for a cryptographically verified assertion;
	// production resolvers must validate the provider token/certificate.
	resolver := PrincipalResolverFunc(func(_ context.Context, request *http.Request) (Principal, error) {
		if request.Header.Get("X-External-Assertion") != "verified" {
			return Principal{}, ErrUnauthenticated
		}
		return NewPrincipal("oidc:alice", RoleAuditor, []string{"tenant-a"})
	})
	authenticator, err := NewExternalAuthenticator(resolver)
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		principal, err := PrincipalFromContext(request.Context())
		if err != nil || principal.ID != "oidc:alice" {
			t.Fatalf("principal = %#v, err=%v", principal, err)
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-External-Assertion", "verified")
	response := httptest.NewRecorder()
	authenticator.Middleware(next).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("verified assertion status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-External-Assertion", "forged")
	response = httptest.NewRecorder()
	authenticator.Middleware(next).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unverified assertion status = %d", response.Code)
	}
}

func TestExternalAuthenticatorRejectsInvalidResolvedPrincipal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		principal Principal
	}{
		{name: "empty id", principal: Principal{ID: "", Role: RoleAuditor}},
		{name: "unsafe id", principal: Principal{ID: "alice\nforged", Role: RoleAuditor}},
		{name: "wildcard tenant", principal: Principal{ID: "alice", Role: RoleAuditor, tenants: map[string]struct{}{"*": {}}}},
		{name: "platform tenant allowlist", principal: Principal{ID: "root", Role: RolePlatformAdmin, tenants: map[string]struct{}{"tenant-a": {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authenticator, err := NewExternalAuthenticator(PrincipalResolverFunc(func(context.Context, *http.Request) (Principal, error) {
				return tc.principal, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			response := httptest.NewRecorder()
			authenticator.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("handler must not run")
			})).ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("invalid principal status = %d", response.Code)
			}
		})
	}
}

func TestNewPrincipalAppliesRoleAndTenantInvariants(t *testing.T) {
	principal, err := NewPrincipal("oidc:alice", RoleTenantAdmin, []string{"tenant-a"})
	if err != nil || !principal.AllowsTenant("tenant-a") || principal.AllowsTenant("tenant-b") {
		t.Fatalf("principal = %#v, err=%v", principal, err)
	}
	for _, tc := range []struct {
		name string
		id   string
		role Role
		tids []string
	}{
		{name: "unknown role", id: "alice", role: Role("unknown")},
		{name: "platform scope", id: "root", role: RolePlatformAdmin, tids: []string{"tenant-a"}},
		{name: "wildcard scope", id: "alice", role: RoleAuditor, tids: []string{"*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPrincipal(tc.id, tc.role, tc.tids); err == nil {
				t.Fatal("invalid principal accepted")
			}
		})
	}
}
