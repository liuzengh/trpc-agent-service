package web

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
)

// Tenant isolation is enforced from the authenticated claims, so handler tests
// must run as a real caller. The helpers below build the three platform
// identities and inject them the way AuthMiddleware.Wrap does.
//
// A context value cannot cross the httptest.Server network boundary, so the
// client sends the identity as a header and asClaims (wrapping the test
// server's handler) turns it back into a context value. Handlers therefore see
// exactly what the real middleware would install.

// testClaimsHeader carries the test identity from client to test server.
const testClaimsHeader = "X-Test-Claims"

func ownerClaims() *auth.Claims {
	return &auth.Claims{UserID: "root", Role: member.RoleOwner}
}

// adminClaims returns the platform admin of a tenant.
func adminClaims(tenantID string) *auth.Claims {
	return &auth.Claims{TenantID: tenantID, UserID: "admin-" + tenantID, Role: member.RoleAdmin}
}

// memberClaims returns an ordinary employee of a tenant.
func memberClaims(tenantID, userID string) *auth.Claims {
	return &auth.Claims{TenantID: tenantID, UserID: userID, Role: member.RoleMember}
}

// asClaims restores the identity carried by the test header. It is a test-only
// stand-in for AuthMiddleware.Wrap and must wrap every test server handler.
func asClaims(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get(testClaimsHeader)
		if parts := strings.Split(raw, "|"); len(parts) == 3 && parts[2] != "" {
			ctx := context.WithValue(r.Context(), AuthUserKey, &auth.Claims{
				TenantID: parts[0], UserID: parts[1], Role: parts[2],
			})
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// claimTransport stamps the caller identity onto every outgoing request.
type claimTransport struct {
	claims *auth.Claims
	base   http.RoundTripper
}

func (t *claimTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	c := t.claims
	req.Header.Set(testClaimsHeader, c.TenantID+"|"+c.UserID+"|"+c.Role)
	return base.RoundTrip(req)
}

// clientAs returns an HTTP client that authenticates as the given claims.
func clientAs(claims *auth.Claims) *http.Client {
	return &http.Client{Transport: &claimTransport{claims: claims}}
}

// getAs performs an authenticated GET.
func getAs(t *testing.T, c *http.Client, url string) *http.Response {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// postAs performs an authenticated POST.
func postAs(t *testing.T, c *http.Client, url, body string) *http.Response {
	t.Helper()
	resp, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

// doAs performs an authenticated request with an explicit method and body.
func doAs(t *testing.T, c *http.Client, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}
