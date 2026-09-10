package query

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func key() []byte { return []byte("0123456789abcdef0123456789abcdef") }

func sign(t *testing.T, claims Claims) string {
	t.Helper()
	token, err := SignToken(key(), claims)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestHMACPrincipalResolverRejectsBadTokens(t *testing.T) {
	resolver, err := NewHMACPrincipalResolver(key(), ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	now := time.Now().Unix()
	good := Claims{Version: 1, TenantID: "tenant-a", TenantVersion: 1, SubjectID: "op",
		CanReadAudit: true, IssuedAt: now, ExpiresAt: now + 60, TokenID: "j1"}
	expired := good
	expired.IssuedAt, expired.ExpiresAt = now-120, now-60
	badSignature := sign(t, good)
	badParts := strings.SplitN(badSignature, ".", 3)
	sig := []byte(badParts[2])
	sig[len(sig)/2] = 'A' ^ 'B' ^ sig[len(sig)/2] // flip a middle, fully-decoded character
	badParts[2] = string(sig)
	badSignature = strings.Join(badParts, ".")

	cases := map[string]string{
		"missing":       "",
		"wrong-prefix":  "gw1" + sign(t, good)[3:],
		"bad-signature": badSignature,
		"expired":       sign(t, expired),
	}
	for name, token := range cases {
		req := httptest.NewRequest(http.MethodGet, "/v1/audit/events", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if _, err := resolver.Resolve(req); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestHMACPrincipalResolverBuildsPrincipal(t *testing.T) {
	resolver, err := NewHMACPrincipalResolver(key(), ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	now := time.Now().Unix()
	token := sign(t, Claims{Version: 1, TenantID: "tenant-a", TenantVersion: 3, SubjectID: "op",
		CanReadAudit: true, CanReadAuditCrossTenant: true, IssuedAt: now, ExpiresAt: now + 60, TokenID: "j1"})
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	principal, err := resolver.Resolve(req)
	if err != nil {
		t.Fatal(err)
	}
	if !principal.Authenticated || principal.SubjectID != "op" || principal.TenantID != "tenant-a" ||
		!principal.CanReadAudit || !principal.CanReadAuditCrossTenant {
		t.Fatalf("principal=%+v", principal)
	}
}

func TestHMACPrincipalResolverEnforcesTenantVersion(t *testing.T) {
	resolver, err := NewHMACPrincipalResolver(key(), ResolverOptions{
		VersionCheck: func(tenantID string, version int64) error {
			if tenantID == "tenant-a" && version == 3 {
				return nil
			}
			return ErrForbidden
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	now := time.Now().Unix()
	token := sign(t, Claims{Version: 1, TenantID: "tenant-a", TenantVersion: 99, SubjectID: "op",
		CanReadAudit: true, IssuedAt: now, ExpiresAt: now + 60, TokenID: "j1"})
	req := httptest.NewRequest(http.MethodGet, "/v1/audit/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if _, err := resolver.Resolve(req); err != ErrForbidden {
		t.Fatalf("err=%v, want ErrForbidden", err)
	}
}
