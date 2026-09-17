package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHMACPrincipalResolverAuthenticatesTenantScopedManager(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := []byte("01234567890123456789012345678901")
	claims := Claims{Version: 1, TenantID: "tenant-a", TenantVersion: 4, SubjectID: "operator", CanManage: true,
		IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(), TokenID: "admin-token"}
	token, err := SignToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewHMACPrincipalResolver(key, HMACPrincipalOptions{Clock: func() time.Time { return now }, ClockSkew: time.Second,
		VersionCheck: func(tenantID string, version int64) error {
			if tenantID != claims.TenantID || version != claims.TenantVersion {
				t.Fatal("unexpected tenant version check")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/configs", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	principal, err := resolver.Resolve(request)
	if err != nil || !principal.Authenticated || !principal.CanManage || principal.TenantID != "tenant-a" || principal.SubjectID != "operator" {
		t.Fatalf("principal=%#v err=%v", principal, err)
	}
}

func TestHMACPrincipalResolverRejectsUnprivilegedStaleAndWrongFormatTokens(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := []byte("01234567890123456789012345678901")
	base := Claims{Version: 1, TenantID: "tenant-a", TenantVersion: 4, SubjectID: "operator", CanManage: true,
		IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(5 * time.Minute).Unix(), TokenID: "admin-token"}
	resolver, err := NewHMACPrincipalResolver(key, HMACPrincipalOptions{Clock: func() time.Time { return now }, ClockSkew: time.Second,
		VersionCheck: func(string, int64) error { return errors.New("stale tenant version") }})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	requestFor := func(token string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/configs", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		return req
	}
	token, err := SignToken(key, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(requestFor(token)); !errors.Is(err, ErrForbidden) {
		t.Fatalf("stale version err=%v", err)
	}
	base.CanManage = false
	if _, err := SignToken(key, base); err == nil {
		t.Fatal("unprivileged token was signed")
	}
	if _, err := resolver.Resolve(requestFor("gw1.invalid.signature")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("wrong format err=%v", err)
	}
}
