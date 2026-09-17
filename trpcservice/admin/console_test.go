package admin

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type consolePrincipalResolver struct{}

func (consolePrincipalResolver) Resolve(r *http.Request) (Principal, error) {
	if r.Header.Get("Authorization") != "Bearer valid-token" {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanManage: true}, nil
}

type consoleCatalog struct{ tenantID string }

func (c consoleCatalog) GetCatalog(_ context.Context, tenantID string) (CatalogSnapshot, error) {
	if tenantID != c.tenantID {
		return CatalogSnapshot{}, ErrForbidden
	}
	return CatalogSnapshot{Tenant: CatalogTenant{TenantID: tenantID, DisplayName: "Tenant A", Status: "active", Version: 2}, Apps: []CatalogApp{{ID: "app", DisplayName: "Assistant"}}}, nil
}

func TestConsoleServesLoginAndUsesSameOriginSessionForCatalog(t *testing.T) {
	principals := consolePrincipalResolver{}
	api := Handler{Principals: principals, Catalog: consoleCatalog{tenantID: "tenant-a"}}
	console := Console{API: api, Principals: principals}
	page := httptest.NewRecorder()
	console.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/admin/", nil))
	if page.Code != http.StatusOK || !bytes.Contains(page.Body.Bytes(), []byte("管理员登录")) || !bytes.Contains(page.Body.Bytes(), []byte("Session 数据平面迁移")) || page.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("page=%d headers=%v body=%q", page.Code, page.Header(), page.Body.String())
	}
	login := httptest.NewRequest(http.MethodPost, "/admin/session", bytes.NewBufferString(`{"token":"valid-token"}`))
	logged := httptest.NewRecorder()
	console.ServeHTTP(logged, login)
	if logged.Code != http.StatusNoContent || len(logged.Result().Cookies()) != 1 || !logged.Result().Cookies()[0].HttpOnly || !logged.Result().Cookies()[0].Secure {
		t.Fatalf("login=%d cookies=%#v", logged.Code, logged.Result().Cookies())
	}
	catalog := httptest.NewRequest(http.MethodGet, "/v1/admin/catalog", nil)
	catalog.AddCookie(logged.Result().Cookies()[0])
	response := httptest.NewRecorder()
	console.ServeHTTP(response, catalog)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"tenant_id":"tenant-a"`)) {
		t.Fatalf("catalog=%d body=%s", response.Code, response.Body.String())
	}
}

func TestConsoleOnlyAllowsInsecureCookieWhenExplicitlyConfigured(t *testing.T) {
	principals := consolePrincipalResolver{}
	console := Console{API: Handler{Principals: principals}, Principals: principals, AllowInsecureSessionCookie: true}
	logged := httptest.NewRecorder()
	console.ServeHTTP(logged, httptest.NewRequest(http.MethodPost, "/admin/session", bytes.NewBufferString(`{"token":"valid-token"}`)))
	if logged.Code != http.StatusNoContent || len(logged.Result().Cookies()) != 1 || logged.Result().Cookies()[0].Secure {
		t.Fatalf("login=%d cookies=%#v", logged.Code, logged.Result().Cookies())
	}
}

func TestConsoleRejectsUnauthenticatedAndCrossOriginCookieMutation(t *testing.T) {
	principals := consolePrincipalResolver{}
	api := Handler{Principals: principals, Catalog: consoleCatalog{tenantID: "tenant-a"}}
	console := Console{API: api, Principals: principals}
	unauthenticated := httptest.NewRecorder()
	console.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/v1/admin/catalog", nil))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d", unauthenticated.Code)
	}
	mutation := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/configs/validate", nil)
	mutation.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "valid-token"})
	mutation.Header.Set("Origin", "https://attacker.invalid")
	denied := httptest.NewRecorder()
	console.ServeHTTP(denied, mutation)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("cross-origin=%d", denied.Code)
	}
}
