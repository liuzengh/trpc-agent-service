package platformbackend

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	identity "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type access struct {
	allowed      bool
	err          error
	tenant, user string
}

func (a *access) IsActiveMember(_ context.Context, t, u string) (bool, error) {
	a.tenant = t
	a.user = u
	return a.allowed, a.err
}
func TestAuthenticatedCatalogRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := domain.NewCatalog([]domain.Entry{{ID: "pg", Revision: 1, Label: "SQL", Kind: domain.PostgreSQL, Roles: []domain.Role{domain.Session}, Enabled: true, TenantIDs: []string{"tenant-a"}}})
	for _, tc := range []struct {
		name   string
		actor  *identity.IdentityContext
		member bool
		err    error
		tenant string
		code   int
		body   string
	}{
		{"anonymous", nil, true, nil, "tenant-a", 401, "UNAUTHENTICATED"},
		{"restricted", &identity.IdentityContext{UserID: "user-a", Restricted: true}, true, nil, "tenant-a", 403, "PASSWORD_CHANGE_REQUIRED"},
		{"nonmember", &identity.IdentityContext{UserID: "user-a"}, false, nil, "tenant-a", 403, "TENANT_FORBIDDEN"},
		{"dependency", &identity.IdentityContext{UserID: "user-a"}, true, errors.New("password=secret"), "tenant-a", 503, "BACKEND_DIRECTORY_UNAVAILABLE"},
		{"member", &identity.IdentityContext{UserID: "user-a"}, true, nil, "tenant-a", 200, `"id":"pg"`},
		{"no-grant", &identity.IdentityContext{UserID: "user-a"}, true, nil, "tenant-b", 200, `"items":[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &access{allowed: tc.member, err: tc.err}
			r := gin.New()
			auth := func(g *gin.Context) {
				if tc.actor != nil {
					g.Request = g.Request.WithContext(identity.WithIdentity(g.Request.Context(), *tc.actor))
				}
				g.Next()
			}
			if _, err := NewModule(Dependencies{Routes: r, Authenticate: auth, TenantAccess: a, Catalog: c}); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/tenants/"+tc.tenant+"/runtime-backends", nil)
			r.ServeHTTP(w, req)
			if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.body) {
				t.Fatal(w.Code, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cache")
			}
			for _, bad := range []string{"tenant_ids", "secret", "password", "target"} {
				if strings.Contains(w.Body.String(), bad) {
					t.Fatal("leaked", bad)
				}
			}
			if tc.actor != nil && !tc.actor.Restricted && (a.tenant != tc.tenant || a.user != tc.actor.UserID) {
				t.Fatal("wrong trusted identity")
			}
		})
	}
}
