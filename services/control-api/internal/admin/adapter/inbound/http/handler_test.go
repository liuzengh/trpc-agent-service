package httpadapter_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	adminhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	identitydomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

func TestHandlerCreatesUserForOperator(t *testing.T) {
	service := &adminServiceStub{operator: true, user: identitydomain.UserAccount{
		ID: "user-2", Username: "alice", Status: identitydomain.AccountStatusActive,
	}}
	router := adminRouter(service, identityapp.IdentityContext{UserID: "operator-1"})
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/admin/users",
		bytes.NewBufferString(`{"username":"alice","display_name":"Alice","temporary_password":"temporary-123"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.createUser.Username != "alice" {
		t.Fatalf("command = %#v", service.createUser)
	}
}

func TestHandlerRejectsNonOperator(t *testing.T) {
	service := &adminServiceStub{operator: false}
	router := adminRouter(service, identityapp.IdentityContext{UserID: "user-1"})
	request := httptest.NewRequest(http.MethodGet, "/v1/admin/capabilities", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func adminRouter(service adminhttp.AdminService, identity identityapp.IdentityContext) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := adminhttp.NewHandler(service)
	group := router.Group("", func(c *gin.Context) {
		c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), identity))
		c.Next()
	}, handler.AuthorizationMiddleware())
	handler.Register(group)
	return router
}

type adminServiceStub struct {
	operator   bool
	user       identitydomain.UserAccount
	createUser identityapp.CreateManagedAccountCommand
}

func (s *adminServiceStub) IsOperator(context.Context, string) (bool, error) { return s.operator, nil }
func (s *adminServiceStub) ListOperators(context.Context) ([]application.OperatorView, error) {
	return nil, nil
}
func (s *adminServiceStub) GrantOperator(context.Context, string, string) (domain.OperatorGrant, error) {
	return domain.OperatorGrant{}, nil
}
func (s *adminServiceStub) RevokeOperator(context.Context, string, string) error { return nil }
func (s *adminServiceStub) CreateUser(_ context.Context, command identityapp.CreateManagedAccountCommand) (identitydomain.UserAccount, error) {
	s.createUser = command
	return s.user, nil
}
func (s *adminServiceStub) ListUsers(context.Context, identityapp.Page) (identityapp.AccountPage, error) {
	return identityapp.AccountPage{}, nil
}
func (s *adminServiceStub) ProvisionTenant(context.Context, tenantapp.ProvisionTenantCommand) (tenantapp.ProvisionTenantResult, error) {
	return tenantapp.ProvisionTenantResult{}, nil
}
func (s *adminServiceStub) ListTenants(context.Context, tenantapp.Page) (tenantapp.TenantPage, error) {
	return tenantapp.TenantPage{}, nil
}
