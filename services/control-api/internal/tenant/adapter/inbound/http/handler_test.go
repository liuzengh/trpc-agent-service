package httpadapter_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/inbound/http"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

func TestHandlerAddsMemberForAuthenticatedOwner(t *testing.T) {
	service := &tenantServiceStub{added: domain.Membership{
		ID: "membership-2", TenantID: "tenant-1", UserID: "user-2",
		Role: domain.MembershipRoleMember, CreatedBy: "owner-1",
		CreatedAt: time.Date(2026, time.August, 31, 15, 0, 0, 0, time.UTC),
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "owner-1"})

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/tenants/tenant-1/members",
		bytes.NewBufferString(`{"user_id":"user-2"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.addCommand.ActorUserID != "owner-1" || service.addCommand.UserID != "user-2" {
		t.Fatalf("command = %#v", service.addCommand)
	}
}

func TestHandlerBlocksRestrictedSession(t *testing.T) {
	service := &tenantServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{
		UserID: "owner-1", Restricted: true,
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/me/tenants", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden ||
		recorder.Body.String() != `{"error":{"code":"PASSWORD_CHANGE_REQUIRED","message":"password change is required"}}` {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerSearchesMemberCandidatesWithPagination(t *testing.T) {
	service := &tenantServiceStub{candidates: tenantapp.MemberCandidatePage{
		Candidates: []tenantapp.MemberCandidate{{
			UserID: "user-2", Username: "alice", DisplayName: "Alice",
		}},
		Offset: 5, Limit: 10, Total: 1,
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "owner-1"})

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/tenants/tenant-1/member-candidates?query=Ali&offset=5&limit=10",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.searchQuery.TenantID != "tenant-1" ||
		service.searchQuery.ActorUserID != "owner-1" ||
		service.searchQuery.Query != "Ali" ||
		service.searchQuery.Page != (tenantapp.Page{Offset: 5, Limit: 10}) {
		t.Fatalf("query = %#v", service.searchQuery)
	}
	want := `{"candidates":[{"user_id":"user-2","username":"alice","display_name":"Alice"}],"limit":10,"offset":5,"total":1}`
	if recorder.Body.String() != want {
		t.Fatalf("body = %s, want %s", recorder.Body.String(), want)
	}
}

func TestHandlerReturnsEmptyMemberCandidateArray(t *testing.T) {
	service := &tenantServiceStub{candidates: tenantapp.MemberCandidatePage{
		Offset: 0, Limit: 20, Total: 0,
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "owner-1"})

	request := httptest.NewRequest(
		http.MethodGet, "/v1/tenants/tenant-1/member-candidates?query=nobody", nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	want := `{"candidates":[],"limit":20,"offset":0,"total":0}`
	if recorder.Code != http.StatusOK || recorder.Body.String() != want {
		t.Fatalf("status/body = %d/%s, want %s", recorder.Code, recorder.Body.String(), want)
	}
}

func TestHandlerRejectsInvalidMemberCandidatePagination(t *testing.T) {
	service := &tenantServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "owner-1"})

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/tenants/tenant-1/member-candidates?query=alice&limit=0",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest || service.searchCalls != 0 {
		t.Fatalf("status/calls/body = %d/%d/%s", recorder.Code, service.searchCalls, recorder.Body.String())
	}
}

func TestHandlerMapsMemberCandidateErrors(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr error
		wantStatus int
		wantCode   string
	}{
		{
			name: "non-owner", serviceErr: tenantapp.ErrTenantForbidden,
			wantStatus: http.StatusForbidden, wantCode: "TENANT_FORBIDDEN",
		},
		{
			name: "invalid query", serviceErr: tenantapp.ErrInvalidCandidateQuery,
			wantStatus: http.StatusBadRequest, wantCode: "INVALID_QUERY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &tenantServiceStub{searchErr: tt.serviceErr}
			router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "actor-1"})

			request := httptest.NewRequest(
				http.MethodGet, "/v1/tenants/tenant-1/member-candidates?query=alice", nil,
			)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus ||
				!bytes.Contains(recorder.Body.Bytes(), []byte(`"code":"`+tt.wantCode+`"`)) {
				t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestHandlerMapsOwnerOnlyMemberAccessToForbidden(t *testing.T) {
	service := &tenantServiceStub{listMembersErr: tenantapp.ErrTenantForbidden}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "member-1"})

	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-1/members", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden ||
		recorder.Body.String() != `{"error":{"code":"TENANT_FORBIDDEN","message":"tenant access is forbidden"}}` {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}

func authenticatedRouter(service httpadapter.TenantService, identity identityapp.IdentityContext) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), identity))
		c.Next()
	})
	httpadapter.NewHandler(service).Register(router)
	return router
}

type tenantServiceStub struct {
	added          domain.Membership
	addCommand     tenantapp.AddMemberCommand
	candidates     tenantapp.MemberCandidatePage
	searchQuery    tenantapp.SearchMemberCandidatesQuery
	searchCalls    int
	searchErr      error
	listMembersErr error
}

func (s *tenantServiceStub) ListMyTenants(context.Context, string) ([]domain.TenantMembership, error) {
	return nil, nil
}

func (s *tenantServiceStub) GetTenant(context.Context, string, string) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, nil
}

func (s *tenantServiceStub) ListMembers(context.Context, string, string) ([]domain.Membership, error) {
	return nil, s.listMembersErr
}

func (s *tenantServiceStub) SearchMemberCandidates(
	_ context.Context,
	query tenantapp.SearchMemberCandidatesQuery,
) (tenantapp.MemberCandidatePage, error) {
	s.searchCalls++
	s.searchQuery = query
	return s.candidates, s.searchErr
}

func (s *tenantServiceStub) AddMember(_ context.Context, command tenantapp.AddMemberCommand) (domain.Membership, error) {
	s.addCommand = command
	return s.added, nil
}

func (s *tenantServiceStub) RemoveMember(context.Context, tenantapp.RemoveMemberCommand) error {
	return nil
}
