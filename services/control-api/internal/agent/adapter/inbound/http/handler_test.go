package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

func TestHandlerRegistersCompleteAgentV1Surface(t *testing.T) {
	router := authenticatedRouter(&agentServiceStub{}, identityapp.IdentityContext{UserID: "usr_1"})
	var got []string
	for _, route := range router.Routes() {
		got = append(got, route.Method+" "+route.Path)
	}
	sort.Strings(got)
	want := []string{
		"GET /v1/tenants/:tenant_id/agents",
		"GET /v1/tenants/:tenant_id/agents/:agent_id",
		"GET /v1/tenants/:tenant_id/agents/:agent_id/draft",
		"GET /v1/tenants/:tenant_id/agents/:agent_id/versions",
		"GET /v1/tenants/:tenant_id/agents/:agent_id/versions/:version_number",
		"PATCH /v1/tenants/:tenant_id/agents/:agent_id",
		"POST /v1/tenants/:tenant_id/agents",
		"POST /v1/tenants/:tenant_id/agents/:agent_id/draft/validate",
		"POST /v1/tenants/:tenant_id/agents/:agent_id/versions",
		"PUT /v1/tenants/:tenant_id/agents/:agent_id/draft",
	}
	if len(got) != len(want) {
		t.Fatalf("routes = %#v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("routes = %#v, want %#v", got, want)
		}
	}
}

func TestHandlerCreatesAgentWithAuthenticatedIdentity(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	service := &agentServiceStub{createResult: application.CreateAgentResult{
		Agent: domain.Agent{ID: "agt_1", TenantID: "tnt_1", Name: "Support", CreatedBy: "usr_1", CreatedAt: now, UpdatedAt: now},
		Draft: domain.AgentDraft{TenantID: "tnt_1", AgentID: "agt_1", Revision: 1, Spec: json.RawMessage(`{}`), UpdatedBy: "usr_1", UpdatedAt: now},
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tnt_1/agents", bytes.NewBufferString(`{"name":"Support","description":"Answers users"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.createCommand.TenantID != "tnt_1" || service.createCommand.ActorUserID != "usr_1" || service.createCommand.Name != "Support" {
		t.Fatalf("command = %#v", service.createCommand)
	}
}

func TestHandlerReturnsStructuredDraftValidationFailure(t *testing.T) {
	report := domain.ValidationReport{
		Valid: false, SchemaVersion: "v1", DraftRevision: 1,
		Diagnostics: []domain.Diagnostic{{
			Code: "AGENT_SPEC_SENSITIVE_FIELD", Severity: domain.SeverityError,
			Pointer: "/api_key", Message: "credential fields are not allowed",
		}},
	}
	service := &agentServiceStub{saveReport: report, saveErr: application.ErrAgentSpecInvalid}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	request := httptest.NewRequest(http.MethodPut, "/v1/tenants/tnt_1/agents/agt_1/draft", bytes.NewBufferString(`{"expected_revision":1,"spec":{"api_key":"secret"}}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Validation domain.ValidationReport `json:"validation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "AGENT_SPEC_INVALID" || response.Validation.Diagnostics[0].Code != "AGENT_SPEC_SENSITIVE_FIELD" {
		t.Fatalf("response = %#v", response)
	}
}

func TestHandlerDistinguishesMissingVersion(t *testing.T) {
	service := &agentServiceStub{getVersionErr: application.ErrAgentVersionNotFound}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tnt_1/agents/agt_1/versions/9", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound || recorder.Body.String() != `{"error":{"code":"AGENT_VERSION_NOT_FOUND","message":"agent version was not found"}}` {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerBlocksRestrictedSession(t *testing.T) {
	router := authenticatedRouter(&agentServiceStub{}, identityapp.IdentityContext{UserID: "usr_1", Restricted: true})
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tnt_1/agents", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || recorder.Body.String() != `{"error":{"code":"PASSWORD_CHANGE_REQUIRED","message":"password change is required"}}` {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}

func authenticatedRouter(service httpadapter.AgentService, identity identityapp.IdentityContext) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), identity))
		c.Next()
	})
	httpadapter.NewHandler(service).Register(router)
	return router
}

type agentServiceStub struct {
	createCommand application.CreateAgentCommand
	createResult  application.CreateAgentResult
	saveReport    domain.ValidationReport
	saveErr       error
	getVersionErr error
}

func (s *agentServiceStub) CreateAgent(_ context.Context, command application.CreateAgentCommand) (application.CreateAgentResult, error) {
	s.createCommand = command
	return s.createResult, nil
}
func (s *agentServiceStub) UpdateAgent(context.Context, application.UpdateAgentCommand) (domain.Agent, error) {
	return domain.Agent{}, nil
}
func (s *agentServiceStub) GetAgent(context.Context, string, string, string) (domain.Agent, error) {
	return domain.Agent{}, nil
}
func (s *agentServiceStub) ListAgents(context.Context, string, string, application.Page) (application.AgentPage, error) {
	return application.AgentPage{Agents: []domain.Agent{}}, nil
}
func (s *agentServiceStub) GetDraft(context.Context, string, string, string) (domain.AgentDraft, error) {
	return domain.AgentDraft{}, nil
}
func (s *agentServiceStub) SaveDraft(context.Context, application.SaveDraftCommand) (domain.AgentDraft, domain.ValidationReport, error) {
	return domain.AgentDraft{}, s.saveReport, s.saveErr
}
func (s *agentServiceStub) ValidateDraft(context.Context, application.ValidateDraftCommand) (domain.ValidationReport, error) {
	return domain.ValidationReport{Diagnostics: []domain.Diagnostic{}}, nil
}
func (s *agentServiceStub) PublishAgentVersion(context.Context, application.PublishVersionCommand) (application.PublishVersionResult, error) {
	return application.PublishVersionResult{}, nil
}
func (s *agentServiceStub) GetAgentVersion(context.Context, string, string, string, int64) (domain.AgentVersion, error) {
	return domain.AgentVersion{}, s.getVersionErr
}
func (s *agentServiceStub) ListAgentVersions(context.Context, string, string, string, application.Page) (application.VersionPage, error) {
	return application.VersionPage{Versions: []domain.AgentVersion{}}, nil
}
