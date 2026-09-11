package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestHandlerRegistersCompleteRuntimeProfileV1Surface(t *testing.T) {
	router := authenticatedRouter(&runtimeProfileServiceStub{}, identityapp.IdentityContext{UserID: "usr_1"})
	var got []string
	for _, route := range router.Routes() {
		got = append(got, route.Method+" "+route.Path)
	}
	sort.Strings(got)
	want := []string{
		"GET /v1/tenants/:tenant_id/runtime-profiles",
		"GET /v1/tenants/:tenant_id/runtime-profiles/:profile_id",
		"GET /v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft",
		"GET /v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions",
		"GET /v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions/:revision_number",
		"PATCH /v1/tenants/:tenant_id/runtime-profiles/:profile_id",
		"POST /v1/tenants/:tenant_id/runtime-profiles",
		"POST /v1/tenants/:tenant_id/runtime-profiles/:profile_id/credentials/update",
		"POST /v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft/validate",
		"POST /v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions",
		"PUT /v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft",
	}
	if len(got) != len(want) {
		t.Fatalf("routes = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("routes = %#v, want %#v", got, want)
		}
	}
}

func TestHandlerCreatesRuntimeProfileWithAuthenticatedIdentity(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	service := &runtimeProfileServiceStub{createResult: application.CreateRuntimeProfileResult{
		Profile: domain.RuntimeProfile{
			ID: "rpf_1", TenantID: "tnt_1", Name: "Production",
			Description: "Runtime defaults", CreatedBy: "usr_1", CreatedAt: now, UpdatedAt: now,
		},
		Draft: domain.ProfileDraft{
			TenantID: "tnt_1", ProfileID: "rpf_1", Revision: 1,
			Spec: json.RawMessage(`{}`), UpdatedBy: "usr_1", UpdatedAt: now,
		},
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	recorder := performJSON(router, http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles",
		`{"name":"Production","description":"Runtime defaults"}`)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.createCommand.TenantID != "tnt_1" ||
		service.createCommand.ActorUserID != "usr_1" ||
		service.createCommand.Name != "Production" {
		t.Fatalf("command = %#v", service.createCommand)
	}
	if !strings.Contains(recorder.Body.String(), `"profile":{"id":"rpf_1"`) ||
		!strings.Contains(recorder.Body.String(), `"draft_revision":1`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestHandlerRejectsMalformedAndLegacyDraftInput(t *testing.T) {
	service := &runtimeProfileServiceStub{}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	cases := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"expected_revision":1,"spec":{},"extra":true}`},
		{name: "missing spec", body: `{"expected_revision":1}`},
		{name: "trailing document", body: `{"expected_revision":1,"spec":{}} {}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			recorder := performJSON(router, http.MethodPut,
				"/v1/tenants/tnt_1/runtime-profiles/rpf_1/draft", test.body)
			if recorder.Code != http.StatusBadRequest ||
				recorder.Body.String() != `{"error":{"code":"INVALID_REQUEST","message":"credential draft request is invalid"}}` {
				t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if service.saveCalls != 0 {
		t.Fatalf("SaveCredentialDraft calls = %d", service.saveCalls)
	}
}

func TestHandlerReturnsStructuredRuntimeProfileValidationFailure(t *testing.T) {
	report := domain.ValidationReport{
		Valid: false, SchemaVersion: "v1", DraftRevision: 1,
		Diagnostics: []domain.Diagnostic{{
			Code: "RUNTIME_PROFILE_SPEC_SENSITIVE_FIELD", Severity: domain.SeverityError,
			Pointer: "/api_key", Message: "credential fields are not allowed",
		}},
	}
	service := &runtimeProfileServiceStub{
		publishErr: application.ErrRuntimeProfileSpecInvalid, publishResults: []application.PublishProfileRevisionResult{{Report: report}},
	}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	recorder := performJSON(router, http.MethodPost,
		"/v1/tenants/tnt_1/runtime-profiles/rpf_1/revisions",
		`{"expected_revision":1}`)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	errorBody := body["error"].(map[string]any)
	validation := body["validation"].(map[string]any)
	diagnostic := validation["diagnostics"].([]any)[0].(map[string]any)
	if errorBody["code"] != "RUNTIME_PROFILE_SPEC_INVALID" ||
		diagnostic["resource_kind"] != nil || diagnostic["resource_key"] != nil {
		t.Fatalf("body = %#v", body)
	}
}

func TestHandlerPublishUsesCreatedThenIdempotentOKWithoutValidationWrapper(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	canonical, report := domain.ValidateForPublication(json.RawMessage(`{"schema_version":"v1","credential_protocol_version":"v1","models":{"primary":{"kind":"openai_compatible","model":"test","base_url":"https://model.example.test/v1","api_key_credential_id":"crd_0123456789abcdef0123456789abcdef","capabilities":["chat"]}},"tools":{},"knowledge":{},"storage":{}}`), 1)
	if !report.Valid {
		t.Fatal(report)
	}
	revision := domain.ProfileRevision{ID: "rpr_1", TenantID: "tnt_1", ProfileID: "rpf_1", RevisionNumber: 1, SourceDraftRevision: 2, SchemaVersion: "v1", Spec: canonical.Document, SpecDigest: canonical.Digest, PublishedBy: "usr_1", PublishedAt: now}

	service := &runtimeProfileServiceStub{publishResults: []application.PublishProfileRevisionResult{
		{Revision: revision, Created: true},
		{Revision: revision, Created: false},
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	for index, wantStatus := range []int{http.StatusCreated, http.StatusOK} {
		recorder := performJSON(router, http.MethodPost,
			"/v1/tenants/tnt_1/runtime-profiles/rpf_1/revisions", `{"expected_revision":2}`)
		if recorder.Code != wantStatus {
			t.Fatalf("call %d status/body = %d/%s", index+1, recorder.Code, recorder.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 1 || body["revision"] == nil || body["validation"] != nil {
			t.Fatalf("call %d body = %s", index+1, recorder.Body.String())
		}
		var revisionBody map[string]json.RawMessage
		if err := json.Unmarshal(body["revision"], &revisionBody); err != nil {
			t.Fatal(err)
		}
		if revisionBody["spec"] != nil || revisionBody["config"] == nil || revisionBody["credential_states"] != nil || strings.Contains(recorder.Body.String(), "crd_") {
			t.Fatalf("call %d published Revision did not preserve static redacted contract: %s", index+1, recorder.Body.String())
		}
	}
}

func TestHandlerRevisionCommandsStayAgentAndEnvironmentIndependent(t *testing.T) {
	canonical, report := domain.ValidateForPublication(json.RawMessage(`{"schema_version":"v1","credential_protocol_version":"v1","models":{},"tools":{},"knowledge":{},"storage":{}}`), 2)
	if !report.Valid {
		t.Fatal(report)
	}
	endpoints := []struct {
		name string
		path string
	}{
		{name: "validate", path: "/draft/validate"},
		{name: "publish", path: "/revisions"},
	}
	requests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "revision only", body: `{"expected_revision":2}`, wantStatus: http.StatusOK},
		{name: "environment", body: `{"expected_revision":2,"environment_id":"default"}`, wantStatus: http.StatusBadRequest},
		{name: "agent version", body: `{"expected_revision":2,"agent_version_id":"agv_1"}`, wantStatus: http.StatusBadRequest},
		{name: "resource bindings", body: `{"expected_revision":2,"bindings":{}}`, wantStatus: http.StatusBadRequest},
	}
	for _, endpoint := range endpoints {
		t.Run(endpoint.name, func(t *testing.T) {
			for _, request := range requests {
				t.Run(request.name, func(t *testing.T) {
					service := &runtimeProfileServiceStub{publishResults: []application.PublishProfileRevisionResult{{
						Revision: domain.ProfileRevision{Spec: canonical.Document, SpecDigest: canonical.Digest, SchemaVersion: "v1"},
					}}}
					router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
					recorder := performJSON(router, http.MethodPost,
						"/v1/tenants/tnt_1/runtime-profiles/rpf_1"+endpoint.path, request.body)
					if recorder.Code != request.wantStatus {
						t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
					}
					if request.wantStatus == http.StatusBadRequest {
						if service.validateCalls != 0 || service.publishCalls != 0 {
							t.Fatalf("invalid input reached service: validate=%d publish=%d", service.validateCalls, service.publishCalls)
						}
						if recorder.Body.String() != `{"error":{"code":"INVALID_REQUEST","message":"expected_revision is required"}}` {
							t.Fatalf("body = %s", recorder.Body.String())
						}
						return
					}
					if endpoint.name == "validate" {
						want := application.ValidateProfileDraftCommand{
							TenantID: "tnt_1", ProfileID: "rpf_1", ActorUserID: "usr_1", ExpectedRevision: 2,
						}
						if service.validateCalls != 1 || service.publishCalls != 0 || service.validateCommand != want {
							t.Fatalf("validate=%d publish=%d command=%#v", service.validateCalls, service.publishCalls, service.validateCommand)
						}
						return
					}
					want := application.PublishProfileRevisionCommand{
						TenantID: "tnt_1", ProfileID: "rpf_1", ActorUserID: "usr_1", ExpectedRevision: 2,
					}
					if service.publishCalls != 1 || service.validateCalls != 0 || service.publishCommand != want {
						t.Fatalf("publish=%d validate=%d command=%#v", service.publishCalls, service.validateCalls, service.publishCommand)
					}
				})
			}
		})
	}
}

func TestHandlerListsRevisionSummariesWithoutSpec(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 30, 0, 0, time.UTC)
	summary := domain.ProfileRevisionSummary{
		ID: "rpr_7", TenantID: "tnt_1", ProfileID: "rpf_1",
		RevisionNumber: 7, SourceDraftRevision: 12, SchemaVersion: "v1",
		SpecDigest:  "sha256:" + strings.Repeat("a", 64),
		PublishedBy: "usr_1", PublishedAt: now,
	}
	service := &runtimeProfileServiceStub{listRevisions: application.ProfileRevisionSummaryPage{
		Revisions: []domain.ProfileRevisionSummary{summary}, Total: 1,
	}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet,
		"/v1/tenants/tnt_1/runtime-profiles/rpf_1/revisions?offset=0&limit=20", nil)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
	var page struct {
		Revisions []json.RawMessage `json:"revisions"`
		Total     int               `json:"total"`
		Offset    int               `json:"offset"`
		Limit     int               `json:"limit"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || page.Offset != 0 || page.Limit != 20 || len(page.Revisions) != 1 {
		t.Fatalf("page = %#v, body = %s", page, recorder.Body.String())
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(page.Revisions[0], &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["spec"]; exists {
		t.Fatalf("Revision summary exposed spec: %s", page.Revisions[0])
	}
	if len(fields) != 9 {
		t.Fatalf("Revision summary fields = %#v", fields)
	}
	var got struct {
		ID                  string    `json:"id"`
		TenantID            string    `json:"tenant_id"`
		ProfileID           string    `json:"profile_id"`
		RevisionNumber      int64     `json:"revision_number"`
		SourceDraftRevision int64     `json:"source_draft_revision"`
		SchemaVersion       string    `json:"schema_version"`
		SpecDigest          string    `json:"spec_digest"`
		PublishedBy         string    `json:"published_by"`
		PublishedAt         time.Time `json:"published_at"`
	}
	if err := json.Unmarshal(page.Revisions[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != summary.ID || got.TenantID != summary.TenantID ||
		got.ProfileID != summary.ProfileID || got.RevisionNumber != summary.RevisionNumber ||
		got.SourceDraftRevision != summary.SourceDraftRevision ||
		got.SchemaVersion != summary.SchemaVersion || got.SpecDigest != summary.SpecDigest ||
		got.PublishedBy != summary.PublishedBy || !got.PublishedAt.Equal(summary.PublishedAt) {
		t.Fatalf("Revision summary = %#v, want %#v", got, summary)
	}
}

func TestHandlerGetsRedactedRevisionWithCredentialStates(t *testing.T) {
	service := &runtimeProfileServiceStub{credentialRead: application.ProfileRead{ID: "rpr_7", TenantID: "tnt_1", ProfileID: "rpf_1", RevisionNumber: 7, SchemaVersion: "v1", CredentialProtocolVersion: "v1", Config: emptyProfileConfig(), CredentialStates: application.CredentialStates{"models": {"primary": {"api_key": {Configured: true, Status: "active", CredentialRevision: 3}}}}}}
	router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
	for _, path := range []string{"/draft", "/revisions/7"} {
		recorder := performJSON(router, http.MethodGet, "/v1/tenants/tnt_1/runtime-profiles/rpf_1"+path, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["config"] == nil || body["credential_states"] == nil || body["spec"] != nil || strings.Contains(recorder.Body.String(), "credential_id") {
			t.Fatalf("unexpected redacted read: %s", recorder.Body.String())
		}
	}
}

func TestHandlerMapsRevisionNotFoundAndTenantForbidden(t *testing.T) {
	t.Run("revision not found", func(t *testing.T) {
		service := &runtimeProfileServiceStub{getRevisionErr: application.ErrProfileRevisionNotFound}
		router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet,
			"/v1/tenants/tnt_1/runtime-profiles/rpf_1/revisions/9", nil)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound ||
			recorder.Body.String() != `{"error":{"code":"RUNTIME_PROFILE_REVISION_NOT_FOUND","message":"runtime profile revision was not found"}}` {
			t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("tenant forbidden", func(t *testing.T) {
		service := &runtimeProfileServiceStub{listProfilesErr: application.ErrTenantForbidden}
		router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tnt_2/runtime-profiles", nil)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden ||
			recorder.Body.String() != `{"error":{"code":"TENANT_FORBIDDEN","message":"tenant access is forbidden"}}` {
			t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestHandlerRequiresUsableIdentity(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		router := gin.New()
		httpadapter.NewHandler(&runtimeProfileServiceStub{}).Register(router)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
			"/v1/tenants/tnt_1/runtime-profiles", nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("restricted", func(t *testing.T) {
		router := authenticatedRouter(&runtimeProfileServiceStub{}, identityapp.IdentityContext{
			UserID: "usr_1", Restricted: true,
		})
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
			"/v1/tenants/tnt_1/runtime-profiles", nil))
		if recorder.Code != http.StatusForbidden ||
			recorder.Body.String() != `{"error":{"code":"PASSWORD_CHANGE_REQUIRED","message":"password change is required"}}` {
			t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
		}
	})
}

func performJSON(router http.Handler, method, target, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func authenticatedRouter(service httpadapter.RuntimeProfileService, identity identityapp.IdentityContext) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(identityapp.WithIdentity(c.Request.Context(), identity))
		c.Next()
	})
	httpadapter.NewHandler(service).Register(router)
	return router
}

type runtimeProfileServiceStub struct {
	createCommand   application.CreateRuntimeProfileCommand
	createResult    application.CreateRuntimeProfileResult
	saveCalls       int
	saveCommand     application.SaveCredentialDraftCommand
	updateCommand   application.UpdateUsedCredentialCommand
	updateCalls     int
	credentialRead  application.ProfileRead
	credentialErr   error
	publishErr      error
	validateCommand application.ValidateProfileDraftCommand
	validateCalls   int
	publishCommand  application.PublishProfileRevisionCommand
	publishResults  []application.PublishProfileRevisionResult
	publishCalls    int
	getRevisionErr  error
	listRevisions   application.ProfileRevisionSummaryPage
	listProfilesErr error
}

func (s *runtimeProfileServiceStub) CreateRuntimeProfile(
	_ context.Context, command application.CreateRuntimeProfileCommand,
) (application.CreateRuntimeProfileResult, error) {
	s.createCommand = command
	return s.createResult, nil
}

func (s *runtimeProfileServiceStub) UpdateRuntimeProfile(
	context.Context, application.UpdateRuntimeProfileCommand,
) (domain.RuntimeProfile, error) {
	return domain.RuntimeProfile{}, nil
}

func (s *runtimeProfileServiceStub) GetRuntimeProfile(
	context.Context, string, string, string,
) (domain.RuntimeProfile, error) {
	return domain.RuntimeProfile{}, nil
}

func (s *runtimeProfileServiceStub) ListRuntimeProfiles(
	context.Context, string, string, application.Page,
) (application.RuntimeProfilePage, error) {
	return application.RuntimeProfilePage{Profiles: []domain.RuntimeProfile{}}, s.listProfilesErr
}

func (s *runtimeProfileServiceStub) ValidateProfileDraft(
	_ context.Context, command application.ValidateProfileDraftCommand,
) (domain.ValidationReport, error) {
	s.validateCalls++
	s.validateCommand = command
	return domain.ValidationReport{Diagnostics: []domain.Diagnostic{}}, nil
}

func (s *runtimeProfileServiceStub) PublishProfileRevision(
	_ context.Context, command application.PublishProfileRevisionCommand,
) (application.PublishProfileRevisionResult, error) {
	s.publishCommand = command
	index := s.publishCalls
	s.publishCalls++
	if index < len(s.publishResults) {
		return s.publishResults[index], s.publishErr
	}
	return application.PublishProfileRevisionResult{}, nil
}

func (s *runtimeProfileServiceStub) ListProfileRevisions(
	context.Context, string, string, string, application.Page,
) (application.ProfileRevisionSummaryPage, error) {
	return s.listRevisions, nil
}

func (s *runtimeProfileServiceStub) GetCredentialDraft(context.Context, string, string, string) (application.ProfileRead, error) {
	return s.credentialRead, s.credentialErr
}
func (s *runtimeProfileServiceStub) GetCredentialRevision(context.Context, string, string, string, int64) (application.ProfileRead, error) {
	return s.credentialRead, s.getRevisionErr
}
func (s *runtimeProfileServiceStub) SaveCredentialDraft(_ context.Context, c application.SaveCredentialDraftCommand) (application.DraftWriteResult, error) {
	s.saveCalls++
	s.saveCommand = c
	return application.DraftWriteResult{ProfileID: c.ProfileID, DraftRevision: c.Write.ExpectedDraftRevision + 1}, s.credentialErr
}
func (s *runtimeProfileServiceStub) UpdateUsedProfileCredential(_ context.Context, c application.UpdateUsedCredentialCommand) (application.CredentialUpdateResult, error) {
	s.updateCalls++
	s.updateCommand = c
	return application.CredentialUpdateResult{CredentialRevision: 2, Status: "active"}, s.credentialErr
}
func emptyProfileConfig() application.ProfileConfig {
	return application.ProfileConfig{Models: map[string]application.ModelConfig{}, Tools: map[string]application.ToolConfig{}, Knowledge: map[string]application.KnowledgeConfig{}, Storage: map[string]application.StorageConfig{}}
}

func TestHandlerRevisionCommandsRejectNonContractJSON(t *testing.T) {
	canonical, report := domain.ValidateForPublication(json.RawMessage(`{"schema_version":"v1","credential_protocol_version":"v1","models":{},"tools":{},"knowledge":{},"storage":{}}`), 2)
	if !report.Valid {
		t.Fatal(report)
	}
	for _, path := range []string{"/draft/validate", "/revisions"} {
		for _, request := range []struct{ name, body, contentType string }{
			{"MIME suffix", `{"expected_revision":2}`, "application/json-invalid"},
			{"duplicate field", `{"expected_revision":1,"expected_revision":2}`, "application/json"},
			{"field case", `{"Expected_Revision":2}`, "application/json"},
		} {
			t.Run(path+"/"+request.name, func(t *testing.T) {
				service := &runtimeProfileServiceStub{publishResults: []application.PublishProfileRevisionResult{{Revision: domain.ProfileRevision{Spec: canonical.Document, SpecDigest: canonical.Digest, SchemaVersion: "v1"}}}}
				router := authenticatedRouter(service, identityapp.IdentityContext{UserID: "usr_1"})
				req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tnt_1/runtime-profiles/rpf_1"+path, strings.NewReader(request.body))
				req.Header.Set("Content-Type", request.contentType)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != http.StatusBadRequest || service.validateCalls != 0 || service.publishCalls != 0 {
					t.Fatalf("non-contract request reached service: status=%d validate=%d publish=%d", w.Code, service.validateCalls, service.publishCalls)
				}
			})
		}
	}
}
