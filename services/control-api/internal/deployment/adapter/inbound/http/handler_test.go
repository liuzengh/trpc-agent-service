package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

const (
	testTenantID     = "tnt_fixture_tenant"
	testDeploymentID = "dpl_fixture_deployment"
	testUserID       = "usr_fixture_user"
)

func TestHandlerRegistersDeploymentAndArtifactRoutes(t *testing.T) {
	router := newTestRouter(&fakeDeploymentService{})
	var got []string
	for _, route := range router.Routes() {
		if strings.Contains(route.Path, "/deployments") {
			got = append(got, route.Method+" "+route.Path)
		}
	}
	sort.Strings(got)
	want := []string{
		"GET /v1/tenants/:tenant_id/deployments",
		"GET /v1/tenants/:tenant_id/deployments/:deployment_id",
		"GET /v1/tenants/:tenant_id/deployments/:deployment_id/revisions",
		"GET /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number",
		"GET /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/artifacts/:filename",
		"PATCH /v1/tenants/:tenant_id/deployments/:deployment_id",
		"POST /v1/tenants/:tenant_id/deployments",
		"POST /v1/tenants/:tenant_id/deployments/:deployment_id/backend-migrations",
		"POST /v1/tenants/:tenant_id/deployments/:deployment_id/revisions",
		"POST /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/knowledge/:resource/import",
		"POST /v1/tenants/:tenant_id/deployments/:deployment_id/validate",
		"PUT /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/artifacts/:filename",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("routes =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestHandlerEightRouteSuccessResponses(t *testing.T) {
	deployment := testDeployment()
	published := testPublishedRevision(t)
	report := testValidationReport(true)
	summary := testRevisionSummary()
	service := &fakeDeploymentService{
		create: func(_ context.Context, command application.CreateDeploymentCommand) (application.CreateDeploymentResult, error) {
			if command.TenantID != testTenantID || command.ActorUserID != testUserID ||
				command.IdempotencyKey != "create-key" || command.Name != "Production" {
				t.Fatalf("create command = %#v", command)
			}
			return application.CreateDeploymentResult{Deployment: deployment, Created: true}, nil
		},
		list: func(_ context.Context, tenantID, userID string, page application.Page) (application.DeploymentPage, error) {
			if tenantID != testTenantID || userID != testUserID || page.Offset != 2 || page.Limit != 5 {
				t.Fatalf("list args = %q %q %#v", tenantID, userID, page)
			}
			return application.DeploymentPage{Deployments: []domain.Deployment{deployment}, Total: 1}, nil
		},
		get: func(_ context.Context, tenantID, deploymentID, userID string) (domain.Deployment, error) {
			if tenantID != testTenantID || deploymentID != testDeploymentID || userID != testUserID {
				t.Fatalf("get args = %q %q %q", tenantID, deploymentID, userID)
			}
			return deployment, nil
		},
		update: func(_ context.Context, command application.UpdateDeploymentCommand) (domain.Deployment, error) {
			if command.ExpectedMetadataRevision != 1 || command.Name == nil || *command.Name != "Production 2" ||
				command.Description != nil || command.ActorUserID != testUserID {
				t.Fatalf("update command = %#v", command)
			}
			value := deployment
			value.Name = "Production 2"
			value.MetadataRevision = 2
			return value, nil
		},
		validate: func(_ context.Context, command application.ValidateDeploymentCommand) (domain.ValidationReport, error) {
			assertInput(t, command.Input)
			if command.DeploymentID != testDeploymentID || command.ActorUserID != testUserID {
				t.Fatalf("validate command = %#v", command)
			}
			return report, nil
		},
		publish: func(_ context.Context, command application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
			assertInput(t, command.Input)
			if command.IdempotencyKey != "publish-key" || command.ExpectedLatestRevisionNumber != nil ||
				command.ActorUserID != testUserID {
				t.Fatalf("publish command = %#v", command)
			}
			return application.PublishDeploymentResult{Published: published, Validation: report, Created: true}, nil
		},
		migrate: func(_ context.Context, command application.MigrateAndPublishCommand) (application.MigrateAndPublishResult, error) {
			assertInput(t, command.Input)
			if command.IdempotencyKey != "migration-key" || command.SourceRevisionNumber != 1 || command.ActorUserID != testUserID {
				t.Fatalf("migration command = %#v", command)
			}
			return application.MigrateAndPublishResult{MemoryScopesCopied: 2, Publication: application.PublishDeploymentResult{Published: published, Validation: report, Created: true}}, nil
		},
		listRevisions: func(_ context.Context, tenantID, deploymentID, userID string, page application.Page) (application.RevisionSummaryPage, error) {
			if tenantID != testTenantID || deploymentID != testDeploymentID || userID != testUserID ||
				page.Offset != 0 || page.Limit != 20 {
				t.Fatalf("list revisions args = %q %q %q %#v", tenantID, deploymentID, userID, page)
			}
			return application.RevisionSummaryPage{Revisions: []domain.DeploymentRevisionSummary{summary}, Total: 1}, nil
		},
		getRevision: func(_ context.Context, tenantID, deploymentID, userID string, number int64) (domain.PublishedRevision, error) {
			if tenantID != testTenantID || deploymentID != testDeploymentID || userID != testUserID || number != 1 {
				t.Fatalf("get revision args = %q %q %q %d", tenantID, deploymentID, userID, number)
			}
			return published, nil
		},
	}
	router := newTestRouter(service)

	for _, test := range []struct {
		name, method, path, body string
		headers                  map[string]string
		status                   int
	}{
		{"create", http.MethodPost, deploymentsPath(), `{"name":"Production","description":"primary"}`, map[string]string{"Idempotency-Key": "create-key"}, http.StatusCreated},
		{"list", http.MethodGet, deploymentsPath() + "?offset=2&limit=5", "", nil, http.StatusOK},
		{"get", http.MethodGet, deploymentPath(), "", nil, http.StatusOK},
		{"patch", http.MethodPatch, deploymentPath(), `{"expected_metadata_revision":1,"name":"Production 2"}`, nil, http.StatusOK},
		{"validate", http.MethodPost, deploymentPath() + "/validate", validInputJSON(), nil, http.StatusOK},
		{"publish", http.MethodPost, deploymentPath() + "/revisions", validPublishJSON("null"), map[string]string{"Idempotency-Key": "publish-key"}, http.StatusCreated},
		{"migrate", http.MethodPost, deploymentPath() + "/backend-migrations", `{"source_revision_number":1,"expected_latest_revision_number":null,"input":{"schema_version":"v1","agent":{"agent_id":"agt_fixture_agent","version_number":3},"profile":{"profile_id":"rpf_fixture_profile","revision_number":2}}}`, map[string]string{"Idempotency-Key": "migration-key"}, http.StatusCreated},
		{"list revisions", http.MethodGet, deploymentPath() + "/revisions", "", nil, http.StatusOK},
		{"get revision", http.MethodGet, deploymentPath() + "/revisions/1", "", nil, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := performRequest(router, test.method, test.path, test.body, activeIdentity(), test.headers)
			if response.Code != test.status {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateAndPublishReplayReturn200(t *testing.T) {
	service := &fakeDeploymentService{
		create: func(context.Context, application.CreateDeploymentCommand) (application.CreateDeploymentResult, error) {
			return application.CreateDeploymentResult{Deployment: testDeployment(), Created: false}, nil
		},
		publish: func(context.Context, application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
			return application.PublishDeploymentResult{
				Published: testPublishedRevision(t), Validation: testValidationReport(true), Created: false,
			}, nil
		},
	}
	router := newTestRouter(service)
	create := performRequest(router, http.MethodPost, deploymentsPath(), `{"name":"Production"}`,
		activeIdentity(), map[string]string{"Idempotency-Key": "create-replay"})
	if create.Code != http.StatusOK {
		t.Fatalf("create replay = %d / %s", create.Code, create.Body.String())
	}
	publish := performRequest(router, http.MethodPost, deploymentPath()+"/revisions", validPublishJSON("null"),
		activeIdentity(), map[string]string{"Idempotency-Key": "publish-replay"})
	if publish.Code != http.StatusOK {
		t.Fatalf("publish replay = %d / %s", publish.Code, publish.Body.String())
	}
}

func TestHandlersRequireUsableIdentityOnEveryRoute(t *testing.T) {
	service := &fakeDeploymentService{}
	router := newTestRouter(service)
	requests := []struct {
		method, path, body string
		headers            map[string]string
	}{
		{http.MethodPost, deploymentsPath(), `{"name":"Production"}`, map[string]string{"Idempotency-Key": "create-key"}},
		{http.MethodGet, deploymentsPath(), "", nil},
		{http.MethodGet, deploymentPath(), "", nil},
		{http.MethodPatch, deploymentPath(), `{"expected_metadata_revision":1,"name":"Production"}`, nil},
		{http.MethodPost, deploymentPath() + "/validate", validInputJSON(), nil},
		{http.MethodPost, deploymentPath() + "/revisions", validPublishJSON("null"), map[string]string{"Idempotency-Key": "publish-key"}},
		{http.MethodGet, deploymentPath() + "/revisions", "", nil},
		{http.MethodGet, deploymentPath() + "/revisions/1", "", nil},
	}
	for index, request := range requests {
		response := performRequest(router, request.method, request.path, request.body, nil, request.headers)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("request %d unauthenticated status = %d / %s", index, response.Code, response.Body.String())
		}
	}
	restricted := activeIdentity()
	restricted.Restricted = true
	response := performRequest(router, http.MethodGet, deploymentPath(), "", restricted, nil)
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "PASSWORD_CHANGE_REQUIRED") {
		t.Fatalf("restricted response = %d / %s", response.Code, response.Body.String())
	}
}

func TestApplicationErrorsMapToStableHTTPStatuses(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"invalid metadata", application.ErrInvalidDeployment, 400, "INVALID_DEPLOYMENT_REQUEST"},
		{"invalid input", application.ErrInvalidDeploymentInput, 400, "INVALID_DEPLOYMENT_REQUEST"},
		{"tenant forbidden", application.ErrTenantForbidden, 403, "TENANT_FORBIDDEN"},
		{"deployment not found", application.ErrDeploymentNotFound, 404, "DEPLOYMENT_NOT_FOUND"},
		{"metadata conflict", application.ErrMetadataRevisionConflict, 409, "DEPLOYMENT_METADATA_REVISION_CONFLICT"},
		{"latest conflict", application.ErrLatestRevisionConflict, 409, "DEPLOYMENT_LATEST_REVISION_CONFLICT"},
		{"idempotency conflict", application.ErrIdempotencyConflict, 409, "IDEMPOTENCY_CONFLICT"},
		{"dependency", application.ErrCredentialDependencyUnavailable, 503, "DEPENDENCY_UNAVAILABLE"},
		{"integrity", application.ErrPublicationIntegrity, 500, "INTERNAL_ERROR"},
		{"unknown", errors.New("unexpected credential_id=cred_internal_canary"), 500, "INTERNAL_ERROR"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeDeploymentService{get: func(context.Context, string, string, string) (domain.Deployment, error) {
				return domain.Deployment{}, test.err
			}}
			response := performRequest(newTestRouter(service), http.MethodGet, deploymentPath(), "", activeIdentity(), nil)
			if response.Code != test.status || !strings.Contains(response.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("response = %d / %s", response.Code, response.Body.String())
			}
			if test.status == http.StatusInternalServerError &&
				(response.Header().Get("X-Debug-Deployment-Error") != "" ||
					strings.Contains(response.Body.String(), "cred_internal_canary")) {
				t.Fatalf("internal error leaked implementation detail: headers=%v body=%s", response.Header(), response.Body.String())
			}
		})
	}
}

func TestSourceAndRevisionNotFoundErrorsReturn404(t *testing.T) {
	for _, test := range []struct {
		name, code string
		err        error
	}{
		{"agent", "AGENT_VERSION_NOT_FOUND", application.ErrAgentVersionNotFound},
		{"profile", "RUNTIME_PROFILE_REVISION_NOT_FOUND", application.ErrProfileRevisionNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeDeploymentService{publish: func(context.Context, application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
				return application.PublishDeploymentResult{}, test.err
			}}
			response := performRequest(newTestRouter(service), http.MethodPost, deploymentPath()+"/revisions",
				validPublishJSON("null"), activeIdentity(), map[string]string{"Idempotency-Key": "publish-key"})
			if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), test.code) {
				t.Fatalf("response = %d / %s", response.Code, response.Body.String())
			}
		})
	}
	service := &fakeDeploymentService{getRevision: func(context.Context, string, string, string, int64) (domain.PublishedRevision, error) {
		return domain.PublishedRevision{}, application.ErrDeploymentRevisionNotFound
	}}
	response := performRequest(newTestRouter(service), http.MethodGet, deploymentPath()+"/revisions/1", "", activeIdentity(), nil)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), "DEPLOYMENT_REVISION_NOT_FOUND") {
		t.Fatalf("revision response = %d / %s", response.Code, response.Body.String())
	}
}

func TestPublishValidationFailureReturnsTyped422(t *testing.T) {
	report := testValidationReport(false)
	service := &fakeDeploymentService{publish: func(context.Context, application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
		return application.PublishDeploymentResult{Validation: report}, application.ErrDeploymentRevisionInvalid
	}}
	response := performRequest(newTestRouter(service), http.MethodPost, deploymentPath()+"/revisions",
		validPublishJSON("null"), activeIdentity(), map[string]string{"Idempotency-Key": "publish-invalid"})
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d / %s", response.Code, response.Body.String())
	}
	var body struct {
		Error      errorBody                          `json:"error"`
		Validation deploymentValidationReportResponse `json:"validation"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "DEPLOYMENT_REVISION_INVALID" || body.Validation.Valid ||
		len(body.Validation.Diagnostics) != 1 || body.Validation.Diagnostics[0].Code != domain.DiagnosticResourceMissing {
		t.Fatalf("422 body = %#v", body)
	}
}

func TestOnlyCredentialDependencyErrorReturns503(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"sentinel", application.ErrCredentialDependencyUnavailable, 503},
		{"wrapped sentinel", errors.Join(errors.New("owner read"), application.ErrCredentialDependencyUnavailable), 503},
		{"same text is not sentinel", errors.New("profile credential dependency unavailable"), 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeDeploymentService{validate: func(context.Context, application.ValidateDeploymentCommand) (domain.ValidationReport, error) {
				return domain.ValidationReport{}, test.err
			}}
			response := performRequest(newTestRouter(service), http.MethodPost, deploymentPath()+"/validate",
				validInputJSON(), activeIdentity(), nil)
			if response.Code != test.status {
				t.Fatalf("status = %d / %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestRequestBodiesAreLimitedTo32KiB(t *testing.T) {
	large := `{"name":"` + strings.Repeat("x", int(maxDeploymentRequestBytes)) + `"}`
	for _, request := range []struct {
		method, path string
		headers      map[string]string
	}{
		{http.MethodPost, deploymentsPath(), map[string]string{"Idempotency-Key": "create-key"}},
		{http.MethodPatch, deploymentPath(), nil},
		{http.MethodPost, deploymentPath() + "/validate", nil},
		{http.MethodPost, deploymentPath() + "/revisions", map[string]string{"Idempotency-Key": "publish-key"}},
	} {
		response := performRequest(newTestRouter(&fakeDeploymentService{}), request.method, request.path,
			large, activeIdentity(), request.headers)
		if response.Code != http.StatusRequestEntityTooLarge || !strings.Contains(response.Body.String(), "PAYLOAD_TOO_LARGE") {
			t.Fatalf("%s %s = %d / %s", request.method, request.path, response.Code, response.Body.String())
		}
	}
}

func TestClosedDeploymentInputRejectsEnvironmentBindingsUnknownDuplicateAndTrailingData(t *testing.T) {
	valid := validInputJSON()
	cases := map[string]string{
		"environment":      strings.TrimSuffix(valid, "}") + `,"environment_id":"env"}`,
		"binding map":      strings.TrimSuffix(valid, "}") + `,"tool_bindings":{"search":"web"}}`,
		"unknown root":     strings.TrimSuffix(valid, "}") + `,"unknown":true}`,
		"unknown agent":    strings.Replace(valid, `"version_number":3`, `"version_number":3,"latest":true`, 1),
		"case variant":     strings.Replace(valid, `"agent_id"`, `"Agent_ID"`, 1),
		"duplicate root":   strings.Replace(valid, `"schema_version":"v1"`, `"schema_version":"v1","schema_version":"v1"`, 1),
		"duplicate nested": strings.Replace(valid, `"agent_id":"agt_fixture_agent"`, `"agent_id":"agt_fixture_agent","agent_id":"agt_other"`, 1),
		"trailing value":   valid + `{}`,
		"null id":          strings.Replace(valid, `"agent_id":"agt_fixture_agent"`, `"agent_id":null`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			called := false
			service := &fakeDeploymentService{validate: func(context.Context, application.ValidateDeploymentCommand) (domain.ValidationReport, error) {
				called = true
				return domain.ValidationReport{}, nil
			}}
			response := performRequest(newTestRouter(service), http.MethodPost, deploymentPath()+"/validate",
				body, activeIdentity(), nil)
			if response.Code != http.StatusBadRequest || called {
				t.Fatalf("response = %d / %s, called=%t", response.Code, response.Body.String(), called)
			}
		})
	}
}

func TestCreatePatchAndPublishRejectUnknownDuplicateNullOrMissingFields(t *testing.T) {
	for _, test := range []struct {
		name, method, path, body string
		headers                  map[string]string
	}{
		{"create unknown", http.MethodPost, deploymentsPath(), `{"name":"x","environment":"prod"}`, map[string]string{"Idempotency-Key": "key"}},
		{"create duplicate", http.MethodPost, deploymentsPath(), `{"name":"x","name":"y"}`, map[string]string{"Idempotency-Key": "key"}},
		{"create null", http.MethodPost, deploymentsPath(), `{"name":null}`, map[string]string{"Idempotency-Key": "key"}},
		{"create name over max before normalization", http.MethodPost, deploymentsPath(), `{"name":"` + strings.Repeat(" ", 128) + `x"}`, map[string]string{"Idempotency-Key": "key"}},
		{"create description over max before normalization", http.MethodPost, deploymentsPath(), `{"name":"x","description":"` + strings.Repeat(" ", 4097) + `"}`, map[string]string{"Idempotency-Key": "key"}},
		{"patch expected only", http.MethodPatch, deploymentPath(), `{"expected_metadata_revision":1}`, nil},
		{"patch null", http.MethodPatch, deploymentPath(), `{"expected_metadata_revision":1,"description":null}`, nil},
		{"publish expected missing", http.MethodPost, deploymentPath() + "/revisions", `{"input":` + validInputJSON() + `}`, map[string]string{"Idempotency-Key": "key"}},
		{"publish input missing", http.MethodPost, deploymentPath() + "/revisions", `{"expected_latest_revision_number":null}`, map[string]string{"Idempotency-Key": "key"}},
		{"publish unknown", http.MethodPost, deploymentPath() + "/revisions", strings.TrimSuffix(validPublishJSON("null"), "}") + `,"activate":true}`, map[string]string{"Idempotency-Key": "key"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := performRequest(newTestRouter(&fakeDeploymentService{}), test.method, test.path,
				test.body, activeIdentity(), test.headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d / %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestIdempotencyHeaderAndParametersAreValidated(t *testing.T) {
	for _, key := range []string{"", "has space", strings.Repeat("x", 129)} {
		response := performRequest(newTestRouter(&fakeDeploymentService{}), http.MethodPost, deploymentsPath(),
			`{"name":"Production"}`, activeIdentity(), map[string]string{"Idempotency-Key": key})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("idempotency %q = %d / %s", key, response.Code, response.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodPost, deploymentsPath(), strings.NewReader(`{"name":"Production"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("Idempotency-Key", "first")
	request.Header.Add("Idempotency-Key", "second")
	request = request.WithContext(identityapp.WithIdentity(request.Context(), *activeIdentity()))
	duplicateHeader := httptest.NewRecorder()
	newTestRouter(&fakeDeploymentService{}).ServeHTTP(duplicateHeader, request)
	if duplicateHeader.Code != http.StatusBadRequest {
		t.Fatalf("duplicate Idempotency-Key = %d / %s", duplicateHeader.Code, duplicateHeader.Body.String())
	}
	for _, path := range []string{
		deploymentPath() + "/revisions/0",
		deploymentPath() + "/revisions/not-a-number",
		deploymentsPath() + "/" + strings.Repeat("x", 129),
		deploymentsPath() + "?offset=-1",
		deploymentsPath() + "?limit=0",
		deploymentsPath() + "?limit=101",
		deploymentsPath() + "?limit=1&limit=2",
		deploymentsPath() + "?cursor=x",
	} {
		response := performRequest(newTestRouter(&fakeDeploymentService{}), http.MethodGet, path, "", activeIdentity(), nil)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("path %q = %d / %s", path, response.Code, response.Body.String())
		}
	}
}

func TestPublishAcceptsPositiveExpectedLatestAndRejectsInvalidValues(t *testing.T) {
	service := &fakeDeploymentService{publish: func(_ context.Context, command application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
		if command.ExpectedLatestRevisionNumber == nil || *command.ExpectedLatestRevisionNumber != 7 {
			t.Fatalf("expected latest = %#v", command.ExpectedLatestRevisionNumber)
		}
		return application.PublishDeploymentResult{
			Published: testPublishedRevision(t), Validation: testValidationReport(true), Created: false,
		}, nil
	}}
	router := newTestRouter(service)
	response := performRequest(router, http.MethodPost, deploymentPath()+"/revisions",
		validPublishJSON("7"), activeIdentity(), map[string]string{"Idempotency-Key": "publish-next"})
	if response.Code != http.StatusOK {
		t.Fatalf("positive expected latest = %d / %s", response.Code, response.Body.String())
	}
	for _, expected := range []string{"0", "-1", `"7"`, "1.0"} {
		response := performRequest(newTestRouter(&fakeDeploymentService{}), http.MethodPost,
			deploymentPath()+"/revisions", validPublishJSON(expected), activeIdentity(),
			map[string]string{"Idempotency-Key": "publish-invalid-expected"})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("expected latest %s = %d / %s", expected, response.Code, response.Body.String())
		}
	}
}

func TestContentTypeAndTopLevelObjectAreStrict(t *testing.T) {
	for _, test := range []struct {
		body, contentType string
		status            int
	}{
		{validInputJSON(), "application/json; charset=utf-8", 200},
		{validInputJSON(), "text/json", 400},
		{`[]`, "application/json", 400},
		{`null`, "application/json", 400},
		{``, "application/json", 400},
	} {
		service := &fakeDeploymentService{validate: func(context.Context, application.ValidateDeploymentCommand) (domain.ValidationReport, error) {
			return testValidationReport(true), nil
		}}
		response := performRequestWithContentType(newTestRouter(service), http.MethodPost,
			deploymentPath()+"/validate", test.body, activeIdentity(), nil, test.contentType)
		if response.Code != test.status {
			t.Fatalf("%q/%q = %d / %s", test.body, test.contentType, response.Code, response.Body.String())
		}
	}
}

func TestFullRevisionResponseNeverSerializesInternalManifestOrCredentialID(t *testing.T) {
	published := testPublishedRevision(t)
	published.Manifest.Content = json.RawMessage(`{"credential_id":"crd_internal","value":"internal-secret-canary"}`)
	service := &fakeDeploymentService{getRevision: func(context.Context, string, string, string, int64) (domain.PublishedRevision, error) {
		return published, nil
	}}
	response := performRequest(newTestRouter(service), http.MethodGet, deploymentPath()+"/revisions/1", "", activeIdentity(), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d / %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{
		"internal-secret-canary", "credential_id", "agent_version_id", "agent_spec_digest",
		"profile_revision_id", "profile_spec_digest", `"manifest":`, `"content":`,
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("response leaked %q: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, `"credential_present":true`) || !strings.Contains(body, `"manifest_view":`) {
		t.Fatalf("public manifest projection missing: %s", body)
	}
}

func TestUnsafeManifestViewFailsClosedWithoutEchoingSecret(t *testing.T) {
	published := testPublishedRevision(t)
	published.ManifestView = json.RawMessage(`{"credential_id":"crd_internal","value":"secret-response-canary"}`)
	service := &fakeDeploymentService{getRevision: func(context.Context, string, string, string, int64) (domain.PublishedRevision, error) {
		return published, nil
	}}
	response := performRequest(newTestRouter(service), http.MethodGet, deploymentPath()+"/revisions/1", "", activeIdentity(), nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d / %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret-response-canary") || strings.Contains(response.Body.String(), "crd_internal") {
		t.Fatalf("unsafe view was echoed: %s", response.Body.String())
	}
}

func TestSummaryAndEmptyPagesStayMetadataOnlyAndUseArrays(t *testing.T) {
	service := &fakeDeploymentService{
		list: func(context.Context, string, string, application.Page) (application.DeploymentPage, error) {
			return application.DeploymentPage{}, nil
		},
		listRevisions: func(context.Context, string, string, string, application.Page) (application.RevisionSummaryPage, error) {
			return application.RevisionSummaryPage{Revisions: []domain.DeploymentRevisionSummary{testRevisionSummary()}, Total: 1}, nil
		},
	}
	router := newTestRouter(service)
	deployments := performRequest(router, http.MethodGet, deploymentsPath(), "", activeIdentity(), nil)
	if deployments.Code != http.StatusOK || !strings.Contains(deployments.Body.String(), `"deployments":[]`) {
		t.Fatalf("deployment page = %d / %s", deployments.Code, deployments.Body.String())
	}
	revisions := performRequest(router, http.MethodGet, deploymentPath()+"/revisions", "", activeIdentity(), nil)
	if revisions.Code != http.StatusOK {
		t.Fatalf("revision page = %d / %s", revisions.Code, revisions.Body.String())
	}
	for _, forbidden := range []string{`"input":`, `"manifest_view":`, `"manifest":`, `"content":`} {
		if strings.Contains(revisions.Body.String(), forbidden) {
			t.Fatalf("summary page contains %s: %s", forbidden, revisions.Body.String())
		}
	}
}

type fakeDeploymentService struct {
	create        func(context.Context, application.CreateDeploymentCommand) (application.CreateDeploymentResult, error)
	update        func(context.Context, application.UpdateDeploymentCommand) (domain.Deployment, error)
	validate      func(context.Context, application.ValidateDeploymentCommand) (domain.ValidationReport, error)
	publish       func(context.Context, application.PublishDeploymentCommand) (application.PublishDeploymentResult, error)
	migrate       func(context.Context, application.MigrateAndPublishCommand) (application.MigrateAndPublishResult, error)
	get           func(context.Context, string, string, string) (domain.Deployment, error)
	list          func(context.Context, string, string, application.Page) (application.DeploymentPage, error)
	getRevision   func(context.Context, string, string, string, int64) (domain.PublishedRevision, error)
	listRevisions func(context.Context, string, string, string, application.Page) (application.RevisionSummaryPage, error)
}

func (f *fakeDeploymentService) MigrateAndPublish(ctx context.Context, command application.MigrateAndPublishCommand) (application.MigrateAndPublishResult, error) {
	if f.migrate != nil {
		return f.migrate(ctx, command)
	}
	return application.MigrateAndPublishResult{}, errors.New("unexpected MigrateAndPublish call")
}

func (f *fakeDeploymentService) CreateDeployment(ctx context.Context, command application.CreateDeploymentCommand) (application.CreateDeploymentResult, error) {
	if f.create != nil {
		return f.create(ctx, command)
	}
	return application.CreateDeploymentResult{}, errors.New("unexpected CreateDeployment call")
}

func (f *fakeDeploymentService) UpdateDeploymentMetadata(ctx context.Context, command application.UpdateDeploymentCommand) (domain.Deployment, error) {
	if f.update != nil {
		return f.update(ctx, command)
	}
	return domain.Deployment{}, errors.New("unexpected UpdateDeploymentMetadata call")
}

func (f *fakeDeploymentService) ValidateDeploymentRevision(ctx context.Context, command application.ValidateDeploymentCommand) (domain.ValidationReport, error) {
	if f.validate != nil {
		return f.validate(ctx, command)
	}
	return domain.ValidationReport{}, errors.New("unexpected ValidateDeploymentRevision call")
}

func (f *fakeDeploymentService) PublishDeploymentRevision(ctx context.Context, command application.PublishDeploymentCommand) (application.PublishDeploymentResult, error) {
	if f.publish != nil {
		return f.publish(ctx, command)
	}
	return application.PublishDeploymentResult{}, errors.New("unexpected PublishDeploymentRevision call")
}

func (f *fakeDeploymentService) GetDeployment(ctx context.Context, tenantID, deploymentID, userID string) (domain.Deployment, error) {
	if f.get != nil {
		return f.get(ctx, tenantID, deploymentID, userID)
	}
	return domain.Deployment{}, errors.New("unexpected GetDeployment call")
}

func (f *fakeDeploymentService) ListDeployments(ctx context.Context, tenantID, userID string, page application.Page) (application.DeploymentPage, error) {
	if f.list != nil {
		return f.list(ctx, tenantID, userID, page)
	}
	return application.DeploymentPage{}, errors.New("unexpected ListDeployments call")
}

func (f *fakeDeploymentService) GetDeploymentRevision(ctx context.Context, tenantID, deploymentID, userID string, number int64) (domain.PublishedRevision, error) {
	if f.getRevision != nil {
		return f.getRevision(ctx, tenantID, deploymentID, userID, number)
	}
	return domain.PublishedRevision{}, errors.New("unexpected GetDeploymentRevision call")
}

func (f *fakeDeploymentService) ListDeploymentRevisions(ctx context.Context, tenantID, deploymentID, userID string, page application.Page) (application.RevisionSummaryPage, error) {
	if f.listRevisions != nil {
		return f.listRevisions(ctx, tenantID, deploymentID, userID, page)
	}
	return application.RevisionSummaryPage{}, errors.New("unexpected ListDeploymentRevisions call")
}

func newTestRouter(service DeploymentService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	NewHandler(service).Register(router)
	return router
}

func performRequest(
	router http.Handler,
	method, path, body string,
	identity *identityapp.IdentityContext,
	headers map[string]string,
) *httptest.ResponseRecorder {
	return performRequestWithContentType(router, method, path, body, identity, headers, "application/json")
}

func performRequestWithContentType(
	router http.Handler,
	method, path, body string,
	identity *identityapp.IdentityContext,
	headers map[string]string,
	contentType string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" || method == http.MethodPost || method == http.MethodPatch {
		request.Header.Set("Content-Type", contentType)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	if identity != nil {
		request = request.WithContext(identityapp.WithIdentity(request.Context(), *identity))
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func activeIdentity() *identityapp.IdentityContext {
	return &identityapp.IdentityContext{UserID: testUserID, SessionID: "ses_fixture", Username: "fixture"}
}

func deploymentsPath() string {
	return "/v1/tenants/" + testTenantID + "/deployments"
}

func deploymentPath() string {
	return deploymentsPath() + "/" + testDeploymentID
}

func validInputJSON() string {
	return `{"schema_version":"v1","agent":{"agent_id":"agt_fixture_agent","version_number":3},"profile":{"profile_id":"rpf_fixture_profile","revision_number":2}}`
}

func validPublishJSON(expected string) string {
	return `{"expected_latest_revision_number":` + expected + `,"input":` + validInputJSON() + `}`
}

func assertInput(t *testing.T, input domain.DeploymentInput) {
	t.Helper()
	if input.SchemaVersion != domain.SchemaVersionV1 || input.Agent.AgentID != "agt_fixture_agent" ||
		input.Agent.VersionNumber != 3 || input.Profile.ProfileID != "rpf_fixture_profile" ||
		input.Profile.RevisionNumber != 2 {
		t.Fatalf("input = %#v", input)
	}
}

func testDeployment() domain.Deployment {
	now := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	return domain.Deployment{
		ID: testDeploymentID, TenantID: testTenantID, Name: "Production",
		Description: "primary", MetadataRevision: 1, CreatedBy: testUserID,
		CreatedAt: now, UpdatedAt: now,
	}
}

func testValidationReport(valid bool) domain.ValidationReport {
	diagnostics := []domain.Diagnostic{}
	if !valid {
		category, name := "tools", "search"
		diagnostics = append(diagnostics, domain.Diagnostic{
			Code: domain.DiagnosticResourceMissing, Severity: domain.SeverityError,
			Source: domain.DiagnosticSourceAgent, Path: "/requirements/tools/search",
			Category: &category, Name: &name, Message: "same-name resource is missing",
		})
	}
	return domain.ValidationReport{
		Valid: valid, CompilerVersion: domain.CompilerVersionV1,
		PlatformContractDigest: "sha256:" + strings.Repeat("1", 64),
		Diagnostics:            diagnostics,
	}
}

func testPublishedRevision(t *testing.T) domain.PublishedRevision {
	t.Helper()
	viewPath := filepath.Join("..", "..", "..", "..", "..", "..", "..", "api", "schemas", "deployment", "v1", "examples", "valid", "runtime-manifest-view.json")
	view, err := os.ReadFile(viewPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	input := domain.DeploymentInput{
		SchemaVersion: domain.SchemaVersionV1,
		Agent:         domain.AgentInput{AgentID: "agt_fixture_agent", VersionNumber: 3},
		Profile:       domain.ProfileInput{ProfileID: "rpf_fixture_profile", RevisionNumber: 2},
	}
	return domain.PublishedRevision{
		Revision: domain.DeploymentRevision{
			ID: "dpr_fixture_revision", TenantID: testTenantID,
			DeploymentID: testDeploymentID, RevisionNumber: 1,
			SchemaVersion: domain.SchemaVersionV1, Input: input,
			InputDigest:    "sha256:" + strings.Repeat("2", 64),
			AgentVersionID: "agv_internal", AgentSchemaVersion: "v1",
			AgentSpecDigest:   "sha256:" + strings.Repeat("3", 64),
			ProfileRevisionID: "rpr_internal", ProfileSchemaVersion: "v1",
			ProfileSpecDigest: "sha256:" + strings.Repeat("4", 64),
			PublishedBy:       testUserID, PublishedAt: now,
		},
		Manifest: domain.RuntimeManifest{
			ID: "rmf_fixture_manifest", Content: json.RawMessage(`{"credential_id":"crd_internal"}`),
		},
		ManifestID: "rmf_fixture_manifest", ManifestDigest: "sha256:" + strings.Repeat("5", 64),
		ManifestView: view,
	}
}

func testRevisionSummary() domain.DeploymentRevisionSummary {
	now := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	return domain.DeploymentRevisionSummary{
		ID: "dpr_fixture_revision", TenantID: testTenantID,
		DeploymentID: testDeploymentID, RevisionNumber: 1, SchemaVersion: "v1",
		AgentID: "agt_fixture_agent", AgentVersionNumber: 3,
		ProfileID: "rpf_fixture_profile", ProfileRevisionNumber: 2,
		InputDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestID:  "rmf_fixture_manifest", ManifestDigest: "sha256:" + strings.Repeat("5", 64),
		PublishedBy: testUserID, PublishedAt: now,
	}
}
