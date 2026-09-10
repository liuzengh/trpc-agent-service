package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	appmemory "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configmemory "github.com/liuzengh/trpc-agent-service/trpcservice/config/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

type headerPrincipalResolver struct{}

func (headerPrincipalResolver) Resolve(r *http.Request) (Principal, error) {
	return Principal{Authenticated: r.Header.Get("X-Authenticated") == "true", TenantID: r.Header.Get("X-Admin-Tenant"), SubjectID: "operator", CanManage: r.Header.Get("X-Can-Manage") != "false", CanManageReleases: r.Header.Get("X-Can-Manage-Releases") == "true"}, nil
}

func TestAdminHTTPStagesCandidateAndRequiresReleasePrivilege(t *testing.T) {
	handler, tenants := adminSetup(t)
	payload := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1}
	body, _ := json.Marshal(payload)
	request := func(method, path string, value []byte) *http.Request {
		req := httptest.NewRequest(method, path, bytes.NewReader(value))
		req.Header.Set("X-Authenticated", "true")
		req.Header.Set("X-Admin-Tenant", "tenant-a")
		req.Header.Set("X-Reason-Code", "test")
		req.Header.Set("X-Correlation-ID", "correlation")
		req.Header.Set("X-Trace-ID", "trace")
		return req
	}
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", body))
	if first.Code != http.StatusCreated {
		t.Fatalf("publish=%d %s", first.Code, first.Body.String())
	}
	var baseline config.PublishResult
	if err := json.Unmarshal(first.Body.Bytes(), &baseline); err != nil {
		t.Fatal(err)
	}
	payload.PolicyVersion = 2
	body, _ = json.Marshal(payload)
	staged := httptest.NewRecorder()
	handler.ServeHTTP(staged, request(http.MethodPost, "/v1/tenants/tenant-a/configs/stage?expected_version="+strconv.FormatInt(baseline.Tenant.Version, 10), body))
	if staged.Code != http.StatusCreated {
		t.Fatalf("stage=%d %s", staged.Code, staged.Body.String())
	}
	var candidate config.Snapshot
	if err := json.Unmarshal(staged.Body.Bytes(), &candidate); err != nil {
		t.Fatal(err)
	}
	current, err := tenants.Get(context.Background(), "tenant-a")
	if err != nil || current.ActiveConfigVersion != baseline.Snapshot.ConfigVersion {
		t.Fatalf("stage changed tenant=%#v err=%v", current, err)
	}
	releaseBody, _ := json.Marshal(config.ReleaseCreateInput{ReleaseID: "release-http", Percentage: 0, Salt: "0123456789abcdef", Targets: []config.ReleaseTarget{{TenantID: "tenant-a", BaselineConfigVersion: baseline.Snapshot.ConfigVersion, CandidateConfigVersion: candidate.ConfigVersion}}})
	forbidden := httptest.NewRecorder()
	handler.ServeHTTP(forbidden, request(http.MethodPost, "/v1/config-releases", releaseBody))
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("release privilege=%d", forbidden.Code)
	}
	allowed := request(http.MethodPost, "/v1/config-releases", releaseBody)
	allowed.Header.Set("X-Can-Manage-Releases", "true")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, allowed)
	if created.Code != http.StatusCreated {
		t.Fatalf("release=%d %s", created.Code, created.Body.String())
	}
}

func adminSetup(t *testing.T) (*Handler, *tenantmemory.Repository) {
	t.Helper()
	ctx := context.Background()
	meta := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "setup", CorrelationID: "correlation", TraceID: "trace"}
	tenants := tenantmemory.New()
	if _, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "A"}, ChangeMetadata: meta}); err != nil {
		t.Fatal(err)
	}
	apps := appmemory.New()
	appMeta := agentapp.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "setup", CorrelationID: "correlation", TraceID: "trace"}
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: "tenant-a", AgentAppID: "app", ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "help", ModelProfileID: "mock", ModelProfileVersion: 1}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: "tenant-a", AgentAppID: "app", Revision: rev.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: appMeta}); err != nil {
		t.Fatal(err)
	}
	handler := &Handler{Service: Service{Configs: configmemory.New(tenants, apps)}, Principals: headerPrincipalResolver{}}
	return handler, tenants
}

func TestAdminHTTPValidatePublishAndTenantScope(t *testing.T) {
	handler, tenants := adminSetup(t)
	payload := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1}
	body, _ := json.Marshal(payload)
	request := func(method, path, principalTenant string) *http.Request {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("X-Authenticated", "true")
		req.Header.Set("X-Admin-Tenant", principalTenant)
		req.Header.Set("X-Reason-Code", "test")
		req.Header.Set("X-Correlation-ID", "correlation")
		req.Header.Set("X-Trace-ID", "trace")
		return req
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/validate", "tenant-a"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("validate=%d %s", rec.Code, rec.Body.String())
	}
	current, err := tenants.Get(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 || current.ActiveConfigVersion != 0 {
		t.Fatalf("validate had side effects: %#v", current)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "tenant-b"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("scope=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "tenant-a"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("publish=%d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodGet, "/v1/tenants/tenant-a/configs", "tenant-a"))
	if rec.Code != http.StatusOK {
		t.Fatalf("get=%d %s", rec.Code, rec.Body.String())
	}
}

func TestAdminHTTPAuthenticationRollbackVersionReadAndCAS(t *testing.T) {
	handler, _ := adminSetup(t)
	payload := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1}
	body, _ := json.Marshal(payload)
	request := func(method, path string) *http.Request {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("X-Authenticated", "true")
		req.Header.Set("X-Admin-Tenant", "tenant-a")
		req.Header.Set("X-Reason-Code", "test")
		req.Header.Set("X-Correlation-ID", "correlation")
		req.Header.Set("X-Trace-ID", "trace")
		return req
	}

	unauthenticated := request(http.MethodPost, "/v1/tenants/tenant-a/configs/validate")
	unauthenticated.Header.Del("X-Authenticated")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, unauthenticated)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d", recorder.Code)
	}

	forbidden := request(http.MethodPost, "/v1/tenants/tenant-a/configs/validate")
	forbidden.Header.Set("X-Can-Manage", "false")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, forbidden)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("forbidden=%d", recorder.Code)
	}

	missingMetadata := request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1")
	missingMetadata.Header.Del("X-Reason-Code")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, missingMetadata)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("missing metadata=%d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1"))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("first publish=%d %s", recorder.Code, recorder.Body.String())
	}
	var first config.PublishResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1"))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("stale publish=%d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request(http.MethodGet, "/v1/tenants/tenant-a/configs/"+strconv.FormatInt(first.Snapshot.ConfigVersion, 10)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("version read=%d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version="+strconv.FormatInt(first.Tenant.Version, 10)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("second publish=%d %s", recorder.Code, recorder.Body.String())
	}
	var second config.PublishResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request(http.MethodPost, "/v1/tenants/tenant-a/configs/rollback?expected_version="+strconv.FormatInt(second.Tenant.Version, 10)+"&target_version="+strconv.FormatInt(first.Snapshot.ConfigVersion, 10)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("rollback=%d %s", recorder.Code, recorder.Body.String())
	}
	var rolled config.PublishResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &rolled); err != nil || rolled.Snapshot.ConfigVersion <= second.Snapshot.ConfigVersion || rolled.Snapshot.Payload.PolicyVersion != 1 {
		t.Fatalf("rollback result=%#v err=%v", rolled, err)
	}
}
