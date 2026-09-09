package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type cancellationObservingControlPlanePersistence struct {
	entered    chan struct{}
	canceled   chan struct{}
	enterOnce  sync.Once
	cancelOnce sync.Once
}

type saveFailingControlPlanePersistence struct {
	snapshot controlPlaneSnapshot
	revision int64
}

func (p saveFailingControlPlanePersistence) Load(context.Context) (controlPlaneSnapshot, int64, error) {
	return p.snapshot, p.revision, nil
}

func (saveFailingControlPlanePersistence) Save(context.Context, controlPlaneSnapshot, int64) (int64, error) {
	return 0, errors.New("database unavailable")
}

func (saveFailingControlPlanePersistence) Close() error { return nil }

type loadFailingAfterSaveControlPlanePersistence struct {
	snapshot  controlPlaneSnapshot
	revision  int64
	failLoads bool
}

func (p *loadFailingAfterSaveControlPlanePersistence) Load(context.Context) (controlPlaneSnapshot, int64, error) {
	if p.failLoads {
		return controlPlaneSnapshot{}, 0, errors.New("database unavailable")
	}
	return p.snapshot, p.revision, nil
}

func (p *loadFailingAfterSaveControlPlanePersistence) Save(_ context.Context, snapshot controlPlaneSnapshot, revision int64) (int64, error) {
	p.snapshot = snapshot
	p.revision = revision + 1
	p.failLoads = true
	return p.revision, nil
}

func (loadFailingAfterSaveControlPlanePersistence) Close() error { return nil }

func (p *cancellationObservingControlPlanePersistence) Load(ctx context.Context) (controlPlaneSnapshot, int64, error) {
	p.enterOnce.Do(func() { close(p.entered) })
	<-ctx.Done()
	p.cancelOnce.Do(func() { close(p.canceled) })
	return controlPlaneSnapshot{}, 0, ctx.Err()
}

func (*cancellationObservingControlPlanePersistence) Save(context.Context, controlPlaneSnapshot, int64) (int64, error) {
	return 0, nil
}

func (*cancellationObservingControlPlanePersistence) Close() error { return nil }

func TestControlPlaneLoadStopsWhenHTTPRequestIsCanceled(t *testing.T) {
	platform := NewInMemoryControlPlane()
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	defer handler.Close()
	if created, err := platform.createApp(context.Background(), AgentApp{ID: "stale-app", TenantID: "tenant-one", Name: "Stale App"}); err != nil || !created {
		t.Fatal("seed stale App")
	}
	persistence := &cancellationObservingControlPlanePersistence{entered: make(chan struct{}), canceled: make(chan struct{})}
	platform.persistence = persistence

	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/agent-apps", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-persistence.entered:
	case <-time.After(time.Second):
		t.Fatal("Control Plane load did not start")
	}
	cancel()
	select {
	case <-persistence.canceled:
	case <-time.After(time.Second):
		t.Fatal("Control Plane load did not receive request cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not return after cancellation")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	var apiError errorResponse
	if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil {
		t.Fatal(err)
	}
	if apiError.Error.Code != "control_plane_unavailable" {
		t.Fatalf("error code = %q", apiError.Error.Code)
	}
}

func TestControlPlaneFailureDoesNotBecomeResourceConflict(t *testing.T) {
	platform := NewInMemoryControlPlane()
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	defer handler.Close()
	platform.persistence = unavailableControlPlanePersistence{}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/agent-apps", strings.NewReader(`{"id":"new-app","name":"New App"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	var apiError errorResponse
	if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil {
		t.Fatal(err)
	}
	if apiError.Error.Code != "control_plane_unavailable" {
		t.Fatalf("error code = %q", apiError.Error.Code)
	}
}

func TestControlPlaneSaveFailureIsClassifiedFromCurrentRequest(t *testing.T) {
	platform := NewInMemoryControlPlane()
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureGovernance(handler.governance)
	if created, err := platform.createApp(context.Background(), AgentApp{ID: "app-one", TenantID: "tenant-one", Name: "App One"}); err != nil || !created {
		t.Fatal("seed App")
	}
	platform.persistence = saveFailingControlPlanePersistence{snapshot: controlPlaneSnapshotFrom(platform), revision: platform.persistenceRevision}

	requests := []struct {
		path string
		body string
	}{
		{path: "/api/v1/admin/governance/policy", body: `{"agent_app_id":"app-one","token_budget":1000}`},
		{path: "/api/v1/chat/bindings", body: `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"conversation-one","external_user_id":"user-one"}`},
	}
	for _, item := range requests {
		request := httptest.NewRequest(http.MethodPost, item.path, strings.NewReader(item.body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("POST %s status = %d, want %d", item.path, response.Code, http.StatusServiceUnavailable)
		}
		var apiError errorResponse
		if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil {
			t.Fatal(err)
		}
		if apiError.Error.Code != "control_plane_unavailable" {
			t.Fatalf("POST %s error code = %q", item.path, apiError.Error.Code)
		}
	}
}

func TestRuntimeStatusDoesNotClassifyBackendClosingAsControlPlaneFailure(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleTenantAdmin}}})
	defer handler.Close()
	handler.backends.beginClose()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/runtime/status", nil)
	request = request.WithContext(WithTenantContext(request.Context(), TenantContext{TenantID: "tenant-one", Role: RoleTenantAdmin}))
	response := httptest.NewRecorder()

	handler.handleRuntimeStatus(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "control_plane_unavailable") || !strings.Contains(response.Body.String(), `"id":"dependency-storage"`) {
		t.Fatalf("runtime status = %s", response.Body.String())
	}
}

func TestChannelBindingReportsControlPlaneFailureAfterBindingIsPersisted(t *testing.T) {
	platform := NewInMemoryControlPlane()
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	defer handler.Close()
	if created, err := platform.createApp(context.Background(), AgentApp{ID: "app-one", TenantID: "tenant-one", Name: "App One"}); err != nil || !created {
		t.Fatal("seed App")
	}
	platform.persistence = &loadFailingAfterSaveControlPlanePersistence{snapshot: controlPlaneSnapshotFrom(platform), revision: platform.persistenceRevision}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/chat/bindings", strings.NewReader(`{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"conversation-one","external_user_id":"user-one"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	var apiError errorResponse
	if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil {
		t.Fatal(err)
	}
	if apiError.Error.Code != "control_plane_unavailable" {
		t.Fatalf("error code = %q", apiError.Error.Code)
	}
}

func TestSQLiteControlPlaneSurvivesGatewayRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-plane.db")
	runtimePath := filepath.Join(t.TempDir(), "runtime.db")
	if err := MigrateSQLiteControlPlane(path); err != nil {
		t.Fatalf("migrate control plane: %v", err)
	}

	identity := DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{
		{TenantID: "tenant-one", TenantName: "Tenant One", Role: RolePlatformAdmin},
	}}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}

	openGateway := func() (*httptest.Server, *AdminHandler) {
		t.Helper()
		controlPlane, err := NewSQLiteControlPlane(path)
		if err != nil {
			t.Fatalf("open control plane: %v", err)
		}
		handler := NewAdminHandler(controlPlane, identity)
		handler.ConfigureBackendCatalog("", runtimePath)
		if err := handler.ConfigureBackendSelections(""); err != nil {
			t.Fatalf("configure backend selections: %v", err)
		}
		handler.ConfigureGovernance(NewGovernanceCenter())
		return httptest.NewServer(handler), handler
	}
	post := func(server *httptest.Server, path, body, key string, want int, target any) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			var apiError errorResponse
			_ = json.NewDecoder(response.Body).Decode(&apiError)
			t.Fatalf("POST %s = %d (%s), want %d", path, response.StatusCode, apiError.Error.Code, want)
		}
		if target != nil {
			if err := json.NewDecoder(response.Body).Decode(target); err != nil {
				t.Fatalf("decode POST %s: %v", path, err)
			}
		}
	}

	first, firstHandler := openGateway()
	post(first, "/api/v1/admin/agent-apps", `{"id":"durable-app","name":"Durable App"}`, "", http.StatusCreated, nil)
	post(first, "/api/v1/admin/deployments", `{"id":"durable-deployment","agent_app_id":"durable-app"}`, "", http.StatusCreated, nil)
	var version DeploymentVersion
	post(first, "/api/v1/admin/deployments/durable-deployment/versions", `{"config":{"runner":"deterministic"}}`, "durable-version", http.StatusCreated, &version)
	post(first, "/api/v1/admin/deployments/durable-deployment/transition", `{"status":"published","version_id":"`+version.ID+`"}`, "", http.StatusOK, nil)
	post(first, "/api/v1/admin/deployments/durable-deployment/transition", `{"status":"active"}`, "", http.StatusOK, nil)
	post(first, "/api/v1/admin/governance/policy", `{"agent_app_id":"durable-app","token_budget":1000}`, "", http.StatusOK, nil)
	post(first, "/api/v1/admin/storage/backend", `{"backend":"sqlite"}`, "", http.StatusOK, nil)
	var binding ChannelBinding
	post(first, "/api/v1/chat/bindings", `{"channel":"mock","app_id":"durable-app","conversation_type":"single","external_conversation_id":"durable-user","external_user_id":"durable-user","secret":"durable-secret"}`, "", http.StatusCreated, &binding)
	first.Close()
	if err := firstHandler.Close(); err != nil {
		t.Fatalf("close first Gateway: %v", err)
	}

	second, secondHandler := openGateway()
	defer second.Close()
	defer secondHandler.Close()

	response, err := client.Get(second.URL + "/api/v1/admin/deployments")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var deployments struct {
		Items []Deployment `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&deployments); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(deployments.Items) != 1 || deployments.Items[0].Status != DeploymentActive {
		t.Fatalf("deployments after restart = %d %#v", response.StatusCode, deployments.Items)
	}
	bindingsResponse, err := client.Get(second.URL + "/api/v1/chat/bindings")
	if err != nil {
		t.Fatal(err)
	}
	defer bindingsResponse.Body.Close()
	var bindings struct {
		Items []ChannelBinding `json:"items"`
	}
	if err := json.NewDecoder(bindingsResponse.Body).Decode(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindingsResponse.StatusCode != http.StatusOK || len(bindings.Items) != 1 || bindings.Items[0].ID != binding.ID {
		t.Fatalf("bindings after restart = %d %#v", bindingsResponse.StatusCode, bindings.Items)
	}
	policyResponse, err := client.Get(second.URL + "/api/v1/admin/governance/policy?app_id=durable-app")
	if err != nil {
		t.Fatal(err)
	}
	defer policyResponse.Body.Close()
	if policyResponse.StatusCode != http.StatusOK {
		t.Fatalf("governance policy after restart = %d", policyResponse.StatusCode)
	}
	backendResponse, err := client.Get(second.URL + "/api/v1/admin/storage/backend")
	if err != nil {
		t.Fatal(err)
	}
	defer backendResponse.Body.Close()
	var backend struct {
		Backend string `json:"backend"`
	}
	if err := json.NewDecoder(backendResponse.Body).Decode(&backend); err != nil {
		t.Fatal(err)
	}
	if backendResponse.StatusCode != http.StatusOK || backend.Backend != "sqlite" {
		t.Fatalf("backend after restart = %d/%q", backendResponse.StatusCode, backend.Backend)
	}

	var run GatewayResponse
	post(second, "/api/v1/admin/run", `{"app_id":"durable-app","session_id":"durable-session","input":"hello"}`, "", http.StatusOK, &run)
	if run.Output != "echo:hello" || run.SessionID != "durable-session" {
		t.Fatalf("run after restart = %#v", run)
	}
}

func TestPostgresControlPlaneIsSharedAcrossGateways(t *testing.T) {
	dsn := os.Getenv("TRPC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TRPC_TEST_POSTGRES_DSN is not configured")
	}
	if err := MigratePostgresControlPlane(dsn); err != nil {
		t.Fatal(err)
	}
	identity := DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{
		TenantID: "tenant-shared", TenantName: "Shared", Role: RolePlatformAdmin,
	}}}
	open := func() (*httptest.Server, *AdminHandler) {
		store, err := NewPostgresControlPlane(dsn)
		if err != nil {
			t.Fatal(err)
		}
		handler := NewAdminHandler(store, identity)
		return httptest.NewServer(handler), handler
	}
	gatewayA, handlerA := open()
	defer gatewayA.Close()
	defer handlerA.Close()
	gatewayB, handlerB := open()
	defer gatewayB.Close()
	defer handlerB.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	suffix := fmt.Sprint(time.Now().UnixNano())
	appID := "shared-app-" + suffix
	deploymentID := "shared-deployment-" + suffix
	post := func(url, body, key string, target any) {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			t.Fatalf("POST %s = %d", url, response.StatusCode)
		}
		if target != nil {
			if err := json.NewDecoder(response.Body).Decode(target); err != nil {
				t.Fatal(err)
			}
		}
	}
	post(gatewayA.URL+"/api/v1/admin/agent-apps", fmt.Sprintf(`{"id":%q,"name":"Shared App"}`, appID), "", nil)
	post(gatewayA.URL+"/api/v1/admin/deployments", fmt.Sprintf(`{"id":%q,"agent_app_id":%q}`, deploymentID, appID), "", nil)
	var version DeploymentVersion
	post(gatewayA.URL+"/api/v1/admin/deployments/"+deploymentID+"/versions", `{"config":{"runner":"deterministic"}}`, "shared-version-"+suffix, &version)
	post(gatewayA.URL+"/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"published","version_id":"`+version.ID+`"}`, "", nil)
	post(gatewayA.URL+"/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"active"}`, "", nil)

	response, err := client.Get(gatewayB.URL + "/api/v1/admin/deployments")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var list struct {
		Items []Deployment `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, deployment := range list.Items {
		if deployment.ID == deploymentID && deployment.Status == DeploymentActive {
			found = true
		}
	}
	if response.StatusCode != http.StatusOK || !found {
		t.Fatalf("Gateway B deployments = %d %#v", response.StatusCode, list.Items)
	}
	var run GatewayResponse
	post(gatewayB.URL+"/api/v1/admin/run", fmt.Sprintf(`{"app_id":%q,"session_id":"shared-session","input":"hello"}`, appID), "", &run)
	if run.Output != "echo:hello" {
		t.Fatalf("Gateway B output = %q", run.Output)
	}
}
