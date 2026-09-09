package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type capturedRun struct {
	tenant  TenantContext
	request RunnerRequest
}

type tenantCapturingRunner struct {
	runs chan capturedRun
}

func (r tenantCapturingRunner) Run(ctx context.Context, request RunnerRequest) (RunnerResponse, error) {
	tenant, _ := TenantContextFromContext(ctx)
	r.runs <- capturedRun{tenant: tenant, request: request}
	return RunnerResponse{Output: "captured:" + request.Input}, nil
}

func postJSONWithKey(t *testing.T, client *http.Client, url, body, key string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
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
	return response
}

func requireJSONResponse(t *testing.T, client *http.Client, url, body, idempotencyKey string, wantStatus int, target any) {
	t.Helper()
	response := postJSONWithKey(t, client, url, body, idempotencyKey)
	if target != nil {
		decodeResponse(t, response, wantStatus, target)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("POST %s = %d, want %d: %s", url, response.StatusCode, wantStatus, data)
	}
}

func newDevelopmentClient(t *testing.T, identity DevelopmentIdentity) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewServer(NewAdminHandler(NewInMemoryControlPlane(), identity))
	jar, err := cookiejar.New(nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return server, &http.Client{Jar: jar}
}

func TestDevelopmentIdentityCanOnlySwitchToServerApprovedTenant(t *testing.T) {
	server := httptest.NewServer(NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{
		ID: "developer", Name: "Local Developer",
		Assignments: []TenantAssignment{{TenantID: "tenant-a", TenantName: "Tenant A", Role: RolePlatformAdmin}},
	}))
	defer server.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}

	resp, err := client.Get(server.URL + "/api/v1/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var identity identityResponse
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || identity.ActiveTenantID != "tenant-a" || identity.Assignments[0].Role != RolePlatformAdmin {
		t.Fatalf("identity = %#v, status = %d", identity, resp.StatusCode)
	}

	resp, err = client.Post(server.URL+"/api/v1/auth/switch-tenant", "application/json", strings.NewReader(`{"tenant_id":"tenant-forged"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var apiErr errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden || apiErr.Error.Code != "tenant_not_assigned" {
		t.Fatalf("got %d/%q", resp.StatusCode, apiErr.Error.Code)
	}
}

func TestDevelopmentIdentityConcurrentSwitchAndReadIsRaceFree(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "developer", Assignments: []TenantAssignment{
		{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
		{TenantID: "tenant-two", TenantName: "Two", Role: RoleViewer},
	}})
	defer server.Close()
	response, err := client.Get(server.URL + "/api/v1/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(2)
		go func(index int) {
			defer wg.Done()
			tenant := "tenant-one"
			if index%2 == 1 {
				tenant = "tenant-two"
			}
			response, err := client.Post(server.URL+"/api/v1/auth/switch-tenant", "application/json", bytes.NewBufferString(`{"tenant_id":"`+tenant+`"}`))
			if err == nil {
				response.Body.Close()
			}
		}(i)
		go func() {
			defer wg.Done()
			response, err := client.Get(server.URL + "/api/v1/auth/me")
			if err == nil {
				response.Body.Close()
			}
		}()
	}
	wg.Wait()
}

func TestDevelopmentSessionsRemainBounded(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "developer", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleViewer}}})
	for i := 0; i < maxDevelopmentSessions+20; i++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil))
	}
	handler.mu.Lock()
	count := len(handler.sessions)
	handler.mu.Unlock()
	if count != maxDevelopmentSessions {
		t.Fatalf("sessions = %d, want %d", count, maxDevelopmentSessions)
	}
}

func TestTenantManagementIsServerAuthorizedAndDeterministic(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{
		ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-home", TenantName: "Home", Role: RolePlatformAdmin}},
	})
	defer server.Close()

	response, err := client.Post(server.URL+"/api/v1/admin/tenants", "application/json", bytes.NewBufferString(`{"id":"tenant-east","name":"East Team"}`))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", response.StatusCode)
	}
	response.Body.Close()

	response, err = client.Get(server.URL + "/api/v1/admin/tenants/tenant-east")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var tenant Tenant
	if err := json.NewDecoder(response.Body).Decode(&tenant); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || tenant.ID != "tenant-east" || tenant.Name != "East Team" {
		t.Fatalf("tenant = %#v, status = %d", tenant, response.StatusCode)
	}

	duplicate, err := client.Post(server.URL+"/api/v1/admin/tenants", "application/json", bytes.NewBufferString(`{"id":"tenant-east","name":"Other"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Body.Close()
	var apiErr errorResponse
	_ = json.NewDecoder(duplicate.Body).Decode(&apiErr)
	if duplicate.StatusCode != http.StatusConflict || apiErr.Error.Code != "tenant_exists" {
		t.Fatalf("duplicate = %d/%q", duplicate.StatusCode, apiErr.Error.Code)
	}
}

func TestViewerCannotCreateTenantWithForgedTenantHeader(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{
		ID: "viewer", Assignments: []TenantAssignment{{TenantID: "tenant-view", TenantName: "View", Role: RoleViewer}},
	})
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/admin/tenants", bytes.NewBufferString(`{"id":"forged","name":"Forged"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Tenant-ID", "tenant-admin")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var apiErr errorResponse
	_ = json.NewDecoder(response.Body).Decode(&apiErr)
	if response.StatusCode != http.StatusForbidden || apiErr.Error.Code != "forbidden" {
		t.Fatalf("got %d/%q", response.StatusCode, apiErr.Error.Code)
	}
}

func TestAgentAppsAreIsolatedByTrustedTenantContext(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{
		{TenantID: "tenant-one", TenantName: "One", Role: RoleTenantAdmin},
		{TenantID: "tenant-two", TenantName: "Two", Role: RoleTenantAdmin},
	}})
	defer server.Close()

	create := func(id, name string) {
		t.Helper()
		response, err := client.Post(server.URL+"/api/v1/admin/agent-apps", "application/json", bytes.NewBufferString(`{"id":"`+id+`","name":"`+name+`","tenant_id":"tenant-forged"}`))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("create %s = %d", id, response.StatusCode)
		}
	}
	create("app-one", "App One")
	switchResponse, err := client.Post(server.URL+"/api/v1/auth/switch-tenant", "application/json", bytes.NewBufferString(`{"tenant_id":"tenant-two"}`))
	if err != nil {
		t.Fatal(err)
	}
	switchResponse.Body.Close()
	create("app-two", "App Two")

	response, err := client.Get(server.URL + "/api/v1/admin/agent-apps")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var list struct {
		Items []AgentApp `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].ID != "app-two" || list.Items[0].TenantID != "tenant-two" {
		t.Fatalf("isolated list = %#v", list.Items)
	}

	response, err = client.Get(server.URL + "/api/v1/admin/agent-apps/app-one")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var apiErr errorResponse
	_ = json.NewDecoder(response.Body).Decode(&apiErr)
	if response.StatusCode != http.StatusNotFound || apiErr.Error.Code != "agent_app_not_found" {
		t.Fatalf("cross-tenant detail = %d/%q", response.StatusCode, apiErr.Error.Code)
	}
}

func TestDeploymentVersionsAreOrderedAndImmutable(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RoleTenantAdmin}}})
	defer server.Close()
	postJSON := func(path, body string, want int) *http.Response {
		t.Helper()
		response, err := client.Post(server.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			response.Body.Close()
			t.Fatalf("POST %s = %d, want %d", path, response.StatusCode, want)
		}
		return response
	}
	postJSON("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App One"}`, http.StatusCreated).Body.Close()
	postJSON("/api/v1/admin/deployments", `{"id":"deploy-one","agent_app_id":"app-one"}`, http.StatusCreated).Body.Close()

	first := postJSONWithKey(t, client, server.URL+"/api/v1/admin/deployments/deploy-one/versions", `{"config":{"model":"fake-v1","nested":{"temperature":0}}}`, "version-one")
	if first.StatusCode != http.StatusCreated {
		first.Body.Close()
		t.Fatalf("first version = %d, want %d", first.StatusCode, http.StatusCreated)
	}
	var versionOne DeploymentVersion
	if err := json.NewDecoder(first.Body).Decode(&versionOne); err != nil {
		t.Fatal(err)
	}
	first.Body.Close()
	second := postJSONWithKey(t, client, server.URL+"/api/v1/admin/deployments/deploy-one/versions", `{"config":{"model":"fake-v2"}}`, "version-two")
	if second.StatusCode != http.StatusCreated {
		second.Body.Close()
		t.Fatalf("second version = %d, want %d", second.StatusCode, http.StatusCreated)
	}
	var versionTwo DeploymentVersion
	if err := json.NewDecoder(second.Body).Decode(&versionTwo); err != nil {
		t.Fatal(err)
	}
	second.Body.Close()
	if versionOne.Number != 1 || versionTwo.Number != 2 || versionOne.ID == versionTwo.ID {
		t.Fatalf("versions = %#v, %#v", versionOne, versionTwo)
	}

	immutable := postJSON("/api/v1/admin/deployments/deploy-one/versions/"+versionOne.ID, `{"config":{"model":"changed"}}`, http.StatusMethodNotAllowed)
	defer immutable.Body.Close()
	var apiErr errorResponse
	_ = json.NewDecoder(immutable.Body).Decode(&apiErr)
	if apiErr.Error.Code != "immutable_version" {
		t.Fatalf("immutable error = %q", apiErr.Error.Code)
	}
}

func TestDeploymentVersionCreationIsIdempotent(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RoleTenantAdmin}}})
	defer server.Close()

	create := func(path, body string) {
		t.Helper()
		response, err := client.Post(server.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("POST %s = %d, want %d", path, response.StatusCode, http.StatusCreated)
		}
	}
	create("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App One"}`)
	create("/api/v1/admin/deployments", `{"id":"deploy-one","agent_app_id":"app-one"}`)
	versionURL := server.URL + "/api/v1/admin/deployments/deploy-one/versions"

	missing := postJSONWithKey(t, client, versionURL, `{"config":{"model":"fake"}}`, "")
	assertAPIError(t, missing, http.StatusBadRequest, "idempotency_key_required")
	invalid := postJSONWithKey(t, client, versionURL, `{"config":{"model":"fake"}}`, strings.Repeat("x", 129))
	assertAPIError(t, invalid, http.StatusBadRequest, "invalid_idempotency_key")

	firstResponse := postJSONWithKey(t, client, versionURL, `{"config":{"model":"fake","nested":{"temperature":0}}}`, "create-version")
	var first DeploymentVersion
	decodeResponse(t, firstResponse, http.StatusCreated, &first)
	replayResponse := postJSONWithKey(t, client, versionURL, `{"config":{"nested":{"temperature":0},"model":"fake"}}`, "create-version")
	var replay DeploymentVersion
	decodeResponse(t, replayResponse, http.StatusCreated, &replay)
	if first.ID != replay.ID || first.Number != replay.Number || !first.CreatedAt.Equal(replay.CreatedAt) {
		t.Fatalf("replay = %#v, want original %#v", replay, first)
	}

	conflict := postJSONWithKey(t, client, versionURL, `{"config":{"model":"different"}}`, "create-version")
	assertAPIError(t, conflict, http.StatusConflict, "idempotency_key_reused")
	secondResponse := postJSONWithKey(t, client, versionURL, `{"config":{"model":"fake","nested":{"temperature":0}}}`, "create-same-config-again")
	var second DeploymentVersion
	decodeResponse(t, secondResponse, http.StatusCreated, &second)
	if second.Number != 2 || second.ID == first.ID {
		t.Fatalf("second intentional version = %#v, first = %#v", second, first)
	}

	const concurrentRequests = 16
	versions := make(chan DeploymentVersion, concurrentRequests)
	errors := make(chan error, concurrentRequests)
	var wait sync.WaitGroup
	for i := 0; i < concurrentRequests; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := postJSONWithKey(t, client, versionURL, `{"config":{"model":"concurrent"}}`, "concurrent-version")
			defer response.Body.Close()
			if response.StatusCode != http.StatusCreated {
				errors <- fmt.Errorf("status = %d", response.StatusCode)
				return
			}
			var version DeploymentVersion
			if err := json.NewDecoder(response.Body).Decode(&version); err != nil {
				errors <- err
				return
			}
			versions <- version
		}()
	}
	wait.Wait()
	close(errors)
	close(versions)
	for err := range errors {
		t.Fatal(err)
	}
	for version := range versions {
		if version.Number != 3 || version.ID != "deploy-one-v3" {
			t.Fatalf("concurrent replay = %#v", version)
		}
	}

	listResponse, err := client.Get(versionURL)
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Items []DeploymentVersion `json:"items"`
	}
	decodeResponse(t, listResponse, http.StatusOK, &list)
	if len(list.Items) != 3 {
		t.Fatalf("versions after replay = %d, want 3", len(list.Items))
	}
}

func decodeResponse(t *testing.T, response *http.Response, wantStatus int, target any) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("response = %d, want %d: %s", response.StatusCode, wantStatus, data)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func assertAPIError(t *testing.T, response *http.Response, wantStatus int, wantCode string) {
	t.Helper()
	var apiErr errorResponse
	decodeResponse(t, response, wantStatus, &apiErr)
	if apiErr.Error.Code != wantCode {
		t.Fatalf("error code = %q, want %q", apiErr.Error.Code, wantCode)
	}
}

func TestDeploymentLifecycleControlsRoutedExecution(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	defer server.Close()
	post := func(path, body string, want int) []byte {
		t.Helper()
		response, err := client.Post(server.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("POST %s = %d, want %d: %s", path, response.StatusCode, want, data)
		}
		return data
	}
	post("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App One"}`, http.StatusCreated)
	post("/api/v1/admin/deployments", `{"id":"deploy-one","agent_app_id":"app-one"}`, http.StatusCreated)
	var version DeploymentVersion
	versionResponse := postJSONWithKey(t, client, server.URL+"/api/v1/admin/deployments/deploy-one/versions", `{"config":{"runner":"fake"}}`, "lifecycle-version")
	if versionResponse.StatusCode != http.StatusCreated {
		versionResponse.Body.Close()
		t.Fatalf("version = %d, want %d", versionResponse.StatusCode, http.StatusCreated)
	}
	versionData, err := io.ReadAll(versionResponse.Body)
	versionResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(versionData, &version); err != nil {
		t.Fatal(err)
	}
	post("/api/v1/admin/run", `{"app_id":"app-one","session_id":"s1","input":"hello"}`, http.StatusNotFound)
	post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"active"}`, http.StatusConflict)
	post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"published","version_id":"`+version.ID+`"}`, http.StatusOK)
	post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"active"}`, http.StatusOK)
	var result GatewayResponse
	if err := json.Unmarshal(post("/api/v1/admin/run", `{"app_id":"app-one","session_id":"s1","input":"hello"}`, http.StatusOK), &result); err != nil {
		t.Fatal(err)
	}
	if result.Output != "echo:hello" || result.SessionID != "s1" {
		t.Fatalf("result = %#v", result)
	}
	post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"paused"}`, http.StatusOK)
	post("/api/v1/admin/run", `{"app_id":"app-one","session_id":"s2","input":"hello"}`, http.StatusNotFound)
}

func TestAgentAppAllowsOnlyOneActiveDeployment(t *testing.T) {
	store := NewInMemoryControlPlane()
	handler := NewAdminHandler(store, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}})
	runnerRequests := make(chan RunnerRequest, 1)
	handler.ConfigureRuntime(capturingRunner{request: runnerRequests}, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	post := func(path, body, idempotencyKey string, wantStatus int, target any) {
		t.Helper()
		requireJSONResponse(t, client, server.URL+path, body, idempotencyKey, wantStatus, target)
	}

	post("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App One"}`, "", http.StatusCreated, nil)
	for _, deploymentID := range []string{"deploy-one", "deploy-two"} {
		post("/api/v1/admin/deployments", `{"id":"`+deploymentID+`","agent_app_id":"app-one"}`, "", http.StatusCreated, nil)
		var version DeploymentVersion
		post("/api/v1/admin/deployments/"+deploymentID+"/versions", `{"config":{"runner":"`+deploymentID+`"}}`, "shared-version-key", http.StatusCreated, &version)
		post("/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"published","version_id":"`+version.ID+`"}`, "", http.StatusOK, nil)
	}

	type activationResult struct {
		deploymentID string
		status       int
		code         string
		err          error
	}
	results := make(chan activationResult, 2)
	var wait sync.WaitGroup
	for _, deploymentID := range []string{"deploy-one", "deploy-two"} {
		wait.Add(1)
		go func(deploymentID string) {
			defer wait.Done()
			response, err := client.Post(server.URL+"/api/v1/admin/deployments/"+deploymentID+"/transition", "application/json", bytes.NewBufferString(`{"status":"active"}`))
			if err != nil {
				results <- activationResult{deploymentID: deploymentID, err: err}
				return
			}
			defer response.Body.Close()
			result := activationResult{deploymentID: deploymentID, status: response.StatusCode}
			if response.StatusCode == http.StatusConflict {
				var apiErr errorResponse
				if err := json.NewDecoder(response.Body).Decode(&apiErr); err != nil {
					result.err = err
				} else {
					result.code = apiErr.Error.Code
				}
			}
			results <- result
		}(deploymentID)
	}
	wait.Wait()
	close(results)
	activeID := ""
	conflicts := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.status {
		case http.StatusOK:
			activeID = result.deploymentID
		case http.StatusConflict:
			conflicts++
			if result.code != "agent_app_already_has_active_deployment" {
				t.Fatalf("activation conflict code = %q", result.code)
			}
		default:
			t.Fatalf("activation %s = %d", result.deploymentID, result.status)
		}
	}
	if activeID == "" || conflicts != 1 {
		t.Fatalf("activeID = %q, conflicts = %d", activeID, conflicts)
	}

	response, err := client.Get(server.URL + "/api/v1/admin/deployments")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Items []Deployment `json:"items"`
	}
	decodeResponse(t, response, http.StatusOK, &list)
	activeCount := 0
	for _, deployment := range list.Items {
		if deployment.Status == DeploymentActive {
			activeCount++
			if deployment.ID != activeID {
				t.Fatalf("active Deployment = %q, want %q", deployment.ID, activeID)
			}
		} else if deployment.Status != DeploymentPublished {
			t.Fatalf("non-active Deployment = %#v", deployment)
		}
	}
	if len(list.Items) != 2 || activeCount != 1 {
		t.Fatalf("deployments after conflict = %#v", list.Items)
	}

	var result GatewayResponse
	post("/api/v1/admin/run", `{"app_id":"app-one","session_id":"session-one","input":"hello"}`, "", http.StatusOK, &result)
	if result.Output != "captured" {
		t.Fatalf("run result = %#v", result)
	}
	request := <-runnerRequests
	if request.DeploymentID != activeID || request.VersionID != activeID+"-v1" {
		t.Fatalf("Runner request = %#v", request)
	}
}

func TestTwoTenantRoutesResolveScopedDeploymentVersions(t *testing.T) {
	store := NewInMemoryControlPlane()
	runs := make(chan capturedRun, 3)
	handler := NewAdminHandler(store, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{
		{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
		{TenantID: "tenant-two", TenantName: "Two", Role: RolePlatformAdmin},
	}})
	handler.ConfigureRuntime(tenantCapturingRunner{runs: runs}, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	post := func(path, body, key string, wantStatus int, target any) {
		t.Helper()
		requireJSONResponse(t, client, server.URL+path, body, key, wantStatus, target)
	}
	activate := func(appID, deploymentID, key string) DeploymentVersion {
		t.Helper()
		post("/api/v1/admin/agent-apps", `{"id":"`+appID+`","name":"Scoped App"}`, "", http.StatusCreated, nil)
		post("/api/v1/admin/deployments", `{"id":"`+deploymentID+`","agent_app_id":"`+appID+`"}`, "", http.StatusCreated, nil)
		var version DeploymentVersion
		post("/api/v1/admin/deployments/"+deploymentID+"/versions", `{"config":{"scope":"`+deploymentID+`"}}`, key, http.StatusCreated, &version)
		post("/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"published","version_id":"`+version.ID+`"}`, "", http.StatusOK, nil)
		post("/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"active"}`, "", http.StatusOK, nil)
		return version
	}

	versionOne := activate("app-one", "deploy-one", "shared-version-key")
	var responseOne GatewayResponse
	post("/api/v1/admin/run", `{"app_id":"app-one","session_id":"session-one","input":"one"}`, "", http.StatusOK, &responseOne)
	runOne := <-runs
	if responseOne.Output != "captured:one" || runOne.tenant.TenantID != "tenant-one" || runOne.request.AppID != "app-one" || runOne.request.SessionID != "session-one" || runOne.request.DeploymentID != "deploy-one" || runOne.request.VersionID != versionOne.ID {
		t.Fatalf("tenant one route = response %#v, run %#v", responseOne, runOne)
	}

	post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, "", http.StatusOK, nil)
	versionTwo := activate("app-two", "deploy-two", "shared-version-key")
	var responseTwo GatewayResponse
	post("/api/v1/admin/run", `{"app_id":"app-two","session_id":"session-two","input":"two"}`, "", http.StatusOK, &responseTwo)
	runTwo := <-runs
	if responseTwo.Output != "captured:two" || runTwo.tenant.TenantID != "tenant-two" || runTwo.request.AppID != "app-two" || runTwo.request.SessionID != "session-two" || runTwo.request.DeploymentID != "deploy-two" || runTwo.request.VersionID != versionTwo.ID {
		t.Fatalf("tenant two route = response %#v, run %#v", responseTwo, runTwo)
	}

	crossTenant := postJSONWithKey(t, client, server.URL+"/api/v1/admin/run", `{"app_id":"app-one","session_id":"cross-tenant","input":"guess"}`, "")
	assertAPIError(t, crossTenant, http.StatusNotFound, "active_deployment_not_found")
	select {
	case unexpected := <-runs:
		t.Fatalf("cross-Tenant request reached Runner: %#v", unexpected)
	default:
	}
}

func TestSameDeploymentAndVersionIDsRemainTenantScoped(t *testing.T) {
	store := NewInMemoryControlPlane()
	runs := make(chan capturedRun, 2)
	handler := NewAdminHandler(store, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{
		{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
		{TenantID: "tenant-two", TenantName: "Two", Role: RolePlatformAdmin},
	}})
	handler.ConfigureRuntime(tenantCapturingRunner{runs: runs}, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	post := func(path, body, key string, wantStatus int, target any) {
		t.Helper()
		requireJSONResponse(t, client, server.URL+path, body, key, wantStatus, target)
	}
	activate := func(marker string) {
		t.Helper()
		post("/api/v1/admin/agent-apps", `{"id":"same-app","name":"Scoped App"}`, "", http.StatusCreated, nil)
		post("/api/v1/admin/deployments", `{"id":"same-deploy","agent_app_id":"same-app"}`, "", http.StatusCreated, nil)
		post("/api/v1/admin/deployments/same-deploy/versions", `{"config":{"tenant_marker":"`+marker+`"}}`, marker, http.StatusCreated, nil)
		post("/api/v1/admin/deployments/same-deploy/transition", `{"status":"published","version_id":"same-deploy-v1"}`, "", http.StatusOK, nil)
		post("/api/v1/admin/deployments/same-deploy/transition", `{"status":"active"}`, "", http.StatusOK, nil)
	}
	activate("one")
	post("/api/v1/admin/run", `{"app_id":"same-app","session_id":"session-one","input":"one"}`, "", http.StatusOK, nil)
	first := <-runs
	if first.request.Version == nil || first.request.Version.Config["tenant_marker"] != "one" {
		t.Fatalf("tenant one version = %#v", first.request.Version)
	}
	post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, "", http.StatusOK, nil)
	activate("two")
	post("/api/v1/admin/run", `{"app_id":"same-app","session_id":"session-two","input":"two"}`, "", http.StatusOK, nil)
	second := <-runs
	if second.request.Version == nil || second.request.Version.Config["tenant_marker"] != "two" {
		t.Fatalf("tenant two version = %#v", second.request.Version)
	}
}
