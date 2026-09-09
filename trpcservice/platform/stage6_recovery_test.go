package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type unavailableHealthStore struct{ DataStore }

func (s *unavailableHealthStore) Health(context.Context) BackendHealth {
	return BackendHealth{Backend: "postgres", Status: "unavailable", Checked: time.Now().UTC()}
}

type slowListStore struct{ DataStore }

func (s *slowListStore) ListSessionEvents(ctx context.Context, tenantID, sessionID string, after uint64) ([]SessionEvent, error) {
	select {
	case <-time.After(3 * storageOperationTimeout):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.DataStore.ListSessionEvents(ctx, tenantID, sessionID, after)
}

func TestRuntimeStatusReportsDependencyHealthWithoutDiagnostics(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleTenantAdmin}}})
	defer handler.Close()
	handler.ConfigureDataStore(&unavailableHealthStore{DataStore: NewInMemoryStore()})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/runtime/status", nil)
	request = request.WithContext(WithTenantContext(request.Context(), TenantContext{TenantID: "tenant-one", Role: RoleTenantAdmin}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Items []RuntimeComponentStatus `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	var dependency *RuntimeComponentStatus
	for index := range result.Items {
		if result.Items[index].Role == ComponentDependency {
			dependency = &result.Items[index]
		}
	}
	if dependency == nil || dependency.Available || dependency.Lifecycle != LifecycleUnavailable || strings.Contains(response.Body.String(), "connection") {
		t.Fatalf("dependency status=%#v body=%s", dependency, response.Body.String())
	}
}

func TestStorageTimeoutUsesBoundedContextAndStableError(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleTenantAdmin}}})
	defer handler.Close()
	handler.ConfigureDataStore(&slowListStore{DataStore: NewInMemoryStore()})
	if _, err := handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	_, err := handler.startChatRun(chatRunOptions{
		tenant: TenantContext{TenantID: "tenant-one", Role: RoleOperator, UserID: "operator"},
		appID:  "app-one", sessionID: "session-one", input: "hello", requestID: "request-storage-timeout",
	})
	if err == nil || err.Error() != "storage_timeout" {
		t.Fatalf("storage timeout error=%v", err)
	}
}

func TestServerOwnedRuntimeTimeoutProducesOneCancelledTerminalEvent(t *testing.T) {
	runner := &chatBlockingRunner{started: make(chan struct{}, 1), once: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	if _, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", RuntimeTimeoutMS: 100,
	}); err != nil {
		t.Fatal(err)
	}
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-timeout"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.cancelled"); err != nil {
		t.Fatal(err)
	}
	events := chatEventsForTest(t, client, "session-one")
	terminalCount := 0
	for _, event := range events {
		if event.IdempotencyKey == "request-timeout:terminal" {
			terminalCount++
		}
	}
	if terminalCount != 1 {
		t.Fatalf("terminal events=%d, want 1", terminalCount)
	}
}

func TestDeploymentGrayRoutingIsDeterministicAndTenantScoped(t *testing.T) {
	store := activeTestPlatform(t)
	deployment, _, _ := store.deployment(context.Background(), "tenant-one", "deploy-one")
	second, _, ok, _ := store.createVersion(context.Background(), deployment, "runtime-version-2", map[string]any{"model": "fake-v2"})
	if !ok {
		t.Fatal("create second version")
	}
	if _, _, ok, _ := store.startRollout(context.Background(), deployment, second.ID, 50); !ok {
		t.Fatal("start rollout")
	}
	requests := make(chan RunnerRequest, 32)
	runtime := NewRuntime(store, capturingRunner{request: requests}, nil)
	for index := 0; index < 20; index++ {
		_, err := runtime.Handle(context.Background(), TenantContext{TenantID: "tenant-one", Role: RoleOperator}, GatewayRequest{
			AppID: "app-one", SessionID: "session", Input: "hello", RequestID: fmt.Sprintf("request-%d", index),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	versions := map[string]bool{}
	for index := 0; index < 20; index++ {
		versions[(<-requests).VersionID] = true
	}
	if len(versions) != 2 || !versions["deploy-one-v1"] || !versions[second.ID] {
		t.Fatalf("gray versions=%#v", versions)
	}
}

func TestDeploymentRolloutAndRollbackRequireConfirmationAndAudit(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleTenantAdmin}}})
	defer handler.Close()
	deployment, _, _ := handler.platform.deployment(context.Background(), "tenant-one", "deploy-one")
	second, _, ok, _ := handler.platform.createVersion(context.Background(), deployment, "rollout-version-2", map[string]any{"model": "fake-v2"})
	if !ok {
		t.Fatal("create second version")
	}
	post := func(path, body string, role Role) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		request = request.WithContext(WithTenantContext(request.Context(), TenantContext{TenantID: "tenant-one", Role: role, UserID: "operator"}))
		request.Header.Set("X-Request-ID", "rollout-request")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := post("/api/v1/admin/deployments/deploy-one/rollout", fmt.Sprintf(`{"target_version_id":%q,"gray_percentage":50,"confirm":false}`, second.ID), RoleOperator); response.Code != http.StatusBadRequest {
		t.Fatalf("unconfirmed rollout=%d %s", response.Code, response.Body.String())
	}
	viewerHandler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{ID: "viewer", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleViewer}}})
	defer viewerHandler.Close()
	viewerRequest := httptest.NewRequest(http.MethodPost, "/api/v1/admin/deployments/deploy-one/rollout", strings.NewReader(fmt.Sprintf(`{"target_version_id":%q,"gray_percentage":50,"confirm":true}`, second.ID)))
	viewerResponse := httptest.NewRecorder()
	viewerHandler.ServeHTTP(viewerResponse, viewerRequest)
	if viewerResponse.Code != http.StatusForbidden {
		t.Fatalf("viewer rollout=%d %s", viewerResponse.Code, viewerResponse.Body.String())
	}
	response := post("/api/v1/admin/deployments/deploy-one/rollout", fmt.Sprintf(`{"target_version_id":%q,"gray_percentage":50,"confirm":true}`, second.ID), RoleOperator)
	if response.Code != http.StatusOK {
		t.Fatalf("rollout=%d %s", response.Code, response.Body.String())
	}
	var updated Deployment
	if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.RolloutStatus != DeploymentRolloutInProgress || updated.TargetVersionID != second.ID || updated.CurrentVersionID != "deploy-one-v1" || updated.GrayPercentage != 50 {
		t.Fatalf("rollout=%#v", updated)
	}
	previewRequest := httptest.NewRequest(http.MethodGet, "/api/v1/admin/deployments/deploy-one/rollback-preview", nil)
	previewRequest = previewRequest.WithContext(WithTenantContext(previewRequest.Context(), TenantContext{TenantID: "tenant-one", Role: RoleOperator}))
	previewResponse := httptest.NewRecorder()
	handler.ServeHTTP(previewResponse, previewRequest)
	if previewResponse.Code != http.StatusOK {
		t.Fatalf("preview=%d %s", previewResponse.Code, previewResponse.Body.String())
	}
	if response := post("/api/v1/admin/deployments/deploy-one/rollback", `{"confirm":true}`, RoleOperator); response.Code != http.StatusOK {
		t.Fatalf("rollback=%d %s", response.Code, response.Body.String())
	}
	audits := handler.governance.AuditEvents(AuditQuery{TenantID: "tenant-one", RequestID: "rollout-request"})
	decisions := map[string]bool{}
	for _, audit := range audits {
		decisions[audit.Decision] = true
	}
	if !decisions["deployment.rollout.updated"] || !decisions["deployment.rollback.confirmed"] {
		t.Fatalf("audit decisions=%#v", decisions)
	}
}

func TestCapacityRunIsBoundedDeterministicAndTenantScoped(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	var result CapacityTestResult
	client.post("/api/v1/admin/capacity", `{"agent_app_id":"app-one","concurrency":2,"runs":4,"timeout_ms":1000}`, nil, http.StatusAccepted, &result)
	if result.Status != "running" || result.TenantID != "tenant-one" || result.TraceID == "" {
		t.Fatalf("initial result=%#v", result)
	}

	var completed CapacityTestResult
	deadline := time.Now().Add(2 * time.Second)
	for {
		response := client.do(http.MethodGet, "/api/v1/admin/capacity/"+result.ID, "", nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("capacity result=%d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&completed); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		if completed.Status != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity run did not finish: %#v", completed)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if completed.Status != "completed" || completed.Completed != 4 || completed.Failed != 0 || completed.EstimatedTokens != 0 || completed.FirstBottleneck == "" {
		t.Fatalf("capacity result=%#v", completed)
	}
	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response := client.do(http.MethodGet, "/api/v1/admin/capacity/"+result.ID, "", nil)
	assertChannelAPIError(t, response, http.StatusNotFound, "capacity_run_not_found")
}

func TestCapacityPlanReportsNodeTokenAndBackendDemand(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	var started CapacityTestResult
	client.post("/api/v1/admin/capacity", `{
		"agent_app_id":"app-one",
		"concurrency":4,
		"runs":8,
		"timeout_ms":1000,
		"peak_im_callbacks_per_second":120,
		"average_tokens_per_session":800,
		"redis_operations_per_session":6,
		"sql_operations_per_session":4,
		"headroom_percent":25
	}`, nil, http.StatusAccepted, &started)

	var completed CapacityTestResult
	deadline := time.Now().Add(2 * time.Second)
	for {
		response := client.do(http.MethodGet, "/api/v1/admin/capacity/"+started.ID, "", nil)
		if err := json.NewDecoder(response.Body).Decode(&completed); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		if completed.Status != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity plan did not finish: %#v", completed)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if completed.Status != "completed" || completed.SessionsPerNode < 1 || completed.RecommendedWorkerNodes < 1 {
		t.Fatalf("capacity node plan = %#v", completed)
	}
	if completed.AverageTokensPerSession != 800 || completed.TokenThroughputPerSecond != 96000 {
		t.Fatalf("capacity token plan = %#v", completed)
	}
	if completed.IMCallbackPeakQPS != 120 || completed.RedisQPS != 720 || completed.SQLQPS != 480 || completed.HeadroomPercent != 25 {
		t.Fatalf("capacity backend plan = %#v", completed)
	}
}

func TestCapacityRunIsValidatedAndCancelledWithoutActiveWork(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	invalid := client.do(http.MethodPost, "/api/v1/admin/capacity", `{"agent_app_id":"app-one","concurrency":11,"runs":101,"timeout_ms":99}`, nil)
	assertChannelAPIError(t, invalid, http.StatusBadRequest, "invalid_capacity_request")

	var result CapacityTestResult
	client.post("/api/v1/admin/capacity", `{"agent_app_id":"app-one","concurrency":1,"runs":100,"timeout_ms":5000}`, nil, http.StatusAccepted, &result)
	client.post("/api/v1/admin/capacity/"+result.ID+"/cancel", "", nil, http.StatusAccepted, nil)

	var cancelled CapacityTestResult
	deadline := time.Now().Add(2 * time.Second)
	for {
		response := client.do(http.MethodGet, "/api/v1/admin/capacity/"+result.ID, "", nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("cancelled capacity result=%d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&cancelled); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		if cancelled.Status != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity run was not cancelled: %#v", cancelled)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cancelled.Status != "cancelled" || cancelled.Active != 0 || cancelled.Completed+cancelled.Failed > 100 ||
		cancelled.Concurrency > maxCapacityConcurrency || cancelled.Runs > maxCapacityRuns {
		t.Fatalf("cancelled capacity result=%#v", cancelled)
	}
}

func TestCapacityRunRespectsGovernanceResourceExhaustion(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	if _, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", RateLimit: 1, RateWindowSeconds: 60,
	}); err != nil {
		t.Fatal(err)
	}
	client.post("/api/v1/admin/capacity", `{"agent_app_id":"app-one","concurrency":1,"runs":1,"timeout_ms":1000}`, nil, http.StatusAccepted, nil)
	response := client.do(http.MethodPost, "/api/v1/admin/capacity", `{"agent_app_id":"app-one","concurrency":1,"runs":1,"timeout_ms":1000}`, nil)
	assertChannelAPIError(t, response, http.StatusTooManyRequests, "tenant_rate_limited")
}

func TestRollbackDoesNotCancelInFlightVersionExecution(t *testing.T) {
	runner := &blockingRunner{entered: make(chan string, 1), release: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")

	var second DeploymentVersion
	client.post("/api/v1/admin/deployments/deploy-one/versions", `{"config":{"runner":"fake-v2"}}`, map[string]string{"Idempotency-Key": "rollback-inflight"}, http.StatusCreated, &second)
	client.post("/api/v1/admin/deployments/deploy-one/rollout", fmt.Sprintf(`{"target_version_id":%q,"gray_percentage":100,"confirm":true}`, second.ID), nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-rollback-inflight"}, http.StatusAccepted, nil)

	select {
	case <-runner.entered:
	case <-time.After(time.Second):
		t.Fatal("active run did not start")
	}

	client.post("/api/v1/admin/deployments/deploy-one/rollback", `{"confirm":true}`, nil, http.StatusOK, nil)
	runner.release <- struct{}{}
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeFaultInjectionIsDevelopmentOnlyAndStable(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/admin/operations/faults", `{"agent_app_id":"app-one","scenario":"tool_error","delay_ms":0}`, nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-tool-error"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.failed"); err != nil {
		t.Fatal(err)
	}
	client.handler.ConfigureFaultInjection(false)
	response := client.do(http.MethodPost, "/api/v1/admin/operations/faults", `{"agent_app_id":"app-one","scenario":"none","delay_ms":0}`, nil)
	assertChannelAPIError(t, response, http.StatusForbidden, "fault_injection_disabled")
}

func TestProductionModeAllowsChannelBindingsButBlocksFaultInjection(t *testing.T) {
	provider := NewJWTIdentityProvider(JWTIdentityConfig{
		Issuer: "issuer", Audience: "audience", HMACSecret: []byte("secret"),
	}, map[string]DevelopmentIdentity{
		"operator": {ID: "operator", Name: "Operator", Assignments: []TenantAssignment{
			{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
		}},
	})
	client := newChannelTestClient(t, EchoRunner{})
	client.handler.ConfigureIdentityProvider(provider)
	token := signTestJWT(t, []byte("secret"), map[string]any{
		"iss": "issuer", "aud": "audience", "sub": "operator", "exp": time.Now().Add(time.Hour).Unix(),
	})
	client.post("/api/v1/auth/login", `{"token":"`+token+`"}`, nil, http.StatusOK, nil)
	client.activateApp("app-one", "deploy-one")

	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"user","external_user_id":"user"}`, nil, http.StatusCreated, &binding)
	if binding.ID == "" || binding.SessionID == "" {
		t.Fatalf("binding = %#v", binding)
	}

	mockFaults := client.do(http.MethodPost, "/api/v1/chat/mock/faults", `{"scenario":"message_length"}`, nil)
	assertChannelAPIError(t, mockFaults, http.StatusForbidden, "fault_injection_disabled")
	runtimeFaults := client.do(http.MethodPost, "/api/v1/admin/operations/faults", `{"agent_app_id":"app-one","scenario":"tool_error","delay_ms":0}`, nil)
	assertChannelAPIError(t, runtimeFaults, http.StatusForbidden, "fault_injection_disabled")
}

func TestDeploymentHighRiskOperationsFailClosedWhenAuditCannotPersist(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}},
	})
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	handler.governance.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	deployment, _, _ := handler.platform.deployment(context.Background(), "tenant-one", "deploy-one")
	second, _, ok, _ := handler.platform.createVersion(context.Background(), deployment, "audit-failure-v2", map[string]any{"model": "fake-v2"})
	if !ok {
		t.Fatal("create second version")
	}

	post := func(path, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		request = request.WithContext(WithTenantContext(request.Context(), TenantContext{
			TenantID: "tenant-one", Role: RoleOperator, UserID: "operator",
		}))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	rolloutResponse := post("/api/v1/admin/deployments/deploy-one/rollout", fmt.Sprintf(`{"target_version_id":%q,"gray_percentage":50,"confirm":true}`, second.ID))
	assertChannelAPIError(t, rolloutResponse.Result(), http.StatusServiceUnavailable, "audit_unavailable")
	current, _, _ := handler.platform.deployment(context.Background(), "tenant-one", "deploy-one")
	if current.VersionID != "deploy-one-v1" || current.TargetVersionID != "" || current.GrayPercentage != 0 {
		t.Fatalf("rollout changed after audit failure: %#v", current)
	}

	if _, _, ok, _ := handler.platform.startRollout(context.Background(), deployment, second.ID, 50); !ok {
		t.Fatal("prepare rollback state")
	}
	rollbackResponse := post("/api/v1/admin/deployments/deploy-one/rollback", `{"confirm":true}`)
	assertChannelAPIError(t, rollbackResponse.Result(), http.StatusServiceUnavailable, "audit_unavailable")
	current, _, _ = handler.platform.deployment(context.Background(), "tenant-one", "deploy-one")
	if current.VersionID != "deploy-one-v1" || current.TargetVersionID != second.ID || current.GrayPercentage != 50 {
		t.Fatalf("rollback changed after audit failure: %#v", current)
	}
}
