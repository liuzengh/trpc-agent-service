package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGovernanceManagementAPIIsTenantScopedAndRoleEnforced(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{
		{TenantID: "tenant-a", TenantName: "A", Role: RoleTenantAdmin},
		{TenantID: "tenant-b", TenantName: "B", Role: RoleViewer},
	}})
	defer server.Close()
	requireJSONResponse(t, client, server.URL+"/api/v1/admin/agent-apps", `{"id":"app-a","name":"App A"}`, "", http.StatusCreated, nil)

	var policy TenantPolicy
	requireJSONResponse(t, client, server.URL+"/api/v1/admin/governance/policy", `{
		"agent_app_id":"app-a","allowed_tools":["search"],"allowed_mcp":["calendar"],
		"dangerous_tools":[],"denied_input_patterns":["blocked"],"redacted_patterns":["secret"],
		"token_budget":100,"estimated_tokens_per_run":5,"rate_limit":10,"rate_window_seconds":60
	}`, "", http.StatusOK, &policy)
	if policy.TenantID != "tenant-a" || policy.Revision != 1 {
		t.Fatalf("policy = %#v", policy)
	}

	response, err := client.Get(server.URL + "/api/v1/admin/governance/policy?app_id=app-a")
	if err != nil {
		t.Fatal(err)
	}
	decodeResponse(t, response, http.StatusOK, &policy)

	var auditList struct {
		Items []AuditEvent `json:"items"`
	}
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/audit?decision=policy.updated")
	decodeResponse(t, response, http.StatusOK, &auditList)
	if len(auditList.Items) != 1 || auditList.Items[0].TenantID != "tenant-a" {
		t.Fatalf("audits = %#v", auditList.Items)
	}

	requireJSONResponse(t, client, server.URL+"/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-b"}`, "", http.StatusOK, new(identityResponse))
	response = postJSONWithKey(t, client, server.URL+"/api/v1/admin/governance/policy", `{"agent_app_id":"app-a"}`, "")
	assertAPIError(t, response, http.StatusForbidden, "forbidden")
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/policy?app_id=app-a")
	assertAPIError(t, response, http.StatusNotFound, "policy_not_found")
}

func TestGovernancePolicyTreatsRedactionPatternsAsWriteOnly(t *testing.T) {
	server, client := newDevelopmentClient(t, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleTenantAdmin}}})
	defer server.Close()
	requireJSONResponse(t, client, server.URL+"/api/v1/admin/agent-apps", `{"id":"app-a","name":"App A"}`, "", http.StatusCreated, nil)
	response := postJSONWithKey(t, client, server.URL+"/api/v1/admin/governance/policy", `{"agent_app_id":"app-a","redacted_patterns":["stage5-secret-canary"]}`, "")
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || bytes.Contains(data, []byte("stage5-secret-canary")) || !bytes.Contains(data, []byte("[REDACTED]")) {
		t.Fatalf("response = %d %s", response.StatusCode, data)
	}
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/policy?app_id=app-a")
	data, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || bytes.Contains(data, []byte("stage5-secret-canary")) {
		t.Fatalf("read response = %d %s", response.StatusCode, data)
	}
}

func TestBackendAuthorizationDenialProducesAuditEvent(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "viewer", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleViewer}}})
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	response, _ := client.Post(server.URL+"/api/v1/admin/tenants", "application/json", bytes.NewBufferString(`{"id":"tenant-x","name":"Tenant X"}`))
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d", response.StatusCode)
	}
	audits := handler.governance.AuditEvents(AuditQuery{TenantID: "tenant-a", Decision: "authorization.denied"})
	if len(audits) != 1 || audits[0].UserID != "viewer" {
		t.Fatalf("audits = %#v", audits)
	}
}

func TestSuccessfulMutationBecomesAuditUnavailableWhenAuditCannotPersist(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	handler.governance.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	response := postJSONWithKey(t, client, server.URL+"/api/v1/admin/tenants", `{"id":"tenant-new","name":"Tenant New"}`, "")
	assertAPIError(t, response, http.StatusServiceUnavailable, "audit_unavailable")
}

func TestAuthMeBecomesAuditUnavailableWhenAuditCannotPersist(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	handler.governance.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	response, err := client.Get(server.URL + "/api/v1/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	assertAPIError(t, response, http.StatusServiceUnavailable, "audit_unavailable")
}

func TestGovernancePolicyRunsBeforeChatRunnerAndPropagatesTrace(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	_, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", DeniedInputPatterns: []string{"blocked"}, RedactedPatterns: []string{"canary-secret"}, TokenBudget: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"blocked"}`, map[string]string{"X-Request-ID": "request-denied"})
	assertChannelAPIError(t, response, http.StatusForbidden, "policy_denied")
	select {
	case request := <-runs:
		t.Fatalf("denied input reached Runner: %#v", request)
	case <-time.After(30 * time.Millisecond):
	}

	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello canary-secret"}`, map[string]string{"X-Request-ID": "request-allowed"}, http.StatusAccepted, nil)
	select {
	case request := <-runs:
		if request.Input != "hello [REDACTED]" {
			t.Fatalf("Runner input = %q", request.Input)
		}
	case <-time.After(time.Second):
		t.Fatal("allowed input did not reach Runner")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		audits := client.handler.governance.AuditEvents(AuditQuery{TenantID: "tenant-one", RequestID: "request-allowed"})
		for _, audit := range audits {
			if audit.Decision != "run.completed" {
				continue
			}
			trace, found := client.handler.governance.Trace("tenant-one", audit.TraceID, "")
			if !found || trace.RequestID != "request-allowed" {
				t.Fatalf("trace = %#v, found = %v", trace, found)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("completion audit was not recorded")
}

func TestStorageStartFailureReleasesGovernanceReservation(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.handler.ConfigureDataStore(&listFailingStore{DataStore: NewInMemoryStore()})
	client.activateApp("app-one", "deploy-one")
	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", TokenBudget: 1, EstimatedTokensPerRun: 1,
	})
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-storage-failure"})
	assertChannelAPIError(t, response, http.StatusServiceUnavailable, "storage_unavailable")
	metrics := client.handler.governance.Metrics("tenant-one")
	if metrics.Active != 0 || metrics.Tokens != 0 || metrics.Failed != 1 {
		t.Fatalf("metrics after storage failure = %#v", metrics)
	}
}

type listFailingStore struct {
	DataStore
	calls int
}

func (s *listFailingStore) ListSessionEvents(ctx context.Context, tenantID, sessionID string, after uint64) ([]SessionEvent, error) {
	s.calls++
	if s.calls > 1 {
		return nil, errors.New("storage unavailable")
	}
	return s.DataStore.ListSessionEvents(ctx, tenantID, sessionID, after)
}

func TestOutputGuardrailBuffersSplitSecretBeforeSessionPersistence(t *testing.T) {
	client := newChannelTestClient(t, splitSecretRunner{})
	client.activateApp("app-one", "deploy-one")
	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", DeniedOutputPatterns: []string{"stage5-secret"},
	})
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-split"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	events := chatEventsForTest(t, client, "session-one")
	redacted := false
	for _, event := range events {
		if bytes.Contains(event.Payload, []byte("stage5-secret")) {
			t.Fatalf("secret persisted in Session Event: %s", event.Payload)
		}
		if bytes.Contains(event.Payload, []byte("[REDACTED]")) {
			redacted = true
		}
	}
	if !redacted {
		t.Fatalf("redacted output missing: %#v", events)
	}
}

type splitSecretRunner struct{}

func (splitSecretRunner) Run(context.Context, RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{Output: "stage5-secret"}, nil
}

func (splitSecretRunner) RunEvents(_ context.Context, request RunnerRequest) (<-chan RuntimeEvent, error) {
	events := make(chan RuntimeEvent, 4)
	events <- RuntimeEvent{Type: "message.delta", Data: map[string]string{"delta": "stage5-"}}
	events <- RuntimeEvent{Type: "message.delta", Data: map[string]string{"delta": "secret"}}
	events <- RuntimeEvent{Type: "message.completed", Data: map[string]string{"output": "stage5-secret"}}
	events <- RuntimeEvent{Type: "run.completed", Data: map[string]string{"request_id": request.RequestID}}
	close(events)
	return events, nil
}

func (splitSecretRunner) Close() error { return nil }

func TestExternalIMUserPolicyDeniesBeforeRunner(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", AllowedIMUsers: []string{"allowed-user"},
	})
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", body); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("provider error = %v", err)
	}
	select {
	case request := <-runs:
		t.Fatalf("denied provider message reached Runner: %#v", request)
	case <-time.After(30 * time.Millisecond):
	}
	deliveries := runtime.Deliveries("tenant-one")
	if len(deliveries) != 1 || deliveries[0].Code != "im_user_denied" || deliveries[0].Status != "rejected" {
		t.Fatalf("deliveries = %#v", deliveries)
	}
}

func TestProviderRouteAccountAndMissingPolicyFailClosedBeforeRunner(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClientWithoutPolicy(t, requestCapturingRunner{requests: runs}, DevelopmentIdentity{
		ID:   "developer",
		Name: "Developer",
		Assignments: []TenantAssignment{
			{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
		},
	})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ProviderAccount: "bot-a", ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)

	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot-b", body); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("provider account error = %v", err)
	}
	deliveries := runtime.Deliveries("tenant-one")
	if len(deliveries) != 1 || deliveries[0].Code != "provider_account_denied" || deliveries[0].Status != "rejected" {
		t.Fatalf("provider account deliveries = %#v", deliveries)
	}

	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot-a", body); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("missing-policy error = %v", err)
	}
	deliveries = runtime.Deliveries("tenant-one")
	if len(deliveries) != 1 || deliveries[0].Code != "policy_unavailable" || deliveries[0].Status != "rejected" {
		t.Fatalf("missing-policy deliveries = %#v", deliveries)
	}
	select {
	case request := <-runs:
		t.Fatalf("closed request reached Runner: %#v", request)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestDeploymentToolPolicyGatesDeclarationsWithoutPreflightConfirmation(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.post("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/deployments", `{"id":"deploy-one","agent_app_id":"app-one"}`, nil, http.StatusCreated, nil)
	var version DeploymentVersion
	client.post("/api/v1/admin/deployments/deploy-one/versions", `{"config":{"runner":"mock","tools":["deploy"],"mcp":["calendar"]}}`, map[string]string{"Idempotency-Key": "governance-tools"}, http.StatusCreated, &version)
	client.post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"published","version_id":"`+version.ID+`"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"active"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"search"}, AllowedMCP: []string{"calendar"}})
	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"ship"}`, map[string]string{"X-Request-ID": "request-denied-tool"})
	assertChannelAPIError(t, response, http.StatusForbidden, "tool_not_allowed")

	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"deploy"}, AllowedMCP: []string{"calendar"}, DangerousTools: []string{"deploy"}})
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"ship"}`, map[string]string{"X-Request-ID": "request-confirm"}, http.StatusAccepted, nil)
	select {
	case request := <-runs:
		if request.TraceID == "" || request.Input != "ship" {
			t.Fatalf("Runner request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("allowed Tool declaration did not reach Runner")
	}
	select {
	case request := <-runs:
		t.Fatalf("Tool request reached Runner more than once: %#v", request)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestGovernanceConfirmationMetricsAndTraceAPIs(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-a", TenantName: "A", Role: RoleOperator}}})
	_, _ = handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", UserID: "operator", SessionID: "session-a", RequestID: "request-a", Input: "ship", RequiredTools: []string{"deploy"}}
	result, _ := handler.governance.Evaluate(context.Background(), request)
	var pendingErr *GovernanceError
	if err := handler.governance.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"production"}`)); !errors.As(err, &pendingErr) || pendingErr.ConfirmationID == "" {
		t.Fatalf("pending Tool confirmation error = %v", err)
	}
	server, client := newHandlerClient(t, handler)
	defer server.Close()

	var confirmations struct {
		Items []ToolConfirmation `json:"items"`
	}
	response, _ := client.Get(server.URL + "/api/v1/admin/governance/confirmations")
	decodeResponse(t, response, http.StatusOK, &confirmations)
	if len(confirmations.Items) != 1 || confirmations.Items[0].ID != pendingErr.ConfirmationID {
		t.Fatalf("confirmations = %#v", confirmations.Items)
	}
	var decided ToolConfirmation
	requireJSONResponse(t, client, server.URL+"/api/v1/admin/governance/confirmations/"+pendingErr.ConfirmationID+"/decision", `{"approve":true}`, "", http.StatusOK, &decided)
	if decided.Status != ConfirmationApproved {
		t.Fatalf("decision = %#v", decided)
	}
	store, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.ListSessionEvents(context.Background(), "tenant-a", "session-a", 0)
	release()
	if err != nil {
		t.Fatal(err)
	}
	foundConfirmationEvent := false
	for _, event := range events {
		if event.Type == "tool.confirmation.approved" && event.IdempotencyKey == "request-a:confirmation-approved" {
			foundConfirmationEvent = true
			break
		}
	}
	if !foundConfirmationEvent {
		t.Fatalf("approved confirmation Session Event missing: %#v", events)
	}

	var metrics TenantMetrics
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/metrics")
	decodeResponse(t, response, http.StatusOK, &metrics)
	if metrics.Requests == 0 {
		t.Fatalf("metrics = %#v", metrics)
	}
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/metrics?from=2026-01-01T00:00:00Z&to=2026-03-15T00:00:00Z")
	assertAPIError(t, response, http.StatusBadRequest, "invalid_metrics_query")
	var trace PlatformTrace
	response, _ = client.Get(server.URL + "/api/v1/admin/governance/traces?trace_id=" + result.TraceID)
	decodeResponse(t, response, http.StatusOK, &trace)
	if trace.TraceID != result.TraceID || len(trace.Spans) == 0 {
		t.Fatalf("trace = %#v", trace)
	}
	for index, span := range trace.Spans {
		if span.SpanID == "" || index > 0 && span.ParentSpanID != trace.Spans[index-1].SpanID {
			t.Fatalf("trace parent chain = %#v", trace.Spans)
		}
	}

	encoded, _ := json.Marshal(confirmations)
	if string(encoded) == "" {
		t.Fatal("confirmation response was not serializable")
	}
}

func TestInternalPrometheusMetricsRequireAuthenticationAndExposeTenantCounters(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.handler.ConfigureInternalGovernance("metrics-secret")
	if _, err := client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	result, err := client.handler.governance.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-one", AgentAppID: "app-one", RequestID: "request-one", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.handler.governance.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-one", AgentAppID: "app-one", RequestID: "request-one", Tokens: 4}); err != nil {
		t.Fatal(err)
	}
	unauthorized, err := client.client.Get(client.server.URL + "/internal/metrics")
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized metrics status = %d", unauthorized.StatusCode)
	}
	request, _ := http.NewRequest(http.MethodGet, client.server.URL+"/internal/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	response, err := client.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `trpc_agent_requests_total{tenant_id="tenant-one"} 1`) || !strings.Contains(string(body), `trpc_agent_tokens_total{tenant_id="tenant-one"} 4`) {
		t.Fatalf("metrics response = %d %s (trace %s)", response.StatusCode, body, result.TraceID)
	}
}

func TestConfirmationDecisionWaitsForPendingChatRunToExit(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleOperator}}})
	defer handler.Close()
	_, _ = handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
	})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", UserID: "operator", SessionID: "session-a", RequestID: "request-a", Input: "ship"}
	result, err := handler.governance.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.governance.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"production"}`))
	var pending *GovernanceError
	if !errors.As(err, &pending) {
		t.Fatalf("pending confirmation error = %v", err)
	}
	done := make(chan struct{})
	handler.chatMu.Lock()
	handler.activeRuns[chatRunKey("tenant-a", "session-a", "request-a")] = activeChatRun{input: "ship", done: done}
	handler.chatMu.Unlock()
	server, client := newHandlerClient(t, handler)
	defer server.Close()

	type responseResult struct {
		response *http.Response
		err      error
	}
	responseDone := make(chan responseResult, 1)
	go func() {
		httpRequest, requestErr := http.NewRequest(http.MethodPost, server.URL+"/api/v1/admin/governance/confirmations/"+pending.ConfirmationID+"/decision", strings.NewReader(`{"approve":false}`))
		if requestErr == nil {
			httpRequest.Header.Set("Content-Type", "application/json")
		}
		if requestErr != nil {
			responseDone <- responseResult{err: requestErr}
			return
		}
		response, requestErr := client.Do(httpRequest)
		responseDone <- responseResult{response: response, err: requestErr}
	}()
	select {
	case result := <-responseDone:
		if result.response != nil {
			result.response.Body.Close()
		}
		t.Fatal("decision returned before the pending run exited")
	case <-time.After(30 * time.Millisecond):
	}
	close(done)
	decisionResult := <-responseDone
	if decisionResult.err != nil {
		t.Fatal(decisionResult.err)
	}
	decodeResponse(t, decisionResult.response, http.StatusOK, &ToolConfirmation{})
	store, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.ListSessionEvents(context.Background(), "tenant-a", "session-a", 0)
	release()
	if err != nil {
		t.Fatal(err)
	}
	terminal := chatTerminalEvent(events, "request-a")
	if terminal == nil || terminal.Type != "run.failed" {
		t.Fatalf("rejected confirmation terminal = %#v", terminal)
	}
	metrics := handler.governance.Metrics("tenant-a")
	if metrics.Active != 0 || metrics.Failed != 1 {
		t.Fatalf("rejected confirmation metrics = %#v", metrics)
	}
}

func TestConfirmationListReconcilesExpiryAndPersistsSessionEvent(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-a", TenantName: "A", Role: RoleOperator}}})
	handler.governance.now = func() time.Time { return now }
	_, _ = handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", UserID: "operator", SessionID: "session-a", RequestID: "request-a"}
	result, err := handler.governance.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.governance.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"stage5"}`)); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("pending confirmation = %v", err)
	}
	now = now.Add(16 * time.Minute)
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	var listed struct {
		Items []ToolConfirmation `json:"items"`
	}
	response, err := client.Get(server.URL + "/api/v1/admin/governance/confirmations")
	if err != nil {
		t.Fatal(err)
	}
	decodeResponse(t, response, http.StatusOK, &listed)
	if len(listed.Items) != 1 || listed.Items[0].Status != ConfirmationExpired {
		t.Fatalf("expired confirmations = %#v", listed.Items)
	}
	store, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.ListSessionEvents(context.Background(), "tenant-a", "session-a", 0)
	release()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type == "tool.confirmation.expired" && event.IdempotencyKey == "request-a:confirmation-expired" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expired confirmation Session Event missing: %#v", events)
	}
}

func newHandlerClient(t *testing.T, handler *AdminHandler) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewServer(handler)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return server, &http.Client{Jar: jar}
}
