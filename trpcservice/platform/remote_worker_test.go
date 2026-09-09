package platform

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
)

func TestWorkerServerCloseIsIdempotent(t *testing.T) {
	server := NewWorkerServer(WorkerServerConfig{})
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteWorkerPreservesResolvedIdentityTraceAndVersion(t *testing.T) {
	requests := make(chan RunnerRequest, 1)
	versions := make(chan DeploymentVersion, 1)
	server := httptest.NewServer(NewWorkerServer(WorkerServerConfig{
		Token: "worker-secret",
		BeforeRun: func(request RunnerRequest, version DeploymentVersion) {
			requests <- request
			versions <- version
		},
	}))
	defer server.Close()

	runtime := NewRuntime(activeTestPlatform(t), NewRemoteRunnerAdapter(server.URL, "worker-secret"), lifecycle.New())
	events, err := runtime.Stream(context.Background(), TenantContext{TenantID: "tenant-one", UserID: "user-one", Role: RoleOperator}, GatewayRequest{
		AppID: "app-one", SessionID: "session-one", Input: "hello", RequestID: "request-one", TraceID: "trace-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	for event := range events {
		if event.Type == "run.failed" {
			t.Fatalf("worker event failed: %#v", event)
		}
	}

	select {
	case request := <-requests:
		if request.TenantID != "tenant-one" || request.AppID != "app-one" || request.SessionID != "session-one" ||
			request.RequestID != "request-one" || request.TraceID != "trace-one" || request.VersionID != "deploy-one-v1" {
			t.Fatalf("worker request = %#v", request)
		}
		if !validTraceParent(request.TraceParent) {
			t.Fatalf("traceparent = %q", request.TraceParent)
		}
	case <-time.After(time.Second):
		t.Fatal("worker request was not captured")
	}
	select {
	case version := <-versions:
		if version.ID != "deploy-one-v1" || version.Config["model"] != "fake" {
			t.Fatalf("worker version = %#v", version)
		}
	case <-time.After(time.Second):
		t.Fatal("worker version was not captured")
	}
}

func TestExecutionManifestRejectsTamperingExpiryAndUnknownKey(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	request := RunnerRequest{TenantID: "tenant-one", AppID: "app-one", SessionID: "session-one", RequestID: "request-one", TraceID: "0123456789abcdef0123456789abcdef", TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
	token, err := signExecutionManifest(request, "current", []byte("manifest-secret"), now)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	tamperedPayload := "A" + parts[1][1:]
	if parts[1][0] == 'A' {
		tamperedPayload = "B" + parts[1][1:]
	}
	tampered := parts[0] + "." + tamperedPayload + "." + parts[2]
	if _, err := verifyExecutionManifest(tampered, map[string][]byte{"current": []byte("manifest-secret")}, now); err == nil {
		t.Fatal("tampered manifest was accepted")
	}
	if _, err := verifyExecutionManifest(token, map[string][]byte{"next": []byte("manifest-secret")}, now); err == nil {
		t.Fatal("unknown key was accepted")
	}
	expired, err := signExecutionManifest(request, "old", []byte("old-manifest-secret"), now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyExecutionManifest(expired, map[string][]byte{"old": []byte("old-manifest-secret"), "current": []byte("manifest-secret")}, now); err == nil {
		t.Fatal("expired manifest was accepted")
	}
}

func TestRemoteToolGovernanceCreatesSharedConfirmationAndFailsClosed(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureInternalGovernance("governance-secret")
	policy, err := handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := GovernanceRequest{TenantID: "tenant-one", AgentAppID: "app-one", UserID: "user-one", SessionID: "session-one", RequestID: "request-governed", RequiredTools: []string{"deploy"}, PolicyRevision: policy.Revision}
	result, err := handler.governance.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	client := NewRemoteToolGovernance(server.URL, "governance-secret")
	err = client.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"production"}`))
	var governanceErr *GovernanceError
	if !errors.As(err, &governanceErr) || governanceErr.Code != "confirmation_required" || governanceErr.ConfirmationID == "" {
		t.Fatalf("authorization error = %#v", err)
	}
	confirmations := handler.governance.Confirmations("tenant-one")
	if len(confirmations) != 1 || confirmations[0].ID != governanceErr.ConfirmationID {
		t.Fatalf("shared confirmations = %#v", confirmations)
	}
	server.Close()
	err = client.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", nil)
	if !IsGovernanceError(err, "governance_unavailable") {
		t.Fatalf("outage error = %v", err)
	}
}

func TestRemoteWorkerUnavailableAndRestartRecoveryUseStableErrors(t *testing.T) {
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			writeError(w, http.StatusServiceUnavailable, "worker_unavailable", "worker unavailable")
			return
		}
		NewWorkerServer(WorkerServerConfig{Token: "worker-secret"}).ServeHTTP(w, r)
	}))
	defer server.Close()

	runtime := NewRuntime(activeTestPlatform(t), NewRemoteRunnerAdapter(server.URL, "worker-secret"), lifecycle.New())
	tenant := TenantContext{TenantID: "tenant-one", UserID: "user-one", Role: RoleOperator}
	unavailable.Store(true)
	_, err := runtime.Stream(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "session-one", Input: "hello"})
	var runtimeErr *runtimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.code != "worker_unavailable" {
		t.Fatalf("unavailable error = %v", err)
	}

	unavailable.Store(false)
	events, err := runtime.Stream(context.Background(), tenant, GatewayRequest{AppID: "app-one", SessionID: "session-one", Input: "hello", RequestID: "request-two", TraceID: "trace-two"})
	if err != nil {
		t.Fatal(err)
	}
	for event := range events {
		if event.Type == "run.failed" {
			t.Fatalf("restarted worker failed: %#v", event)
		}
	}
}

func TestRemoteRunnerUnexpectedEOFEmitsFailedTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"Type\":\"message.delta\",\"Data\":{\"delta\":\"partial\"}}\n\n")
	}))
	defer server.Close()
	adapter := NewSignedRemoteRunnerAdapter(server.URL, "worker-secret", "current", []byte("manifest-secret"))
	events, err := adapter.RunEvents(context.Background(), RunnerRequest{
		TenantID: "tenant-one", AppID: "app-one", SessionID: "session-one", RequestID: "request-one",
		TraceID: "0123456789abcdef0123456789abcdef", TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		DeploymentID: "deploy-one", VersionID: "deploy-one-v1", Input: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	var collected []RuntimeEvent
	for event := range events {
		collected = append(collected, event)
	}
	if len(collected) != 2 || collected[1].Type != "run.failed" || collected[1].Data["error"] != "worker_unavailable" {
		t.Fatalf("unexpected EOF events = %#v", collected)
	}
}
