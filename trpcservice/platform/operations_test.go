package platform

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
)

func TestOperationsDrainTracksActiveChatStreamWork(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}},
	})
	life := lifecycle.New()
	runner := &blockingRunner{entered: make(chan string, 1), release: make(chan struct{})}
	handler.ConfigureRuntime(runner, life)
	if _, err := handler.governance.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-one", AgentAppID: "app-one",
	}); err != nil {
		t.Fatal(err)
	}
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	authenticateTestClient(t, client, server.URL)
	controlClient := &http.Client{Jar: client.Jar}

	response := mustPost(t, client, server.URL+"/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("session status = %d", response.StatusCode)
	}
	response.Body.Close()
	response = mustPost(t, client, server.URL+"/api/v1/chat/sessions/session-one/messages", `{"input":"active"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("message status = %d", response.StatusCode)
	}
	response.Body.Close()
	select {
	case <-runner.entered:
	case <-time.After(time.Second):
		t.Fatal("chat stream run did not start")
	}

	var status DrainStatus
	decodeTestJSON(t, mustGet(t, controlClient, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainIdle || status.ActiveExecutions != 1 {
		t.Fatalf("active chat stream status = %#v", status)
	}
	assertAPIError(t, mustPost(t, controlClient, server.URL+"/api/v1/admin/operations/drain", `{"confirm":true}`), http.StatusAccepted, "")
	decodeTestJSON(t, mustGet(t, controlClient, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainDraining || status.ActiveExecutions != 1 {
		t.Fatalf("draining chat stream status = %#v", status)
	}

	runner.release <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	for {
		decodeTestJSON(t, mustGet(t, controlClient, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
		if status.State == DrainClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("chat stream drain did not close: %#v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOperationsDrainFailsClosedWhenAuditCannotPersist(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}},
	})
	handler.ConfigureRuntime(EchoRunner{}, lifecycle.New())
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	authenticateTestClient(t, client, server.URL)
	handler.governance.SetPersistencePath(filepath.Join(blocker, "governance.json"))

	assertAPIError(t, mustPost(t, client, server.URL+"/api/v1/admin/operations/drain", `{"confirm":true}`), http.StatusServiceUnavailable, "audit_unavailable")
	var status DrainStatus
	decodeTestJSON(t, mustGet(t, client, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainIdle {
		t.Fatalf("drain state after audit failure = %#v", status)
	}
}

func TestOperationsDrainRequiresConfirmationAndRecordsAudit(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}},
	})
	handler.ConfigureRuntime(EchoRunner{}, lifecycle.New())
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	authenticateTestClient(t, client, server.URL)

	var status DrainStatus
	decodeTestJSON(t, mustGet(t, client, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainIdle {
		t.Fatalf("initial status = %#v", status)
	}
	assertAPIError(t, mustPost(t, client, server.URL+"/api/v1/admin/operations/drain", `{"confirm":false}`), http.StatusBadRequest, "drain_confirmation_required")
	assertAPIError(t, mustPost(t, client, server.URL+"/api/v1/admin/operations/drain", `{"confirm":true}`), http.StatusAccepted, "")

	decodeTestJSON(t, mustGet(t, client, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainClosed {
		t.Fatalf("draining status = %#v", status)
	}

	var audits struct {
		Items []AuditEvent `json:"items"`
	}
	decodeTestJSON(t, mustGet(t, client, server.URL+"/api/v1/admin/governance/audit?decision=operations.drain.started"), http.StatusOK, &audits)
	if len(audits.Items) != 1 || audits.Items[0].RequestID == "" || audits.Items[0].TraceID == "" {
		t.Fatalf("drain audits = %#v", audits.Items)
	}
}

func TestOperationsDrainRejectsNewWorkAndClosesAfterActiveWork(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}},
	})
	life := lifecycle.New()
	runner := &blockingRunner{entered: make(chan string, 1), release: make(chan struct{})}
	handler.ConfigureRuntime(runner, life)
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	authenticateTestClient(t, client, server.URL)
	controlClient := &http.Client{Jar: client.Jar}

	runDone := make(chan error, 1)
	go func() {
		response, err := client.Post(server.URL+"/api/v1/admin/run", "application/json", strings.NewReader(`{"app_id":"app-one","session_id":"session-one","input":"active"}`))
		if err != nil {
			runDone <- err
			return
		}
		response.Body.Close()
		runDone <- nil
	}()
	select {
	case <-runner.entered:
	case <-time.After(time.Second):
		t.Fatal("active run did not start")
	}

	assertAPIError(t, mustPost(t, controlClient, server.URL+"/api/v1/admin/operations/drain", `{"confirm":true}`), http.StatusAccepted, "")
	assertAPIError(t, mustPost(t, controlClient, server.URL+"/api/v1/admin/run", `{"app_id":"app-one","session_id":"session-two","input":"new"}`), http.StatusServiceUnavailable, "service_closing")

	var status DrainStatus
	decodeTestJSON(t, mustGet(t, controlClient, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
	if status.State != DrainDraining || status.ActiveExecutions != 1 {
		t.Fatalf("drain status = %#v", status)
	}

	runner.release <- struct{}{}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		decodeTestJSON(t, mustGet(t, controlClient, server.URL+"/api/v1/admin/operations/drain"), http.StatusOK, &status)
		if status.State == DrainClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain did not close: %#v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOperationsDrainIsDeniedForViewer(t *testing.T) {
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{
		ID: "viewer", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleViewer}},
	})
	handler.ConfigureRuntime(EchoRunner{}, lifecycle.New())
	server, client := newHandlerClient(t, handler)
	defer server.Close()
	authenticateTestClient(t, client, server.URL)
	assertAPIError(t, mustPost(t, client, server.URL+"/api/v1/admin/operations/drain", `{"confirm":true}`), http.StatusForbidden, "forbidden")
}

func mustPost(t *testing.T, client *http.Client, url, body string) *http.Response {
	t.Helper()
	response, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func authenticateTestClient(t *testing.T, client *http.Client, serverURL string) {
	t.Helper()
	response := mustGet(t, client, serverURL+"/api/v1/auth/me")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("authenticate status = %d", response.StatusCode)
	}
}

func mustGet(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeTestJSON(t *testing.T, response *http.Response, wantStatus int, target any) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("response status = %d, want %d: %s", response.StatusCode, wantStatus, data)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}
