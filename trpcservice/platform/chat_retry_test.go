package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type flakyTerminalStore struct {
	DataStore
	mu               sync.Mutex
	terminalAttempts int
}

type governanceRecoveryRunner struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

type confirmationStreamingRunner struct {
	center      *GovernanceCenter
	mu          sync.Mutex
	invocations int
}

func (r *confirmationStreamingRunner) Run(context.Context, RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{}, errors.New("streaming only")
}

func (r *confirmationStreamingRunner) RunEvents(_ context.Context, request RunnerRequest) (<-chan RuntimeEvent, error) {
	events := make(chan RuntimeEvent, 3)
	governanceRequest := GovernanceRequest{
		TenantID: request.TenantID, AgentAppID: request.AppID, UserID: request.UserID,
		SessionID: request.SessionID, RequestID: request.RequestID, RequiredTools: []string{"deploy"},
	}
	if err := r.center.AuthorizeTool(context.Background(), governanceRequest, request.TraceID, "deploy", []byte(`{"target":"production"}`)); err != nil {
		events <- RuntimeEvent{Type: "run.failed", Data: map[string]string{"error": err.Error()}}
		close(events)
		return events, nil
	}
	r.mu.Lock()
	r.invocations++
	r.mu.Unlock()
	_ = r.center.CompleteTool(context.Background(), governanceRequest, request.TraceID, "deploy", nil)
	events <- RuntimeEvent{Type: "message.delta", Data: map[string]string{"delta": "deployed"}}
	events <- RuntimeEvent{Type: "message.completed", Data: map[string]string{"output": "deployed"}}
	close(events)
	return events, nil
}

func (r *confirmationStreamingRunner) Close() error { return nil }

func (r *governanceRecoveryRunner) Run(_ context.Context, _ RunnerRequest) (RunnerResponse, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		close(r.started)
		<-r.release
		return RunnerResponse{}, errors.New("deterministic failure")
	}
	return RunnerResponse{Output: "recovered"}, nil
}

func (s *flakyTerminalStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if event.IdempotencyKey != "request-one:terminal" {
		return s.DataStore.AppendSessionEvent(ctx, event)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminalAttempts++
	if s.terminalAttempts == 1 {
		return errors.New("transient storage failure")
	}
	return s.DataStore.AppendSessionEvent(ctx, event)
}

func TestChatTerminalEventRetriesTransientStorageFailure(t *testing.T) {
	store := &flakyTerminalStore{DataStore: NewInMemoryStore()}
	client := newChannelTestClient(t, failingRunner{})
	client.handler.ConfigureDataStore(store)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)

	if err := waitForChatEvent(client, "session-one", "run.failed"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	attempts := store.terminalAttempts
	store.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("terminal attempts = %d, want 2", attempts)
	}
}

func TestChatRetryReconcilesGovernanceAfterFailurePersistenceRecovers(t *testing.T) {
	runner := &governanceRecoveryRunner{started: make(chan struct{}), release: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	<-runner.started

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	client.handler.governance.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	close(runner.release)
	client.handler.chatWG.Wait()

	for _, event := range chatEventsForTest(t, client, "session-one") {
		if event.Type == "run.failed" || event.Type == "run.cancelled" || event.Type == "run.completed" {
			t.Fatalf("governance failure wrote terminal Session Event: %#v", event)
		}
	}
	if metrics := client.handler.governance.Metrics("tenant-one"); metrics.Active != 1 {
		t.Fatalf("active executions = %d, want reservation retained for retry", metrics.Active)
	}

	client.handler.governance.SetPersistencePath(filepath.Join(t.TempDir(), "governance.json"))
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	if metrics := client.handler.governance.Metrics("tenant-one"); metrics.Active != 0 || metrics.Completed != 1 {
		t.Fatalf("reconciled metrics = %#v", metrics)
	}
}

func TestChatDangerousToolConfirmationResumesSameRequestAtMostOnce(t *testing.T) {
	runner := &confirmationStreamingRunner{}
	client := newChannelTestClient(t, runner)
	runner.center = client.handler.governance
	client.post("/api/v1/admin/agent-apps", `{"id":"app-one","name":"App"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/admin/deployments", `{"id":"deploy-one","agent_app_id":"app-one"}`, nil, http.StatusCreated, nil)
	var version DeploymentVersion
	client.post("/api/v1/admin/deployments/deploy-one/versions", `{"config":{"runner":"mock","tools":["deploy"]}}`, map[string]string{"Idempotency-Key": "confirmation-version"}, http.StatusCreated, &version)
	client.post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"published","version_id":"`+version.ID+`"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/admin/deployments/deploy-one/transition", `{"status":"active"}`, nil, http.StatusOK, nil)
	_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-one", AgentAppID: "app-one", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"ship"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "tool.confirmation.pending"); err != nil {
		t.Fatal(err)
	}
	for _, event := range chatEventsForTest(t, client, "session-one") {
		if event.Type == "run.failed" || event.Type == "run.completed" {
			t.Fatalf("pending confirmation wrote terminal event: %#v", event)
		}
	}
	pending := client.handler.governance.Confirmations("tenant-one")[0]
	if _, err := client.handler.governance.DecideConfirmation(context.Background(), "tenant-one", pending.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"ship"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	runner.mu.Lock()
	invocations := runner.invocations
	runner.mu.Unlock()
	if invocations != 1 {
		t.Fatalf("Tool invocations = %d, want 1", invocations)
	}
}

func TestChatRetryAfterFailureReturnsOriginalRunWithoutDuplicateExecution(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	<-runs
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}

	var result chatRunResponse
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusOK, &result)
	if result.Status != "completed" || result.RequestID != "request-one" {
		t.Fatalf("retry result = %#v", result)
	}
	events := chatEventsForTest(t, client, "session-one")
	inputs := 0
	for _, event := range events {
		if event.Type == "message.input" {
			inputs++
		}
	}
	if inputs != 1 {
		t.Fatalf("input events = %d, want 1", inputs)
	}
}

func TestChatReplayResumesInterruptedRun(t *testing.T) {
	tests := []struct {
		name            string
		persistStarted  bool
		wantStartedRuns int
	}{
		{name: "after input persistence", persistStarted: false, wantStartedRuns: 1},
		{name: "after started persistence", persistStarted: true, wantStartedRuns: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runs := make(chan RunnerRequest, 1)
			client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
			client.activateApp("app-one", "deploy-one")
			client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

			store, releaseStore, err := client.handler.acquireStore(context.Background(), "tenant-one")
			if err != nil {
				t.Fatal(err)
			}
			inputPayload, err := json.Marshal(map[string]string{
				"app_id": "app-one", "input": "hello", "request_id": "request-one", "user_id": "developer",
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.AppendSessionEvent(context.Background(), SessionEvent{
				TenantID: "tenant-one", SessionID: "session-one", IdempotencyKey: "request-one:input",
				Type: "message.input", Payload: inputPayload,
			}); err != nil {
				t.Fatal(err)
			}
			if test.persistStarted {
				startedPayload, err := json.Marshal(map[string]string{"app_id": "app-one", "request_id": "request-one"})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.AppendSessionEvent(context.Background(), SessionEvent{
					TenantID: "tenant-one", SessionID: "session-one", IdempotencyKey: "request-one:started",
					Type: "run.started", Payload: startedPayload,
				}); err != nil {
					t.Fatal(err)
				}
			}
			releaseStore()

			client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
			select {
			case request := <-runs:
				if request.Input != "hello" || request.RequestID != "request-one" {
					t.Fatalf("Runner request = %#v", request)
				}
			case <-time.After(time.Second):
				t.Fatal("interrupted run was not resumed")
			}
			if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
				t.Fatal(err)
			}

			events := chatEventsForTest(t, client, "session-one")
			inputs, startedRuns := 0, 0
			for _, event := range events {
				switch event.Type {
				case "message.input":
					inputs++
				case "run.started":
					startedRuns++
				}
			}
			if inputs != 1 || startedRuns != test.wantStartedRuns {
				t.Fatalf("inputs = %d, started runs = %d, want 1 and %d", inputs, startedRuns, test.wantStartedRuns)
			}
		})
	}
}

func TestConcurrentChatRetriesShareOneLogicalRun(t *testing.T) {
	runner := &chatBlockingRunner{started: make(chan struct{}, 1), once: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	results := make(chan chatRunResponse, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var result chatRunResponse
			response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"})
			defer response.Body.Close()
			if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
				client.T.Fatal(err)
			}
			results <- result
		}()
	}
	<-runner.started
	client.post("/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil, http.StatusOK, nil)
	for i := 0; i < 2; i++ {
		if result := <-results; result.RequestID != "request-one" {
			client.T.Fatalf("result = %#v", result)
		}
	}
	if err := waitForChatEvent(client, "session-one", "run.cancelled"); err != nil {
		t.Fatal(err)
	}
	events := chatEventsForTest(t, client, "session-one")
	inputs, terminals := 0, 0
	for _, event := range events {
		if event.Type == "message.input" {
			inputs++
		}
		if event.Type == "run.cancelled" {
			terminals++
		}
	}
	if inputs != 1 || terminals != 1 {
		t.Fatalf("inputs = %d, terminals = %d", inputs, terminals)
	}
}

func TestConcurrentChatReplayWithDifferentInputIsRejected(t *testing.T) {
	runner := &chatBlockingRunner{started: make(chan struct{}, 1), once: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}

	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"different"}`, map[string]string{"X-Request-ID": "request-one"})
	assertChannelAPIError(t, response, http.StatusConflict, "idempotency_key_reused")

	client.post("/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil, http.StatusOK, nil)
	if err := waitForChatEvent(client, "session-one", "run.cancelled"); err != nil {
		t.Fatal(err)
	}
}
