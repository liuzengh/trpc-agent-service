package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

type failingExecutionMemoryStore struct{ DataStore }

func (s failingExecutionMemoryStore) PutMemory(context.Context, MemoryRecord) error {
	return errors.New("memory unavailable")
}

func TestChatSessionAPIPersistsHistoryAndPropagatesRequestID(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var session Session
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, &session)
	if session.ID != "session-one" || session.TenantID != "tenant-one" || session.AppID != "app-one" {
		t.Fatalf("session = %#v", session)
	}

	var accepted chatRunResponse
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, &accepted)
	if accepted.RequestID != "request-one" || accepted.Status != "running" {
		t.Fatalf("accepted = %#v", accepted)
	}
	select {
	case request := <-runs:
		if request.RequestID != "request-one" || request.AppID != "app-one" || request.SessionID != "session-one" || request.Input != "hello" {
			t.Fatalf("Runner request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("chat run did not reach Runner")
	}
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusOK, &accepted)
	if accepted.Status != "completed" {
		t.Fatalf("replay status = %#v", accepted)
	}

	events := chatEventsForTest(t, client, "session-one")
	inputCount := 0
	for _, event := range events {
		if event.Type == "message.input" {
			inputCount++
		}
	}
	if inputCount != 1 || len(events) < 6 {
		t.Fatalf("events = %#v", events)
	}
}

func TestChatSessionAPIScopesSessionsAndRejectsViewerWrites(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var session Session
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, &session)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "message.completed"); err != nil {
		t.Fatal(err)
	}

	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response := client.do(http.MethodGet, "/api/v1/chat/sessions/session-one/events", "", nil)
	assertChannelAPIError(t, response, http.StatusNotFound, "chat_session_not_found")
	response = client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"guess"}`, nil)
	assertChannelAPIError(t, response, http.StatusForbidden, "forbidden")

	select {
	case request := <-runs:
		if request.Input != "hello" {
			t.Fatalf("unexpected Runner request = %#v", request)
		}
	default:
	}
}

func TestChatCancelValidatesTenantSessionAndRun(t *testing.T) {
	client := newChannelTestClientWithIdentity(t, EchoRunner{}, DevelopmentIdentity{
		ID:   "developer",
		Name: "Developer",
		Assignments: []TenantAssignment{
			{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
			{TenantID: "tenant-two", TenantName: "Two", Role: RoleOperator},
		},
	})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/cancel", `{"request_id":"missing"}`, nil)
	assertChannelAPIError(t, response, http.StatusNotFound, "chat_run_not_found")

	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response = client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil)
	assertChannelAPIError(t, response, http.StatusNotFound, "chat_session_not_found")

	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-one"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}
	var result chatRunResponse
	client.post("/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil, http.StatusOK, &result)
	if result.Status != "completed" {
		t.Fatalf("cancel result = %#v", result)
	}
}

func TestChatSessionAPIRejectsReusedRequestIDWithDifferentInput(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	<-runs
	if err := waitForChatEvent(client, "session-one", "message.completed"); err != nil {
		t.Fatal(err)
	}

	response := client.do(http.MethodPost, "/api/v1/chat/sessions/session-one/messages", `{"input":"different"}`, map[string]string{"X-Request-ID": "request-one"})
	assertChannelAPIError(t, response, http.StatusConflict, "idempotency_key_reused")
	select {
	case request := <-runs:
		t.Fatalf("conflicting replay reached Runner: %#v", request)
	default:
	}
}

func TestChatSessionAPIReturnsConflictForExistingSession(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	response := client.do(http.MethodPost, "/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one","user_id":"different"}`, nil)
	assertChannelAPIError(t, response, http.StatusConflict, "chat_session_exists")
}

func TestChatChannelRejectsOutOfOrderProviderSequence(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"group","external_conversation_id":"group","external_user_id":"user"}`, nil, http.StatusCreated, &binding)

	second := fmt.Sprintf(`{"binding_id":%q,"message_id":"second","sequence":2,"text":"hello"}`, binding.ID)
	client.post("/api/v1/chat/channels/mock/callback", second, signedMockCallback(t, binding, second), http.StatusAccepted, nil)
	first := fmt.Sprintf(`{"binding_id":%q,"message_id":"first","sequence":1,"text":"hello"}`, binding.ID)
	response := client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", first, signedMockCallback(t, binding, first))
	assertChannelAPIError(t, response, http.StatusConflict, "channel_message_out_of_order")

	if err := waitForChatEvent(client, binding.SessionID, "message.completed"); err != nil {
		t.Fatal(err)
	}
	events := chatEventsForTest(t, client, binding.SessionID)
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

func TestChatChannelPersistsCompletedReplyAsMemory(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.handler.ConfigureSessionLeases(&losingLeaseManager{lost: make(chan struct{})})
	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"conversation","external_user_id":"user"}`, nil, http.StatusCreated, &binding)

	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"message-one","sequence":1,"text":"remember this"}`, binding.ID)
	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, nil)
	if err := waitForChatEvent(client, binding.SessionID, "run.completed"); err != nil {
		t.Fatal(err)
	}

	response := client.do(http.MethodGet, "/api/v1/admin/memory/"+binding.SessionID, "", nil)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("memory status = %d", response.StatusCode)
	}
	var result struct {
		Items []MemoryRecord `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Key != "latest_agent_reply" || result.Items[0].Value != "echo:remember this" || result.Items[0].FencingToken != 42 {
		t.Fatalf("memory = %#v", result.Items)
	}
}

func TestChatChannelDoesNotReplyWhenMemoryWriteFails(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.handler.ConfigureDataStore(failingExecutionMemoryStore{DataStore: NewInMemoryStore()})
	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"conversation","external_user_id":"user"}`, nil, http.StatusCreated, &binding)

	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"message-one","sequence":1,"text":"hello"}`, binding.ID)
	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, nil)
	if err := waitForChatEvent(client, binding.SessionID, "run.failed"); err != nil {
		t.Fatal(err)
	}

	events := chatEventsForTest(t, client, binding.SessionID)
	foundMemoryFailure := false
	for _, event := range events {
		if event.Type == "channel.reply" || event.Type == "run.completed" {
			t.Fatalf("unexpected success event after Memory failure: %#v", event)
		}
		if event.Type == "memory.write.failed" {
			foundMemoryFailure = true
		}
	}
	if !foundMemoryFailure {
		t.Fatalf("events = %#v", events)
	}
}
