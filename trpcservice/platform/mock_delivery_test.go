package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestMockChannelFaultControlsExposeStableDeliveryOutcomes(t *testing.T) {
	runs := make(chan RunnerRequest, 4)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var faults struct {
		Scenario string `json:"scenario"`
	}
	client.post("/api/v1/chat/mock/faults", `{"scenario":"message_length"}`, nil, http.StatusOK, &faults)
	if faults.Scenario != "message_length" {
		t.Fatalf("fault scenario = %q", faults.Scenario)
	}

	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"user","external_user_id":"user"}`, nil, http.StatusCreated, &binding)
	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"message-one","sequence":1,"text":"hi"}`, binding.ID)
	var accepted chatRunResponse
	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, &accepted)

	if err := waitForChatEvent(client, binding.SessionID, "channel.delivery"); err != nil {
		t.Fatal(err)
	}
	events := chatEventsForTest(t, client, binding.SessionID)
	var deliveryEvent *SessionEvent
	for index := range events {
		if events[index].Type == "channel.delivery" {
			deliveryEvent = &events[index]
			break
		}
	}
	if deliveryEvent == nil {
		t.Fatalf("events = %#v", events)
	}
	var payload struct {
		Status string `json:"status"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal(deliveryEvent.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != "failed" || payload.Code != "channel_message_too_long" {
		t.Fatalf("delivery payload = %#v", payload)
	}
}

func TestMockChannelTimeoutAndRetryAreBounded(t *testing.T) {
	channel := NewMockChannel()
	binding := ChannelBinding{TenantID: "tenant-one", ID: "binding-one"}

	if _, err := channel.ConfigureFaults("tenant-one", "", "timeout"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := channel.Send(ctx, binding, ChannelReply{MessageID: "message-one", Text: "hello"})
	if channelErrorCode(err) != "channel_timeout" {
		t.Fatalf("cancelled timeout code = %q", channelErrorCode(err))
	}

	if _, err := channel.ConfigureFaults("tenant-one", "", "retry"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	delivery, err := channel.Send(context.Background(), binding, ChannelReply{MessageID: "message-two", Text: "hello"})
	if channelErrorCode(err) != "channel_retry_exhausted" || delivery.Attempts != 3 || delivery.Status != "failed" {
		t.Fatalf("retry delivery = %#v, error = %v", delivery, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("retry took %s", elapsed)
	}
}

func TestChatWorkspaceMockFaultsAreSessionScoped(t *testing.T) {
	runs := make(chan RunnerRequest, 2)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var session Session
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, &session)
	client.post("/api/v1/chat/mock/faults", `{"scenario":"message_length","session_id":"session-one"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hi"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "channel.delivery"); err != nil {
		t.Fatal(err)
	}
	delivery := chatEventsForTest(t, client, "session-one")
	var found bool
	var payload struct {
		Code string `json:"code"`
	}
	for _, event := range delivery {
		if event.Type != "channel.delivery" {
			continue
		}
		found = true
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
	}
	if !found || payload.Code != "channel_message_too_long" {
		t.Fatalf("delivery found = %v, payload = %#v", found, payload)
	}

	client.post("/api/v1/chat/mock/faults", `{"scenario":"none","session_id":"session-one"}`, nil, http.StatusOK, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"ok"}`, map[string]string{"X-Request-ID": "request-two"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "channel.reply"); err != nil {
		t.Fatal(err)
	}
}

func TestMockChannelInboundFaultsUseStablePublicErrors(t *testing.T) {
	runs := make(chan RunnerRequest, 4)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"group","external_conversation_id":"group","external_user_id":"user"}`, nil, http.StatusCreated, &binding)

	client.post("/api/v1/chat/mock/faults", `{"scenario":"message_length"}`, nil, http.StatusOK, nil)
	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"long","sequence":1,"text":"too long"}`, binding.ID)
	response := client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body))
	assertChannelAPIError(t, response, http.StatusBadRequest, "channel_message_too_long")

	client.post("/api/v1/chat/mock/faults", `{"scenario":"attachment"}`, nil, http.StatusOK, nil)
	body = fmt.Sprintf(`{"binding_id":%q,"message_id":"attachment","sequence":2,"text":"hello","attachment_name":"large.bin","attachment_size":16}`, binding.ID)
	response = client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body))
	assertChannelAPIError(t, response, http.StatusBadRequest, "channel_attachment_rejected")

	client.post("/api/v1/chat/mock/faults", `{"scenario":"rate_limit"}`, nil, http.StatusOK, nil)
	body = fmt.Sprintf(`{"binding_id":%q,"message_id":"first","sequence":1,"text":"hello"}`, binding.ID)
	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, nil)
	body = fmt.Sprintf(`{"binding_id":%q,"message_id":"second","sequence":2,"text":"hello"}`, binding.ID)
	response = client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body))
	assertChannelAPIError(t, response, http.StatusTooManyRequests, "channel_rate_limited")

	client.post("/api/v1/chat/mock/faults", `{"scenario":"none"}`, nil, http.StatusOK, nil)
	body = fmt.Sprintf(`{"binding_id":%q,"message_id":"bad-signature","sequence":3,"text":"hello"}`, binding.ID)
	response = client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, map[string]string{"X-Mock-Signature": "invalid"})
	assertChannelAPIError(t, response, http.StatusUnauthorized, "channel_signature_invalid")

	select {
	case request := <-runs:
		if request.Input != "hello" || request.SessionID != binding.SessionID {
			t.Fatalf("unexpected Runner request = %#v", request)
		}
	default:
		t.Fatal("accepted inbound message did not reach Runner")
	}
}

func chatEventsForTest(t *testing.T, client *channelTestClient, sessionID string) []SessionEvent {
	t.Helper()
	response := client.do(http.MethodGet, "/api/v1/chat/sessions/"+sessionID+"/events", "", nil)
	defer response.Body.Close()
	var result struct {
		Items []SessionEvent `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result.Items
}

func waitForChatEvent(client *channelTestClient, sessionID, eventType string) error {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, event := range chatEventsForTest(client.T, client, sessionID) {
			if event.Type == eventType {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", eventType)
}

func assertChannelAPIError(t *testing.T, response *http.Response, wantStatus int, wantCode string) {
	t.Helper()
	defer response.Body.Close()
	var apiErr errorResponse
	if err := json.NewDecoder(response.Body).Decode(&apiErr); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus || apiErr.Error.Code != wantCode {
		t.Fatalf("channel error = %d/%#v, want %d/%s", response.StatusCode, apiErr, wantStatus, wantCode)
	}
}
