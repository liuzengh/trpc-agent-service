package platform

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type channelTestClient struct {
	*testing.T
	server  *httptest.Server
	client  *http.Client
	handler *AdminHandler
}

func newChannelTestClient(t *testing.T, runner RunnerAdapter) *channelTestClient {
	return newChannelTestClientWithIdentity(t, runner, DevelopmentIdentity{
		ID:   "developer",
		Name: "Developer",
		Assignments: []TenantAssignment{
			{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin},
			{TenantID: "tenant-two", TenantName: "Two", Role: RoleViewer},
		},
	})
}

func newChannelTestClientWithIdentity(t *testing.T, runner RunnerAdapter, identity DevelopmentIdentity) *channelTestClient {
	client := newChannelTestClientWithoutPolicy(t, runner, identity)
	for _, assignment := range identity.Assignments {
		if assignment.TenantID == "" {
			continue
		}
		_, _ = client.handler.governance.PutPolicy(context.Background(), TenantPolicy{TenantID: assignment.TenantID, AgentAppID: "app-one"})
	}
	return client
}

func newChannelTestClientWithoutPolicy(t *testing.T, runner RunnerAdapter, identity DevelopmentIdentity) *channelTestClient {
	t.Helper()
	handler := NewAdminHandler(NewInMemoryControlPlane(), identity)
	handler.ConfigureRuntime(runner, nil)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &channelTestClient{T: t, server: server, client: &http.Client{Jar: jar}, handler: handler}
}

func (c *channelTestClient) do(method, path, body string, headers map[string]string) *http.Response {
	c.Helper()
	request, err := http.NewRequest(method, c.server.URL+path, bytes.NewBufferString(body))
	if err != nil {
		c.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := c.client.Do(request)
	if err != nil {
		c.Fatal(err)
	}
	return response
}

func (c *channelTestClient) post(path, body string, headers map[string]string, wantStatus int, target any) {
	c.Helper()
	response := c.do(http.MethodPost, path, body, headers)
	defer response.Body.Close()
	if target == nil {
		if response.StatusCode != wantStatus {
			c.Fatalf("POST %s = %d, want %d", path, response.StatusCode, wantStatus)
		}
		return
	}
	if response.StatusCode != wantStatus {
		c.Fatalf("POST %s = %d, want %d", path, response.StatusCode, wantStatus)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		c.Fatal(err)
	}
}

func (c *channelTestClient) activateApp(appID, deploymentID string) {
	c.Helper()
	c.post("/api/v1/admin/agent-apps", fmt.Sprintf(`{"id":%q,"name":"App"}`, appID), nil, http.StatusCreated, nil)
	c.post("/api/v1/admin/deployments", fmt.Sprintf(`{"id":%q,"agent_app_id":%q}`, deploymentID, appID), nil, http.StatusCreated, nil)
	var version DeploymentVersion
	c.post("/api/v1/admin/deployments/"+deploymentID+"/versions", `{"config":{"runner":"mock"}}`, map[string]string{"Idempotency-Key": "channel-key"}, http.StatusCreated, &version)
	c.post("/api/v1/admin/deployments/"+deploymentID+"/transition", fmt.Sprintf(`{"status":"published","version_id":%q}`, version.ID), nil, http.StatusOK, nil)
	c.post("/api/v1/admin/deployments/"+deploymentID+"/transition", `{"status":"active"}`, nil, http.StatusOK, nil)
}

func signedMockCallback(t *testing.T, binding ChannelBinding, body string) map[string]string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(binding.Secret))
	_, _ = mac.Write([]byte(body))
	return map[string]string{"X-Mock-Signature": hex.EncodeToString(mac.Sum(nil))}
}

func TestMockChannelSignedCallbackRunsAndRepliesWithoutDuplicates(t *testing.T) {
	runs := make(chan RunnerRequest, 4)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"external-user","external_user_id":"external-user"}`, nil, http.StatusCreated, &binding)
	if binding.ID == "" || binding.SessionID == "" || binding.Secret == "" || binding.TenantID != "tenant-one" || binding.AppID != "app-one" {
		t.Fatalf("binding = %#v", binding)
	}

	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"message-one","sequence":1,"text":"hello"}`, binding.ID)
	var accepted struct {
		RequestID string `json:"request_id"`
		SessionID string `json:"session_id"`
	}
	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, &accepted)
	if accepted.RequestID == "" || accepted.SessionID != binding.SessionID {
		t.Fatalf("accepted callback = %#v", accepted)
	}

	select {
	case request := <-runs:
		if request.RequestID != accepted.RequestID || request.AppID != "app-one" || request.SessionID != binding.SessionID || request.Input != "hello" {
			t.Fatalf("Runner request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback run")
	}
	if err := waitForChatEvent(client, binding.SessionID, "channel.reply"); err != nil {
		t.Fatal(err)
	}

	var events struct {
		Items []SessionEvent `json:"items"`
	}
	response := client.do(http.MethodGet, "/api/v1/admin/sessions/"+binding.SessionID+"/events", "", nil)
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	hasInput := false
	for _, event := range events.Items {
		if event.Type == "message.input" {
			hasInput = true
			break
		}
	}
	hasReply := false
	for _, event := range events.Items {
		if event.Type == "channel.reply" {
			hasReply = true
			break
		}
	}
	if len(events.Items) < 4 || !hasInput || !hasReply {
		t.Fatalf("events = %#v", events.Items)
	}

	client.post("/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body), http.StatusAccepted, &accepted)
	duplicateWithHigherSequence := fmt.Sprintf(`{"binding_id":%q,"message_id":"message-one","sequence":2,"text":"hello"}`, binding.ID)
	client.post("/api/v1/chat/channels/mock/callback", duplicateWithHigherSequence, signedMockCallback(t, binding, duplicateWithHigherSequence), http.StatusAccepted, &accepted)
	select {
	case request := <-runs:
		t.Fatalf("duplicate callback executed Runner: %#v", request)
	default:
	}
}

func TestMockChannelCannotReadAnotherTenantBinding(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")

	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"group","external_conversation_id":"group-one","external_user_id":"user-one"}`, nil, http.StatusCreated, &binding)
	body := fmt.Sprintf(`{"binding_id":%q,"message_id":"secret","sequence":1,"text":"secret"}`, binding.ID)
	headers := signedMockCallback(t, binding, body)

	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response := client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, headers)
	defer response.Body.Close()
	var apiErr errorResponse
	_ = json.NewDecoder(response.Body).Decode(&apiErr)
	if response.StatusCode != http.StatusNotFound || apiErr.Error.Code != "channel_binding_not_found" {
		t.Fatalf("cross-tenant callback = %d/%#v", response.StatusCode, apiErr)
	}
	select {
	case request := <-runs:
		t.Fatalf("cross-tenant callback reached Runner: %#v", request)
	default:
	}
}

func TestMockChannelRejectsInvalidProviderRequestID(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	var binding ChannelBinding
	client.post("/api/v1/chat/bindings", `{"channel":"mock","app_id":"app-one","conversation_type":"single","external_conversation_id":"user","external_user_id":"user"}`, nil, http.StatusCreated, &binding)

	body := fmt.Sprintf(`{"binding_id":%q,"message_id":%q,"sequence":1,"text":"hello"}`, binding.ID, strings.Repeat("x", 121))
	response := client.do(http.MethodPost, "/api/v1/chat/channels/mock/callback", body, signedMockCallback(t, binding, body))
	assertChannelAPIError(t, response, http.StatusBadRequest, "channel_callback_invalid")
}

type requestCapturingRunner struct{ requests chan RunnerRequest }

func (r requestCapturingRunner) Run(_ context.Context, request RunnerRequest) (RunnerResponse, error) {
	r.requests <- request
	return RunnerResponse{Output: "echo:" + request.Input}, nil
}
