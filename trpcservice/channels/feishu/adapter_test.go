package feishu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

func TestFeishuURLVerification(t *testing.T) {
	t.Setenv("FEISHU_VERIFY", "verify-token")
	adapter := newTestAdapter(t, testFeishuSnapshot(), nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		t.Fatal("URL verification unexpectedly invoked Gateway")
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/feishu/cli-test/events",
		bytes.NewBufferString(`{"type":"url_verification","challenge":"challenge-1","token":"verify-token"}`),
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "challenge-1") {
		t.Fatalf("URL verification response = %d %s", response.Code, response.Body.String())
	}
}

func TestFeishuEncryptedURLVerificationWithoutSignatureHeaders(t *testing.T) {
	t.Setenv("FEISHU_VERIFY", "verify-token")
	t.Setenv("FEISHU_ENCRYPT", "encrypt-key")
	snapshot := testFeishuSnapshot()
	snapshot.Binding.Config["encrypt_key"] = "env:FEISHU_ENCRYPT"
	adapter := newTestAdapter(t, snapshot, nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		t.Fatal("URL verification unexpectedly invoked Gateway")
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	plaintext := []byte(`{"type":"url_verification","challenge":"encrypted-challenge","token":"verify-token"}`)
	encrypted := encryptCallbackForTest(t, plaintext, "encrypt-key")
	body, err := json.Marshal(map[string]string{"encrypt": encrypted})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/feishu/cli-test/events",
		bytes.NewReader(body),
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), "encrypted-challenge") {
		t.Fatalf("encrypted URL verification = %d %s", response.Code, response.Body.String())
	}
}

func TestFeishuEncryptedGroupEvent(t *testing.T) {
	t.Setenv("FEISHU_VERIFY", "verify-token")
	t.Setenv("FEISHU_ENCRYPT", "encrypt-key")
	snapshot := testFeishuSnapshot()
	snapshot.Binding.Config["encrypt_key"] = "env:FEISHU_ENCRYPT"
	adapter := newTestAdapter(t, snapshot, nil, "")
	now := time.Unix(1787716800, 0)
	adapter.now = func() time.Time { return now }
	mux := http.NewServeMux()
	var inbound gateway.InboundMessage
	if err := adapter.Run(context.Background(), mux, func(
		_ context.Context,
		message gateway.InboundMessage,
	) (gateway.Result, error) {
		inbound = message
		return gateway.Result{Outcome: gateway.OutcomeIngested, SessionID: "session-1"}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	payload := []byte(`{
		"schema":"2.0",
		"header":{"event_id":"event-1","event_type":"im.message.receive_v1","token":"verify-token","app_id":"cli-test"},
		"event":{
			"sender":{"sender_id":{"open_id":"ou-user"},"sender_type":"user"},
			"message":{
				"message_id":"message-1","chat_id":"oc-group","chat_type":"group",
				"message_type":"text","content":"{\"text\":\"@_user_1 hello\"}",
				"mentions":[{"key":"@_user_1","id":{"open_id":"ou-bot"}}]
			}
		}
	}`)
	encrypted := encryptCallbackForTest(t, payload, "encrypt-key")
	body, err := json.Marshal(map[string]string{"encrypt": encrypted})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	nonce := "nonce-1"
	hash := sha256.New()
	_, _ = hash.Write([]byte(timestamp + nonce + "encrypt-key"))
	_, _ = hash.Write(body)
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/feishu/cli-test/events",
		bytes.NewReader(body),
	)
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", hex.EncodeToString(hash.Sum(nil)))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("event response = %d %s", response.Code, response.Body.String())
	}
	if inbound.MsgID != "event-1" || inbound.SenderID != "ou-user" ||
		inbound.GroupID != "oc-group" || !inbound.AddressedToBot || inbound.Text != "hello" {
		t.Fatalf("inbound = %#v", inbound)
	}
}

func TestFeishuRejectsInvalidSignature(t *testing.T) {
	t.Setenv("FEISHU_VERIFY", "verify-token")
	t.Setenv("FEISHU_ENCRYPT", "encrypt-key")
	snapshot := testFeishuSnapshot()
	snapshot.Binding.Config["encrypt_key"] = "env:FEISHU_ENCRYPT"
	adapter := newTestAdapter(t, snapshot, nil, "")
	now := time.Unix(1787716800, 0)
	adapter.now = func() time.Time { return now }
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		t.Fatal("invalid signature reached Gateway")
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/feishu/cli-test/events",
		bytes.NewBufferString(`{"encrypt":"invalid"}`),
	)
	request.Header.Set("X-Lark-Request-Timestamp", strconv.FormatInt(now.Unix(), 10))
	request.Header.Set("X-Lark-Request-Nonce", "nonce")
	request.Header.Set("X-Lark-Signature", "wrong")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}

func TestFeishuReplierCreatesAndUpdatesCard(t *testing.T) {
	t.Setenv("FEISHU_VERIFY", "verify-token")
	t.Setenv("FEISHU_SECRET", "app-secret")
	var mu sync.Mutex
	authCalls, createCalls, patchCalls := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
			authCalls++
			writeJSON(w, http.StatusOK, map[string]any{
				"code": 0, "tenant_access_token": "tenant-token", "expire": 7200,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/open-apis/im/v1/messages":
			createCalls++
			if r.Header.Get("Authorization") != "Bearer tenant-token" {
				t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"code": 0, "data": map[string]string{"message_id": "om-card"},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/open-apis/im/v1/messages/om-card":
			patchCalls++
			writeJSON(w, http.StatusOK, map[string]any{"code": 0})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	snapshot := testFeishuSnapshot()
	snapshot.Binding.Config["app_secret"] = "env:FEISHU_SECRET"
	adapter := newTestAdapter(t, snapshot, server.Client(), server.URL)
	events := make(chan reply.Event, 3)
	events <- reply.Event{Type: "text_delta", Text: "hello"}
	events <- reply.Event{Type: "text_delta", Text: " world"}
	events <- reply.Event{Type: "done"}
	close(events)
	err := adapter.NewReplier(snapshot).Reply(
		context.Background(),
		"session-1",
		gateway.InboundMessage{
			Channel:  channelType,
			SenderID: "ou-user",
			Raw:      replyTarget{ChatID: "oc-chat"},
		},
		events,
	)
	if err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if authCalls != 1 || createCalls != 1 || patchCalls != 1 {
		t.Fatalf("API calls auth=%d create=%d patch=%d", authCalls, createCalls, patchCalls)
	}
}

func TestDecodeFeishuAPIHTTPError(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusBadRequest,
		Body: io.NopCloser(strings.NewReader(
			`{"code":230001,"msg":"invalid receive_id"}`,
		)),
	}
	err := decodeAPIResponse(response, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "230001") {
		t.Fatalf("decodeAPIResponse() error = %v", err)
	}
}

func newTestAdapter(
	t *testing.T,
	snapshot tenant.Snapshot,
	client *http.Client,
	baseURL string,
) *Adapter {
	t.Helper()
	adapter, err := New(&feishuCache{snapshot: snapshot}, client, baseURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func testFeishuSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: channelType, RouteKey: "cli-test", IsActive: true,
			Config: map[string]string{
				"app_id":             "cli-test",
				"bot_open_id":        "ou-bot",
				"verification_token": "env:FEISHU_VERIFY",
			},
		},
	}
}

type feishuCache struct {
	snapshot tenant.Snapshot
	err      error
}

func (f *feishuCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return f.snapshot, f.err
}
func (f *feishuCache) Invalidate(string, string) {}

var _ channels.Adapter = (*Adapter)(nil)
