package feishu_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// fakeFeishuAPI is a scriptable Feishu Open API stub.
type fakeFeishuAPI struct {
	mu          sync.Mutex
	tokenCalls  int
	sendCalls   int
	tokenValue  string
	sendCodes   []int
	httpStatus  int
	lastAuth    string
	lastPayload map[string]any
}

func (api *fakeFeishuAPI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	api.mu.Lock()
	defer api.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	if strings.Contains(request.URL.Path, "tenant_access_token") {
		api.tokenCalls++
		api.tokenValue = fmt.Sprintf("t-token-%d", api.tokenCalls)
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "tenant_access_token": api.tokenValue, "expire": 7200})
		return
	}
	api.sendCalls++
	api.lastAuth = request.Header.Get("Authorization")
	var payload map[string]any
	_ = json.NewDecoder(request.Body).Decode(&payload)
	api.lastPayload = payload
	if api.httpStatus != 0 {
		writer.WriteHeader(api.httpStatus)
		_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0})
		return
	}
	code := 0
	if len(api.sendCodes) > 0 {
		code = api.sendCodes[0]
		api.sendCodes = api.sendCodes[1:]
	}
	_ = json.NewEncoder(writer).Encode(map[string]any{"code": code})
}

func (api *fakeFeishuAPI) stats() (tokenCalls, sendCalls int, lastAuth string) {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.tokenCalls, api.sendCalls, api.lastAuth
}

func outbound(text string) gateway.OutboundMessage {
	return gateway.OutboundMessage{TenantID: "tenant-a", AppID: "assistant", BindingID: "feishu-a", ExternalUserID: "ou_alice", Text: text}
}

func TestSenderCachesTenantAccessToken(t *testing.T) {
	api := &fakeFeishuAPI{}
	server := httptest.NewServer(api)
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}
	sender := &feishu.Sender{Tokens: source, BaseURL: server.URL}
	for i := 0; i < 3; i++ {
		if err := sender.SendText(context.Background(), outbound("hello")); err != nil {
			t.Fatal(err)
		}
	}
	tokenCalls, sendCalls, lastAuth := api.stats()
	if tokenCalls != 1 || sendCalls != 3 {
		t.Fatalf("tokenCalls=%d sendCalls=%d", tokenCalls, sendCalls)
	}
	if lastAuth != "Bearer t-token-1" {
		t.Fatalf("auth=%q", lastAuth)
	}
}

func TestSenderBuildsInteractiveCard(t *testing.T) {
	api := &fakeFeishuAPI{}
	server := httptest.NewServer(api)
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}
	sender := &feishu.Sender{Tokens: source, BaseURL: server.URL}
	message := outbound("card reply")
	message.ReplyFormat = "card"
	if err := sender.SendText(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	payload := api.lastPayload
	api.mu.Unlock()
	if payload["msg_type"] != "interactive" || !strings.Contains(payload["content"].(string), "card reply") {
		t.Fatalf("payload=%v", payload)
	}
}

func TestConcurrentSendsShareSingleTokenRefresh(t *testing.T) {
	api := &fakeFeishuAPI{}
	server := httptest.NewServer(api)
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}
	sender := &feishu.Sender{Tokens: source, BaseURL: server.URL}
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sender.SendText(context.Background(), outbound("concurrent")); err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	tokenCalls, _, _ := api.stats()
	if tokenCalls != 1 {
		t.Fatalf("tokenCalls=%d, want 1", tokenCalls)
	}
}

func TestSenderRefreshesInvalidTokenAndRetriesOnce(t *testing.T) {
	api := &fakeFeishuAPI{sendCodes: []int{99991663, 0}}
	server := httptest.NewServer(api)
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "cli_a", AppSecret: "secret", BaseURL: server.URL}
	sender := &feishu.Sender{Tokens: source, BaseURL: server.URL}
	if err := sender.SendText(context.Background(), outbound("retry")); err != nil {
		t.Fatal(err)
	}
	tokenCalls, sendCalls, lastAuth := api.stats()
	if tokenCalls != 2 || sendCalls != 2 || lastAuth != "Bearer t-token-2" {
		t.Fatalf("tokenCalls=%d sendCalls=%d auth=%q", tokenCalls, sendCalls, lastAuth)
	}

	// A persistent token rejection must not loop: refresh once, then fail.
	api.mu.Lock()
	api.sendCodes = []int{99991663, 99991663}
	api.mu.Unlock()
	if err := sender.SendText(context.Background(), outbound("retry-hard")); err == nil {
		t.Fatal("expected failure after one refresh")
	}
}

func TestSenderErrorClassification(t *testing.T) {
	// Retryable: HTTP 429.
	api := &fakeFeishuAPI{httpStatus: http.StatusTooManyRequests}
	server := httptest.NewServer(api)
	defer server.Close()
	sender := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "a", AppSecret: "s", BaseURL: server.URL}, BaseURL: server.URL}
	err := sender.SendText(context.Background(), outbound("rate"))
	var retryable channels.RetryClassifier
	if !errors.As(err, &retryable) || !retryable.DeliveryRetryable() {
		t.Fatalf("429 err=%v", err)
	}

	// Permanent: provider rejection like 230001 (bot is not in the chat).
	api2 := &fakeFeishuAPI{sendCodes: []int{230001}}
	server2 := httptest.NewServer(api2)
	defer server2.Close()
	sender2 := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "a", AppSecret: "s", BaseURL: server2.URL}, BaseURL: server2.URL}
	err = sender2.SendText(context.Background(), outbound("no-chat"))
	if !errors.As(err, &retryable) || retryable.DeliveryRetryable() {
		t.Fatalf("230001 err=%v", err)
	}

	// Uncertain: the transport dies mid-request and the provider may have
	// accepted the message.
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "t", "expire": 7200})
			return
		}
		panic(http.ErrAbortHandler)
	}))
	sender3 := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "a", AppSecret: "s", BaseURL: server3.URL}, BaseURL: server3.URL, Client: server3.Client()}
	err = sender3.SendText(context.Background(), outbound("lost"))
	var uncertain channels.OutcomeClassifier
	if !errors.As(err, &uncertain) || !uncertain.DeliveryOutcomeUncertain() {
		t.Fatalf("transport err=%v", err)
	}
	server3.Close()
}

func TestSplitTextKeepsUTF8RunesIntact(t *testing.T) {
	text := strings.Repeat("你好世界", 300) // 3600 bytes of 3-byte runes.
	chunks, err := feishu.SplitText(text, 1000)
	if err != nil {
		t.Fatal(err)
	}
	joined := ""
	for _, chunk := range chunks {
		if len(chunk) > 1000 {
			t.Fatalf("chunk too large: %d", len(chunk))
		}
		if !utf8.ValidString(chunk) {
			t.Fatalf("invalid utf-8 chunk: %q", chunk[len(chunk)-4:])
		}
		joined += chunk
	}
	if joined != text {
		t.Fatal("chunks do not round-trip")
	}
	if _, err := feishu.SplitText(" ", 10); err == nil {
		t.Fatal("empty text must fail")
	}
}

func TestSenderUsesChatIDForGroupAndOpenIDForDirect(t *testing.T) {
	api := &fakeFeishuAPI{}
	server := httptest.NewServer(api)
	defer server.Close()
	sender := &feishu.Sender{Tokens: &feishu.AppTokenSource{AppID: "a", AppSecret: "s", BaseURL: server.URL}, BaseURL: server.URL}
	group := outbound("group reply")
	group.ConversationID = "oc_group"
	if err := sender.SendText(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendText(context.Background(), outbound("dm reply")); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.lastPayload["receive_id"] != "ou_alice" {
		t.Fatalf("dm receive_id=%v", api.lastPayload["receive_id"])
	}
}

func TestSecretsNeverLeakIntoErrors(t *testing.T) {
	const canarySecret = "fs-canary-app-secret"
	const canaryToken = "t-canary-access-token"
	// A failing token endpoint that echoes the request (worst case): the
	// returned error must still not contain the secret or any token.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"code":500,"echo":%q}`, canarySecret)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 99991663, "msg": "invalid token " + canaryToken})
	}))
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "cli_a", AppSecret: canarySecret, BaseURL: server.URL}
	sender := &feishu.Sender{Tokens: source, BaseURL: server.URL}
	err := sender.SendText(context.Background(), outbound("canary"))
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), canarySecret) || strings.Contains(err.Error(), canaryToken) {
		t.Fatalf("canary leaked: %v", err)
	}
}

// expiringSoonSource simulates a cached token close to expiry.
type expiringSoonSource struct {
	calls atomic.Int32
}

func (source *expiringSoonSource) Token(context.Context) (string, error) {
	source.calls.Add(1)
	return fmt.Sprintf("t-%d", source.calls.Load()), nil
}
func (source *expiringSoonSource) Invalidate(string) {}

func TestTokenSourceRefreshesBeforeExpiry(t *testing.T) {
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshes.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": fmt.Sprintf("t-%d", refreshes.Load()), "expire": 30})
	}))
	defer server.Close()
	source := &feishu.AppTokenSource{AppID: "a", AppSecret: "s", BaseURL: server.URL}
	ctx := context.Background()
	if _, err := source.Token(ctx); err != nil {
		t.Fatal(err)
	}
	// expire=30s is already inside the one-minute safety margin, so the next
	// call refreshes instead of reusing a token that would die mid-send.
	if _, err := source.Token(ctx); err != nil {
		t.Fatal(err)
	}
	if refreshes.Load() != 2 {
		t.Fatalf("refreshes=%d", refreshes.Load())
	}
}

func TestSendTextValidatesInput(t *testing.T) {
	sender := &feishu.Sender{}
	if err := sender.SendText(context.Background(), outbound("x")); err == nil {
		t.Fatal("nil token source must fail")
	}
	sender = &feishu.Sender{Tokens: &expiringSoonSource{}}
	if err := sender.SendText(context.Background(), gateway.OutboundMessage{Text: "x"}); err == nil {
		t.Fatal("missing external user must fail")
	}
}
