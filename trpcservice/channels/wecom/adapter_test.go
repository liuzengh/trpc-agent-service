package wecom

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

func TestWeComHandshake(t *testing.T) {
	setWeComTestSecrets(t)
	snapshot := testWeComSnapshot()
	adapter := newTestAdapter(t, snapshot, nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, noopSink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	encrypted, err := encryptMessage(
		[]byte("challenge-ok"), "corp-a", testEncodingAESKey(),
	)
	if err != nil {
		t.Fatalf("encryptMessage() error = %v", err)
	}
	query := url.Values{
		"timestamp":     {"100"},
		"nonce":         {"nonce"},
		"echostr":       {encrypted},
		"msg_signature": {messageSignature("callback-token", "100", "nonce", encrypted)},
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"/channels/wecom/corp-a/callback?"+query.Encode(),
		nil,
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "challenge-ok" {
		t.Fatalf("handshake response = %d %q", response.Code, response.Body.String())
	}
}

func TestWeComEncryptedInboundMessage(t *testing.T) {
	setWeComTestSecrets(t)
	snapshot := testWeComSnapshot()
	adapter := newTestAdapter(t, snapshot, nil, "")
	mux := http.NewServeMux()
	var inbound gateway.InboundMessage
	if err := adapter.Run(context.Background(), mux, func(
		_ context.Context,
		message gateway.InboundMessage,
	) (gateway.Result, error) {
		inbound = message
		return gateway.Result{Outcome: gateway.OutcomeIngested}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	plaintext := []byte(`<xml>
		<ToUserName><![CDATA[corp-a]]></ToUserName>
		<FromUserName><![CDATA[user-1]]></FromUserName>
		<CreateTime>100</CreateTime>
		<MsgType><![CDATA[text]]></MsgType>
		<Content><![CDATA[hello]]></Content>
		<MsgId>msg-1</MsgId>
		<AgentID>1000002</AgentID>
	</xml>`)
	encrypted, err := encryptMessage(plaintext, "corp-a", testEncodingAESKey())
	if err != nil {
		t.Fatalf("encryptMessage() error = %v", err)
	}
	body, err := xml.Marshal(encryptedEnvelope{Encrypt: encrypted})
	if err != nil {
		t.Fatalf("xml.Marshal() error = %v", err)
	}
	query := url.Values{
		"timestamp":     {"100"},
		"nonce":         {"nonce"},
		"msg_signature": {messageSignature("callback-token", "100", "nonce", encrypted)},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/wecom/corp-a/callback?"+query.Encode(),
		bytes.NewReader(body),
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "success" {
		t.Fatalf("callback response = %d %q", response.Code, response.Body.String())
	}
	if inbound.MsgID != "msg-1" || inbound.SenderID != "user-1" ||
		inbound.Text != "hello" || inbound.Channel != channelType {
		t.Fatalf("inbound = %#v", inbound)
	}
}

func TestWeComRejectsCorpMismatch(t *testing.T) {
	setWeComTestSecrets(t)
	snapshot := testWeComSnapshot()
	snapshot.Binding.Config["corp_id"] = "corp-b"
	adapter := newTestAdapter(t, snapshot, nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		t.Fatal("corp mismatch reached Gateway")
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	encrypted, err := encryptMessage([]byte(`<xml/>`), "corp-a", testEncodingAESKey())
	if err != nil {
		t.Fatalf("encryptMessage() error = %v", err)
	}
	body, _ := xml.Marshal(encryptedEnvelope{Encrypt: encrypted})
	query := url.Values{
		"timestamp":     {"100"},
		"nonce":         {"nonce"},
		"msg_signature": {messageSignature("callback-token", "100", "nonce", encrypted)},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/channels/wecom/corp-a/callback?"+query.Encode(),
		bytes.NewReader(body),
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}

func TestWeComReplierTokenCacheAndUTF8Segments(t *testing.T) {
	setWeComTestSecrets(t)
	var mu sync.Mutex
	tokenCalls := 0
	var segments []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokenCalls++
			writeTestJSON(w, map[string]any{
				"errcode": 0, "errmsg": "ok",
				"access_token": "access-token", "expires_in": 7200,
			})
		case "/cgi-bin/message/send":
			var body struct {
				Text struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode send body: %v", err)
			}
			segments = append(segments, body.Text.Content)
			writeTestJSON(w, map[string]any{"errcode": 0, "errmsg": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	snapshot := testWeComSnapshot()
	adapter := newTestAdapter(t, snapshot, server.Client(), server.URL)
	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta", Text: strings.Repeat("你", 1000)}
	events <- reply.Event{Type: "done"}
	close(events)
	err := adapter.NewReplier(snapshot).Reply(
		context.Background(),
		"session-1",
		gateway.InboundMessage{
			Channel: channelType,
			Raw:     replyTarget{UserID: "user-1"},
		},
		events,
	)
	if err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if tokenCalls != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls)
	}
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	if len(segments[0]) > maxTextBytes || len(segments[1]) > maxTextBytes ||
		strings.Join(segments, "") != strings.Repeat("你", 1000) {
		t.Fatalf("invalid UTF-8 segmentation: lengths %d, %d", len(segments[0]), len(segments[1]))
	}
}

func TestWeComValidation(t *testing.T) {
	if _, err := New(nil, nil, ""); err == nil {
		t.Fatal("New() accepted nil cache")
	}
	adapter := newTestAdapter(t, testWeComSnapshot(), nil, "")
	if err := adapter.Run(context.Background(), nil, nil); err == nil {
		t.Fatal("Run() accepted nil dependencies")
	}
}

func TestWeComCallbackErrorResponses(t *testing.T) {
	setWeComTestSecrets(t)
	if got := (&Adapter{}).Type(); got != channelType {
		t.Fatalf("Type() = %q", got)
	}
	tests := []struct {
		name       string
		cacheErr   error
		method     string
		body       string
		query      string
		wantStatus int
	}{
		{
			name:       "binding not found",
			cacheErr:   tenant.ErrNotFound,
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "binding unavailable",
			cacheErr:   context.DeadlineExceeded,
			method:     http.MethodGet,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "invalid handshake signature",
			method:     http.MethodGet,
			query:      "timestamp=1&nonce=n&echostr=x&msg_signature=wrong",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid callback XML",
			method:     http.MethodPost,
			body:       "<broken",
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testWeComSnapshot()
			adapter, err := New(&wecomCache{snapshot: snapshot, err: test.cacheErr}, nil, "")
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			mux := http.NewServeMux()
			if err := adapter.Run(context.Background(), mux, noopSink); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			path := "/channels/wecom/corp-a/callback"
			if test.query != "" {
				path += "?" + test.query
			}
			request := httptest.NewRequest(test.method, path, strings.NewReader(test.body))
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
}

func TestWeComHandshakeDecryptFailure(t *testing.T) {
	setWeComTestSecrets(t)
	adapter := newTestAdapter(t, testWeComSnapshot(), nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, noopSink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	encrypted := base64.StdEncoding.EncodeToString(make([]byte, 32))
	query := url.Values{
		"timestamp":     {"100"},
		"nonce":         {"nonce"},
		"echostr":       {encrypted},
		"msg_signature": {messageSignature("callback-token", "100", "nonce", encrypted)},
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"/channels/wecom/corp-a/callback?"+query.Encode(),
		nil,
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}

func TestWeComMessageFailurePaths(t *testing.T) {
	setWeComTestSecrets(t)
	textMessage := []byte(`<xml>
		<FromUserName>user-1</FromUserName><MsgType>text</MsgType>
		<Content>hello</Content><MsgId>msg-1</MsgId>
	</xml>`)
	tests := []struct {
		name       string
		plaintext  []byte
		mutate     func(*http.Request)
		sink       channels.Sink
		wantStatus int
	}{
		{
			name:      "invalid signature",
			plaintext: textMessage,
			mutate: func(request *http.Request) {
				query := request.URL.Query()
				query.Set("msg_signature", "wrong")
				request.URL.RawQuery = query.Encode()
			},
			sink:       noopSink,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid decrypted XML",
			plaintext:  []byte("not XML"),
			sink:       noopSink,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:      "Gateway error",
			plaintext: textMessage,
			sink: func(context.Context, gateway.InboundMessage) (gateway.Result, error) {
				return gateway.Result{}, context.DeadlineExceeded
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:      "Agent offline",
			plaintext: textMessage,
			sink: func(context.Context, gateway.InboundMessage) (gateway.Result, error) {
				return gateway.Result{Outcome: gateway.OutcomeAgentOffline}, nil
			},
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := newTestAdapter(t, testWeComSnapshot(), nil, "")
			mux := http.NewServeMux()
			if err := adapter.Run(context.Background(), mux, test.sink); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			request := encryptedCallbackRequest(t, test.plaintext, "corp-a")
			if test.mutate != nil {
				test.mutate(request)
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
}

func TestWeComMissingCallbackSecret(t *testing.T) {
	setWeComTestSecrets(t)
	snapshot := testWeComSnapshot()
	delete(snapshot.Binding.Config, "token")
	adapter := newTestAdapter(t, snapshot, nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, noopSink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := httptest.NewRequest(
		http.MethodGet, "/channels/wecom/corp-a/callback", nil,
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
}

func TestWeComIgnoresNonTextMessage(t *testing.T) {
	setWeComTestSecrets(t)
	adapter := newTestAdapter(t, testWeComSnapshot(), nil, "")
	mux := http.NewServeMux()
	if err := adapter.Run(context.Background(), mux, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		t.Fatal("non-text message reached Gateway")
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	request := encryptedCallbackRequest(t, []byte(`<xml>
		<FromUserName>user-1</FromUserName><MsgType>image</MsgType><MsgId>msg-image</MsgId>
	</xml>`), "corp-a")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "success" {
		t.Fatalf("response = %d %q", response.Code, response.Body.String())
	}
}

func TestWeComReplierRefreshesInvalidToken(t *testing.T) {
	setWeComTestSecrets(t)
	var mu sync.Mutex
	tokenCalls, sendCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokenCalls++
			writeTestJSON(w, map[string]any{
				"errcode": 0, "access_token": "token-" + strconv.Itoa(tokenCalls), "expires_in": 7200,
			})
		case "/cgi-bin/message/send":
			sendCalls++
			if sendCalls == 1 {
				writeTestJSON(w, map[string]any{"errcode": 40014, "errmsg": "invalid token"})
			} else {
				writeTestJSON(w, map[string]any{"errcode": 0, "errmsg": "ok"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	snapshot := testWeComSnapshot()
	adapter := newTestAdapter(t, snapshot, server.Client(), server.URL)
	events := make(chan reply.Event, 1)
	events <- reply.Event{Type: "done"}
	close(events)
	if err := adapter.NewReplier(snapshot).Reply(
		context.Background(), "session",
		gateway.InboundMessage{Raw: replyTarget{UserID: "user-1"}},
		events,
	); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if tokenCalls != 2 || sendCalls != 2 {
		t.Fatalf("calls token=%d send=%d, want 2 each", tokenCalls, sendCalls)
	}
}

func TestWeComReplierMissingTargetDrains(t *testing.T) {
	setWeComTestSecrets(t)
	adapter := newTestAdapter(t, testWeComSnapshot(), nil, "")
	events := make(chan reply.Event, 1)
	events <- reply.Event{Type: "done"}
	close(events)
	if err := adapter.NewReplier(testWeComSnapshot()).Reply(
		context.Background(), "session", gateway.InboundMessage{}, events,
	); err == nil {
		t.Fatal("Reply() accepted missing target")
	}
	if _, ok := <-events; ok {
		t.Fatal("Reply() did not drain events")
	}
}

func TestWeComReplierRejectsInvalidAgentID(t *testing.T) {
	setWeComTestSecrets(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(w, map[string]any{
			"errcode": 0, "access_token": "token", "expires_in": 7200,
		})
	}))
	defer server.Close()
	snapshot := testWeComSnapshot()
	snapshot.Binding.Config["agent_id"] = "invalid"
	adapter := newTestAdapter(t, snapshot, server.Client(), server.URL)
	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "tool_call", ToolName: "danger"}
	events <- reply.Event{Type: "error", Error: "denied"}
	close(events)
	if err := adapter.NewReplier(snapshot).Reply(
		context.Background(), "session",
		gateway.InboundMessage{Raw: replyTarget{UserID: "user-1"}},
		events,
	); err == nil {
		t.Fatal("Reply() accepted invalid agent_id")
	}
}

func TestDecodeWeComAPIHTTPError(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader("upstream failed")),
	}
	if err := decodeAPIResponse(response, &apiResult{}); err == nil {
		t.Fatal("decodeAPIResponse() accepted HTTP error")
	}
	if got := splitUTF8("hello", 0); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("splitUTF8(max=0) = %v", got)
	}
}

func encryptedCallbackRequest(t *testing.T, plaintext []byte, receiveID string) *http.Request {
	t.Helper()
	encrypted, err := encryptMessage(plaintext, receiveID, testEncodingAESKey())
	if err != nil {
		t.Fatalf("encryptMessage() error = %v", err)
	}
	body, err := xml.Marshal(encryptedEnvelope{Encrypt: encrypted})
	if err != nil {
		t.Fatalf("xml.Marshal() error = %v", err)
	}
	query := url.Values{
		"timestamp":     {"100"},
		"nonce":         {"nonce"},
		"msg_signature": {messageSignature("callback-token", "100", "nonce", encrypted)},
	}
	return httptest.NewRequest(
		http.MethodPost,
		"/channels/wecom/corp-a/callback?"+query.Encode(),
		bytes.NewReader(body),
	)
}

func newTestAdapter(
	t *testing.T,
	snapshot tenant.Snapshot,
	client *http.Client,
	baseURL string,
) *Adapter {
	t.Helper()
	adapter, err := New(&wecomCache{snapshot: snapshot}, client, baseURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func testWeComSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: channelType, RouteKey: "corp-a", IsActive: true,
			Config: map[string]string{
				"corp_id":          "corp-a",
				"agent_id":         "1000002",
				"token":            "env:WECOM_TOKEN",
				"encoding_aes_key": "env:WECOM_AES_KEY",
				"corp_secret":      "env:WECOM_CORP_SECRET",
			},
		},
	}
}

func setWeComTestSecrets(t *testing.T) {
	t.Helper()
	t.Setenv("WECOM_TOKEN", "callback-token")
	t.Setenv("WECOM_AES_KEY", testEncodingAESKey())
	t.Setenv("WECOM_CORP_SECRET", "corp-secret")
}

type wecomCache struct {
	snapshot tenant.Snapshot
	err      error
}

func (w *wecomCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return w.snapshot, w.err
}
func (w *wecomCache) Invalidate(string, string) {}

func noopSink(context.Context, gateway.InboundMessage) (gateway.Result, error) {
	return gateway.Result{}, nil
}

func writeTestJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

var _ channels.Adapter = (*Adapter)(nil)
