package wecommcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAuthorizedSessionSampleIsPrivateAndDoesNotReadMessages(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Error("unexpected request")
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "sample-server", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
		case "notifications/initialized":
			w.WriteHeader(202)
			return
		case "tools/call":
			if req.Params.Name != sessionsTool {
				t.Error("read another tool")
				w.WriteHeader(400)
				return
			}
			calls++
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": `{"extra_identity_context":"private-context","sessions":[{"chat_id":"private-chat-id","chat_name":"private-name"}]}`}}}
		default:
			t.Error("unexpected RPC method")
			w.WriteHeader(400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report, err := sample(ctx, server.URL+"?apikey=test-key", server.Client(), sessionsTool, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(report)
	if calls != 1 || strings.Contains(string(encoded), "private-chat-id") || strings.Contains(string(encoded), "private-name") {
		t.Fatal("sample was not limited or redacted")
	}
	file, err := os.Stat(report.File)
	if err != nil {
		t.Fatal(err)
	}
	if file.Mode().Perm() != 0600 {
		t.Fatal("sample permissions are not private")
	}
	data, err := os.ReadFile(report.File)
	if err != nil || !strings.Contains(string(data), "private-chat-id") {
		t.Fatal("private snapshot missing")
	}
	if strings.Contains(string(data), "private-name") || strings.Contains(string(data), "private-context") {
		t.Fatal("unnecessary identity data persisted")
	}
	if _, err := sample(ctx, server.URL, server.Client(), "message_aibot_send", nil, t.TempDir()); err == nil {
		t.Fatal("sampling allowed send")
	}
}

type sampleRoundTripper func(*http.Request) (*http.Response, error)

func (fn sampleRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestSampleToolArgumentsAndBudgetAreExact(t *testing.T) {
	const endpoint = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=sample-test"
	args := map[string]any{"chat_id": "authorized-chat", "begin_time": "2026-09-06 15:29:20", "end_time": "2026-09-06 15:33:20"}
	calls := 0
	guard := &discoveryHTTP{endpoint: endpoint, methods: map[string]int{}, allowedCalls: map[string]map[string]any{messagesTool: args}, callBudget: map[string]int{messagesTool: 1}, client: &http.Client{Transport: sampleRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}}
	for _, tc := range []struct {
		name, chat, begin string
		allowed           bool
	}{
		{messagesTool, "other-chat", "2026-09-06 15:29:20", false},
		{messagesTool, "authorized-chat", "2026-08-30 00:00:00", false},
		{"message_aibot_send", "authorized-chat", "2026-09-06 15:29:20", false},
		{messagesTool, "authorized-chat", "2026-09-06 15:29:20", true},
		{messagesTool, "authorized-chat", "2026-09-06 15:29:20", false},
	} {
		body, _ := json.Marshal(map[string]any{"method": "tools/call", "params": map[string]any{"name": tc.name, "arguments": map[string]any{"chat_id": tc.chat, "begin_time": tc.begin, "end_time": "2026-09-06 15:33:20"}}})
		req, _ := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
		response, err := guard.Handle(context.Background(), nil, req)
		if (err == nil) != tc.allowed {
			t.Fatalf("guard allowed=%t err=%v", tc.allowed, err)
		}
		if response != nil {
			_ = response.Body.Close()
		}
	}
	if calls != 1 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
}

func TestSampleWindowRejectsAmbiguousOrOldSessions(t *testing.T) {
	now := time.Date(2026, 9, 6, 7, 35, 0, 0, time.UTC)
	snapshot := privateSnapshot{Tool: sessionsTool, CapturedAt: now}
	var good sessionListPayload
	_ = json.Unmarshal([]byte(`{"errcode":0,"sessions":[{"chat_id":"test-chat","chat_type":"single","last_msg_time":"2026-09-06 15:31:20"}]}`), &good)
	args, err := testMessageQuery(snapshot, good, now)
	if err != nil || args["begin_time"] != "2026-09-06 15:29:20" || args["end_time"] != "2026-09-06 15:33:20" {
		t.Fatalf("window=%v err=%v", args, err)
	}
	for _, mutate := range []func(*sessionListPayload){
		func(p *sessionListPayload) { p.Sessions = nil },
		func(p *sessionListPayload) { p.Sessions = append(p.Sessions, p.Sessions[0]) },
		func(p *sessionListPayload) { p.Sessions[0].ChatType = "group" },
		func(p *sessionListPayload) { p.Sessions[0].LastMessageTime = "2026-09-05 15:31:20" },
	} {
		encoded, _ := json.Marshal(good)
		var input sessionListPayload
		_ = json.Unmarshal(encoded, &input)
		mutate(&input)
		if _, err := testMessageQuery(snapshot, input, now); err == nil {
			t.Fatal("unsafe session query accepted")
		}
	}
	if _, err := testMessageQuery(snapshot, good, now.Add(time.Hour)); err == nil {
		t.Fatal("expired snapshot accepted")
	}
}
