package wecommcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestGroupSampleSelectsOnlyUniqueGroupWithBoundedWindow(t *testing.T) {
	now := time.Date(2026, 9, 6, 8, 1, 35, 0, time.UTC)
	snapshot := privateSnapshot{Tool: sessionsTool, CapturedAt: now}
	const fixture = `{"errcode":0,"sessions":[{"chat_id":"test-group","chat_type":"group","last_msg_time":"2026-09-06 15:59:38"},{"chat_id":"private-chat","chat_type":"single","last_msg_time":"2026-09-06 15:31:20"}]}`
	var good sessionListPayload
	if err := json.Unmarshal([]byte(fixture), &good); err != nil {
		t.Fatal(err)
	}
	args, err := testGroupMessageQuery(snapshot, good, now)
	if err != nil || len(args) != 3 || args["chat_id"] != "test-group" || args["begin_time"] != "2026-09-06 15:57:38" || args["end_time"] != "2026-09-06 16:01:38" {
		t.Fatalf("window=%v err=%v", args, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*sessionListPayload)
	}{
		{"no groups", func(p *sessionListPayload) { p.Sessions = p.Sessions[1:] }},
		{"multiple groups", func(p *sessionListPayload) { p.Sessions = append(p.Sessions, p.Sessions[0]) }},
		{"unknown group type", func(p *sessionListPayload) { p.Sessions[0].ChatType = "unknown" }},
		{"stale group", func(p *sessionListPayload) { p.Sessions[0].LastMessageTime = "2026-09-05 15:59:38" }},
		{"invalid timestamp", func(p *sessionListPayload) { p.Sessions[0].LastMessageTime = "not-a-time" }},
		{"empty chat", func(p *sessionListPayload) { p.Sessions[0].ChatID = "" }},
		{"invalid chat", func(p *sessionListPayload) { p.Sessions[0].ChatID = "bad\nchat" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input sessionListPayload
			_ = json.Unmarshal([]byte(fixture), &input)
			tc.mutate(&input)
			if _, err := testGroupMessageQuery(snapshot, input, now); err == nil {
				t.Fatal("unsafe group query accepted")
			}
		})
	}
	for _, stamp := range []time.Time{now.Add(time.Hour), now.Add(-time.Minute)} {
		if _, err := testGroupMessageQuery(snapshot, good, stamp); err == nil {
			t.Fatal("expired or future snapshot accepted")
		}
	}
	if _, err := testMessageQuery(snapshot, good, now); err == nil {
		t.Fatal("private mode accepted group snapshot")
	}
}

func TestGroupSampleRejectsMissingOrWrongEndpointBinding(t *testing.T) {
	const endpoint = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=test-key"
	for _, binding := range []string{"", endpointHash(endpoint + "-different")} {
		data, err := json.Marshal(privateSnapshot{Tool: sessionsTool, CapturedAt: time.Now(), EndpointHash: binding, Payload: json.RawMessage(`{"errcode":0,"sessions":[]}`)})
		if err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(t.TempDir(), "sessions.json")
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = ReadUniqueTestGroupMessageSnapshot(context.Background(), endpoint, file, t.TempDir())
		if err == nil || (!strings.Contains(err.Error(), "endpoint binding") && !strings.Contains(err.Error(), "different MCP configuration")) {
			t.Fatal("configuration binding did not reject snapshot before querying")
		}
	}
}

func TestAuthorizedGroupMessageSampleUsesObservedShape(t *testing.T) {
	fixture, err := os.ReadFile("testdata/group_messages.json")
	if err != nil {
		t.Fatal(err)
	}
	const groupID = "synthetic-group-id"
	args := map[string]any{"chat_id": groupID, "begin_time": "2026-09-06 15:57:38", "end_time": "2026-09-06 16:01:38"}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
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
			actual, _ := json.Marshal(req.Params.Arguments)
			want, _ := json.Marshal(args)
			if req.Params.Name != messagesTool || string(actual) != string(want) {
				t.Error("read outside authorized group/window")
				w.WriteHeader(400)
				return
			}
			calls++
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": string(fixture)}}}
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
	report, err := sample(ctx, server.URL+"?apikey=test-key", server.Client(), messagesTool, args, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || report.IsError || report.Status["messages_count"] != json.Number("1") || report.Status["has_more"] != false || report.Window["begin_time"] != args["begin_time"] || report.Window["end_time"] != args["end_time"] {
		t.Fatal("unexpected group sample report")
	}
	encoded, _ := json.Marshal(report)
	for _, private := range []string{groupID, "synthetic-user", "synthetic-bot", "TRPC-WECOM-TEST-001"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("private message or identity leaked into report")
		}
	}
	data, err := os.ReadFile(report.File)
	if err != nil {
		t.Fatal(err)
	}
	var saved privateSnapshot
	if json.Unmarshal(data, &saved) != nil || !strings.Contains(string(saved.Payload), "TRPC-WECOM-TEST-001") {
		t.Fatal("private message sample missing")
	}
	// The real sample did not contain a stable message ID. The sampling layer
	// must not manufacture one and make it appear to be a provider guarantee.
	var payload struct {
		Messages []map[string]any `json:"messages"`
	}
	if json.Unmarshal(saved.Payload, &payload) != nil || len(payload.Messages) != 1 {
		t.Fatal("invalid message payload")
	}
	for _, key := range []string{"id", "msgid", "msg_id", "message_id"} {
		if _, exists := payload.Messages[0][key]; exists {
			t.Fatal("sample unexpectedly acquired a message ID")
		}
	}
}
