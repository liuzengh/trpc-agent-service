package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func botBridgeBinding() *tenant.ChannelBinding {
	return &tenant.ChannelBinding{Type: "wecom_bot", AccountID: "bot-local-1", Token: strings.Repeat("b", 32)}
}

func botBridgeEnvelope() map[string]any {
	return map[string]any{
		"received_at": "2000-01-02T03:04:05Z",
		"frame": map[string]any{"cmd": "aibot_msg_callback", "headers": map[string]any{"req_id": "request-7"}, "body": map[string]any{
			"msgid": "message-9", "aibotid": "bot-local-1", "chattype": "single",
			"from": map[string]any{"userid": "user-1"}, "msgtype": "text", "text": map[string]any{"content": "你好"},
		}},
	}
}

func TestWeComBotBridgeAuthentication(t *testing.T) {
	a, binding := NewWeComBotAdapter(""), botBridgeBinding()
	for _, test := range []struct {
		name   string
		values []string
		valid  bool
	}{
		{"correct", []string{binding.Token}, true}, {"missing", nil, false},
		{"wrong", []string{strings.Repeat("x", 32)}, false}, {"duplicate", []string{binding.Token, binding.Token}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader("{}"))
			for _, value := range test.values {
				req.Header.Add(WeComBotBridgeTokenHeader, value)
			}
			err := a.VerifySignature(req, binding)
			if (err == nil) != test.valid {
				t.Fatalf("VerifySignature=%v valid=%v", err, test.valid)
			}
			if err != nil && strings.Contains(err.Error(), binding.Token) {
				t.Fatal("credential leaked")
			}
		})
	}
}

func TestWeComBotMessageIdentityAndIgnoredEvents(t *testing.T) {
	for _, test := range []struct {
		name                    string
		mutate                  func(map[string]any)
		group, ignored, invalid bool
	}{
		{name: "single"},
		{name: "group", group: true, mutate: func(e map[string]any) {
			b := e["frame"].(map[string]any)["body"].(map[string]any)
			b["chattype"], b["chatid"] = "group", "group-2"
		}},
		{name: "event", ignored: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["cmd"] = "aibot_event_callback" }},
		{name: "image", ignored: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["body"].(map[string]any)["msgtype"] = "image" }},
		{name: "foreign bot", invalid: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["body"].(map[string]any)["aibotid"] = "other-bot" }},
		{name: "missing req id", invalid: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["headers"] = map[string]any{} }},
		{name: "control character id", invalid: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["body"].(map[string]any)["msgid"] = "msg\n9" }},
		{name: "group lacks chat id", invalid: true, mutate: func(e map[string]any) { e["frame"].(map[string]any)["body"].(map[string]any)["chattype"] = "group" }},
		{name: "missing timestamp", invalid: true, mutate: func(e map[string]any) { delete(e, "received_at") }},
		{name: "non UTC timestamp", invalid: true, mutate: func(e map[string]any) { e["received_at"] = "2000-01-02T03:04:05+08:00" }},
		{name: "oversize reply id", invalid: true, mutate: func(e map[string]any) {
			e["frame"].(map[string]any)["headers"].(map[string]any)["req_id"] = strings.Repeat("r", 257)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope := botBridgeEnvelope()
			if test.mutate != nil {
				test.mutate(envelope)
			}
			data, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			msg, err := NewWeComBotAdapter("").ParseInbound(httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(data)), botBridgeBinding())
			if test.ignored {
				if !errors.Is(err, ErrIgnoredInbound) || msg != nil {
					t.Fatalf("ignored callback=%v %v", msg, err)
				}
				return
			}
			if test.invalid {
				if err == nil || errors.Is(err, ErrIgnoredInbound) || msg != nil {
					t.Fatalf("invalid callback=%v %v", msg, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			conversation := "user-1"
			if test.group {
				conversation = "group-2"
			}
			if msg.ChannelType != "wecom_bot" || msg.MessageID != "message-9" || msg.ReplyToID != "request-7" || msg.ConversationID != conversation || msg.IsGroupChat != test.group || msg.ExternalUserID != "user-1" {
				t.Fatalf("wrong callback mapping: %#v", msg)
			}
			msg.TenantID, msg.ChannelAccountID = "tenant-1", "bot-local-1"
			identity, err := BuildSessionIdentity(msg)
			if err != nil {
				t.Fatal(err)
			}
			if (identity.SessionOwnerID == msg.ExternalUserID) == test.group {
				t.Fatal("actor and group owner confused")
			}
		})
	}
	for _, body := range []string{"{}{}", "{", string([]byte{'{', 0xff, '}'})} {
		if _, err := NewWeComBotAdapter("").ParseInbound(httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body)), botBridgeBinding()); err == nil {
			t.Fatal("malformed JSON accepted")
		}
	}
}

func botBridgeReply() *OutboundMessage {
	return &OutboundMessage{ConversationID: "group-2", ReplyToID: "request-7", DeliveryID: "outbox-42-0", Content: "已经处理", ContentType: "text"}
}

func TestWeComBotReplyBridgeContractAndProgress(t *testing.T) {
	binding := botBridgeBinding()
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(previous)
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/reply" || r.Header.Get(WeComBotBridgeTokenHeader) != binding.Token || r.Header.Get("traceparent") == "" {
			t.Error("bridge method/path/auth/trace contract missing")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body) != 5 || body["bot_id"] != binding.AccountID || body["request_id"] != "request-7" || body["conversation_id"] != "group-2" || body["delivery_id"] != "outbox-42-0" || body["content"] != "已经处理" {
			t.Errorf("invalid bridge payload: %#v", body)
		}
		_, _ = io.WriteString(w, `{"status":"sent"}`)
	}))
	defer server.Close()
	a, msg := NewWeComBotAdapter(server.URL), botBridgeReply()
	if err := a.SendReply(ctx, binding, msg); err != nil {
		t.Fatal(err)
	}
	if msg.Metadata[deliveryCompleteKey] != "true" || msg.Metadata[deliveryNextKey] != "1" || calls.Load() != 1 {
		t.Fatalf("missing durable progress: %#v", msg.Metadata)
	}
	msg.Metadata[deliveryCursorKey] = "1"
	if err := a.SendReply(ctx, binding, msg); err == nil || calls.Load() != 1 {
		t.Fatal("completed cursor caused another send")
	}
	if a.SupportsStreaming() || !errors.Is(a.SendStreamChunk(ctx, binding, nil), ErrStreamingUnsupported) {
		t.Fatal("unexpected incremental streaming capability")
	}
}

func TestWeComBotReplyFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name               string
		code               int
		body               string
		unknown, permanent bool
	}{
		{"not dispatched", 503, `{"status":"not_sent"}`, false, false},
		{"quota rejected", 429, `{"status":"not_sent"}`, false, false},
		{"ambiguous quota response", 429, `{}`, true, false},
		{"ambiguous 503", 503, `{"status":"unavailable"}`, true, false},
		{"unknown conflict", 409, `{"status":"unknown"}`, true, false},
		{"bad gateway", 502, `{}`, true, false}, {"timeout", 504, `{}`, true, false},
		{"malformed success", 200, `{}`, true, false}, {"trailing response", 200, `{"status":"sent"}{}`, true, false},
		{"credential rejection", 401, `{}`, false, true}, {"invalid message", 400, `{}`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.code)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			msg := botBridgeReply()
			err := NewWeComBotAdapter(server.URL).SendReply(context.Background(), botBridgeBinding(), msg)
			permanent, _ := DeliveryFailure(err)
			if err == nil || permanent != test.permanent || DeliveryOutcomeUnknown(err) != test.unknown || msg.Metadata[deliveryCompleteKey] != "" {
				t.Fatalf("wrong classification: err=%v permanent=%v unknown=%v", err, permanent, DeliveryOutcomeUnknown(err))
			}
		})
	}
	for _, dispatched := range []bool{false, true} {
		a := NewWeComBotAdapter("http://127.0.0.1:1")
		a.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if dispatched {
				httptrace.ContextClientTrace(r.Context()).GotConn(httptrace.GotConnInfo{})
			}
			return nil, errors.New("secret-bearing transport message")
		})
		err := a.SendReply(context.Background(), botBridgeBinding(), botBridgeReply())
		if err == nil || DeliveryOutcomeUnknown(err) != dispatched || strings.Contains(err.Error(), "secret-bearing") {
			t.Fatalf("transport classification=%v", err)
		}
	}
}

func TestWeComBotBridgeRejectsRedirectAndInvalidPayload(t *testing.T) {
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	a := NewWeComBotAdapter(source.URL)
	if err := a.SendReply(context.Background(), botBridgeBinding(), botBridgeReply()); err == nil || !DeliveryOutcomeUnknown(err) || reached.Load() != 0 {
		t.Fatal("bridge redirect was followed or treated as success")
	}
	for _, mutate := range []func(*OutboundMessage){
		func(m *OutboundMessage) { m.Content = strings.Repeat("a", MaxWeComBotReplyBytes+1) },
		func(m *OutboundMessage) { m.DeliveryID = "" }, func(m *OutboundMessage) { m.ReplyToID = "" },
	} {
		msg := botBridgeReply()
		mutate(msg)
		err := a.SendReply(context.Background(), botBridgeBinding(), msg)
		if permanent, _ := DeliveryFailure(err); err == nil || !permanent {
			t.Fatalf("invalid outbound accepted: %v", err)
		}
	}
	for _, value := range []string{"", "http://user:pass@localhost", "http://localhost/path", "http://localhost?secret=x", "ftp://localhost", "http://localhost:0", "http://localhost/#x"} {
		if ValidateWeComBotBridgeURL(value) == nil {
			t.Errorf("invalid bridge origin accepted: %q", value)
		}
	}
	for _, value := range []string{"http://127.0.0.1:9094", "http://wecom-bot-connector:9094", "https://bridge.example"} {
		if err := ValidateWeComBotBridgeURL(value); err != nil {
			t.Errorf("valid operator origin rejected: %v", err)
		}
	}
}
