package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const fixtureEndpoint = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=credential-canary"

func fixtureBinding() controlplane.ChannelBinding {
	return controlplane.ChannelBinding{ID: "binding", TenantID: "tenant", AppID: "app", AccountID: "bot", ChannelType: ChannelType, Status: controlplane.StatusActive, SecretRef: "env://TEST_MCP_KEY", Version: 1, Config: json.RawMessage(`{"allowed_chat_ids":["group-1"],"allowed_user_ids":["human-1","human-2"],"mention_prefix":"@testbot","timezone":"UTC","start_at":"2026-09-06T00:00:00Z","dedupe_mode":"fingerprint-v1"}`)}
}

func fixtureClient(t *testing.T, business func(string, map[string]any) (any, error)) *http.Client {
	t.Helper()
	return &http.Client{Transport: sampleRoundTripper(func(req *http.Request) (*http.Response, error) {
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if req.Method != http.MethodPost || req.URL.String() != fixtureEndpoint || json.NewDecoder(req.Body).Decode(&rpc) != nil {
			t.Error("unexpected runtime request")
			return nil, errors.New("invalid request")
		}
		var result any
		switch rpc.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "fixture", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
		case "notifications/initialized":
			return &http.Response{StatusCode: 202, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		case "tools/call":
			payload, err := business(rpc.Params.Name, rpc.Params.Arguments)
			if err != nil {
				return nil, err
			}
			body, _ := json.Marshal(payload)
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": string(body)}}}
		default:
			t.Error("unexpected runtime RPC method")
			return nil, errors.New("unapproved method")
		}
		encoded, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": result})
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(encoded)))}, nil
	})}
}

func fixtureMessage(user, text, stamp string) map[string]any {
	return map[string]any{"userid": user, "user_name": "unused-name", "msg_type": "text", "send_time": stamp, "text": map[string]string{"content": text}}
}
func fixturePage(messages []any, more bool, cursor string) any {
	return map[string]any{"errcode": 0, "messages_count": len(messages), "messages": messages, "has_more": more, "next_cursor": cursor, "extra_identity_context": "untrusted-context-never-a-prompt"}
}

func TestReadWindowPaginatesFiltersAndSorts(t *testing.T) {
	b := fixtureBinding()
	from := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	calls := 0
	client := fixtureClient(t, func(name string, args map[string]any) (any, error) {
		calls++
		if name != messagesTool || args["chat_id"] != "group-1" || args["begin_time"] != "2026-09-06 00:00:00" || args["end_time"] != "2026-09-06 00:01:00" {
			t.Error("out of scope")
		}
		if calls == 1 {
			if _, ok := args["cursor"]; ok {
				t.Error("initial cursor not empty")
			}
			return fixturePage([]any{fixtureMessage("human-1", "@testbot later", "2026-09-06 00:00:30"), fixtureMessage("bot-user", "@testbot loop", "2026-09-06 00:00:25"), fixtureMessage("human-1", "@testbot-other ignore", "2026-09-06 00:00:21")}, true, "page-2"), nil
		}
		if args["cursor"] != "page-2" {
			t.Error("cursor changed")
		}
		return fixturePage([]any{fixtureMessage("human-2", "@testbot earlier", "2026-09-06 00:00:10"), fixtureMessage("human-1", "no mention", "2026-09-06 00:00:15"), fixtureMessage("human-1", "@testbot boundary", "2026-09-06 00:01:00")}, false, ""), nil
	})
	a, _ := New(secret.StaticStore{b.SecretRef: fixtureEndpoint}, NewMemoryStore(), client)
	items, err := a.ReadWindow(context.Background(), b, "group-1", from, from.Add(time.Minute))
	if err != nil || calls != 2 || len(items) != 2 {
		t.Fatalf("read: count=%d calls=%d err=%v", len(items), calls, err)
	}
	if items[0].Text != "earlier" || items[1].Text != "later" || items[0].ReplyTarget != "group-1" || !strings.HasPrefix(items[0].ExternalMessageID, "mcp_fp1_") {
		t.Fatal("normalization or ordering failed")
	}
}

func TestReadRejectsUnverifiablePagesAndScopes(t *testing.T) {
	b := fixtureBinding()
	from := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"missing_cursor", "repeated_cursor", "missing_count", "bad_time", "media", "provider_error"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			client := fixtureClient(t, func(string, map[string]any) (any, error) {
				calls++
				m := fixtureMessage("human-1", "@testbot message", "2026-09-06 00:00:01")
				p := fixturePage([]any{m}, false, "").(map[string]any)
				switch kind {
				case "missing_cursor":
					p["has_more"] = true
				case "repeated_cursor":
					p["has_more"] = true
					p["next_cursor"] = "same"
				case "missing_count":
					delete(p, "messages_count")
				case "bad_time":
					m["send_time"] = "invalid"
				case "media":
					m["msg_type"] = "image"
				case "provider_error":
					p["errcode"] = 400
					p["errmsg"] = "credential-canary"
				}
				return p, nil
			})
			a, _ := New(secret.StaticStore{b.SecretRef: fixtureEndpoint}, NewMemoryStore(), client)
			if _, err := a.ReadWindow(context.Background(), b, "group-1", from, from.Add(time.Minute)); err == nil || strings.Contains(err.Error(), "credential-canary") {
				t.Fatal("unsafe page accepted or leaked")
			}
			before := calls
			if _, err := a.ReadWindow(context.Background(), b, "other-group", from, from.Add(time.Minute)); err == nil || calls != before {
				t.Fatal("unapproved group read")
			}
		})
	}
}

func TestFingerprintContractExplicitlyMergesSameSecondIdenticalText(t *testing.T) {
	b := fixtureBinding()
	cfg, _ := ParseBinding(b)
	from := cfg.Start()
	one := fixtureMessage("human-1", "@testbot repeated", "2026-09-06 00:00:01")
	two := fixtureMessage("human-1", "@testbot repeated", "2026-09-06 00:00:01")
	two["user_name"] = "renamed"
	three := fixtureMessage("human-2", "@testbot repeated", "2026-09-06 00:00:01")
	raw, _ := json.Marshal(fixturePage([]any{one, two, three}, false, ""))
	items, _, _, _, err := decodePage(raw, b, cfg, "group-1", from, from.Add(time.Minute))
	if err != nil || len(items) != 3 || items[0].ExternalMessageID != items[1].ExternalMessageID || items[0].ExternalMessageID == items[2].ExternalMessageID {
		t.Fatal("fingerprint contract changed")
	}
}

func TestDisplayNameSpacesAndAdjacentMentionAreExplicit(t *testing.T) {
	b := fixtureBinding()
	cfg, _ := ParseBinding(b)
	cfg.MentionPrefix = "@Agent 智能助手"
	cfg.MentionStyle = "prefix"
	b.Config, _ = json.Marshal(cfg)
	if _, err := ParseBinding(b); err != nil {
		t.Fatal("display name with spaces rejected")
	}
	from := cfg.Start()
	raw, _ := json.Marshal(fixturePage([]any{fixtureMessage("human-1", "@Agent 智能助手你好", "2026-09-06 00:00:01")}, false, ""))
	items, _, _, _, err := decodePage(raw, b, cfg, "group-1", from, from.Add(time.Minute))
	if err != nil || len(items) != 1 || items[0].Text != "你好" {
		t.Fatal("adjacent UI mention not decoded")
	}
	cfg.MentionStyle = "whitespace"
	if items, _, _, _, err := decodePage(raw, b, cfg, "group-1", from, from.Add(time.Minute)); err != nil || len(items) != 0 {
		t.Fatal("strict mention mode implicitly relaxed")
	}
}

func TestBatchQuarantinesIndividualRecordsWithoutLosingText(t *testing.T) {
	b := fixtureBinding()
	cfg, _ := ParseBinding(b)
	from := cfg.Start()
	media := fixtureMessage("human-1", "secret-media-canary", "2026-09-06 00:00:02")
	media["msg_type"] = "image"
	broken := fixtureMessage("human-1", "secret-canary", "2026-09-06 00:00:03")
	broken["text"] = 123
	raw, _ := json.Marshal(fixturePage([]any{fixtureMessage("human-1", "@testbot first", "2026-09-06 00:00:01"), media, broken, 123, fixtureMessage("other-human", "ignored", "bad-date"), fixtureMessage("human-1", "@testbot last", "2026-09-06 00:00:04")}, false, ""))
	batch, _, _, count, err := decodePageBatch(raw, b, cfg, "group-1", from, from.Add(time.Minute))
	if err != nil || count != 6 || len(batch.Messages) != 2 || len(batch.Rejected) != 3 {
		t.Fatalf("batch lost messages: %+v %v", batch, err)
	}
	encoded, _ := json.Marshal(batch.Rejected)
	if strings.Contains(string(encoded), "canary") || strings.Contains(string(encoded), "human-1") {
		t.Fatal("quarantine leaked message content or sender")
	}
	for _, r := range batch.Rejected {
		if !validRejection(r) {
			t.Fatal("invalid rejection metadata")
		}
	}
}

type finishFailStore struct{ Store }

func (s finishFailStore) FinishDelivery(context.Context, DeliveryKey, string, string) error {
	return errors.New("database unavailable")
}

func TestSenderNeverResendsKnownOrUnknownAttempts(t *testing.T) {
	for _, kind := range []string{"sent", "rejected", "unknown", "missing_receipt", "save_failure"} {
		t.Run(kind, func(t *testing.T) {
			b := fixtureBinding()
			calls := 0
			var store Store = NewMemoryStore()
			if kind == "save_failure" {
				store = finishFailStore{store}
			}
			client := fixtureClient(t, func(name string, args map[string]any) (any, error) {
				calls++
				if name != replyTool || args["chat_id"] != "group-1" || args["msg_type"] != "markdown" {
					t.Error("unexpected sender scope")
				}
				if kind == "unknown" {
					return nil, errors.New("connection lost credential-canary")
				}
				if kind == "rejected" {
					return map[string]any{"errcode": 42, "success": false, "errmsg": "credential-canary"}, nil
				}
				if kind == "missing_receipt" {
					return map[string]any{"errcode": 0}, nil
				}
				return map[string]any{"errcode": 0, "success": true}, nil
			})
			a, _ := New(secret.StaticStore{b.SecretRef: fixtureEndpoint}, store, client)
			msg := channels.OutboundMessage{OutboundID: "outbound-1", Text: "reply", ReplyTarget: "group-1"}
			for i := 0; i < 2; i++ {
				receipt, err := a.Send(context.Background(), b, msg)
				if kind == "sent" {
					if err != nil || receipt.ProviderMessageID != "" || receipt.SentAt.IsZero() {
						t.Fatal("invalid success receipt")
					}
				} else {
					var delivery *channels.DeliveryError
					if !errors.As(err, &delivery) || delivery.Retryable || delivery.Unknown != (kind != "rejected") || strings.Contains(err.Error(), "credential-canary") {
						t.Fatal("invalid delivery classification")
					}
				}
			}
			if calls != 1 {
				t.Fatalf("sent %d times", calls)
			}
			msg.Text = "different"
			if _, err := a.Send(context.Background(), b, msg); err == nil || calls != 1 {
				t.Fatal("conflicting reuse not rejected")
			}
			msg.OutboundID = "new"
			msg.ReplyTarget = "other-group"
			if _, err := a.Send(context.Background(), b, msg); err == nil || calls != 1 {
				t.Fatal("unapproved target not rejected")
			}
		})
	}
}

func TestTenantPurposeGrantsBeforeAnyNetwork(t *testing.T) {
	b := fixtureBinding()
	t.Setenv("TEST_MCP_KEY", fixtureEndpoint)
	grants, _ := secret.NewEnvStore([]secret.Grant{{TenantID: b.TenantID, Purpose: secret.WeComMCPRead, Reference: b.SecretRef}})
	calls := 0
	a, _ := New(grants, NewMemoryStore(), fixtureClient(t, func(string, map[string]any) (any, error) { calls++; return fixturePage(nil, false, ""), nil }))
	if _, err := a.Send(context.Background(), b, channels.OutboundMessage{OutboundID: "out", Text: "hello", ReplyTarget: "group-1"}); err == nil || calls != 0 {
		t.Fatal("read grant allowed send")
	}
	b.TenantID = "other-tenant"
	cfg, _ := ParseBinding(b)
	if _, err := a.ReadWindow(context.Background(), b, "group-1", cfg.Start(), cfg.Start().Add(time.Minute)); err == nil || calls != 0 {
		t.Fatal("cross-tenant secret read")
	}
}
