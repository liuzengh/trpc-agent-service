package ilink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

func TestILinkPollCursorAndContextToken(t *testing.T) {
	t.Setenv("ILINK_BOT_TOKEN", "bot-token")
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	var mu sync.Mutex
	requestCursor := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/getupdates" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer bot-token" ||
			r.Header.Get("AuthorizationType") != "ilink_bot_token" {
			t.Errorf("authorization headers are invalid")
		}
		var body struct {
			Cursor string `json:"get_updates_buf"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode poll body: %v", err)
		}
		mu.Lock()
		requestCursor = body.Cursor
		mu.Unlock()
		writeILinkJSON(w, map[string]any{
			"ret": 0,
			"msgs": []any{
				map[string]any{
					"message_id":    101,
					"from_user_id":  "wx-user",
					"message_type":  1,
					"context_token": "ctx-101",
					"item_list": []any{
						map[string]any{
							"type": 1, "text_item": map[string]string{"text": "hello"},
						},
					},
				},
			},
			"get_updates_buf": "cursor-2",
		})
	}))
	defer server.Close()
	snapshot := testILinkSnapshot()
	adapter := newTestAdapter(t, snapshot, redisClient, server.Client(), server.URL)
	var inbound gateway.InboundMessage
	err := adapter.pollOnce(context.Background(), "bot-a", func(
		_ context.Context,
		message gateway.InboundMessage,
	) (gateway.Result, error) {
		inbound = message
		return gateway.Result{Outcome: gateway.OutcomeIngested}, nil
	})
	if err != nil {
		t.Fatalf("pollOnce() error = %v", err)
	}
	if inbound.MsgID != "101" || inbound.SenderID != "wx-user" || inbound.Text != "hello" {
		t.Fatalf("inbound = %#v", inbound)
	}
	if got, err := redisClient.Get(context.Background(), cursorKey("binding-a")).Result(); err != nil || got != "cursor-2" {
		t.Fatalf("cursor = %q, %v", got, err)
	}
	sessionID := "tenant-a:ilink:p2p:wx-user"
	if got, err := redisClient.Get(context.Background(), contextKey(sessionID)).Result(); err != nil || got != "ctx-101" {
		t.Fatalf("context token = %q, %v", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requestCursor != "" {
		t.Fatalf("initial request cursor = %q, want empty", requestCursor)
	}
}

func TestILinkReplierUsesCachedContext(t *testing.T) {
	t.Setenv("ILINK_BOT_TOKEN", "bot-token")
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	sessionID := "tenant-a:ilink:p2p:wx-user"
	if err := redisClient.Set(
		context.Background(), contextKey(sessionID), "ctx-cache", contextTTL,
	).Err(); err != nil {
		t.Fatalf("seed context token: %v", err)
	}
	var sent struct {
		Message struct {
			ToUserID     string `json:"to_user_id"`
			ContextToken string `json:"context_token"`
			Items        []struct {
				Text struct {
					Text string `json:"text"`
				} `json:"text_item"`
			} `json:"item_list"`
		} `json:"msg"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Errorf("decode send body: %v", err)
		}
		writeILinkJSON(w, map[string]any{"ret": 0})
	}))
	defer server.Close()
	snapshot := testILinkSnapshot()
	adapter := newTestAdapter(t, snapshot, redisClient, server.Client(), server.URL)
	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta", Text: "reply"}
	events <- reply.Event{Type: "done"}
	close(events)
	err := adapter.NewReplier(snapshot).Reply(
		context.Background(),
		sessionID,
		gateway.InboundMessage{SenderID: "wx-user"},
		events,
	)
	if err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	if sent.Message.ToUserID != "wx-user" ||
		sent.Message.ContextToken != "ctx-cache" ||
		len(sent.Message.Items) != 1 ||
		sent.Message.Items[0].Text.Text != "reply" {
		t.Fatalf("sent message = %#v", sent)
	}
}

func TestILinkContextExpiry(t *testing.T) {
	t.Setenv("ILINK_BOT_TOKEN", "bot-token")
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	adapter := newTestAdapter(t, testILinkSnapshot(), redisClient, nil, "")
	events := make(chan reply.Event)
	close(events)
	err := adapter.NewReplier(testILinkSnapshot()).Reply(
		context.Background(),
		"tenant-a:ilink:p2p:wx-user",
		gateway.InboundMessage{SenderID: "wx-user"},
		events,
	)
	if err != ErrContextExpired {
		t.Fatalf("Reply() error = %v, want ErrContextExpired", err)
	}
}

func TestILinkRunStopsWithContext(t *testing.T) {
	t.Setenv("ILINK_BOT_TOKEN", "bot-token")
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeILinkJSON(w, map[string]any{"ret": 0, "get_updates_buf": "cursor"})
	}))
	defer server.Close()
	adapter := newTestAdapter(
		t, testILinkSnapshot(), redisClient, server.Client(), server.URL,
	)
	adapter.routeKeys = []string{"bot-a"}
	adapter.backoff = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	if err := adapter.Run(ctx, nil, func(
		context.Context,
		gateway.InboundMessage,
	) (gateway.Result, error) {
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	cancel()
	adapter.Wait()
}

func newTestAdapter(
	t *testing.T,
	snapshot tenant.Snapshot,
	redisClient redis.Cmdable,
	client *http.Client,
	baseURL string,
) *Adapter {
	t.Helper()
	adapter, err := New(
		&ilinkCache{snapshot: snapshot}, redisClient, client, baseURL, nil,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func testILinkSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: channelType, RouteKey: "bot-a", IsActive: true,
			Config: map[string]string{"bot_token": "env:ILINK_BOT_TOKEN"},
		},
	}
}

type ilinkCache struct {
	snapshot tenant.Snapshot
	err      error
}

func (i *ilinkCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return i.snapshot, i.err
}
func (i *ilinkCache) Invalidate(string, string) {}

func writeILinkJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

var _ channels.Adapter = (*Adapter)(nil)
