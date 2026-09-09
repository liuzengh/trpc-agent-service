package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestLoadBotConfigUsesPlatformCredentialsOnly(t *testing.T) {
	values := map[string]string{
		"TRPC_TELEGRAM_BOT_USERNAME": "agent_bot",
		"TRPC_TELEGRAM_BOT_TOKEN":    "telegram-token",
		"TRPC_WECOM_BOT_ID":          "wecom-bot-id",
		"TRPC_WECOM_BOT_SECRET":      "wecom-secret",
	}
	config := LoadBotConfig(func(key string) string { return values[key] })
	if config.TelegramUsername != "agent_bot" || config.TelegramToken != "telegram-token" || config.WeComBotID != "wecom-bot-id" || config.WeComSecret != "wecom-secret" {
		t.Fatalf("config = %#v", config)
	}
}

func TestProviderRouteAPIIsPlatformAdminOnly(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	runtime := NewProviderRuntime(BotConfig{}, NewBotTenantAllowlist(), nil)
	client.handler.ConfigureProviderRuntime(runtime)
	var route BotRoute
	client.post("/api/v1/admin/providers/routes", `{"provider":"telegram","external_subject":"123","tenant_id":"tenant-one","app_id":"app-one","conversation_type":"group"}`, nil, http.StatusCreated, &route)
	if route.ExternalSubject != "123" || route.TenantID != "tenant-one" {
		t.Fatalf("route = %#v", route)
	}
	if !route.Enabled {
		t.Fatal("new provider route should be enabled")
	}
	response := client.do(http.MethodPatch, "/api/v1/admin/providers/routes?provider=telegram&external_subject=123", `{"tenant_id":"tenant-one","app_id":"app-one","conversation_type":"group","enabled":false}`, nil)
	var disabled BotRoute
	decodeResponse(t, response, http.StatusOK, &disabled)
	if disabled.Enabled {
		t.Fatalf("updated route = %#v", disabled)
	}
	if _, ok := runtime.Routes().Resolve(ChannelTelegram, "123"); ok {
		t.Fatal("disabled provider route resolved")
	}
	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response = client.do(http.MethodPost, "/api/v1/admin/providers/routes", `{"provider":"telegram","external_subject":"456","tenant_id":"tenant-two","app_id":"app-one"}`, nil)
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer create status = %d", response.StatusCode)
	}
}

func TestProviderReplayUsesAllowlistAndDoesNotRequireCredentials(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	runtime := NewProviderRuntime(BotConfig{}, NewBotTenantAllowlist(), nil)
	client.handler.ConfigureProviderRuntime(runtime)
	client.post("/api/v1/admin/providers/routes", `{"provider":"telegram","external_subject":"123","tenant_id":"tenant-one","app_id":"app-one","conversation_type":"single"}`, nil, http.StatusCreated, nil)
	var accepted chatRunResponse
	client.post("/api/v1/admin/providers/replay", `{"provider":"telegram","external_subject":"123","text":"hello-replay"}`, nil, http.StatusAccepted, &accepted)
	if accepted.SessionID == "" || accepted.RequestID == "" {
		t.Fatalf("accepted replay = %#v", accepted)
	}
	if err := waitForChatEvent(client, accepted.SessionID, "channel.reply"); err != nil {
		t.Fatal(err)
	}
	response := client.do(http.MethodGet, "/api/v1/admin/providers/deliveries", "", nil)
	var deliveries struct {
		Items []ProviderDelivery `json:"items"`
	}
	decodeResponse(t, response, http.StatusOK, &deliveries)
	deadline := time.Now().Add(time.Second)
	for len(deliveries.Items) != 1 || deliveries.Items[0].Status != "delivered" {
		if time.Now().After(deadline) {
			t.Fatalf("deliveries = %#v", deliveries.Items)
		}
		time.Sleep(time.Millisecond)
		response = client.do(http.MethodGet, "/api/v1/admin/providers/deliveries", "", nil)
		decodeResponse(t, response, http.StatusOK, &deliveries)
	}
	if deliveries.Items[0].RequestID != accepted.RequestID {
		t.Fatalf("delivery request = %q, want %q", deliveries.Items[0].RequestID, accepted.RequestID)
	}
	events := chatEventsForTest(t, client, accepted.SessionID)
	for _, event := range events {
		if event.Type == "channel.reply" && strings.Contains(string(event.Payload), "echo:hello-replay") {
			client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
			response = client.do(http.MethodGet, "/api/v1/admin/providers/deliveries", "", nil)
			decodeResponse(t, response, http.StatusOK, &deliveries)
			if len(deliveries.Items) != 0 {
				t.Fatalf("cross-tenant deliveries = %#v", deliveries.Items)
			}
			return
		}
	}
	t.Fatalf("replay events = %#v", events)
}

func TestProviderStatusIsFilteredByTenantRoutes(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelEnterpriseWeChat, ExternalSubject: "chat-two", TenantID: "tenant-two", AppID: "app-two"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	runtime.Start(context.Background())
	defer runtime.Close()
	client.post("/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-two"}`, nil, http.StatusOK, nil)
	response := client.do(http.MethodGet, "/api/v1/admin/providers/status", "", nil)
	var statuses struct {
		Items []BotStatus `json:"items"`
	}
	decodeResponse(t, response, http.StatusOK, &statuses)
	if len(statuses.Items) != 1 || statuses.Items[0].Provider != ChannelEnterpriseWeChat {
		t.Fatalf("tenant provider statuses = %#v", statuses.Items)
	}
}

func TestMalformedProviderMessagesAreRejectedWithoutRunnerExecution(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	runtime := NewProviderRuntime(BotConfig{}, nil, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", []byte(`{"update_id":41,"message":{}}`)); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("telegram error = %v", err)
	}
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelEnterpriseWeChat, "bot", []byte(`{"cmd":"aibot_msg_callback","body":{}}`)); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("wecom error = %v", err)
	}
	deliveries := runtime.Deliveries("")
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %#v", deliveries)
	}
	for _, delivery := range deliveries {
		if delivery.Status != "rejected" || delivery.Code != "callback_invalid" || delivery.RequestID == "" {
			t.Fatalf("invalid delivery = %#v", delivery)
		}
	}
}

func TestTelegramPollAdvancesOffsetForMalformedUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "redacted"}, nil, func(_ context.Context, _ string, _ string, _ []byte) error {
		cancel()
		return ErrProviderMessageIgnored
	})
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"ok":true,"result":[{"update_id":17,"message":{}}]}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(body)), Header: make(http.Header)}, nil
	})}
	offset := int64(0)
	if err := runtime.pollTelegram(ctx, &offset); !errors.Is(err, context.Canceled) {
		t.Fatalf("poll error = %v", err)
	}
	if offset != 18 {
		t.Fatalf("offset = %d", offset)
	}
}

func TestProviderInboundReservationCanBeReleasedAfterStartupFailure(t *testing.T) {
	runtime := NewProviderRuntime(BotConfig{}, nil, nil)
	route := BotRoute{Provider: ChannelTelegram, ExternalSubject: "chat-one"}
	if code := runtime.acceptInbound(route, "message-one", 7); code != "" {
		t.Fatalf("first accept code = %q", code)
	}
	runtime.releaseInbound(route, "message-one")
	if code := runtime.acceptInbound(route, "message-one", 7); code != "" {
		t.Fatalf("accept after release code = %q", code)
	}
}

type longProviderReplyRunner struct{}

func (longProviderReplyRunner) Run(context.Context, RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{Output: strings.Repeat("x", 4097)}, nil
}

func TestProviderReplyLengthFailureUpdatesLatestDelivery(t *testing.T) {
	client := newChannelTestClient(t, longProviderReplyRunner{})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "unused"}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	body := []byte(`{"update_id":30,"message":{"message_id":30,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", body); err != nil {
		t.Fatal(err)
	}
	if err := waitForChatEvent(client, providerSessionID(ChannelTelegram, "bot", "123"), "run.completed"); err != nil {
		t.Fatal(err)
	}
	deliveries := runtime.Deliveries("tenant-one")
	if len(deliveries) != 1 || deliveries[0].Status != "terminal_failed" || deliveries[0].Code != "channel_message_too_long" {
		t.Fatalf("deliveries = %#v", deliveries)
	}
}

func TestTelegramProviderMessageRoutesToRunner(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "agent_bot", body); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-runs:
		if request.TenantID != "tenant-one" || request.AppID != "app-one" || request.UserID != "456" || request.Input != "hello" {
			t.Fatalf("request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("provider message did not reach Runner")
	}
}

func TestTelegramProviderMessageRepliesWithAgentOutput(t *testing.T) {
	requests := make(chan map[string]any, 1)
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "test-token"}, NewBotTenantAllowlist(), nil)
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/bottest-token/sendMessage" {
			t.Fatalf("request path = %q", request.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		requests <- payload
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true,"result":{"message_id":10}}`)), Header: make(http.Header)}, nil
	})}
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	if err := runtime.Routes().Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle}); err != nil {
		t.Fatal(err)
	}
	client.handler.ConfigureProviderRuntime(runtime)

	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "agent_bot", body); err != nil {
		t.Fatal(err)
	}

	select {
	case payload := <-requests:
		if payload["chat_id"] != "123" || payload["text"] != "echo:hello" {
			t.Fatalf("sendMessage payload = %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("Telegram reply was not sent")
	}
}

func TestTelegramSendRetriesRateLimitAndRecordsAttempts(t *testing.T) {
	attempts := 0
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "test-token"}, nil, nil)
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewBufferString(`{"ok":false,"parameters":{"retry_after":0}}`)), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true}`)), Header: make(http.Header)}, nil
	})}
	binding := ChannelBinding{Channel: ChannelTelegram, ConversationID: "123", TenantID: "tenant-one", AppID: "app-one"}
	reply := ChannelReply{MessageID: "request-one", Text: "hello"}
	if err := runtime.sendTelegram(context.Background(), binding, reply); err != nil {
		t.Fatal(err)
	}
	deliveries := runtime.Deliveries("tenant-one")
	if attempts != 2 || len(deliveries) != 1 || deliveries[0].Status != "delivered" || deliveries[0].Attempts != 2 {
		t.Fatalf("attempts=%d deliveries=%#v", attempts, deliveries)
	}
}

func TestTelegramSendReportsRetryExhaustionAndCancellation(t *testing.T) {
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "test-token"}, nil, nil)
	runtime.telegramRequestTimeout = time.Millisecond
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	binding := ChannelBinding{Channel: ChannelTelegram, ConversationID: "123", TenantID: "tenant-one", AppID: "app-one"}
	reply := ChannelReply{MessageID: "request-timeout", Text: "hello"}
	if err := runtime.sendTelegram(context.Background(), binding, reply); channelErrorCode(err) != "retry_exhausted" {
		t.Fatalf("timeout error = %v", err)
	}
	if got := runtime.Deliveries("tenant-one"); len(got) != 1 || got[0].Code != "retry_exhausted" || got[0].Attempts != 3 {
		t.Fatalf("timeout delivery = %#v", got)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	reply.MessageID = "request-cancelled"
	if err := runtime.sendTelegram(cancelled, binding, reply); channelErrorCode(err) != "cancelled" {
		t.Fatalf("cancelled error = %v", err)
	}
	if got := runtime.Deliveries("tenant-one"); len(got) != 1 || got[0].Code != "cancelled" || got[0].Attempts != 1 {
		t.Fatalf("cancelled delivery = %#v", got)
	}
}

func TestTelegramPollDoesNotAdvanceOffsetWhenProcessingFails(t *testing.T) {
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "redacted"}, nil, func(context.Context, string, string, []byte) error { return context.Canceled })
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"ok": true, "result": []any{map[string]any{"update_id": 7}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	offset := int64(0)
	if err := runtime.pollTelegram(context.Background(), &offset); err == nil || offset != 0 {
		t.Fatalf("error=%v offset=%d", err, offset)
	}
}

func TestTelegramPollAdvancesOffsetForUnsupportedMedia(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "redacted"}, nil, func(_ context.Context, _ string, _ string, body []byte) error {
		calls++
		cancel()
		return ErrProviderMessageIgnored
	})
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"ok":true,"result":[{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"photo":[{"file_id":"photo"}]}}]}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(body)), Header: make(http.Header)}, nil
	})}
	offset := int64(0)
	if err := runtime.pollTelegram(ctx, &offset); !errors.Is(err, context.Canceled) {
		t.Fatalf("poll error = %v", err)
	}
	if calls != 1 || offset != 8 {
		t.Fatalf("calls=%d offset=%d", calls, offset)
	}
}

func TestTelegramPrivateChatFallsBackToAllowlistedUser(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "456", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)
	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123,"type":"private"},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "agent_bot", body); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-runs:
		if request.UserID != "456" || request.Input != "hello" {
			t.Fatalf("request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("private-chat user fallback did not reach Runner")
	}
}

func TestProviderMessagesExposeDisabledUnmappedAndUnsupportedOutcomes(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Update(ChannelTelegram, "123", BotRoute{TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "124", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{}, routes, nil)
	client.handler.ConfigureProviderRuntime(runtime)

	disabled := []byte(`{"update_id":10,"message":{"message_id":10,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", disabled); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("disabled error = %v", err)
	}
	unmapped := []byte(`{"update_id":11,"message":{"message_id":11,"chat":{"id":999},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", unmapped); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("unmapped error = %v", err)
	}
	media := []byte(`{"update_id":12,"message":{"message_id":12,"chat":{"id":124},"from":{"id":456},"photo":[{"file_id":"photo"}]}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", media); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("media error = %v", err)
	}

	deliveries := runtime.Deliveries("")
	codes := map[string]bool{}
	for _, delivery := range deliveries {
		if delivery.Status == "rejected" {
			codes[delivery.Code] = true
		}
	}
	for _, code := range []string{"disabled", "unmapped", "unsupported_media"} {
		if !codes[code] {
			t.Fatalf("missing %q in deliveries %#v", code, deliveries)
		}
	}
}

func TestProviderMessagesRejectDuplicateAndOutOfOrderUpdates(t *testing.T) {
	runs := make(chan RunnerRequest, 3)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "123", TenantID: "tenant-one", AppID: "app-one"}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{TelegramToken: "test-token"}, routes, nil)
	sends := make(chan struct{}, 3)
	runtime.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		sends <- struct{}{}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true}`)), Header: make(http.Header)}, nil
	})}
	client.handler.ConfigureProviderRuntime(runtime)
	first := []byte(`{"update_id":20,"message":{"message_id":20,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("first update did not reach Runner")
	}
	if err := waitForChatEvent(client, providerSessionID(ChannelTelegram, "bot", "123"), "run.completed"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sends:
	case <-time.After(time.Second):
		t.Fatal("first reply was not sent")
	}
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", first); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("duplicate error = %v", err)
	}
	select {
	case <-sends:
	case <-time.After(time.Second):
		t.Fatal("persisted reply was not resent for duplicate update")
	}
	outOfOrder := []byte(`{"update_id":19,"message":{"message_id":21,"chat":{"id":123},"from":{"id":456},"text":"late"}}`)
	if err := client.handler.ProcessProviderMessage(context.Background(), ChannelTelegram, "bot", outOfOrder); !errors.Is(err, ErrProviderMessageIgnored) {
		t.Fatalf("out-of-order error = %v", err)
	}
	if got := runtime.Deliveries("tenant-one"); len(got) != 1 || got[0].Code != "out_of_order" {
		t.Fatalf("out-of-order delivery = %#v", got)
	}
	select {
	case request := <-runs:
		t.Fatalf("rejected update reached Runner: %#v", request)
	case <-time.After(50 * time.Millisecond):
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestBotTenantAllowlistRejectsConflictingRoute(t *testing.T) {
	allowlist := NewBotTenantAllowlist()
	route := BotRoute{Provider: ChannelTelegram, ExternalSubject: "chat-1", TenantID: "tenant-one", AppID: "app-one"}
	if err := allowlist.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if err := allowlist.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "chat-1", TenantID: "tenant-two", AppID: "app-two"}); err == nil {
		t.Fatal("expected conflicting route to be rejected")
	}
	got, ok := allowlist.Resolve(ChannelTelegram, "chat-1")
	if !ok || got.TenantID != "tenant-one" {
		t.Fatalf("route = %#v/%v", got, ok)
	}
}

func TestBotTenantAllowlistSeparatesProviderAccounts(t *testing.T) {
	allowlist := NewBotTenantAllowlist()
	routes := []BotRoute{
		{Provider: ChannelTelegram, ProviderAccount: "bot-a", ExternalSubject: "shared-chat", TenantID: "tenant-one", AppID: "app-one"},
		{Provider: ChannelTelegram, ProviderAccount: "bot-b", ExternalSubject: "shared-chat", TenantID: "tenant-two", AppID: "app-two"},
	}
	for _, route := range routes {
		if err := allowlist.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range routes {
		got, ok := allowlist.ResolveAccount(want.Provider, want.ProviderAccount, want.ExternalSubject)
		if !ok || got.TenantID != want.TenantID || got.AppID != want.AppID {
			t.Fatalf("route for %s = %#v/%v", want.ProviderAccount, got, ok)
		}
	}
	if _, ok := allowlist.Resolve(ChannelTelegram, "shared-chat"); ok {
		t.Fatal("account-neutral lookup must not choose between multiple Bot accounts")
	}
}

func TestProviderSessionIDSeparatesAccountsAndConversations(t *testing.T) {
	base := providerSessionID(ChannelTelegram, "bot-a", "chat-a")
	if base != providerSessionID(ChannelTelegram, "bot-a", "chat-a") {
		t.Fatal("provider session ID is not deterministic")
	}
	for _, candidate := range []string{
		providerSessionID(ChannelTelegram, "bot-b", "chat-a"),
		providerSessionID(ChannelTelegram, "bot-a", "chat-b"),
		providerSessionID(ChannelEnterpriseWeChat, "bot-a", "chat-a"),
	} {
		if candidate == base {
			t.Fatalf("provider session collision: %q", candidate)
		}
	}
}

func TestBotTenantAllowlistPersistsDisableUpdateAndDelete(t *testing.T) {
	path := t.TempDir() + "/bot-routes.json"
	allowlist, err := NewPersistentBotTenantAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := allowlist.Upsert(BotRoute{Provider: ChannelTelegram, ExternalSubject: "chat-1", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle}); err != nil {
		t.Fatal(err)
	}
	if _, ok := allowlist.Resolve(ChannelTelegram, "chat-1"); !ok {
		t.Fatal("new route should be enabled")
	}
	if _, err := allowlist.Update(ChannelTelegram, "chat-1", BotRoute{TenantID: "tenant-one", AppID: "app-two", ConversationType: ConversationGroup, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, ok := allowlist.Resolve(ChannelTelegram, "chat-1"); ok {
		t.Fatal("disabled route should not resolve")
	}

	reloaded, err := NewPersistentBotTenantAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	items := reloaded.List()
	if len(items) != 1 || items[0].Enabled || items[0].AppID != "app-two" || items[0].ConversationType != ConversationGroup {
		t.Fatalf("reloaded routes = %#v", items)
	}
	if err := reloaded.Delete(ChannelTelegram, "chat-1"); err != nil {
		t.Fatal(err)
	}
	afterDelete, err := NewPersistentBotTenantAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDelete.List()) != 0 {
		t.Fatalf("routes after delete = %#v", afterDelete.List())
	}
}

func TestProviderRoutesShareControlPlaneAcrossHandlers(t *testing.T) {
	store := NewInMemoryControlPlane()
	identity := DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", TenantName: "One", Role: RolePlatformAdmin}}}
	handlerA := NewAdminHandler(store, identity)
	handlerB := NewAdminHandler(store, identity)
	runtimeA := NewProviderRuntime(BotConfig{}, NewBotTenantAllowlist(), nil)
	runtimeB := NewProviderRuntime(BotConfig{}, NewBotTenantAllowlist(), nil)
	handlerA.ConfigureProviderRuntime(runtimeA)
	handlerB.ConfigureProviderRuntime(runtimeB)
	route := BotRoute{Provider: ChannelTelegram, ExternalSubject: "shared-chat", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationSingle}
	if err := runtimeB.Routes().Upsert(route); err != nil {
		t.Fatal(err)
	}
	got, ok, err := runtimeA.Routes().ResolveContext(context.Background(), route.Provider, route.ExternalSubject)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.TenantID != route.TenantID || got.AppID != route.AppID {
		t.Fatalf("shared route = %#v/%v", got, ok)
	}
	if _, err := runtimeB.Routes().Update(route.Provider, route.ExternalSubject, BotRoute{TenantID: route.TenantID, AppID: route.AppID, ConversationType: route.ConversationType, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := runtimeA.Routes().ResolveContext(context.Background(), route.Provider, route.ExternalSubject); err != nil || !ok || got.Enabled {
		t.Fatalf("shared disabled route = %#v/%v, err=%v", got, ok, err)
	}
}

func TestProviderRuntimeMissingCredentialsStaysAvailable(t *testing.T) {
	runtime := NewProviderRuntime(BotConfig{}, nil, nil)
	runtime.Start(context.Background())
	defer runtime.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		statuses := runtime.Statuses()
		if len(statuses) == 2 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("statuses = %#v", runtime.Statuses())
}

func TestWeComSmartBotReplayAuthenticatesAndReplies(t *testing.T) {
	replies := make(chan map[string]any, 1)
	server := httptest.NewServer(websocket.Handler(func(connection *websocket.Conn) {
		var subscribe map[string]any
		if err := websocket.JSON.Receive(connection, &subscribe); err != nil {
			return
		}
		headers := subscribe["headers"].(map[string]any)
		body := subscribe["body"].(map[string]any)
		if subscribe["cmd"] != "aibot_subscribe" || body["bot_id"] != "bot-id" || body["secret"] != "bot-secret" {
			t.Errorf("subscribe frame = %#v", subscribe)
			return
		}
		_ = websocket.JSON.Send(connection, map[string]any{"headers": headers, "errcode": 0, "errmsg": "ok"})
		_ = websocket.JSON.Send(connection, map[string]any{
			"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": "callback-1"},
			"body": map[string]any{"msgid": "message-1", "aibotid": "bot-id", "chatid": "chat-1", "chattype": "group", "from": map[string]string{"userid": "user-1"}, "msgtype": "text", "text": map[string]string{"content": "hello"}},
		})
		var reply map[string]any
		if err := websocket.JSON.Receive(connection, &reply); err != nil {
			return
		}
		replies <- reply
		_ = websocket.JSON.Send(connection, map[string]any{"headers": reply["headers"], "errcode": 0, "errmsg": "ok"})
	}))
	defer server.Close()

	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelEnterpriseWeChat, ExternalSubject: "chat-1", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationGroup}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{WeComBotID: "bot-id", WeComSecret: "bot-secret"}, routes, client.handler.ProcessProviderMessage)
	runtime.wecomEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	runtime.wecomOrigin = server.URL
	client.handler.ConfigureProviderRuntime(runtime)
	runtime.Start(context.Background())
	defer runtime.Close()

	select {
	case reply := <-replies:
		if reply["cmd"] != "aibot_respond_msg" || reply["headers"].(map[string]any)["req_id"] != "callback-1" {
			t.Fatalf("reply frame = %#v", reply)
		}
		body := reply["body"].(map[string]any)
		if body["msgtype"] != "markdown" || body["markdown"].(map[string]any)["content"] != "echo:hello" {
			t.Fatalf("reply body = %#v", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WeCom reply was not sent")
	}
}

func TestWeComConnectionClosesWhenParentContextIsCancelled(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(websocket.Handler(func(connection *websocket.Conn) {
		defer close(closed)
		var subscribe map[string]any
		if websocket.JSON.Receive(connection, &subscribe) != nil {
			return
		}
		_ = websocket.JSON.Send(connection, map[string]any{"headers": subscribe["headers"], "errcode": 0})
		var frame map[string]any
		_ = websocket.JSON.Receive(connection, &frame)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	runtime := NewProviderRuntime(BotConfig{WeComBotID: "bot-id", WeComSecret: "bot-secret"}, nil, func(context.Context, string, string, []byte) error { return nil })
	runtime.wecomEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	runtime.wecomOrigin = server.URL
	runtime.Start(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		statuses := runtime.Statuses()
		if len(statuses) > 0 && statuses[0].Status == ProviderConnected {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("WeCom connection remained open after parent cancellation")
	}
	runtime.Close()
	runtime.Close()
}

func TestWeComReplyTimeoutIsRecordedAsTerminalFailure(t *testing.T) {
	replyReceived := make(chan struct{})
	var replySignalled atomic.Bool
	server := httptest.NewServer(websocket.Handler(func(connection *websocket.Conn) {
		var subscribe map[string]any
		if websocket.JSON.Receive(connection, &subscribe) != nil {
			return
		}
		_ = websocket.JSON.Send(connection, map[string]any{"headers": subscribe["headers"], "errcode": 0})
		_ = websocket.JSON.Send(connection, map[string]any{
			"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": "timeout-callback"},
			"body": map[string]any{"msgid": "timeout-message", "aibotid": "bot-id", "chatid": "chat-timeout", "chattype": "group", "from": map[string]string{"userid": "user-1"}, "msgtype": "text", "text": map[string]string{"content": "hello"}},
		})
		var reply map[string]any
		if websocket.JSON.Receive(connection, &reply) == nil && replySignalled.CompareAndSwap(false, true) {
			close(replyReceived)
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	routes := NewBotTenantAllowlist()
	if err := routes.Upsert(BotRoute{Provider: ChannelEnterpriseWeChat, ExternalSubject: "chat-timeout", TenantID: "tenant-one", AppID: "app-one", ConversationType: ConversationGroup}); err != nil {
		t.Fatal(err)
	}
	runtime := NewProviderRuntime(BotConfig{WeComBotID: "bot-id", WeComSecret: "bot-secret"}, routes, client.handler.ProcessProviderMessage)
	runtime.wecomEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	runtime.wecomOrigin = server.URL
	runtime.wecomRequestTimeout = 10 * time.Millisecond
	client.handler.ConfigureProviderRuntime(runtime)
	runtime.Start(context.Background())
	defer runtime.Close()
	select {
	case <-replyReceived:
	case <-time.After(time.Second):
		t.Fatal("WeCom reply was not sent")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		deliveries := runtime.Deliveries("tenant-one")
		if len(deliveries) == 1 && deliveries[0].Status == "terminal_failed" && deliveries[0].Code == "retry_exhausted" && deliveries[0].Attempts == 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("deliveries = %#v", runtime.Deliveries("tenant-one"))
}

func TestWeComReconnectsAndAuthenticatesAfterDisconnect(t *testing.T) {
	authenticated := make(chan struct{}, 2)
	var connections atomic.Int32
	server := httptest.NewServer(websocket.Handler(func(connection *websocket.Conn) {
		connectionNumber := connections.Add(1)
		var subscribe map[string]any
		if websocket.JSON.Receive(connection, &subscribe) != nil {
			return
		}
		_ = websocket.JSON.Send(connection, map[string]any{"headers": subscribe["headers"], "errcode": 0})
		authenticated <- struct{}{}
		if connectionNumber == 1 {
			return
		}
		var frame map[string]any
		_ = websocket.JSON.Receive(connection, &frame)
	}))
	defer server.Close()
	runtime := NewProviderRuntime(BotConfig{WeComBotID: "bot-id", WeComSecret: "bot-secret"}, nil, func(context.Context, string, string, []byte) error { return nil })
	runtime.wecomEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	runtime.wecomOrigin = server.URL
	runtime.Start(context.Background())
	defer runtime.Close()
	for count := 0; count < 2; count++ {
		select {
		case <-authenticated:
		case <-time.After(2 * time.Second):
			t.Fatalf("authenticated connections = %d", connections.Load())
		}
	}
}
