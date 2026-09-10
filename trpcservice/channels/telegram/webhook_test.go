package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/go-telegram/bot/models"
)

func TestWebhookAuthenticatesPathAndReplaysSafely(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "webhook", "12345")
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	var dispatches atomic.Int32
	dispatcher := &dispatchStub{stream: func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		dispatches.Add(1)
		return eventStream(gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "ok"}, gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}), nil
	}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	defer adapter.Close()
	webhook, err := NewWebhook(adapter, WebhookConfig{Path: "/telegram/hook", SecretToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer webhook.Close()
	body, _ := json.Marshal(models.Update{ID: 99, Message: &models.Message{ID: 1, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, Text: "hello"}})
	request := httptest.NewRequest(http.MethodPost, "http://example.test/telegram/hook", bytesReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
	response := httptest.NewRecorder()
	webhook.ServeHTTP(response, request)
	if response.Code != http.StatusOK || dispatches.Load() != 1 {
		t.Fatalf("first webhook response=%d dispatches=%d", response.Code, dispatches.Load())
	}
	request = httptest.NewRequest(http.MethodPost, "http://example.test/telegram/hook", bytesReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
	response = httptest.NewRecorder()
	webhook.ServeHTTP(response, request)
	if response.Code != http.StatusOK || dispatches.Load() != 1 {
		t.Fatalf("replay response=%d dispatches=%d", response.Code, dispatches.Load())
	}
	bad := httptest.NewRecorder()
	webhook.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "http://example.test/other", bytesReader(body)))
	if bad.Code != http.StatusNotFound {
		t.Fatalf("wrong path status=%d", bad.Code)
	}
}

func TestWebhookCloseCancelsAcceptedDispatch(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "webhook-close", "12345")
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	dispatcher := &dispatchStub{stream: func(ctx context.Context, _ gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		stream := make(chan gateway.DispatchEvent)
		go func() {
			<-ctx.Done()
			close(stream)
		}()
		return stream, nil
	}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	defer adapter.Close()
	webhook, err := NewWebhook(adapter, WebhookConfig{Path: "/telegram/close", SecretToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(models.Update{ID: 101, Message: &models.Message{ID: 1, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, Text: "hello"}})
	request := httptest.NewRequest(http.MethodPost, "http://example.test/telegram/close", bytesReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
	done := make(chan struct{})
	go func() { webhook.ServeHTTP(httptest.NewRecorder(), request); close(done) }()
	if err := waitForDispatch(t, dispatcher); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = webhook.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("webhook close did not cancel accepted dispatch")
	}
	<-done
}

func TestNewWebhookRejectsInvalidConfiguration(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "webhook-config", "12345")
	adapter := newTestAdapter(t, target, &dispatchStub{}, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	defer adapter.Close()
	valid := WebhookConfig{Path: "/telegram/hook", SecretToken: "secret"}
	cases := []struct {
		name   string
		config WebhookConfig
		adapt  *Adapter
	}{
		{name: "nil adapter", config: valid},
		{name: "blank path", config: WebhookConfig{Path: " ", SecretToken: "secret"}, adapt: adapter},
		{name: "relative path", config: WebhookConfig{Path: "telegram/hook", SecretToken: "secret"}, adapt: adapter},
		{name: "query path", config: WebhookConfig{Path: "/telegram/hook?x=1", SecretToken: "secret"}, adapt: adapter},
		{name: "blank secret", config: WebhookConfig{Path: "/telegram/hook"}, adapt: adapter},
		{name: "whitespace secret", config: WebhookConfig{Path: "/telegram/hook", SecretToken: " secret"}, adapt: adapter},
		{name: "negative body limit", config: WebhookConfig{Path: "/telegram/hook", SecretToken: "secret", MaxBodyBytes: -1}, adapt: adapter},
		{name: "root path", config: WebhookConfig{Path: "/", SecretToken: "secret"}, adapt: adapter},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapt := test.adapt
			if adapt == nil && test.name != "nil adapter" {
				adapt = adapter
			}
			if _, err := NewWebhook(adapt, test.config); err == nil {
				t.Fatal("invalid webhook configuration was accepted")
			}
		})
	}
	alias, err := NewWebhook(adapter, WebhookConfig{Path: "/telegram/alias/", Secret: "secret"})
	if err != nil {
		t.Fatalf("secret alias configuration rejected: %v", err)
	}
	if alias.path != "/telegram/alias" {
		t.Fatalf("normalized path = %q", alias.path)
	}
	_ = alias.Close()
}

func TestWebhookServeHTTPMapsAuthenticationAndAdapterOutcomes(t *testing.T) {
	body := webhookUpdateBody(200, "hello")
	cases := []struct {
		name       string
		method     string
		path       string
		secret     string
		body       []byte
		maxBody    int64
		setup      func(*Adapter, *Webhook)
		wantStatus int
	}{
		{name: "wrong method", method: http.MethodGet, path: "/telegram/cases", secret: "secret", body: body, wantStatus: http.StatusMethodNotAllowed},
		{name: "wrong secret", method: http.MethodPost, path: "/telegram/cases", secret: "wrong", body: body, wantStatus: http.StatusForbidden},
		{name: "oversized body", method: http.MethodPost, path: "/telegram/cases", secret: "secret", body: body, maxBody: 1, wantStatus: http.StatusBadRequest},
		{name: "malformed json", method: http.MethodPost, path: "/telegram/cases", secret: "secret", body: []byte("{"), wantStatus: http.StatusBadRequest},
		{name: "trailing json", method: http.MethodPost, path: "/telegram/cases", secret: "secret", body: append(append([]byte(nil), body...), []byte("{}")...), wantStatus: http.StatusBadRequest},
		{name: "unsupported update acknowledged", method: http.MethodPost, path: "/telegram/cases", secret: "secret", body: webhookUpdateBodyWithEdited(201), wantStatus: http.StatusOK},
		{name: "invalid update", method: http.MethodPost, path: "/telegram/cases", secret: "secret", body: webhookUpdateBody(-1, "hello"), wantStatus: http.StatusBadRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapter, webhook := newWebhookCase(t, nil, test.maxBody)
			defer adapter.Close()
			defer webhook.Close()
			if test.setup != nil {
				test.setup(adapter, webhook)
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "http://example.test"+test.path, bytesReader(test.body))
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", test.secret)
			webhook.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}

	for _, test := range []struct {
		name       string
		dispatcher *dispatchStub
		setup      func(*Adapter, *Webhook)
		wantStatus int
	}{
		{name: "closing", wantStatus: http.StatusServiceUnavailable, setup: func(_ *Adapter, webhook *Webhook) { webhook.BeginShutdown() }},
		{name: "closed adapter", wantStatus: http.StatusServiceUnavailable, setup: func(adapter *Adapter, _ *Webhook) { _ = adapter.Close() }},
		{name: "dispatch failure", dispatcher: &dispatchStub{err: errors.New("provider detail")}, wantStatus: http.StatusServiceUnavailable},
		{name: "canceled result", dispatcher: &dispatchStub{err: context.Canceled}, wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, webhook := newWebhookCase(t, test.dispatcher, 0)
			defer adapter.Close()
			defer webhook.Close()
			if test.setup != nil {
				test.setup(adapter, webhook)
			}
			request := httptest.NewRequest(http.MethodPost, "http://example.test/telegram/cases", bytesReader(body))
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "secret")
			response := httptest.NewRecorder()
			webhook.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
}

func TestWebhookServeHTTPHandlesNilReceiversAndRequests(t *testing.T) {
	response := httptest.NewRecorder()
	var webhook *Webhook
	webhook.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "http://example.test/telegram/cases", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("nil webhook status = %d, want %d", response.Code, http.StatusNotFound)
	}
	adapter, webhook := newWebhookCase(t, nil, 0)
	defer adapter.Close()
	defer webhook.Close()
	response = httptest.NewRecorder()
	webhook.ServeHTTP(response, nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("nil request status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func newWebhookCase(t *testing.T, dispatcher *dispatchStub, maxBody int64) (*Adapter, *Webhook) {
	t.Helper()
	if dispatcher == nil {
		dispatcher = &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "ok"}, {Type: gateway.DispatchEventDone, Done: true}}}
	}
	target := newTrustedTarget(t, channels.ChannelTelegram, "webhook-case", "12345")
	adapter := newTestAdapter(t, target, dispatcher, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	webhook, err := NewWebhook(adapter, WebhookConfig{Path: "/telegram/cases", SecretToken: "secret", MaxBodyBytes: maxBody})
	if err != nil {
		_ = adapter.Close()
		t.Fatal(err)
	}
	return adapter, webhook
}

func webhookUpdateBody(updateID int64, text string) []byte {
	body, _ := json.Marshal(models.Update{ID: updateID, Message: &models.Message{ID: 1, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, Text: text}})
	return body
}

func webhookUpdateBodyWithEdited(updateID int64) []byte {
	body, _ := json.Marshal(models.Update{ID: updateID, EditedMessage: &models.Message{ID: 1}})
	return body
}

func bytesReader(value []byte) *bytes.Reader { return bytes.NewReader(value) }

func waitForDispatch(t *testing.T, dispatcher *dispatchStub) error {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(dispatcher.requests()) > 0 {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("dispatch was not admitted")
}
