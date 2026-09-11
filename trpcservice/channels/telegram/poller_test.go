package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestPoller_PollOnceAndOffset(t *testing.T) {
	var callCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)

		if r.URL.Path != "/botbot-test-token/getUpdates" {
			http.NotFound(w, r)
			return
		}

		offset := r.URL.Query().Get("offset")
		w.Header().Set("Content-Type", "application/json")

		if offset == "" || offset == "0" {
			// First batch: updates 101 and 102
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{
				OK: true,
				Result: []update{
					{
						ID: 101,
						Message: &message{
							ID:   1,
							Date: 1700000000,
							Chat: chat{ID: 12345, Type: "private"},
							From: user{ID: 67890},
							Text: "first message",
						},
					},
					{
						ID: 102,
						Message: &message{
							ID:   2,
							Date: 1700000001,
							Chat: chat{ID: 12345, Type: "private"},
							From: user{ID: 67890},
							Text: "second message",
						},
					},
				},
			})
			return
		}

		if offset == "103" {
			// Second batch: empty
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{
				OK:     true,
				Result: []update{},
			})
			return
		}

		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	poller, err := NewPoller(PollerConfig{BotToken: "bot-test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewPoller failed: %v", err)
	}

	ctx := context.Background()
	var received []channels.InboundMessage

	handler := func(ctx context.Context, msg channels.InboundMessage) error {
		received = append(received, msg)
		return nil
	}

	// 1. First poll
	count, err := poller.PollOnce(ctx, handler)
	if err != nil {
		t.Fatalf("PollOnce failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("processed count = %d, want 2", count)
	}
	if poller.Offset() != 103 {
		t.Fatalf("offset = %d, want 103", poller.Offset())
	}
	if len(received) != 2 {
		t.Fatalf("received len = %d, want 2", len(received))
	}
	if received[0].Text != "first message" || received[1].Text != "second message" {
		t.Fatalf("unexpected received texts: %+v", received)
	}
	if received[0].Channel != channels.Telegram {
		t.Fatalf("channel = %q, want telegram", received[0].Channel)
	}
	if received[0].ProviderRequestID != "101" {
		t.Fatalf("provider request ID = %q, want 101", received[0].ProviderRequestID)
	}
	if received[0].TriggerType != channels.TriggerDirect {
		t.Fatalf("trigger = %q, want direct", received[0].TriggerType)
	}

	// 2. Second poll with offset=103
	count, err = poller.PollOnce(ctx, handler)
	if err != nil {
		t.Fatalf("second PollOnce failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("second count = %d, want 0", count)
	}
	if poller.Offset() != 103 {
		t.Fatalf("offset after empty = %d, want 103", poller.Offset())
	}
}

func TestPoller_RateLimitRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          false,
			"error_code":  429,
			"description": "Too Many Requests",
			"parameters": map[string]any{
				"retry_after": 1,
			},
		})
	}))
	defer server.Close()

	poller, err := NewPoller(PollerConfig{BotToken: "bot-test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewPoller failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	count, err := poller.PollOnce(ctx, func(ctx context.Context, msg channels.InboundMessage) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected rate limit to backoff without error, got: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestPollerDownloadsDirectDocument(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/botbot-test-token/getUpdates":
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{
				OK: true,
				Result: []update{{
					ID: 201,
					Message: &message{
						ID: 2, Chat: chat{ID: 123, Type: "private"}, From: user{ID: 456},
						Document: &document{FileID: "file-1", FileName: "report.pdf", MimeType: "application/pdf", FileSize: 12},
					},
				}},
			})
		case "/botbot-test-token/getFile":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": "documents/report.pdf"}})
		case "/file/botbot-test-token/documents/report.pdf":
			_, _ = w.Write([]byte("pdf-fixture"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	poller, err := NewPoller(PollerConfig{BotToken: "bot-test-token", BaseURL: server.URL, HTTPClient: server.Client(), MaxFileBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	var got channels.InboundMessage
	count, err := poller.PollOnce(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		got = message
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("PollOnce() count=%d error=%v", count, err)
	}
	if got.Text != "" || len(got.ReceivedFiles) != 1 || got.ReceivedFiles[0].Name != "report.pdf" || string(got.ReceivedFiles[0].Data) != "pdf-fixture" {
		t.Fatalf("inbound = %#v", got)
	}
}

func TestPollerDoesNotCommitFailedHandlerUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{
			{ID: 301, Message: &message{ID: 1, Chat: chat{ID: 1, Type: "private"}, From: user{ID: 2}, Text: "first"}},
			{ID: 302, Message: &message{ID: 2, Chat: chat{ID: 1, Type: "private"}, From: user{ID: 2}, Text: "second"}},
		}})
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "bot-test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	processed, err := poller.PollOnce(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		if message.Text == "second" {
			return errors.New("Kafka unavailable")
		}
		return nil
	})
	if err == nil || processed != 1 {
		t.Fatalf("PollOnce() processed=%d error=%v", processed, err)
	}
	if poller.Offset() != 302 {
		t.Fatalf("offset = %d, want failed update 302 to be retried", poller.Offset())
	}
}

func TestNormalizeTelegramGroupTextRemovesBotMention(t *testing.T) {
	got, triggered := normalizeTelegramGroupText("@SupportBot 帮我总结一下", "supportbot")
	if !triggered || got != "帮我总结一下" {
		t.Fatalf("normalizeTelegramGroupText() = %q, %v", got, triggered)
	}
	got, triggered = normalizeTelegramGroupText("/help@SupportBot", "supportbot")
	if !triggered || got != "/help" {
		t.Fatalf("command normalization = %q, %v", got, triggered)
	}
	if got, triggered = normalizeTelegramGroupText("大家晚上好", "supportbot"); triggered || got != "大家晚上好" {
		t.Fatalf("ordinary group text = %q, %v", got, triggered)
	}
}

func TestPollerNormalizesGroupMentionAndCommandTrigger(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{
			{ID: 401, Message: &message{ID: 1, Chat: chat{ID: 99, Type: "group"}, From: user{ID: 11}, Text: "@SupportBot 帮我总结"}},
			{ID: 402, Message: &message{ID: 2, Chat: chat{ID: 99, Type: "group"}, From: user{ID: 12}, Text: "/help@SupportBot"}},
		}})
	}))
	defer server.Close()
	poller, err := NewPoller(PollerConfig{BotToken: "bot-test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	poller.botUsername = "supportbot"
	var received []channels.InboundMessage
	count, err := poller.PollOnce(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		received = append(received, message)
		return nil
	})
	if err != nil || count != 2 || len(received) != 2 {
		t.Fatalf("PollOnce() count=%d messages=%d error=%v", count, len(received), err)
	}
	if received[0].Text != "帮我总结" || received[0].TriggerType != channels.TriggerMention || received[0].SenderID != "11" {
		t.Fatalf("mention inbound = %+v", received[0])
	}
	if received[1].Text != "/help" || received[1].TriggerType != channels.TriggerCommand || received[1].SenderID != "12" {
		t.Fatalf("command inbound = %+v", received[1])
	}
}
