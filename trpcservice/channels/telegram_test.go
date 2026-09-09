package channels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

type ingressSinkFunc func(context.Context, message.InboundMessage) (AcceptResult, error)

func (f ingressSinkFunc) Accept(ctx context.Context, inbound message.InboundMessage) (AcceptResult, error) {
	return f(ctx, inbound)
}

type fakeTelegramAPI struct {
	mu         sync.Mutex
	requests   int
	updates    []*models.Update
	pollStatus int
	retryAfter int
	sends      []map[string]string
}

func (f *fakeTelegramAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests++
	status := f.pollStatus
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	switch method {
	case "getMe":
		_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"Agent","username":"agentbot"}}`))
	case "getUpdates":
		if status != 0 {
			w.WriteHeader(status)
			if status == http.StatusTooManyRequests {
				f.mu.Lock()
				retryAfter := f.retryAfter
				f.mu.Unlock()
				_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":%d}}`, retryAfter)
				return
			}
			_, _ = w.Write([]byte(`{"ok":false,"error_code":500}`))
			return
		}
		offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		f.mu.Lock()
		filtered := make([]*models.Update, 0, len(f.updates))
		for _, update := range f.updates {
			if update.ID >= offset {
				filtered = append(filtered, update)
			}
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": filtered})
	case "sendMessage":
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.sends = append(f.sends, map[string]string{"chat_id": r.FormValue("chat_id"), "text": r.FormValue("text")})
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1,"date":1,"chat":{"id":42,"type":"private"}}}`))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeTelegramAPI) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func TestTelegramAdapterFakeBotAPIContract(t *testing.T) {
	fake := &fakeTelegramAPI{updates: []*models.Update{
		{ID: 10, Message: &models.Message{ID: 1, From: &models.User{ID: 7}, Date: 1, Chat: models.Chat{ID: 7, Type: "private"}, Text: "hello"}},
		{ID: 11, Message: &models.Message{ID: 2, From: &models.User{ID: 8}, Date: 2, Chat: models.Chat{ID: -99, Type: "group"}, Text: "😀 @agentbot hello group", Entities: []models.MessageEntity{{Type: models.MessageEntityTypeMention, Offset: 3, Length: 9}}}},
	}}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	defer server.Close()

	adapter, err := NewTelegramAdapter("telegram-a", "123", "test-token", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if fake.requestCount() != 0 {
		t.Fatal("constructor contacted Telegram before Start")
	}

	accepted := make(chan message.InboundMessage, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(_ context.Context, inbound message.InboundMessage) (AcceptResult, error) {
			accepted <- inbound
			return AcceptResult{TaskID: "accepted"}, nil
		}))
	}()

	var got []message.InboundMessage
	for len(got) < 2 {
		select {
		case inbound := <-accepted:
			got = append(got, inbound)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Telegram updates")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got[0].ConversationType != message.ConversationDirect || got[0].ActorUserID != "7" || got[0].PlatformMessageID != "10" {
		t.Fatalf("direct mapping = %#v", got[0])
	}
	if got[1].ConversationType != message.ConversationGroup || got[1].ConversationID != "-99" || got[1].ActorUserID != "8" || got[1].Text != "😀  hello group" {
		t.Fatalf("group mapping = %#v", got[1])
	}
	if adapter.offset != 12 {
		t.Fatalf("offset = %d, want 12", adapter.offset)
	}
	if err := adapter.Send(context.Background(), message.OutboundMessage{ConversationID: "-99", Text: "answer"}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.sends) != 1 || fake.sends[0]["chat_id"] != "-99" || fake.sends[0]["text"] != "answer" {
		t.Fatalf("sendMessage calls = %#v", fake.sends)
	}
}

func TestTelegramOffsetWaitsForDurableIngress(t *testing.T) {
	adapter := &TelegramAdapter{bindingID: "telegram-a", accountID: "123", botUser: &models.User{ID: 123, Username: "agentbot"}}
	update := &models.Update{ID: 21, Message: &models.Message{From: &models.User{ID: 7}, Chat: models.Chat{ID: 7, Type: "private"}, Text: "hello"}}
	wantErr := errors.New("redis unavailable")
	err := adapter.handleUpdate(context.Background(), ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
		return AcceptResult{}, wantErr
	}), update)
	if !errors.Is(err, wantErr) || adapter.offset != 0 {
		t.Fatalf("handleUpdate error=%v offset=%d", err, adapter.offset)
	}
	if err := adapter.handleUpdate(context.Background(), ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
		return AcceptResult{Duplicate: true}, nil
	}), update); err != nil || adapter.offset != 22 {
		t.Fatalf("duplicate acceptance error=%v offset=%d", err, adapter.offset)
	}
}

func TestTelegramPollingClassifies429AndServerErrors(t *testing.T) {
	fake := &fakeTelegramAPI{pollStatus: http.StatusTooManyRequests, retryAfter: 7}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	defer server.Close()
	adapter, err := NewTelegramAdapter("telegram-a", "123", "test-token", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.getUpdates(context.Background())
	var retryErr *telegramRetryError
	if !errors.As(err, &retryErr) || retryErr.after != 7*time.Second {
		t.Fatalf("429 error = %#v", err)
	}
	fake.mu.Lock()
	fake.pollStatus = http.StatusInternalServerError
	fake.mu.Unlock()
	if _, err := adapter.getUpdates(context.Background()); err == nil {
		t.Fatal("500 response was accepted")
	}
}

func TestTelegramStartHonorsCancellationDuringAuthenticationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error_code":401,"description":"unauthorized"}`))
	}))
	defer server.Close()
	adapter, err := NewTelegramAdapter("telegram-a", "123", "test-token", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
			return AcceptResult{}, nil
		}))
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Telegram Start did not stop after cancellation")
	}
}
