package telegram

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestSenderMapsOutboundMessageToTelegramRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got, want := request.URL.Path, "/bottest-token/sendMessage"; got != want {
			t.Errorf("path = %q, want %q", got, want)
		}
		if got, want := request.Method, http.MethodPost; got != want {
			t.Errorf("method = %q, want %q", got, want)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if got, want := string(body), `{"chat_id":"99","text":"hello"}`; got != want {
			t.Errorf("body = %s, want %s", got, want)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	}))
	defer server.Close()

	sender, err := NewSender("test-token", server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}

	receipt, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel:        channels.Telegram,
		ConversationID: "99",
	}, channels.OutboundMessage{Text: "hello"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got, want := receipt.ExternalMessageID, "7"; got != want {
		t.Errorf("ExternalMessageID = %q, want %q", got, want)
	}
}

func TestSenderRejectsInvalidTargetAndTelegramFailure(t *testing.T) {
	sender, err := NewSender("test-token", "https://example.invalid", http.DefaultClient)
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}

	_, err = sender.Send(context.Background(), channels.ReplyTarget{Channel: channels.WeCom, ConversationID: "99"}, channels.OutboundMessage{Text: "hello"})
	if !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("Send() invalid target error = %v, want ErrInvalidTarget", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"ok":false,"description":"too many requests"}`))
	}))
	defer server.Close()
	sender, err = NewSender("test-token", server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewSender() error = %v", err)
	}
	_, err = sender.Send(context.Background(), channels.ReplyTarget{Channel: channels.Telegram, ConversationID: "99"}, channels.OutboundMessage{Text: "hello"})
	if !errors.Is(err, ErrSendRejected) {
		t.Errorf("Send() rejection error = %v, want ErrSendRejected", err)
	}
}
