package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func telegramTarget() channels.ReplyTarget {
	return channels.ReplyTarget{Channel: channels.Telegram, ConversationID: "99"}
}

func TestNewSenderValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name, token, baseURL string
		client               *http.Client
	}{
		{name: "token", baseURL: "https://example.test", client: http.DefaultClient},
		{name: "URL", token: "token", baseURL: "://bad", client: http.DefaultClient},
		{name: "client", token: "token", baseURL: "https://example.test"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSender(test.token, test.baseURL, test.client); err == nil {
				t.Fatal("NewSender() accepted invalid configuration")
			}
		})
	}
}

func TestDefaultCommandsExposeOnlyUsableConversationControls(t *testing.T) {
	commands := DefaultCommands()
	want := []BotCommand{
		{Command: "start", Description: "开始使用"},
		{Command: "new", Description: "开启新会话"},
		{Command: "help", Description: "查看帮助"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("DefaultCommands() = %#v, want %#v", commands, want)
	}
}

func TestSenderSupportsCardsProgressEditsAndControlActions(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.URL.Path)
		writer.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(request.URL.Path, "/sendChatAction") {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"ok":false}`))
			return
		}
		if strings.HasSuffix(request.URL.Path, "/sendMessage") {
			var payload map[string]any
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatalf("decode sendMessage: %v", err)
			}
			if payload["chat_id"] != "99" {
				t.Fatalf("chat_id = %#v", payload["chat_id"])
			}
			_, _ = writer.Write([]byte(`{"ok":true,"result":{"message_id":17}}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	sender, err := NewSender("test-token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	card := &channels.InteractiveCard{
		Title: "订单查询", Body: "已找到订单",
		Actions: []channels.CardAction{
			{Label: "查看", URL: "https://support.example.test/order/42"},
			{Label: "确认", ActionID: "confirm-order"},
			{Label: " "},
			{Label: "无效"},
		},
	}
	receipt, err := sender.Send(context.Background(), telegramTarget(), channels.OutboundMessage{Text: "补充说明", Card: card})
	if err != nil || receipt.ExternalMessageID != "17" {
		t.Fatalf("Send(card) = %#v, %v", receipt, err)
	}
	progress, err := sender.StartProgress(context.Background(), telegramTarget())
	if err != nil || progress.ExternalMessageID != "17" {
		t.Fatalf("StartProgress() = %#v, %v", progress, err)
	}
	if err := sender.UpdateProgress(context.Background(), telegramTarget(), "17", "处理中"); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}
	if err := sender.UpdateProgress(context.Background(), telegramTarget(), "17", " "); err != nil {
		t.Fatalf("UpdateProgress(empty) error = %v", err)
	}
	if err := sender.DeleteMessage(context.Background(), telegramTarget(), "17"); err != nil {
		t.Fatalf("DeleteMessage() error = %v", err)
	}
	if err := sender.AnswerCallback(context.Background(), "callback-1", " 已处理 "); err != nil {
		t.Fatalf("AnswerCallback() error = %v", err)
	}
	if err := sender.SetCommands(context.Background(), DefaultCommands()); err != nil {
		t.Fatalf("SetCommands() error = %v", err)
	}
	if err := sender.SetMenuButtonCommands(context.Background()); err != nil {
		t.Fatalf("SetMenuButtonCommands() error = %v", err)
	}
	if len(methods) < 8 {
		t.Fatalf("telegram methods = %v", methods)
	}
}

func TestSenderSendsFileAndUsesFileReceiptWhenNoText(t *testing.T) {
	directory := t.TempDir()
	filePath := filepath.Join(directory, "support-report.txt")
	if err := os.WriteFile(filePath, []byte("support report"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/bottest-token/sendDocument" {
			http.NotFound(writer, request)
			return
		}
		if err := request.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("ParseMultipartForm() error = %v", err)
		}
		if request.FormValue("chat_id") != "99" {
			t.Fatalf("chat_id = %q", request.FormValue("chat_id"))
		}
		file, header, err := request.FormFile("document")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		if header.Filename != "customer-report.txt" || string(data) != "support report" {
			t.Fatalf("uploaded file = %q %q", header.Filename, data)
		}
		_, _ = writer.Write([]byte(`{"ok":true,"result":{"message_id":23}}`))
	}))
	defer server.Close()
	sender, _ := NewSender("test-token", server.URL, server.Client())
	receipt, err := sender.Send(context.Background(), telegramTarget(), channels.OutboundMessage{
		Files: []channels.OutboundFile{{Path: filePath, Name: "customer-report.txt"}},
	})
	if err != nil || receipt.ExternalMessageID != "23" {
		t.Fatalf("Send(file) = %#v, %v", receipt, err)
	}
}

func TestSenderValidatesMessageOperations(t *testing.T) {
	sender, err := NewSender("test-token", "https://example.test", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	target := telegramTarget()
	if _, err := sender.Send(context.Background(), target, channels.OutboundMessage{}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("Send(empty) error = %v", err)
	}
	if _, err := sender.Send(context.Background(), target, channels.OutboundMessage{
		Text: "updated", UpdateMessageID: "17", Files: []channels.OutboundFile{{Path: "file"}},
	}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("Send(edit with file) error = %v", err)
	}
	if _, err := sender.Send(context.Background(), target, channels.OutboundMessage{Text: "updated", UpdateMessageID: "bad"}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("Send(invalid edit ID) error = %v", err)
	}
	if err := sender.DeleteMessage(context.Background(), target, "0"); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("DeleteMessage(invalid) error = %v", err)
	}
	if err := sender.DeleteMessage(context.Background(), channels.ReplyTarget{Channel: channels.WeCom}, "1"); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("DeleteMessage(invalid target) error = %v", err)
	}
	if err := sender.AnswerCallback(context.Background(), " ", "done"); err == nil {
		t.Fatal("AnswerCallback() accepted blank callback ID")
	}
	if err := sender.SetCommands(context.Background(), nil); err == nil {
		t.Fatal("SetCommands() accepted empty command list")
	}
	if _, err := sender.Send(context.Background(), target, channels.OutboundMessage{Files: []channels.OutboundFile{{Path: " "}}}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("Send(blank file path) error = %v", err)
	}
	if _, err := sender.Send(context.Background(), target, channels.OutboundMessage{Files: []channels.OutboundFile{{Path: filepath.Join(t.TempDir(), "missing.txt")}}}); err == nil || !strings.Contains(err.Error(), "open telegram outbound file") {
		t.Fatalf("Send(missing file) error = %v", err)
	}
}

func TestSenderRejectsMalformedOversizedAndRejectedResponses(t *testing.T) {
	tests := []struct {
		name  string
		serve func(http.ResponseWriter)
	}{
		{name: "invalid json", serve: func(w http.ResponseWriter) { _, _ = w.Write([]byte(`not-json`)) }},
		{name: "oversized", serve: func(w http.ResponseWriter) { _, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1))) }},
		{name: "HTTP status", serve: func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"ok":false}`))
		}},
		{name: "missing message id", serve: func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":0}}`)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { test.serve(w) }))
			defer server.Close()
			sender, _ := NewSender("test-token", server.URL, server.Client())
			_, err := sender.Send(context.Background(), telegramTarget(), channels.OutboundMessage{Text: "status"})
			if !errors.Is(err, ErrSendRejected) {
				t.Fatalf("Send() error = %v, want ErrSendRejected", err)
			}
		})
	}
}

func TestDisplayTextAndKeyboardSkipInvalidActions(t *testing.T) {
	if got := displayText(channels.OutboundMessage{Text: "plain"}); got != "plain" {
		t.Fatalf("displayText(plain) = %q", got)
	}
	card := &channels.InteractiveCard{Title: "标题", Body: "正文", Actions: []channels.CardAction{
		{Label: "链接", URL: "https://support.example.test"},
		{Label: "动作", ActionID: "action-1"},
		{Label: "空"},
		{Label: " "},
	}}
	if got := displayText(channels.OutboundMessage{Text: "补充", Card: card}); got != "标题\n\n正文\n\n补充" {
		t.Fatalf("displayText(card) = %q", got)
	}
	if keyboard := telegramKeyboard(card); keyboard == nil {
		t.Fatal("telegramKeyboard() = nil")
	}
	if keyboard := telegramKeyboard(&channels.InteractiveCard{Actions: []channels.CardAction{{Label: "无效"}}}); keyboard != nil {
		t.Fatalf("telegramKeyboard(invalid) = %#v", keyboard)
	}
}
