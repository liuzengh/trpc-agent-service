package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type testOffsetStore struct {
	offset  int64
	loadErr error
	saveErr error
	saves   []int64
}

func (s *testOffsetStore) Load(context.Context) (int64, error) {
	return s.offset, s.loadErr
}

func (s *testOffsetStore) Save(_ context.Context, offset int64) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if offset > s.offset {
		s.offset = offset
	}
	s.saves = append(s.saves, offset)
	return nil
}

func TestNewPollerDefaultsAndValidatesToken(t *testing.T) {
	if _, err := NewPoller(PollerConfig{}); !errors.Is(err, ErrPollerTokenRequired) {
		t.Fatalf("NewPoller(empty) error = %v", err)
	}
	poller, err := NewPoller(PollerConfig{BotToken: " token "})
	if err != nil {
		t.Fatal(err)
	}
	if poller.botToken != "token" || poller.baseURL != DefaultBaseURL || poller.client == nil || poller.maxFileBytes != defaultMaxFileBytes {
		t.Fatalf("poller defaults = %#v", poller)
	}
	poller.SetOffset(17)
	if poller.Offset() != 17 {
		t.Fatalf("Offset() = %d", poller.Offset())
	}
	poller.commitOffset(16)
	poller.commitOffset(0)
	if poller.Offset() != 17 {
		t.Fatalf("commitOffset regressed offset to %d", poller.Offset())
	}
	if got := fallbackFilename("  report.txt ", "attachment"); got != "report.txt" {
		t.Fatalf("fallbackFilename(value) = %q", got)
	}
	if got := fallbackFilename(" ", "attachment"); got != "attachment" {
		t.Fatalf("fallbackFilename(fallback) = %q", got)
	}
}

func TestPollerLoadsAndPersistsOffsetMonotonically(t *testing.T) {
	store := &testOffsetStore{offset: 41}
	poller, err := NewPoller(PollerConfig{BotToken: "token", OffsetStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.loadPersistedOffset(context.Background()); err != nil {
		t.Fatal(err)
	}
	if poller.Offset() != 41 {
		t.Fatalf("loaded offset = %d, want 41", poller.Offset())
	}
	if err := poller.advanceOffset(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if poller.Offset() != 42 || store.offset != 42 || len(store.saves) != 1 || store.saves[0] != 42 {
		t.Fatalf("advanced offset poller=%d store=%d saves=%v", poller.Offset(), store.offset, store.saves)
	}
	if err := poller.advanceOffset(context.Background(), 40); err != nil {
		t.Fatal(err)
	}
	if poller.Offset() != 42 || store.offset != 42 {
		t.Fatalf("offset regressed poller=%d store=%d", poller.Offset(), store.offset)
	}
}

func TestPollerDoesNotAdvanceMemoryOffsetWhenPersistenceFails(t *testing.T) {
	store := &testOffsetStore{saveErr: errors.New("redis unavailable")}
	poller, err := NewPoller(PollerConfig{BotToken: "token", OffsetStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.advanceOffset(context.Background(), 17); err == nil || !strings.Contains(err.Error(), "persist telegram update offset") {
		t.Fatalf("advanceOffset() error = %v", err)
	}
	if poller.Offset() != 0 {
		t.Fatalf("memory offset = %d, want 0 after persistence failure", poller.Offset())
	}
}

func TestPollerHandlesCallbackAndAnswersTelegram(t *testing.T) {
	var answered atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bottest-token/getUpdates":
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{{
				ID: 501,
				CallbackQuery: &callbackQuery{
					ID: "callback-501", From: user{ID: 42}, Data: "confirm-order",
					Message: &message{ID: 77, Chat: chat{ID: 99, Type: "private"}},
				},
			}}})
		case "/bottest-token/answerCallbackQuery":
			answered.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	var received channels.InboundMessage
	count, err := poller.PollOnce(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		received = message
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("PollOnce() count=%d error=%v", count, err)
	}
	if received.TriggerType != channels.TriggerAction || received.Action == nil || received.Action.ActionID != "confirm-order" || received.Action.CallbackID != "callback-501" || received.Action.OriginMessageID != "77" {
		t.Fatalf("callback inbound = %#v", received)
	}
	if answered.Load() != 1 || poller.Offset() != 502 {
		t.Fatalf("answered=%d offset=%d", answered.Load(), poller.Offset())
	}
}

func TestPollerCallbackHandlerFailureDoesNotCommitOffset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{{
			ID:            601,
			CallbackQuery: &callbackQuery{ID: "callback-601", From: user{ID: 42}, Message: &message{ID: 77, Chat: chat{ID: 99}}},
		}}})
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	count, err := poller.PollOnce(context.Background(), func(context.Context, channels.InboundMessage) error {
		return errors.New("dispatch unavailable")
	})
	if err == nil || !strings.Contains(err.Error(), "callback-601 handler") || count != 0 || poller.Offset() != 0 {
		t.Fatalf("PollOnce() count=%d offset=%d error=%v", count, poller.Offset(), err)
	}
}

func TestPollerSkipsInvalidOrUntriggeredUpdatesWhileAdvancingOffset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{
			{ID: 701},
			{ID: 702, Message: &message{ID: 0, Chat: chat{ID: 99}, From: user{ID: 42}, Text: "invalid"}},
			{ID: 703, Message: &message{ID: 3, Chat: chat{ID: 99, Type: "group"}, From: user{ID: 42}, Text: "普通群消息"}},
			{ID: 704, Message: &message{ID: 4, Chat: chat{ID: 99, Type: "private"}, From: user{ID: 42}, Text: " ", Caption: " "}},
		}})
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	poller.botUsername = "supportbot"
	count, err := poller.PollOnce(context.Background(), func(context.Context, channels.InboundMessage) error {
		t.Fatal("handler should not be called")
		return nil
	})
	if err != nil || count != 0 || poller.Offset() != 705 {
		t.Fatalf("PollOnce() count=%d offset=%d error=%v", count, poller.Offset(), err)
	}
}

func TestPollerDownloadsSupportedMediaTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/bottest-token/getUpdates":
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{{
				ID: 801,
				Message: &message{
					ID: 8, Chat: chat{ID: 99, Type: "private"}, From: user{ID: 42}, Caption: "附件",
					Photo: []photoSize{{FileID: "photo-small", Width: 10, Height: 10, FileSize: 5}, {FileID: "photo-large", Width: 20, Height: 20, FileSize: 10}},
					Video: &mediaFile{FileID: "video", MimeType: "video/mp4", FileSize: 10},
					Audio: &mediaFile{FileID: "audio", FileName: "call.mp3", MimeType: "audio/mpeg", FileSize: 10},
					Voice: &mediaFile{FileID: "voice", MimeType: "audio/ogg", FileSize: 10},
				},
			}}})
		case r.URL.Path == "/bottest-token/getFile":
			fileID := r.URL.Query().Get("file_id")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": "files/" + fileID}})
		case strings.HasPrefix(r.URL.Path, "/file/bottest-token/files/"):
			_, _ = w.Write([]byte(strings.TrimPrefix(r.URL.Path, "/file/bottest-token/files/")))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client(), MaxFileBytes: 1024})
	var received channels.InboundMessage
	count, err := poller.PollOnce(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		received = message
		return nil
	})
	if err != nil || count != 1 || len(received.ReceivedFiles) != 4 {
		t.Fatalf("PollOnce() count=%d files=%#v error=%v", count, received.ReceivedFiles, err)
	}
	if string(received.ReceivedFiles[0].Data) != "photo-large" || received.ReceivedFiles[0].Name != "photo.jpg" {
		t.Fatalf("selected photo = %#v", received.ReceivedFiles[0])
	}
	if received.ReceivedFiles[1].Name != "video.mp4" || received.ReceivedFiles[2].Name != "call.mp3" || received.ReceivedFiles[3].Name != "voice.ogg" {
		t.Fatalf("media filenames = %#v", received.ReceivedFiles)
	}
}

func TestPollerRejectsOversizedAttachmentBeforeDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true, Result: []update{{
			ID: 901, Message: &message{ID: 9, Chat: chat{ID: 99}, From: user{ID: 42}, Document: &document{FileID: "large", FileName: "large.bin", FileSize: 11}},
		}}})
	}))
	defer server.Close()
	var reported atomic.Int32
	poller, _ := NewPoller(PollerConfig{
		BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client(), MaxFileBytes: 10,
		OnError: func(error) { reported.Add(1) },
	})
	count, err := poller.PollOnce(context.Background(), nil)
	if err == nil || count != 0 || reported.Load() != 1 || !strings.Contains(err.Error(), "exceeds 10 bytes") {
		t.Fatalf("PollOnce() count=%d reported=%d error=%v", count, reported.Load(), err)
	}
}

func TestPollerPollOnceHTTPAndEnvelopeFailures(t *testing.T) {
	tests := []struct {
		name  string
		serve func(http.ResponseWriter)
		want  string
	}{
		{name: "HTTP", serve: func(w http.ResponseWriter) { w.WriteHeader(http.StatusBadGateway) }, want: "HTTP status 502"},
		{name: "decode", serve: func(w http.ResponseWriter) { _, _ = w.Write([]byte(`not-json`)) }, want: "decode getUpdates response"},
		{name: "rejected", serve: func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"ok":false,"description":"rejected","error_code":400}`))
		}, want: "rejected"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { test.serve(w) }))
			defer server.Close()
			poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client()})
			if _, err := poller.PollOnce(context.Background(), nil); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PollOnce() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestPollerRateLimitHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"parameters":{}}`))
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client()})
	if _, err := poller.PollOnce(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("PollOnce(rate limited canceled) error = %v", err)
	}
}

func TestPollerRunLoadsIdentityRegistersCommandsAndMenuAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ready, commandCalls, menuCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bottest-token/getMe":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"username": "SupportBot"}})
		case "/bottest-token/setMyCommands":
			commandCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/bottest-token/setChatMenuButton":
			menuCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/bottest-token/getUpdates":
			cancel()
			_ = json.NewEncoder(w).Encode(getUpdatesResponse{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{
		BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client(),
		OnReady: func() { ready.Add(1) },
	})
	err := poller.Run(ctx, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if ready.Load() < 1 || commandCalls.Load() != 1 || menuCalls.Load() != 1 || poller.botUsername != "SupportBot" {
		t.Fatalf("ready=%d commands=%d menu=%d username=%q", ready.Load(), commandCalls.Load(), menuCalls.Load(), poller.botUsername)
	}
}

func TestPollerRunReportsIdentityFailure(t *testing.T) {
	var reported atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	poller, _ := NewPoller(PollerConfig{
		BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client(),
		OnError: func(error) { reported.Add(1) },
	})
	if err := poller.Run(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "getMe HTTP status 401") || reported.Load() != 1 {
		t.Fatalf("Run() error=%v reported=%d", err, reported.Load())
	}
}

func TestPollerDownloadFileRejectsInvalidProviderResponses(t *testing.T) {
	tests := []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		want  string
	}{
		{name: "metadata status", serve: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }, want: "getFile HTTP status 502"},
		{name: "metadata decode", serve: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`bad`)) }, want: "decode getFile response"},
		{name: "missing path", serve: func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"ok":true,"result":{"file_path":""}}`))
		}, want: "did not contain a file path"},
		{name: "download status", serve: func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/getFile") {
				_, _ = w.Write([]byte(`{"ok":true,"result":{"file_path":"files/a"}}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}, want: "file download HTTP status 404"},
		{name: "download oversized", serve: func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/getFile") {
				_, _ = w.Write([]byte(`{"ok":true,"result":{"file_path":"files/a"}}`))
				return
			}
			_, _ = w.Write([]byte("123456"))
		}, want: "exceeds 5 bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(test.serve))
			defer server.Close()
			poller, _ := NewPoller(PollerConfig{BotToken: "test-token", BaseURL: server.URL, HTTPClient: server.Client(), MaxFileBytes: 5})
			if _, err := poller.downloadFile(context.Background(), "file-a"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("downloadFile() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestNormalizeTelegramGroupTextBoundaries(t *testing.T) {
	if got, triggered := normalizeTelegramGroupText(" ", "supportbot"); got != "" || triggered {
		t.Fatalf("empty = %q, %v", got, triggered)
	}
	if got, triggered := normalizeTelegramGroupText("普通消息", ""); got != "普通消息" || !triggered {
		t.Fatalf("isolated poller = %q, %v", got, triggered)
	}
	if got, triggered := normalizeTelegramGroupText("你好，@SupportBot！", "@supportbot"); got != "你好，" || !triggered {
		t.Fatalf("punctuated mention = %q, %v", got, triggered)
	}
	if telegramConversationScope(" SUPERGROUP ") != channels.ConversationGroup || telegramConversationScope("private") != channels.ConversationDirect {
		t.Fatal("telegramConversationScope() returned wrong scope")
	}
}

func TestPollerReportErrorIgnoresNil(t *testing.T) {
	var calls atomic.Int32
	poller, _ := NewPoller(PollerConfig{BotToken: "token", OnError: func(error) { calls.Add(1) }})
	poller.reportError(nil)
	poller.reportError(errors.New("failure"))
	if calls.Load() != 1 {
		t.Fatalf("error callbacks = %d", calls.Load())
	}
	poller.reportReady()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
}
