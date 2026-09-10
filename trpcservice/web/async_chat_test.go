package web

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

type subscribeHookReplySubscriber struct {
	onSubscribe func()
}

func TestConsoleChatStagesMultipartFilesBeforePublishing(t *testing.T) {
	producer := &recordingProducer{}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Producer = producer
	})

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for key, value := range map[string]string{
		"tenant_id": "example", "app_code": "support", "conversation_id": "conversation-1",
		"request_id": "11111111-1111-4111-8111-111111111111",
	} {
		if err := form.WriteField(key, value); err != nil {
			t.Fatalf("WriteField(%q) error = %v", key, err)
		}
	}
	file, err := form.CreateFormFile("files", "notes.txt")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err := file.Write([]byte("hello attachment")); err != nil {
		t.Fatalf("write multipart file error = %v", err)
	}
	if err := form.Close(); err != nil {
		t.Fatalf("multipart Close() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/chat", &body)
	request.Header.Set("Content-Type", form.FormDataContentType())
	sessionID, err := handler.store.Create(context.Background(), identity.SessionUser{PlatformUserID: "console-admin", IsSystemAdmin: true, Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}, time.Hour)
	if err != nil {
		t.Fatalf("create session error = %v", err)
	}
	const csrf = "chat-upload-csrf"
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.AddCookie(&http.Cookie{Name: "csrf_token", Value: csrf})
	request.Header.Set("X-CSRF-Token", csrf)
	recorder := httptest.NewRecorder()
	handler.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("chat upload status = %d, want 202: %s", recorder.Code, recorder.Body.String())
	}
	if len(producer.published) != 1 {
		t.Fatalf("published envelopes = %d, want 1", len(producer.published))
	}
	payload, err := messaging.DecodeInboundPayload(producer.published[0])
	if err != nil {
		t.Fatalf("DecodeInboundPayload() error = %v", err)
	}
	if len(payload.Inbound.Files) != 1 {
		t.Fatalf("inbound files = %d, want 1", len(payload.Inbound.Files))
	}
	upload := payload.Inbound.Files[0]
	if upload.Name != "notes.txt" || upload.MimeType != "text/plain; charset=utf-8" {
		t.Fatalf("inbound file = %#v", upload)
	}
	version := upload.Version
	stored, err := handler.artifacts.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "example/support", UserID: "console-admin", SessionID: producer.published[0].SessionKey,
	}, upload.ArtifactName, &version)
	if err != nil {
		t.Fatalf("LoadArtifact() error = %v", err)
	}
	if stored == nil || string(stored.Data) != "hello attachment" {
		t.Fatalf("stored artifact = %#v", stored)
	}
}

func (s subscribeHookReplySubscriber) Subscribe(context.Context, string, string, string, string) (<-chan WebStreamEvent, func(), error) {
	s.onSubscribe()
	replies := make(chan WebStreamEvent)
	close(replies)
	return replies, func() {}, nil
}

func TestConsoleChatStreamRechecksDurableReplyAfterSubscribe(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	var subscribeErr error
	subscriber := subscribeHookReplySubscriber{
		onSubscribe: func() {
			_, subscribeErr = stateStore.RecordExecution(context.Background(), storage.ExecutionRecord{
				TenantID:        "example",
				AppCode:         "support",
				SessionKey:      "example/support/web/conversation-1",
				MessageID:       "request-1",
				Channel:         "web",
				BindingID:       "web-console",
				TraceID:         "trace-1",
				Action:          "agent_reply",
				Result:          "queued",
				AuditDetail:     "reply",
				OutboxType:      "channel_reply",
				OutboxPayload:   []byte("{\"channel\":\"web\",\"conversation_id\":\"conversation-1\",\"web_owner_id\":\"console-admin\",\"text\":\"race-safe reply\"}"),
				OutboxRequestID: "request-1",
			})
		},
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Replies = stateStore
		dependencies.ReplySubscriber = subscriber
	})

	recorder := handler.request(t, "GET", "/api/v1/chat/stream?tenant=example&event_id=request-1", "")
	if subscribeErr != nil {
		t.Fatalf("persist reply during Subscribe() error = %v", subscribeErr)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("chat stream status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); !containsSSEReply(body, "race-safe reply") {
		t.Fatalf("chat stream lost reply committed during Subscribe(): %s", body)
	}
}

type replayReplySubscriber struct {
	afterID string
	events  []WebStreamEvent
}

func (s *replayReplySubscriber) Subscribe(_ context.Context, _ string, _ string, _ string, afterID string) (<-chan WebStreamEvent, func(), error) {
	s.afterID = afterID
	output := make(chan WebStreamEvent, len(s.events))
	for _, event := range s.events {
		output <- event
	}
	close(output)
	return output, func() {}, nil
}

func TestConsoleChatStreamResumesAfterLastEventID(t *testing.T) {
	subscriber := &replayReplySubscriber{events: []WebStreamEvent{
		{ID: "2-0", Type: "delta", Content: "world"},
		{ID: "3-0", Type: "done", Reply: "hello world"},
	}}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.ReplySubscriber = subscriber
	})

	recorder := requestWithLastEventID(t, handler, "/api/v1/chat/stream?tenant=example&event_id=request-2", "1-0")
	if recorder.Code != http.StatusOK {
		t.Fatalf("chat stream status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if subscriber.afterID != "1-0" {
		t.Fatalf("Subscribe() afterID = %q, want %q", subscriber.afterID, "1-0")
	}
	body := recorder.Body.String()
	for _, expected := range []string{"id: 2-0", "\"content\":\"world\"", "id: 3-0", "\"reply\":\"hello world\""} {
		if !strings.Contains(body, expected) {
			t.Fatalf("chat stream body %q does not contain %q", body, expected)
		}
	}
}

func requestWithLastEventID(t *testing.T, console *testConsole, path, lastEventID string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	sessionID, err := console.store.Create(context.Background(), identity.SessionUser{PlatformUserID: "console-admin", IsSystemAdmin: true, Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}, time.Hour)
	if err != nil {
		t.Fatalf("create test session: %v", err)
	}
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.Header.Set("Last-Event-ID", lastEventID)
	recorder := httptest.NewRecorder()
	console.handler.ServeHTTP(recorder, request)
	return recorder
}

func containsSSEReply(body, reply string) bool {
	return strings.Contains(body, "\"reply\":\""+reply+"\"")
}

func TestConsoleChatStreamRejectsAnotherUsersStoredReply(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	_, err := stateStore.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: "example/support/web/conversation-1",
		MessageID: "request-private", Channel: "web", BindingID: "web-console", TraceID: "trace-private",
		Action: "agent_reply", Result: "queued", AuditDetail: "reply", OutboxType: "channel_reply",
		OutboxPayload:   []byte("{\"channel\":\"web\",\"conversation_id\":\"conversation-1\",\"web_owner_id\":\"reply-owner\",\"text\":\"private reply\"}"),
		OutboxRequestID: "request-private",
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Replies = stateStore
	})
	member := identity.SessionUser{
		PlatformUserID: "another-member",
		Tenants:        []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}},
	}
	recorder := handler.requestAs(t, member, http.MethodGet, "/api/v1/chat/stream?tenant=example&event_id=request-private", "")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("chat stream status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "private reply") {
		t.Fatalf("chat stream leaked another users reply: %s", recorder.Body.String())
	}
}
