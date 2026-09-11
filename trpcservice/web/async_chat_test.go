package web

import (
	"bytes"
	"context"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

type countingReplyStore struct {
	*storage.MemoryStateStore
	reads atomic.Int32
}

func (s *countingReplyStore) FindOutboxByRequestID(ctx context.Context, tenantID, requestID string) (storage.OutboxEvent, error) {
	s.reads.Add(1)
	return s.MemoryStateStore.FindOutboxByRequestID(ctx, tenantID, requestID)
}

type delayedDoneReplySubscriber struct{}

func (delayedDoneReplySubscriber) Subscribe(ctx context.Context, _, _, _, _ string) (<-chan WebStreamEvent, func(), error) {
	replies := make(chan WebStreamEvent, 1)
	streamCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(replies)
		timer := time.NewTimer(chatStreamHeartbeat + 100*time.Millisecond)
		defer timer.Stop()
		select {
		case <-streamCtx.Done():
			return
		case <-timer.C:
			replies <- WebStreamEvent{ID: "done-1", Type: "done", Reply: "stream reply"}
		}
	}()
	return replies, cancel, nil
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

func TestConsoleChatStreamHeartbeatDoesNotPollDurableReply(t *testing.T) {
	store := &countingReplyStore{MemoryStateStore: storage.NewMemoryStateStore()}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.Replies = store
		dependencies.ReplySubscriber = delayedDoneReplySubscriber{}
	})
	recorder := requestWithLastEventID(t, handler, "/api/v1/chat/stream?tenant=example&event_id=request-heartbeat", "")
	if recorder.Code != http.StatusOK || !containsSSEReply(recorder.Body.String(), "stream reply") {
		t.Fatalf("chat stream = %d: %s", recorder.Code, recorder.Body.String())
	}
	if got := store.reads.Load(); got != 2 {
		t.Fatalf("durable reply reads = %d, want exactly initial + post-subscribe race check", got)
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

func TestConsoleChatValidatesRequestBeforePublish(t *testing.T) {
	producer := &recordingProducer{}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Producer = producer })
	authorized := identity.SessionUser{
		PlatformUserID: "console-admin",
		IsSystemAdmin:  true,
		Tenants:        []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}},
	}

	tests := []struct {
		name   string
		method string
		body   string
		user   identity.SessionUser
		want   int
	}{
		{
			name:   "method",
			method: http.MethodGet,
			user:   authorized,
			want:   http.StatusMethodNotAllowed,
		},
		{
			name:   "missing request id",
			method: http.MethodPost,
			body:   `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello"}`,
			user:   authorized,
			want:   http.StatusBadRequest,
		},
		{
			name:   "invalid request id",
			method: http.MethodPost,
			body:   `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello","request_id":"not-a-uuid"}`,
			user:   authorized,
			want:   http.StatusBadRequest,
		},
		{
			name:   "empty content",
			method: http.MethodPost,
			body:   `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"   ","request_id":"11111111-2222-4333-8444-555555555555"}`,
			user:   authorized,
			want:   http.StatusBadRequest,
		},
		{
			name:   "tenant access denied",
			method: http.MethodPost,
			body:   `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello","request_id":"22222222-2222-4222-8222-222222222222"}`,
			user:   identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "other", Role: identity.RoleMember, Status: "active"}}},
			want:   http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := handler.requestAs(t, test.user, test.method, "/api/v1/chat", test.body)
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, test.want, recorder.Body.String())
			}
		})
	}
	if len(producer.published) != 0 {
		t.Fatalf("invalid requests published %d envelope(s)", len(producer.published))
	}
}

func TestConsoleChatReleasesIdempotencyLeaseAfterPublishFailure(t *testing.T) {
	producer := &recordingProducer{err: errors.New("broker unavailable")}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Producer = producer })
	body := `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello","request_id":"33333333-3333-4333-8333-333333333333"}`

	first := handler.request(t, http.MethodPost, "/api/v1/chat", body)
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first publish = %d, want 500: %s", first.Code, first.Body.String())
	}
	producer.err = nil
	second := handler.request(t, http.MethodPost, "/api/v1/chat", body)
	if second.Code != http.StatusAccepted {
		t.Fatalf("retry after publish failure = %d, want 202: %s", second.Code, second.Body.String())
	}
	if len(producer.published) != 1 {
		t.Fatalf("successful published envelopes = %d, want 1", len(producer.published))
	}
}

func TestConsoleChatRejectsTooManyUploads(t *testing.T) {
	handler := testConsoleHandler(t)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for key, value := range map[string]string{
		"tenant_id": "example", "app_code": "support", "conversation_id": "conversation-1",
		"request_id": "44444444-4444-4444-8444-444444444444",
	} {
		if err := form.WriteField(key, value); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < maxChatFiles+1; index++ {
		file, err := form.CreateFormFile("files", "note.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("content")); err != nil {
			t.Fatal(err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/chat", &body)
	request.Header.Set("Content-Type", form.FormDataContentType())
	sessionID, err := handler.store.Create(context.Background(), identity.SessionUser{PlatformUserID: "console-admin", IsSystemAdmin: true}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	const csrf = "upload-limit-csrf"
	request.AddCookie(&http.Cookie{Name: "dsh_session", Value: sessionID})
	request.AddCookie(&http.Cookie{Name: "csrf_token", Value: csrf})
	request.Header.Set("X-CSRF-Token", csrf)
	recorder := httptest.NewRecorder()
	handler.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "at most") {
		t.Fatalf("too many uploads = %d %s", recorder.Code, recorder.Body.String())
	}
}

type failingReplySubscriber struct{ err error }

func (s failingReplySubscriber) Subscribe(context.Context, string, string, string, string) (<-chan WebStreamEvent, func(), error) {
	return nil, nil, s.err
}

func TestConsoleChatStreamValidatesParametersAndUnavailableSubscriber(t *testing.T) {
	handler := testConsoleHandler(t)
	for _, path := range []string{
		"/api/v1/chat/stream",
		"/api/v1/chat/stream?tenant=example",
		"/api/v1/chat/stream?event_id=request-1",
	} {
		recorder := handler.request(t, http.MethodGet, path, "")
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d, want 400", path, recorder.Code)
		}
	}

	invalidCursor := requestWithLastEventID(t, handler, "/api/v1/chat/stream?tenant=example&event_id=request-1", "not-a-stream-id")
	if invalidCursor.Code != http.StatusBadRequest {
		t.Fatalf("invalid Last-Event-ID = %d, want 400", invalidCursor.Code)
	}

	unavailable := handler.request(t, http.MethodGet, "/api/v1/chat/stream?tenant=example&event_id=request-1", "")
	if unavailable.Code != http.StatusOK || !strings.Contains(unavailable.Body.String(), "web reply stream is unavailable") {
		t.Fatalf("unavailable stream = %d %s", unavailable.Code, unavailable.Body.String())
	}

	failing := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.ReplySubscriber = failingReplySubscriber{err: errors.New("stream unavailable")}
	})
	failure := failing.request(t, http.MethodGet, "/api/v1/chat/stream?tenant=example&event_id=request-2", "")
	if failure.Code != http.StatusOK || !strings.Contains(failure.Body.String(), "subscribe queued reply failed") {
		t.Fatalf("subscription failure = %d %s", failure.Code, failure.Body.String())
	}
}

func TestConsoleChatStreamReturnsCompletedOwnedReplyImmediately(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	_, err := stateStore.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: "example/support/session/conversation-1",
		MessageID: "request-complete", Channel: "web", BindingID: "web-console", TraceID: "trace-complete",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web",
		OutboxPayload:   []byte(`{"channel":"web","conversation_id":"conversation-1","web_owner_id":"console-admin","text":"completed reply"}`),
		OutboxRequestID: "request-complete",
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	claimed, err := stateStore.ClaimPendingOutbox(context.Background(), "example", "sender", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimPendingOutbox() = %#v, %v", claimed, err)
	}
	if err := stateStore.CompleteOutboxDelivery(context.Background(), "example", claimed[0].ID, "sender", "delivery-1"); err != nil {
		t.Fatalf("CompleteOutboxDelivery() error = %v", err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Replies = stateStore })
	recorder := handler.request(t, http.MethodGet, "/api/v1/chat/stream?tenant=example&event_id=request-complete", "")
	if recorder.Code != http.StatusOK || !containsSSEReply(recorder.Body.String(), "completed reply") || !strings.Contains(recorder.Body.String(), "id: delivery-1") {
		t.Fatalf("completed stream = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestConsoleChatStreamReturnsCompletedOwnedCardImmediately(t *testing.T) {
	stateStore := storage.NewMemoryStateStore()
	_, err := stateStore.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: "example/support/session/conversation-card",
		MessageID: "request-card", Channel: "web", BindingID: "web-console", TraceID: "trace-card",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web",
		OutboxPayload:   []byte(`{"channel":"web","conversation_id":"conversation-card","web_owner_id":"console-admin","text":"","card":{"title":"订单信息","body":"已找到订单","actions":[{"label":"查看","url":"https://support.example.test/orders/42"}]}}`),
		OutboxRequestID: "request-card",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := stateStore.ClaimPendingOutbox(context.Background(), "example", "sender", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimPendingOutbox() = %#v, %v", claimed, err)
	}
	if err := stateStore.CompleteOutboxDelivery(context.Background(), "example", claimed[0].ID, "sender", "delivery-card"); err != nil {
		t.Fatal(err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Replies = stateStore })
	recorder := handler.request(t, http.MethodGet, "/api/v1/chat/stream?tenant=example&event_id=request-card", "")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"title":"订单信息"`) || !strings.Contains(recorder.Body.String(), `id: delivery-card`) {
		t.Fatalf("completed card stream = %d %s", recorder.Code, recorder.Body.String())
	}
}
