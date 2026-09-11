package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type pagingSessionService struct {
	agentsession.Service
	events      []event.Event
	pageCalls   int
	windowCalls int
	fullCalls   int
}

type failingAgentSessionProvider struct{ err error }

func (p failingAgentSessionProvider) Session(context.Context, config.TenantConfig) (agentsession.Service, error) {
	return nil, p.err
}

type failingOutboxBatchStore struct {
	*storage.MemoryStateStore
	err error
}

func (s *failingOutboxBatchStore) FindOutboxByRequestIDs(context.Context, string, []string) ([]storage.OutboxEvent, error) {
	return nil, s.err
}

func (s *pagingSessionService) GetSession(_ context.Context, key agentsession.Key, opts ...agentsession.Option) (*agentsession.Session, error) {
	options := agentsession.Options{}
	for _, option := range opts {
		option(&options)
	}
	if options.EventPage == nil {
		s.fullCalls++
		return &agentsession.Session{ID: key.SessionID, AppName: key.AppName, UserID: key.UserID, Events: append([]event.Event(nil), s.events...)}, nil
	}
	s.pageCalls++
	end := len(s.events) - options.EventPage.Offset
	if end < 0 {
		end = 0
	}
	start := end - options.EventPage.Limit
	if start < 0 {
		start = 0
	}
	return &agentsession.Session{ID: key.SessionID, AppName: key.AppName, UserID: key.UserID, Events: append([]event.Event(nil), s.events[start:end]...)}, nil
}

func (s *pagingSessionService) GetEventWindow(_ context.Context, request agentsession.EventWindowRequest) (*agentsession.EventWindow, error) {
	s.windowCalls++
	anchor := -1
	for index := range s.events {
		if s.events[index].ID == request.AnchorEventID {
			anchor = index
			break
		}
	}
	if anchor < 0 {
		return nil, fmt.Errorf("%w: %s", agentsession.ErrEventWindowAnchorNotFound, request.AnchorEventID)
	}
	start := anchor - request.Before
	if start < 0 {
		start = 0
	}
	end := anchor + request.After + 1
	if end > len(s.events) {
		end = len(s.events)
	}
	entries := make([]agentsession.EventWindowEntry, 0, end-start)
	for index := start; index < end; index++ {
		entries = append(entries, agentsession.EventWindowEntry{Event: s.events[index], CreatedAt: s.events[index].Timestamp})
	}
	return &agentsession.EventWindow{SessionKey: request.Key, AnchorEventID: request.AnchorEventID, Entries: entries}, nil
}

func TestProjectChatMessagesKeepsUserAndFinalAssistantText(t *testing.T) {
	t.Parallel()
	sess := &agentsession.Session{Events: []event.Event{
		*event.NewResponseEvent("inv-1", "user", &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage("你好")}},
		}),
		*event.NewResponseEvent("inv-1", "assistant", &model.Response{
			IsPartial: true,
			Object:    model.ObjectTypeChatCompletionChunk,
			Choices:   []model.Choice{{Message: model.NewAssistantMessage("半")}},
		}),
		*event.NewResponseEvent("inv-1", "assistant", &model.Response{
			Done:    true,
			Object:  model.ObjectTypeChatCompletion,
			Choices: []model.Choice{{Message: model.NewAssistantMessage("你好，我是助手")}},
		}),
		*event.NewResponseEvent("inv-1", "assistant", &model.Response{
			Object:  model.ObjectTypeToolResponse,
			Choices: []model.Choice{{Message: model.NewAssistantMessage("工具原文不应出现")}},
		}),
		*event.NewResponseEvent("inv-1", "assistant", &model.Response{
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "call-1"}}}}},
		}),
	}}
	messages := projectChatMessages(sess)
	if len(messages) != 2 || messages[0].Role != "user" || messages[0].Content != "你好" || messages[1].Content != "你好，我是助手" {
		t.Fatalf("projectChatMessages() = %#v", messages)
	}
	if chatPreview(messages) != "你好" {
		t.Fatalf("chatPreview() = %q", chatPreview(messages))
	}
}

func TestProjectChatMessagesRestoresTextAttachmentPresentation(t *testing.T) {
	t.Parallel()
	sess := &agentsession.Session{Events: []event.Event{
		*event.NewResponseEvent("inv-1", "user", &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage("回答我的问题。\n\n附件「question.txt」内容：\n你是谁？")}},
		}),
	}}
	messages := projectChatMessages(sess)
	if len(messages) != 1 || messages[0].Content != "回答我的问题。" {
		t.Fatalf("projectChatMessages() = %#v", messages)
	}
	if len(messages[0].Attachments) != 1 || messages[0].Attachments[0].Name != "question.txt" {
		t.Fatalf("attachments = %#v", messages[0].Attachments)
	}
	if got := chatPreview(messages); got != "回答我的问题。" {
		t.Fatalf("chatPreview() = %q", got)
	}
}

func TestProjectChatMessagesRestoresAttachmentOnlyMessage(t *testing.T) {
	t.Parallel()
	sess := &agentsession.Session{Events: []event.Event{
		*event.NewResponseEvent("inv-1", "user", &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage("附件「question.txt」内容：\n你是谁？")}},
		}),
	}}
	messages := projectChatMessages(sess)
	if len(messages) != 1 || messages[0].Content != "" || len(messages[0].Attachments) != 1 || messages[0].Attachments[0].Name != "question.txt" {
		t.Fatalf("projectChatMessages() = %#v", messages)
	}
	if got := chatPreview(messages); got != "附件 · question.txt" {
		t.Fatalf("chatPreview() = %q", got)
	}
}

func TestConsoleSessionMessagesValidatesRequestAndBackendFailures(t *testing.T) {
	t.Parallel()

	t.Run("method", func(t *testing.T) {
		handler := testConsoleHandler(t)
		recorder := handler.request(t, http.MethodPost, "/api/v1/sessions/messages?tenant=example&session_key=missing", "")
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405: %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("missing route parameters", func(t *testing.T) {
		handler := testConsoleHandler(t)
		for _, path := range []string{
			"/api/v1/sessions/messages",
			"/api/v1/sessions/messages?tenant=example",
			"/api/v1/sessions/messages?session_key=example%2Fsupport%2Fsession%2F1",
		} {
			recorder := handler.request(t, http.MethodGet, path, "")
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("%s status = %d, want 400: %s", path, recorder.Code, recorder.Body.String())
			}
		}
	})

	t.Run("tenant permission", func(t *testing.T) {
		handler := testConsoleHandler(t)
		outsider := identity.SessionUser{PlatformUserID: "outsider"}
		recorder := handler.requestAs(t, outsider, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=missing", "")
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
		}
	})

	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/session/input-validation"
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentSessions = staticAgentSessionProvider{service: framework}
	})
	resolved, err := handler.state.ResolveSession(context.Background(), storage.SessionRoute{
		TenantID: "example", AppCode: "support", Channel: "web", BindingID: "web-console",
		ConversationID: "input-validation", SubjectID: "console-admin", OwnerPlatformUserID: "console-admin", Scope: "direct",
	}, sessionKey)
	if err != nil || resolved != sessionKey {
		t.Fatalf("ResolveSession() = %q, %v", resolved, err)
	}

	t.Run("missing platform session", func(t *testing.T) {
		recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=missing", "")
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404: %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("unstarted platform session has empty transcript", func(t *testing.T) {
		const emptyKey = "example/support/session/unstarted"
		if _, err := handler.state.ResolveSession(context.Background(), storage.SessionRoute{
			TenantID: "example", AppCode: "support", Channel: "web", BindingID: "web-console",
			ConversationID: "unstarted", SubjectID: "console-admin", OwnerPlatformUserID: "console-admin", Scope: "direct",
		}, emptyKey); err != nil {
			t.Fatal(err)
		}
		recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key="+emptyKey, "")
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"messages":[]`) {
			t.Fatalf("unstarted transcript = %d: %s", recorder.Code, recorder.Body.String())
		}
	})

	for _, raw := range []string{"0", "201", "abc"} {
		t.Run("invalid limit "+raw, func(t *testing.T) {
			recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key="+sessionKey+"&limit="+raw, "")
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "limit must be between") {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
		})
	}

	t.Run("oversized cursor", func(t *testing.T) {
		recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key="+sessionKey+"&before="+strings.Repeat("x", 513), "")
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "not a valid message cursor") {
			t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("session backend resolution failure", func(t *testing.T) {
		wantErr := errors.New("session backend unavailable")
		failing := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
			dependencies.AgentSessions = failingAgentSessionProvider{err: wantErr}
		})
		if _, err := failing.state.ResolveSession(context.Background(), storage.SessionRoute{
			TenantID: "example", AppCode: "support", Channel: "web", BindingID: "web-console",
			ConversationID: "backend-failure", SubjectID: "console-admin", OwnerPlatformUserID: "console-admin", Scope: "direct",
		}, "example/support/session/backend-failure"); err != nil {
			t.Fatal(err)
		}
		if _, err := failing.state.RecordExecution(context.Background(), storage.ExecutionRecord{
			TenantID: "example", AppCode: "support", SessionKey: "example/support/session/backend-failure",
			MessageID: "backend-failure-message", Channel: "web", BindingID: "web-console", TraceID: "backend-failure-trace",
			Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", SubjectID: "console-admin", OwnerPlatformUserID: "console-admin",
		}); err != nil {
			t.Fatal(err)
		}
		recorder := failing.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fbackend-failure", "")
		if recorder.Code != http.StatusInternalServerError || !strings.Contains(recorder.Body.String(), "resolve agent Session backend") {
			t.Fatalf("status = %d, want 500: %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestAttachChatMessageCardsHandlesFallbackErrorsAndMissingCards(t *testing.T) {
	t.Parallel()
	messages := []chatTranscriptMessage{
		{ID: "assistant-1", Role: string(model.RoleAssistant), Content: "reply", RequestID: "request-1"},
		{ID: "user-1", Role: string(model.RoleUser), Content: "question", RequestID: "request-1"},
	}

	t.Run("no finder leaves messages unchanged", func(t *testing.T) {
		api := &consoleAPI{}
		got, err := api.attachChatMessageCards(context.Background(), "tenant-a", messages)
		if err != nil || len(got) != len(messages) || got[0].Card != nil {
			t.Fatalf("attachChatMessageCards() = %#v, %v", got, err)
		}
	})

	t.Run("no assistant request IDs skips finder", func(t *testing.T) {
		finder := &failingOutboxBatchStore{MemoryStateStore: storage.NewMemoryStateStore(), err: errors.New("must not be called")}
		api := &consoleAPI{dependencies: ConsoleDependencies{Replies: finder}}
		got, err := api.attachChatMessageCards(context.Background(), "tenant-a", []chatTranscriptMessage{{Role: string(model.RoleUser), RequestID: "request-1"}, {Role: string(model.RoleAssistant)}})
		if err != nil || len(got) != 2 {
			t.Fatalf("attachChatMessageCards(no IDs) = %#v, %v", got, err)
		}
	})

	t.Run("finder failure is surfaced", func(t *testing.T) {
		wantErr := errors.New("outbox unavailable")
		finder := &failingOutboxBatchStore{MemoryStateStore: storage.NewMemoryStateStore(), err: wantErr}
		api := &consoleAPI{dependencies: ConsoleDependencies{Replies: finder}}
		if _, err := api.attachChatMessageCards(context.Background(), "tenant-a", messages); !errors.Is(err, wantErr) {
			t.Fatalf("attachChatMessageCards() error = %v", err)
		}
	})

	t.Run("malformed durable payload is rejected", func(t *testing.T) {
		state := storage.NewMemoryStateStore()
		if _, err := state.RecordExecution(context.Background(), storage.ExecutionRecord{
			TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
			MessageID: "message-1", Channel: "web", BindingID: "web-console", TraceID: "trace-1",
			Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web", OutboxRequestID: "request-1", OutboxPayload: []byte(`{`),
		}); err != nil {
			t.Fatal(err)
		}
		api := &consoleAPI{dependencies: ConsoleDependencies{State: state}}
		if _, err := api.attachChatMessageCards(context.Background(), "tenant-a", messages); err == nil || !strings.Contains(err.Error(), "decode stored reply card") {
			t.Fatalf("malformed payload error = %v", err)
		}
	})

	t.Run("durable reply without card leaves assistant unchanged", func(t *testing.T) {
		state := storage.NewMemoryStateStore()
		if _, err := state.RecordExecution(context.Background(), storage.ExecutionRecord{
			TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
			MessageID: "message-1", Channel: "web", BindingID: "web-console", TraceID: "trace-1",
			Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web", OutboxRequestID: "request-1", OutboxPayload: []byte(`{"text":"reply"}`),
		}); err != nil {
			t.Fatal(err)
		}
		api := &consoleAPI{dependencies: ConsoleDependencies{State: state}}
		got, err := api.attachChatMessageCards(context.Background(), "tenant-a", messages)
		if err != nil || got[0].Card != nil {
			t.Fatalf("reply without card = %#v, %v", got, err)
		}
	})
}

func TestMatchListedSessionFiltersAppChannelSubjectAndStatus(t *testing.T) {
	t.Parallel()
	entry := storage.Session{
		AppCode: "support", SessionKey: "acme/support/session/hello-1", SubjectID: "admin", Status: "active",
		Conversations: []storage.SessionConversation{{Channel: "web", ConversationID: "hello-1"}},
	}
	if !matchListedSession(entry, "support", "web", "admin", "active") {
		t.Fatal("expected matching session")
	}
	if matchListedSession(entry, "other", "web", "admin", "active") {
		t.Fatal("app filter should reject")
	}
	if matchListedSession(entry, "support", "telegram", "admin", "active") {
		t.Fatal("channel filter should reject")
	}
	if matchListedSession(entry, "support", "web", "other", "active") {
		t.Fatal("subject filter should reject")
	}
	if matchListedSession(entry, "support", "web", "admin", "archived") {
		t.Fatal("status filter should reject")
	}
}

func TestConsoleSessionMessagesReturnsFrameworkTranscript(t *testing.T) {
	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/session/conversation-1"
	sess, err := framework.CreateSession(context.Background(), agentsession.Key{
		AppName: "example/support", UserID: "user-1", SessionID: sessionKey,
	}, nil)
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	userReply := event.NewResponseEvent("inv-1", "user", &model.Response{
		Choices: []model.Choice{{Message: model.NewUserMessage("订单到哪了")}},
	})
	userReply.RequestID = "message-1"
	if err := framework.AppendEvent(context.Background(), sess, userReply); err != nil {
		t.Fatalf("AppendEvent(user) error = %v", err)
	}
	assistantReply := event.NewResponseEvent("inv-1", "assistant", &model.Response{
		Done: true, Object: model.ObjectTypeChatCompletion,
		Choices: []model.Choice{{Message: model.NewAssistantMessage("正在查询订单")}},
	})
	assistantReply.RequestID = "message-1"
	if err := framework.AppendEvent(context.Background(), sess, assistantReply); err != nil {
		t.Fatalf("AppendEvent(assistant) error = %v", err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentSessions = staticAgentSessionProvider{service: framework}
		if lister, ok := dependencies.State.(storage.SessionLister); ok {
			dependencies.Sessions = lister
		}
	})
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: sessionKey,
		MessageID: "message-1", Channel: "web", BindingID: "web-console", ConversationID: "conversation-1", ConversationScope: "direct", TraceID: "trace-1",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web", SubjectID: "user-1", OwnerPlatformUserID: "console-admin",
		OutboxPayload:   []byte(`{"channel":"web","web_owner_id":"console-admin","text":"正在查询订单","card":{"title":"订单信息","body":"已找到订单","actions":[{"label":"查看","url":"https://support.example.test/orders/42"}]}}`),
		OutboxRequestID: "message-1",
	}); err != nil {
		t.Fatalf("seed platform session: %v", err)
	}
	recorder := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fconversation-1", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "订单到哪了") || !strings.Contains(recorder.Body.String(), "正在查询订单") {
		t.Fatalf("transcript = %s", recorder.Body.String())
	}
	latest := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fconversation-1&limit=1", "")
	var latestPage struct {
		Messages   []chatTranscriptMessage `json:"messages"`
		NextCursor string                  `json:"next_cursor"`
	}
	if latest.Code != http.StatusOK || json.Unmarshal(latest.Body.Bytes(), &latestPage) != nil || len(latestPage.Messages) != 1 || latestPage.Messages[0].Content != "正在查询订单" || latestPage.Messages[0].Card == nil || latestPage.Messages[0].Card.Title != "订单信息" || latestPage.NextCursor == "" {
		t.Fatalf("latest transcript page = %d %s", latest.Code, latest.Body.String())
	}
	older := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fconversation-1&limit=1&before="+latestPage.NextCursor, "")
	var olderPage struct {
		Messages   []chatTranscriptMessage `json:"messages"`
		NextCursor string                  `json:"next_cursor"`
	}
	if older.Code != http.StatusOK || json.Unmarshal(older.Body.Bytes(), &olderPage) != nil || len(olderPage.Messages) != 1 || olderPage.Messages[0].Content != "订单到哪了" || olderPage.NextCursor != "" {
		t.Fatalf("older transcript page = %d %s", older.Code, older.Body.String())
	}
	list := handler.request(t, http.MethodGet, "/api/v1/sessions/mine?tenant=example&app=support&channel=web&subject=user-1&status=active", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d: %s", list.Code, list.Body.String())
	}
	var body struct {
		Sessions []struct {
			Preview string
		}
	}
	if err := json.Unmarshal(list.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(body.Sessions) != 1 || body.Sessions[0].Preview != "订单到哪了" {
		t.Fatalf("listed sessions = %#v", body.Sessions)
	}
}

func TestConsoleConversationContentAuditRequiresExplicitGrantAndRecordsRead(t *testing.T) {
	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/session/private-conversation"
	sess, err := framework.CreateSession(context.Background(), agentsession.Key{
		AppName: "example/support", UserID: "other-user", SessionID: sessionKey,
	}, nil)
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if err := framework.AppendEvent(context.Background(), sess, event.NewResponseEvent("inv-private", "user", &model.Response{
		Choices: []model.Choice{{Message: model.NewUserMessage("private message")}},
	})); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentSessions = staticAgentSessionProvider{service: framework}
		if lister, ok := dependencies.State.(storage.SessionLister); ok {
			dependencies.Sessions = lister
		}
	})
	if _, err := handler.state.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "example", AppCode: "support", SessionKey: sessionKey, SubjectID: "other-user", OwnerPlatformUserID: "other-user",
		MessageID: "private-message", Channel: "web", BindingID: "web-console", ConversationID: "private-conversation", ConversationScope: "direct",
		TraceID: "private-trace", Action: "agent_reply", Result: "queued", OutboxType: "channel_reply",
	}); err != nil {
		t.Fatalf("seed private session: %v", err)
	}
	if _, err := handler.artifacts.SaveArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "example/support", UserID: "other-user", SessionID: sessionKey,
	}, "private-report.txt", &agentartifact.Artifact{Data: []byte("private artifact"), MimeType: "text/plain"}); err != nil {
		t.Fatalf("seed private artifact: %v", err)
	}

	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	denied := handler.requestAs(t, admin, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fprivate-conversation", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("tenant admin transcript status = %d, want 403: %s", denied.Code, denied.Body.String())
	}
	deniedArtifact := handler.requestAs(t, admin, http.MethodGet,
		"/api/v1/artifacts?tenant=example&app=support&session="+url.QueryEscape(sessionKey)+"&filename=private-report.txt", "")
	if deniedArtifact.Code != http.StatusForbidden {
		t.Fatalf("tenant admin artifact status = %d, want 403: %s", deniedArtifact.Code, deniedArtifact.Body.String())
	}

	admin.Tenants[0].ConversationContentAudit = true
	allowed := handler.requestAs(t, admin, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fprivate-conversation", "")
	if allowed.Code != http.StatusOK || !strings.Contains(allowed.Body.String(), "private message") {
		t.Fatalf("authorized audit read = %d %s", allowed.Code, allowed.Body.String())
	}
	allowedArtifact := handler.requestAs(t, admin, http.MethodGet,
		"/api/v1/artifacts?tenant=example&app=support&session="+url.QueryEscape(sessionKey)+"&filename=private-report.txt", "")
	if allowedArtifact.Code != http.StatusOK || allowedArtifact.Body.String() != "private artifact" {
		t.Fatalf("authorized artifact audit read = %d %s", allowedArtifact.Code, allowedArtifact.Body.String())
	}
	events, err := handler.state.ListAudit(context.Background(), "example", "")
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	foundContent, foundArtifact := false, false
	for _, audit := range events {
		if audit.Action == "conversation_content_read" && audit.UserID == "tenant-admin" && audit.SessionID == sessionKey {
			foundContent = true
		}
		if audit.Action == "conversation_artifact_read" && audit.UserID == "tenant-admin" && audit.SessionID == sessionKey {
			foundArtifact = true
		}
	}
	if !foundContent || !foundArtifact {
		t.Fatalf("conversation content read audit missing: %+v", events)
	}
}

func TestConsoleSessionMessagesUsesNativeEventPagesAndStableAnchorCursor(t *testing.T) {
	framework := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = framework.Close() })
	const sessionKey = "example/support/session/paged-conversation"

	userEvent := func(requestID, content string) event.Event {
		evt := event.NewResponseEvent("inv-"+requestID, "user", &model.Response{
			Choices: []model.Choice{{Message: model.NewUserMessage(content)}},
		})
		evt.RequestID = requestID
		return *evt
	}
	assistantEvent := func(requestID, content string) event.Event {
		evt := event.NewResponseEvent("inv-"+requestID, "assistant", &model.Response{
			Done: true, Object: model.ObjectTypeChatCompletion,
			Choices: []model.Choice{{Message: model.NewAssistantMessage(content)}},
		})
		evt.RequestID = requestID
		return *evt
	}
	events := []event.Event{
		userEvent("request-old", "较早问题"),
		assistantEvent("request-old", "较早回答"),
	}
	for index := 0; index < 80; index++ {
		events = append(events, *event.NewResponseEvent("inv-tool", "assistant", &model.Response{
			Object:  model.ObjectTypeToolResponse,
			Choices: []model.Choice{{Message: model.NewAssistantMessage(fmt.Sprintf("tool-%d", index))}},
		}))
	}
	events = append(events,
		userEvent("request-new", "最新问题"),
		assistantEvent("request-new", "最新回答"),
	)
	paging := &pagingSessionService{Service: framework, events: events}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.AgentSessions = staticAgentSessionProvider{service: paging}
		if lister, ok := dependencies.State.(storage.SessionLister); ok {
			dependencies.Sessions = lister
		}
	})
	for _, record := range []storage.ExecutionRecord{
		{
			TenantID: "example", AppCode: "support", SessionKey: sessionKey, SubjectID: "user-1",
			MessageID: "request-old", Channel: "web", BindingID: "web-console", ConversationID: "conversation-1", ConversationScope: "direct",
			ActorExternalUserID: "user-old", TriggerType: "direct", TraceID: "trace-old", Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", OwnerPlatformUserID: "console-admin",
		},
		{
			TenantID: "example", AppCode: "support", SessionKey: sessionKey, SubjectID: "user-1",
			MessageID: "request-new", Channel: "web", BindingID: "web-console", ConversationID: "conversation-1", ConversationScope: "direct",
			ActorExternalUserID: "user-new", TriggerType: "direct", TraceID: "trace-new", Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", OwnerPlatformUserID: "console-admin",
		},
	} {
		if _, err := handler.state.RecordExecution(context.Background(), record); err != nil {
			t.Fatalf("seed route %s: %v", record.MessageID, err)
		}
	}

	latest := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fpaged-conversation&limit=2", "")
	var latestPage struct {
		Messages   []chatTranscriptMessage `json:"messages"`
		NextCursor string                  `json:"next_cursor"`
	}
	if latest.Code != http.StatusOK || json.Unmarshal(latest.Body.Bytes(), &latestPage) != nil {
		t.Fatalf("latest page = %d %s", latest.Code, latest.Body.String())
	}
	if len(latestPage.Messages) != 2 || latestPage.Messages[0].Content != "最新问题" || latestPage.Messages[1].Content != "最新回答" || latestPage.NextCursor == "" {
		t.Fatalf("latest messages = %#v, cursor=%q", latestPage.Messages, latestPage.NextCursor)
	}
	if latestPage.Messages[0].Source == nil || latestPage.Messages[0].Source.ActorExternalUserID != "user-new" {
		t.Fatalf("latest user source = %#v", latestPage.Messages[0].Source)
	}
	if paging.pageCalls < 2 || paging.fullCalls != 0 {
		t.Fatalf("native page calls=%d full calls=%d, want multiple native pages and no full transcript read", paging.pageCalls, paging.fullCalls)
	}

	// New events may arrive after the first page. The older cursor is anchored by
	// event ID, so those appends must not shift or duplicate the historical page.
	paging.events = append(paging.events,
		userEvent("request-late", "后来问题"),
		assistantEvent("request-late", "后来回答"),
	)
	older := handler.request(t, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fpaged-conversation&limit=2&before="+latestPage.NextCursor, "")
	var olderPage struct {
		Messages   []chatTranscriptMessage `json:"messages"`
		NextCursor string                  `json:"next_cursor"`
	}
	if older.Code != http.StatusOK || json.Unmarshal(older.Body.Bytes(), &olderPage) != nil {
		t.Fatalf("older page = %d %s", older.Code, older.Body.String())
	}
	if len(olderPage.Messages) != 2 || olderPage.Messages[0].Content != "较早问题" || olderPage.Messages[1].Content != "较早回答" || olderPage.NextCursor != "" {
		t.Fatalf("older messages = %#v, cursor=%q", olderPage.Messages, olderPage.NextCursor)
	}
	if olderPage.Messages[0].Source == nil || olderPage.Messages[0].Source.ActorExternalUserID != "user-old" {
		t.Fatalf("older user source = %#v", olderPage.Messages[0].Source)
	}
	if paging.windowCalls == 0 || paging.fullCalls != 0 {
		t.Fatalf("window calls=%d full calls=%d, want anchored window loading without full transcript read", paging.windowCalls, paging.fullCalls)
	}
}
