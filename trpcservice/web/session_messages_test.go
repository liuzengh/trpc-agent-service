package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
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
	if err := framework.AppendEvent(context.Background(), sess, event.NewResponseEvent("inv-1", "user", &model.Response{
		Choices: []model.Choice{{Message: model.NewUserMessage("订单到哪了")}},
	})); err != nil {
		t.Fatalf("AppendEvent(user) error = %v", err)
	}
	if err := framework.AppendEvent(context.Background(), sess, event.NewResponseEvent("inv-1", "assistant", &model.Response{
		Done: true, Object: model.ObjectTypeChatCompletion,
		Choices: []model.Choice{{Message: model.NewAssistantMessage("正在查询订单")}},
	})); err != nil {
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
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", SubjectID: "user-1", OwnerPlatformUserID: "console-admin",
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
	if latest.Code != http.StatusOK || json.Unmarshal(latest.Body.Bytes(), &latestPage) != nil || len(latestPage.Messages) != 1 || latestPage.Messages[0].Content != "正在查询订单" || latestPage.NextCursor == "" {
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

	admin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleAdmin, Status: "active"}}}
	denied := handler.requestAs(t, admin, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fprivate-conversation", "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("tenant admin transcript status = %d, want 403: %s", denied.Code, denied.Body.String())
	}

	admin.Tenants[0].ConversationContentAudit = true
	allowed := handler.requestAs(t, admin, http.MethodGet, "/api/v1/sessions/messages?tenant=example&session_key=example%2Fsupport%2Fsession%2Fprivate-conversation", "")
	if allowed.Code != http.StatusOK || !strings.Contains(allowed.Body.String(), "private message") {
		t.Fatalf("authorized audit read = %d %s", allowed.Code, allowed.Body.String())
	}
	events, err := handler.state.ListAudit(context.Background(), "example", "")
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	found := false
	for _, audit := range events {
		if audit.Action == "conversation_content_read" && audit.UserID == "tenant-admin" && audit.SessionID == sessionKey {
			found = true
			break
		}
	}
	if !found {
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
