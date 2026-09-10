package storage

import (
	"context"
	"strings"
	"testing"
)

func TestMemoryStateStoreAtomicallyRecordsSessionAuditAndOutbox(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	outboxEvent, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID:      "tenant-a",
		AppCode:       "support",
		SessionKey:    "tenant-a/support/telegram/chat-1",
		MessageID:     "update-42",
		Channel:       "telegram",
		BindingID:     "telegram-bot",
		TraceID:       "trace-42",
		Action:        "agent.reply",
		Result:        "success",
		AuditDetail:   "authorization=Bearer top-secret api_key=another-secret",
		OutboxType:    "agent.reply.completed",
		ModelUsage:    &ModelUsage{ProviderID: "primary", ModelName: "model-a", PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5, CostMicros: 8},
		OutboxPayload: []byte(`{"message_id":"update-42"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}

	session, err := store.GetSession(context.Background(), "tenant-a", "tenant-a/support/telegram/chat-1")
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if got, want := session.LastMessageID, "update-42"; got != want {
		t.Fatalf("last message ID = %q, want %q", got, want)
	}
	if got, want := session.Revision, uint64(1); got != want {
		t.Fatalf("session revision = %d, want %d", got, want)
	}

	audits, err := store.ListAudit(context.Background(), "tenant-a", "trace-42")
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("audit count = %d, want 1", len(audits))
	}
	if strings.Contains(audits[0].Detail, "top-secret") || !strings.HasPrefix(audits[0].Detail, "hmac-sha256:") {
		t.Fatalf("private audit detail = %q, want keyed digest", audits[0].Detail)
	}

	pending, err := store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("ListPendingOutbox() error = %v", err)
	}
	if len(pending) != 1 || pending[0].ID != outboxEvent.ID {
		t.Fatalf("pending outbox = %#v, want event %q", pending, outboxEvent.ID)
	}
	if err := store.MarkOutboxDelivered(context.Background(), "tenant-a", outboxEvent.ID); err != nil {
		t.Fatalf("MarkOutboxDelivered() error = %v", err)
	}
	pending, err = store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("ListPendingOutbox() after delivery error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending outbox after delivery = %d, want 0", len(pending))
	}
}

func TestMemoryStateStoreKeepsAnonymousBindingScopedDirectSubjectsIndependent(t *testing.T) {
	store := NewMemoryStateStore()
	telegram, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "tg-bot",
		ConversationID: "chat-1", ExternalUserID: "user-1", SubjectID: "external:telegram:tg-bot:user-1", Scope: "direct",
	}, "tenant-a/support/session/session-1")
	if err != nil {
		t.Fatalf("ResolveSession(telegram) error = %v", err)
	}
	wecom, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "wecom", BindingID: "wx-bot",
		ConversationID: "chat-9", ExternalUserID: "user-1", SubjectID: "external:wecom:wx-bot:user-1", Scope: "direct",
	}, "tenant-a/support/session/session-2")
	if err != nil {
		t.Fatalf("ResolveSession(wecom) error = %v", err)
	}
	if telegram == wecom {
		t.Fatalf("anonymous binding-scoped subjects unexpectedly shared session %q", telegram)
	}
	for _, sessionKey := range []string{telegram, wecom} {
		if strings.Contains(sessionKey, "/telegram/") || strings.Contains(sessionKey, "/wecom/") {
			t.Fatalf("session key %q leaked the source channel", sessionKey)
		}
	}
}

func TestMemoryStateStoreKeepsDirectSessionsIndependentAcrossChannels(t *testing.T) {
	store := NewMemoryStateStore()
	web, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", BindingID: "web-console",
		ConversationID: "web-conversation", ExternalUserID: "platform-user-1", SubjectID: "platform-user-1", Scope: "direct",
	}, "tenant-a/support/session/web-session")
	if err != nil {
		t.Fatal(err)
	}
	wecom, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "wecom", BindingID: "wecom-bot",
		ConversationID: "wecom-conversation", ExternalUserID: "ming", SubjectID: "platform-user-1", Scope: "direct",
	}, "tenant-a/support/session/wecom-session")
	if err != nil {
		t.Fatal(err)
	}
	if web == wecom {
		t.Fatalf("direct sessions unexpectedly merged across channels: %q", web)
	}
}

func TestMemoryStateStoreClaimsHistoryWithoutChangingFrameworkSubject(t *testing.T) {
	store := NewMemoryStateStore()
	sessionKey, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "trailforge", AppCode: "assistant", Channel: "telegram", BindingID: "tg-main",
		ConversationID: "42", ExternalUserID: "42", SubjectID: "external:telegram:tg-main:42", Scope: "direct",
	}, "trailforge/assistant/session/history-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ClaimSession(context.Background(), "trailforge", sessionKey, "platform-1"); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSession(context.Background(), "trailforge", sessionKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnerPlatformUserID != "platform-1" || got.SubjectID != "external:telegram:tg-main:42" {
		t.Fatalf("claimed session = %+v", got)
	}
}

func TestMemoryStateStoreEndOwnedRouteStartsNewChannelSession(t *testing.T) {
	store := NewMemoryStateStore()
	first, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "trailforge", AppCode: "assistant", Channel: "telegram", BindingID: "tg-main",
		ConversationID: "42", ExternalUserID: "42", SubjectID: "platform-1", OwnerPlatformUserID: "platform-1", Scope: "direct",
	}, "trailforge/assistant/session/canonical-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EndChannelIdentityRoutes(context.Background(), "trailforge", "telegram", "tg-main", "42"); err != nil {
		t.Fatal(err)
	}
	second, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "trailforge", AppCode: "assistant", Channel: "web", BindingID: "web-console",
		ConversationID: "browser-1", ExternalUserID: "platform-1", SubjectID: "platform-1", OwnerPlatformUserID: "platform-1", Scope: "direct",
	}, "trailforge/assistant/session/canonical-2")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("ended channel route unexpectedly reused session %q", first)
	}
}

func TestMemoryStateStorePersistsGroupActorTriggerProjection(t *testing.T) {
	store := NewMemoryStateStore()
	_, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "trailforge", AppCode: "assistant", SessionKey: "trailforge/assistant/session/group-1",
		MessageID: "message-1", Channel: "feishu", BindingID: "fs-main", ConversationID: "oc-group", ConversationScope: "group",
		ExternalUserID: "ou-ming", ActorExternalUserID: "ou-ming", ActorPlatformUserID: "platform-1", TriggerType: "mention",
		TraceID: "trace-1", Action: "agent_reply", Result: "queued", OutboxType: "channel_reply", SubjectID: "group:feishu:fs-main:oc-group",
	})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := store.ListInboundMessageRoutes(context.Background(), "trailforge", "trailforge/assistant/session/group-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].ActorExternalUserID != "ou-ming" || routes[0].ActorPlatformUserID != "platform-1" || routes[0].TriggerType != "mention" {
		t.Fatalf("routes = %+v", routes)
	}
}

func TestMemoryStateStoreKeepsGroupConversationsIndependent(t *testing.T) {
	store := NewMemoryStateStore()
	first, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "feishu", BindingID: "fs-bot",
		ConversationID: "group-a", SubjectID: "user-1", Scope: "group",
	}, "tenant-a/support/session/group-1")
	if err != nil {
		t.Fatalf("ResolveSession(group-a) error = %v", err)
	}
	second, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "feishu", BindingID: "fs-bot",
		ConversationID: "group-b", SubjectID: "user-1", Scope: "group",
	}, "tenant-a/support/session/group-2")
	if err != nil {
		t.Fatalf("ResolveSession(group-b) error = %v", err)
	}
	if first == second {
		t.Fatalf("different groups shared session %q", first)
	}
}

func TestMemoryStateStoreSwitchSessionIsAtomicAndIdempotent(t *testing.T) {
	store := NewMemoryStateStore()
	route := SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "feishu", BindingID: "fs-main",
		ConversationID: "oc-1", ExternalUserID: "ou-1", SubjectID: "user-1", OwnerPlatformUserID: "user-1", Scope: "direct",
	}
	oldKey, err := store.ResolveSession(context.Background(), route, "tenant-a/support/session/old")
	if err != nil {
		t.Fatal(err)
	}
	request := SessionSwitchRequest{
		Route: route, SessionKey: "tenant-a/support/session/new", RequestID: "msg-new-1",
		OutboxType: "channel_reply.feishu", OutboxPayload: []byte(`{"channel":"feishu","conversation_id":"oc-1","text":"已开启新会话。"}`),
	}
	result, err := store.SwitchSession(context.Background(), request)
	if err != nil || result.SessionKey != request.SessionKey || result.Replayed {
		t.Fatalf("SwitchSession() = %+v, %v", result, err)
	}
	resolved, err := store.ResolveSession(context.Background(), route, "tenant-a/support/session/unexpected")
	if err != nil || resolved != request.SessionKey {
		t.Fatalf("ResolveSession() after switch = %q, %v", resolved, err)
	}
	old, err := store.GetSession(context.Background(), "tenant-a", oldKey)
	if err != nil || old.Status != "archived" {
		t.Fatalf("old session = %+v, %v", old, err)
	}
	replayed, err := store.SwitchSession(context.Background(), SessionSwitchRequest{
		Route: route, SessionKey: "tenant-a/support/session/second", RequestID: request.RequestID,
		OutboxType: request.OutboxType, OutboxPayload: request.OutboxPayload,
	})
	if err != nil || !replayed.Replayed || replayed.SessionKey != request.SessionKey {
		t.Fatalf("replayed SwitchSession() = %+v, %v", replayed, err)
	}
	pending, err := store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil || len(pending) != 1 || pending[0].RequestID != request.RequestID || pending[0].AggregateKey != request.SessionKey {
		t.Fatalf("pending outbox = %+v, %v", pending, err)
	}
}

func TestMemoryStateStoreRepairsExistingGroupSubject(t *testing.T) {
	store := NewMemoryStateStore()
	sessionKey, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "feishu", BindingID: "fs-bot",
		ConversationID: "group-a", SubjectID: "legacy-user", Scope: "group",
	}, "tenant-a/support/session/group-1")
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := store.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "feishu", BindingID: "fs-bot",
		ConversationID: "group-a", SubjectID: "group:feishu:fs-bot:group-a", Scope: "group",
	}, "tenant-a/support/session/group-2")
	if err != nil {
		t.Fatal(err)
	}
	if repaired != sessionKey {
		t.Fatalf("repaired group session = %q, want %q", repaired, sessionKey)
	}
	got, err := store.GetSession(context.Background(), "tenant-a", sessionKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.SubjectID != "group:feishu:fs-bot:group-a" || got.OwnerPlatformUserID != "" {
		t.Fatalf("repaired group session = %+v", got)
	}
}
