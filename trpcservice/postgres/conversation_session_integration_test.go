//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNewSessionLifecycleAndRestartPersistence(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	first := newIM05Request(t, p.route, p.binding, "new-session-message-a", "new-session-request-a", channels.MessageTypeText, "A")
	firstResult, err := p.store.Admit(p.ctx, first)
	if err != nil {
		t.Fatalf("admit first message: %v", err)
	}
	principalID, firstSession := executionSession(t, p, firstResult.RequestID)
	if firstSession != channels.DefaultSessionID {
		t.Fatalf("first session = %q, want %q", firstSession, channels.DefaultSessionID)
	}
	active, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve first active session: %v", err)
	}
	if active != firstSession {
		t.Fatalf("first active session = %q, want %q", active, firstSession)
	}

	newRequest := newIM05Request(t, p.route, p.binding, "new-session-command", "new-session-command-request", channels.MessageTypeText, "/new")
	if err := p.store.HandleNewSession(p.ctx, channels.NewSessionRequest{
		RequestID: newRequest.RequestID,
		Input:     *newRequest.ChannelInput,
	}); err != nil {
		t.Fatalf("handle /new: %v", err)
	}
	newActive, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve active session after /new: %v", err)
	}
	if newActive == firstSession || newActive == "" {
		t.Fatalf("active session after /new = %q, want a new non-empty ID", newActive)
	}

	var commandExecutionCount, commandReplyCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, newRequest.RequestID).Scan(&commandExecutionCount); err != nil {
		t.Fatalf("count /new execution: %v", err)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND request_id = $4 AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, newRequest.RequestID).Scan(&commandReplyCount); err != nil {
		t.Fatalf("count /new reply: %v", err)
	}
	if commandExecutionCount != 0 || commandReplyCount != 1 {
		t.Fatalf("/new durable rows = execution:%d command_reply:%d", commandExecutionCount, commandReplyCount)
	}
	var replyText string
	if err := p.pool.QueryRow(p.ctx, `
SELECT payload->>'text'
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND request_id = $4 AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, newRequest.RequestID).Scan(&replyText); err != nil {
		t.Fatalf("read /new reply: %v", err)
	}
	if replyText != channels.NewSessionSuccessReply {
		t.Fatalf("/new reply = %q, want %q", replyText, channels.NewSessionSuccessReply)
	}

	duplicate := newRequest
	duplicate.RequestID = "new-session-command-redelivery"
	if err := p.store.HandleNewSession(p.ctx, channels.NewSessionRequest{
		RequestID: duplicate.RequestID,
		Input:     *duplicate.ChannelInput,
	}); err != nil {
		t.Fatalf("redeliver /new: %v", err)
	}
	redeliveredActive, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve active session after redelivery: %v", err)
	}
	if redeliveredActive != newActive {
		t.Fatalf("redelivery changed active session from %q to %q", newActive, redeliveredActive)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID).Scan(&commandReplyCount); err != nil {
		t.Fatalf("count redelivered /new replies: %v", err)
	}
	if commandReplyCount != 1 {
		t.Fatalf("redelivered /new replies = %d, want 1", commandReplyCount)
	}

	second := newIM05Request(t, p.route, p.binding, "new-session-message-b", "new-session-request-b", channels.MessageTypeText, "B")
	secondResult, err := p.store.Admit(p.ctx, second)
	if err != nil {
		t.Fatalf("admit message after /new: %v", err)
	}
	_, secondSession := executionSession(t, p, secondResult.RequestID)
	if secondSession != newActive {
		t.Fatalf("message after /new session = %q, want %q", secondSession, newActive)
	}
	var oldExecutionCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND session_principal_id = $3
  AND session_id = $4`,
		p.scope.TenantID, p.scope.AppID, principalID, firstSession).Scan(&oldExecutionCount); err != nil {
		t.Fatalf("count old session executions: %v", err)
	}
	if oldExecutionCount != 1 {
		t.Fatalf("old session execution count = %d, want 1", oldExecutionCount)
	}

	restarted, err := platformpostgres.New(
		p.pool,
		platformpostgres.WithChannelIdentityMapping(
			integrationExternalIDHasher{},
			newIntegrationTargetProtector(t, "v1"),
			[]string{"v1"},
		),
	)
	if err != nil {
		t.Fatalf("create restarted store: %v", err)
	}
	restartedActive, err := restarted.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve active session from restarted store: %v", err)
	}
	if restartedActive != newActive {
		t.Fatalf("restarted active session = %q, want %q", restartedActive, newActive)
	}
}

func TestNewSessionGroupMemberDeniedAndReplyUsesOutbox(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newGroupNewSessionRequest(t, p.binding, "new-session-group", "new-session-group-request", "user-member")
	if err := p.store.HandleNewSession(p.ctx, request); err != nil {
		t.Fatalf("handle denied group /new: %v", err)
	}
	var pointerCount, executionCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.conversation_session
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID).Scan(&pointerCount); err != nil {
		t.Fatalf("count denied group pointers: %v", err)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2`, p.scope.TenantID, p.scope.AppID).Scan(&executionCount); err != nil {
		t.Fatalf("count denied group executions: %v", err)
	}
	if pointerCount != 0 || executionCount != 0 {
		t.Fatalf("denied group rows = pointers:%d executions:%d", pointerCount, executionCount)
	}
	var status, reason, replyText string
	if err := p.pool.QueryRow(p.ctx, `
SELECT status, COALESCE(reject_reason, '')
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, "new-session-group").Scan(&status, &reason); err != nil {
		t.Fatalf("read denied group inbox: %v", err)
	}
	if status != "REJECTED" || reason != "IM_ACCESS_DENIED" {
		t.Fatalf("denied group inbox = status:%q reason:%q", status, reason)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT payload->>'text'
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND request_id = $4 AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, request.RequestID).Scan(&replyText); err != nil {
		t.Fatalf("read denied group reply: %v", err)
	}
	if replyText != channels.NewSessionFailureReply {
		t.Fatalf("denied group reply = %q, want %q", replyText, channels.NewSessionFailureReply)
	}
}

func TestNewSessionUsesCanaryIMAccessPolicy(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	seed := newIM05Request(t, p.route, p.binding, "new-session-canary-seed", "new-session-canary-seed-request", channels.MessageTypeText, "seed")
	result, err := p.store.Admit(p.ctx, seed)
	if err != nil {
		t.Fatalf("admit canary seed: %v", err)
	}
	principalID, activeBefore := executionSession(t, p, result.RequestID)

	stable, err := p.store.ResolveAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, "v1")
	if err != nil {
		t.Fatalf("resolve stable config: %v", err)
	}
	canary := stable
	canary.Version = "v2"
	canary.IMAccess = tenant.IMAccessPolicy{AllowedUsers: []string{"different-user"}}
	if err := p.store.InsertAppConfigVersion(p.ctx, canary); err != nil {
		t.Fatalf("insert canary config: %v", err)
	}
	if _, err := p.store.EnableAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID, canary.Version, 100); err != nil {
		t.Fatalf("enable canary config: %v", err)
	}

	command := newIM05Request(t, p.route, p.binding, "new-session-canary-command", "new-session-canary-command-request", channels.MessageTypeText, "/new")
	if err := p.store.HandleNewSession(p.ctx, newSessionRequestFromAdmission(t, command)); err != nil {
		t.Fatalf("handle canary /new: %v", err)
	}
	activeAfter, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve active session after canary denial: %v", err)
	}
	if activeAfter != activeBefore {
		t.Fatalf("canary-denied /new switched session from %q to %q", activeBefore, activeAfter)
	}
	var status, reason, replyText string
	if err := p.pool.QueryRow(p.ctx, `
SELECT i.status, COALESCE(i.reject_reason, ''), o.payload->>'text'
FROM platform.channel_inbox i
JOIN platform.reply_outbox o
  ON o.tenant_id = i.tenant_id AND o.app_id = i.app_id
 AND o.binding_id = i.binding_id AND o.request_id = i.request_id
WHERE i.tenant_id = $1 AND i.app_id = $2 AND i.binding_id = $3
  AND i.external_message_id = $4 AND o.source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, command.IdempotencyKey,
	).Scan(&status, &reason, &replyText); err != nil {
		t.Fatalf("read canary-denied /new: %v", err)
	}
	if status != "REJECTED" || reason != "IM_ACCESS_DENIED" || replyText != channels.NewSessionFailureReply {
		t.Fatalf("canary-denied /new = status:%q reason:%q reply:%q", status, reason, replyText)
	}
}

func TestNewSessionConcurrentCommandsKeepOneScopedPointer(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	first := newIM05Request(t, p.route, p.binding, "new-session-concurrency-first", "new-session-concurrency-first-request", channels.MessageTypeText, "first")
	result, err := p.store.Admit(p.ctx, first)
	if err != nil {
		t.Fatalf("admit concurrency seed: %v", err)
	}
	principalID, _ := executionSession(t, p, result.RequestID)

	requests := []channels.NewSessionRequest{
		newSessionRequestFromAdmission(t, newIM05Request(t, p.route, p.binding, "new-session-concurrency-a", "new-session-concurrency-a-request", channels.MessageTypeText, "/new")),
		newSessionRequestFromAdmission(t, newIM05Request(t, p.route, p.binding, "new-session-concurrency-b", "new-session-concurrency-b-request", channels.MessageTypeText, "/new")),
	}
	results := make(chan error, len(requests))
	var group sync.WaitGroup
	for _, request := range requests {
		request := request
		group.Add(1)
		go func() {
			defer group.Done()
			results <- p.store.HandleNewSession(p.ctx, request)
		}()
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent /new: %v", err)
		}
	}
	active, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("resolve concurrent active session: %v", err)
	}
	if active == channels.DefaultSessionID || active == "" {
		t.Fatalf("concurrent active session = %q", active)
	}
	var pointerCount, version, replyCount, executionCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*), COALESCE(max(version), 0)
FROM platform.conversation_session
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND session_principal_id = $4`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, principalID).Scan(&pointerCount, &version); err != nil {
		t.Fatalf("read concurrent pointer: %v", err)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID).Scan(&replyCount); err != nil {
		t.Fatalf("count concurrent command replies: %v", err)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2`, p.scope.TenantID, p.scope.AppID).Scan(&executionCount); err != nil {
		t.Fatalf("count concurrent executions: %v", err)
	}
	if pointerCount != 1 || version != 3 || replyCount != 2 || executionCount != 1 {
		t.Fatalf("concurrent rows = pointers:%d version:%d replies:%d executions:%d", pointerCount, version, replyCount, executionCount)
	}
}

func TestNewSessionReplyRetryDoesNotRollbackCommittedSwitch(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	first := newIM05Request(t, p.route, p.binding, "new-session-retry-first", "new-session-retry-first-request", channels.MessageTypeText, "first")
	result, err := p.store.Admit(p.ctx, first)
	if err != nil {
		t.Fatalf("admit retry seed: %v", err)
	}
	principalID, _ := executionSession(t, p, result.RequestID)
	command := newIM05Request(t, p.route, p.binding, "new-session-retry-command", "new-session-retry-command-request", channels.MessageTypeText, "/new")
	if err := p.store.HandleNewSession(p.ctx, newSessionRequestFromAdmission(t, command)); err != nil {
		t.Fatalf("handle retry /new: %v", err)
	}
	activeBeforeRetry, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("read active session before reply retry: %v", err)
	}
	claimedCommand := false
	for !claimedCommand {
		deliveries, err := p.store.ClaimReplies(p.ctx, "new-session-retry-owner", time.Minute, 32)
		if err != nil {
			t.Fatalf("claim command reply: %v", err)
		}
		if len(deliveries) == 0 {
			t.Fatal("command reply was not claimable")
		}
		for _, delivery := range deliveries {
			if delivery.Reply.RequestID == command.RequestID {
				target, err := p.store.ResolveReplyTarget(p.ctx, delivery)
				if err != nil {
					t.Fatalf("resolve command reply target: %v", err)
				}
				if target != "user-target-1" {
					t.Fatalf("command reply target = %q, want %q", target, "user-target-1")
				}
				if err := p.store.RetryReply(p.ctx, delivery, "provider_transient", time.Hour, errors.New("provider unavailable")); err != nil {
					t.Fatalf("retry command reply: %v", err)
				}
				claimedCommand = true
				continue
			}
			if err := p.store.CompleteReply(p.ctx, delivery, channels.ProviderReceipt{ProviderMessageID: "integration-test-drain"}); err != nil {
				t.Fatalf("complete unrelated reply: %v", err)
			}
		}
	}
	activeAfterRetry, err := p.store.ResolveActiveSession(p.ctx, p.scope, p.binding.BindingID, principalID)
	if err != nil {
		t.Fatalf("read active session after reply retry: %v", err)
	}
	if activeAfterRetry != activeBeforeRetry {
		t.Fatalf("reply retry changed active session from %q to %q", activeBeforeRetry, activeAfterRetry)
	}
	var status string
	if err := p.pool.QueryRow(p.ctx, `
SELECT status
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND request_id = $4 AND source_kind = 'channel_command'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, command.RequestID).Scan(&status); err != nil {
		t.Fatalf("read retried command reply status: %v", err)
	}
	if status != "PENDING" {
		t.Fatalf("retried command reply status = %q, want PENDING", status)
	}
}

func TestNewSessionTenantAndBindingIsolation(t *testing.T) {
	first := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	firstMessage := newIM05Request(t, first.route, first.binding, "new-session-isolation-a", "new-session-isolation-a-request", channels.MessageTypeText, "A")
	firstResult, err := first.store.Admit(first.ctx, firstMessage)
	if err != nil {
		t.Fatalf("admit first isolation message: %v", err)
	}
	firstPrincipal, _ := executionSession(t, first, firstResult.RequestID)
	firstCommand := newIM05Request(t, first.route, first.binding, "new-session-isolation-a-new", "new-session-isolation-a-new-request", channels.MessageTypeText, "/new")
	if err := first.store.HandleNewSession(first.ctx, newSessionRequestFromAdmission(t, firstCommand)); err != nil {
		t.Fatalf("switch first isolation session: %v", err)
	}
	firstActive, err := first.store.ResolveActiveSession(first.ctx, first.scope, first.binding.BindingID, firstPrincipal)
	if err != nil {
		t.Fatalf("read first isolated pointer: %v", err)
	}

	otherBinding := seedIdentityMappingBinding(t, first.ctx, first.store, first.scope, "feishu-binding", channels.ChannelFeishu)
	otherBindingSnapshot, err := first.store.ResolveBinding(
		first.ctx, first.scope.TenantID, first.scope.AppID, otherBinding.BindingID,
	)
	if err != nil {
		t.Fatalf("resolve other binding: %v", err)
	}
	otherRoute, err := newRouteForBinding(first.ctx, first.store, otherBindingSnapshot.Snapshot())
	if err != nil {
		t.Fatalf("resolve other binding route: %v", err)
	}
	otherMessage := newIM05Request(t, otherRoute, otherBinding, "new-session-isolation-binding", "new-session-isolation-binding-request", channels.MessageTypeText, "binding")
	otherResult, err := first.store.Admit(first.ctx, otherMessage)
	if err != nil {
		t.Fatalf("admit other binding message: %v", err)
	}
	otherFixture := im05Fixture{
		pool: first.pool, ctx: first.ctx, store: first.store,
		scope: first.scope, binding: otherBinding, route: otherRoute,
	}
	otherPrincipal, _ := executionSession(t, otherFixture, otherResult.RequestID)
	otherCommand := newIM05Request(t, otherRoute, otherBinding, "new-session-isolation-binding-new", "new-session-isolation-binding-new-request", channels.MessageTypeText, "/new")
	if err := first.store.HandleNewSession(first.ctx, newSessionRequestFromAdmission(t, otherCommand)); err != nil {
		t.Fatalf("switch other binding session: %v", err)
	}
	otherActive, err := first.store.ResolveActiveSession(first.ctx, first.scope, otherBinding.BindingID, otherPrincipal)
	if err != nil {
		t.Fatalf("read other binding pointer: %v", err)
	}
	if otherActive == firstActive {
		t.Fatal("active session ID crossed binding scope")
	}
	unchangedBindingA, err := first.store.ResolveActiveSession(first.ctx, first.scope, first.binding.BindingID, firstPrincipal)
	if err != nil {
		t.Fatalf("re-read first binding pointer: %v", err)
	}
	if unchangedBindingA != firstActive {
		t.Fatalf("first binding pointer changed after other binding /new: %q -> %q", firstActive, unchangedBindingA)
	}

	secondScope := tenant.Scope{TenantID: fmt.Sprintf("im-new-session-other-%d", time.Now().UnixNano()), AppID: "support"}
	secondBinding := seedIdentityMappingScope(t, first.ctx, first.store, secondScope, "wecom-binding", channels.ChannelWeCom)
	secondBindingSnapshot, err := first.store.ResolveBinding(first.ctx, secondScope.TenantID, secondScope.AppID, secondBinding.BindingID)
	if err != nil {
		t.Fatalf("resolve second tenant binding: %v", err)
	}
	secondRoute, err := newRouteForBinding(first.ctx, first.store, secondBindingSnapshot.Snapshot())
	if err != nil {
		t.Fatalf("resolve second tenant route: %v", err)
	}
	secondMessage := newIM05Request(t, secondRoute, secondBinding, "new-session-isolation-b", "new-session-isolation-b-request", channels.MessageTypeText, "B")
	secondResult, err := first.store.Admit(first.ctx, secondMessage)
	if err != nil {
		t.Fatalf("admit second isolation message: %v", err)
	}
	secondFixture := im05Fixture{
		pool: first.pool, ctx: first.ctx, store: first.store,
		scope: secondScope, binding: secondBinding, route: secondRoute,
	}
	secondPrincipal, _ := executionSession(t, secondFixture, secondResult.RequestID)
	secondCommand := newIM05Request(t, secondRoute, secondBinding, "new-session-isolation-b-new", "new-session-isolation-b-new-request", channels.MessageTypeText, "/new")
	if err := first.store.HandleNewSession(first.ctx, newSessionRequestFromAdmission(t, secondCommand)); err != nil {
		t.Fatalf("switch second isolation session: %v", err)
	}
	secondActive, err := first.store.ResolveActiveSession(first.ctx, secondScope, secondBinding.BindingID, secondPrincipal)
	if err != nil {
		t.Fatalf("read second isolated pointer: %v", err)
	}
	if firstActive == secondActive {
		t.Fatal("active session ID crossed tenant scope")
	}
	unchangedFirst, err := first.store.ResolveActiveSession(first.ctx, first.scope, first.binding.BindingID, firstPrincipal)
	if err != nil {
		t.Fatalf("re-read first isolated pointer: %v", err)
	}
	if unchangedFirst != firstActive {
		t.Fatalf("first tenant pointer changed after second tenant /new: %q -> %q", firstActive, unchangedFirst)
	}
}

func newSessionRequestFromAdmission(t *testing.T, request gateway.AdmissionRequest) channels.NewSessionRequest {
	t.Helper()
	if request.ChannelInput == nil {
		t.Fatal("channel input is required")
	}
	return channels.NewSessionRequest{
		RequestID: request.RequestID,
		Input:     *request.ChannelInput,
	}
}

func newGroupNewSessionRequest(
	t *testing.T,
	binding channels.Binding,
	externalMessageID, requestID, senderID string,
) channels.NewSessionRequest {
	t.Helper()
	input, err := channels.NewChannelInput(
		channels.ChannelInput{
			TenantID:          binding.TenantID,
			AppID:             binding.AppID,
			Channel:           binding.Channel,
			BindingID:         binding.BindingID,
			BindingRevision:   binding.BindingRevision,
			ExternalMessageID: externalMessageID,
			Conversation:      channels.ChannelConversation{Kind: channels.ConversationGroup},
			MessageType:       channels.MessageTypeText,
			Text:              "/new",
		},
		channels.ChannelMappingInput{
			ExternalSenderID:           senderID,
			ExternalChatID:             "group-chat",
			ProviderSenderTarget:       "sender-target",
			ProviderConversationTarget: "conversation-target",
		},
	)
	if err != nil {
		t.Fatalf("build group /new input: %v", err)
	}
	return channels.NewSessionRequest{RequestID: requestID, Input: input}
}

func executionSession(t *testing.T, p im05Fixture, requestID string) (string, string) {
	t.Helper()
	var principalID, sessionID string
	if err := p.pool.QueryRow(p.ctx, `
SELECT session_principal_id, session_id
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, requestID).Scan(&principalID, &sessionID); err != nil {
		t.Fatalf("read execution session %s: %v", requestID, err)
	}
	return principalID, sessionID
}

func newRouteForBinding(
	ctx context.Context,
	store *platformpostgres.Store,
	binding channels.BindingSnapshot,
) (gateway.LocatedChannelBinding, error) {
	return gateway.ResolveChannelBindingRoute(ctx, store, binding.Channel, binding.PublicRouteID)
}
