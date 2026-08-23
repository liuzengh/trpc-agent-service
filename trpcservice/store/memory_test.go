package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

func testInbound() channels.InboundEnvelope {
	return channels.InboundEnvelope{
		TenantID: "tenant", BindingID: "binding", Channel: "feishu",
		ExternalMessageID: "message", ExternalUserID: "user", ExternalConversationID: "chat",
		ConversationType: channels.ConversationP2P, Content: "hello", ReceivedAt: time.Now(), TraceID: "trace",
	}
}

func TestMemoryControlPlaneAndRetryLifecycle(t *testing.T) {
	ctx := context.Background()
	repository := NewMemoryRepository()
	profile := tenant.Tenant{ID: "tenant", Agent: tenant.AgentProfile{ID: "default", Version: "1"}}
	if err := repository.SeedTenants(ctx, []tenant.Tenant{profile}); err != nil {
		t.Fatal(err)
	}
	if tenants, err := repository.ListTenants(ctx); err != nil || len(tenants) != 1 {
		t.Fatalf("tenants=%d err=%v", len(tenants), err)
	}
	if err := repository.CreateAgent(ctx, "tenant", "agent", "Agent"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateAgentVersion(ctx, "tenant", "agent", "1", json.RawMessage(`{"instruction":"help"}`)); err != nil {
		t.Fatal(err)
	}
	if err := repository.PublishAgent(ctx, "tenant", "agent", "1"); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveChannelBinding(ctx, "tenant", tenant.ChannelBinding{ID: "feishu", Type: "feishu"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveBackendProfile(ctx, "tenant", "postgres", tenant.BackendProfile{Session: "postgres"}); err != nil {
		t.Fatal(err)
	}

	message := testInbound()
	if accepted, err := repository.AcceptInbound(ctx, message); err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	dispatch, _ := repository.ClaimDispatch(ctx, "relay", 1, time.Second)
	if err := repository.RetryDispatch(ctx, dispatch[0].ID, nil); err != nil {
		t.Fatal(err)
	}
	if dispatch, _ = repository.ClaimDispatch(ctx, "relay", 1, time.Second); len(dispatch) != 1 {
		t.Fatal("retried dispatch was not claimable")
	}
	if err := repository.StoreReply(ctx, channels.OutboundEnvelope{ID: "reply", TenantID: "tenant", BindingID: "feishu", Channel: "feishu"}); err != nil {
		t.Fatal(err)
	}
	replies, _ := repository.ClaimReplies(ctx, "gateway", 1, time.Second)
	if err := repository.RetryReply(ctx, replies[0].ID, nil); err != nil {
		t.Fatal(err)
	}
	if replies, _ = repository.ClaimReplies(ctx, "gateway", 1, time.Second); len(replies) != 1 || replies[0].Message.Attempts != 2 {
		t.Fatalf("retried replies=%+v", replies)
	}
	if err := repository.CompleteReply(ctx, replies[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := repository.AppendAudit(ctx, AuditLog{TenantID: "tenant"}); err != nil {
		t.Fatal(err)
	}
	stats, err := repository.Stats(ctx)
	if err != nil || stats.InboundTotal != 1 || stats.ReplyReady != 0 || stats.AuditTotal != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestMemoryInboxOutboxIdempotency(t *testing.T) {
	ctx := context.Background()
	repository := NewMemoryRepository()
	message := testInbound()
	accepted, err := repository.AcceptInbound(ctx, message)
	if err != nil || !accepted {
		t.Fatalf("first accept=%v err=%v", accepted, err)
	}
	accepted, err = repository.AcceptInbound(ctx, message)
	if err != nil || accepted {
		t.Fatalf("duplicate accept=%v err=%v", accepted, err)
	}
	tasks, err := repository.ClaimDispatch(ctx, "relay", 10, time.Second)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("dispatch tasks=%d err=%v", len(tasks), err)
	}
	if err := repository.CompleteDispatch(ctx, tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	out := channels.OutboundEnvelope{
		ID: "reply", TenantID: message.TenantID, BindingID: message.BindingID,
		Channel: message.Channel, Content: "answer", TraceID: message.TraceID,
	}
	if err := repository.CommitAgentResult(ctx, message, out); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitAgentResult(ctx, message, out); err != nil {
		t.Fatalf("duplicate commit should be idempotent: %v", err)
	}
	exists, err := repository.ReplyExists(ctx, out.ID)
	if err != nil || !exists {
		t.Fatalf("reply exists=%v err=%v", exists, err)
	}
	replies, err := repository.ClaimReplies(ctx, "gateway", 10, time.Second)
	if err != nil || len(replies) != 1 {
		t.Fatalf("replies=%d err=%v", len(replies), err)
	}
}
