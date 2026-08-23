package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

func TestPostgresInboxOutboxIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repository, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := "integration-" + uuid.NewString()
	profile := tenant.Tenant{
		ID: id, Name: id, Enabled: true,
		Agent: tenant.AgentProfile{ID: "assistant", Version: "1"},
	}
	if err := repository.SeedTenants(ctx, []tenant.Tenant{profile}); err != nil {
		t.Fatal(err)
	}
	if tenants, err := repository.ListTenants(ctx); err != nil || len(tenants) == 0 {
		t.Fatalf("tenants=%d err=%v", len(tenants), err)
	}
	if err := repository.CreateAgent(ctx, id, "agent", "Agent"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateAgentVersion(ctx, id, "agent", "1", json.RawMessage(`{"instruction":"help"}`)); err != nil {
		t.Fatal(err)
	}
	if err := repository.PublishAgent(ctx, id, "agent", "1"); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveChannelBinding(ctx, id, tenant.ChannelBinding{ID: "binding", Type: "feishu", CredentialRef: "fileenv:FEISHU"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveBackendProfile(ctx, id, "postgres", tenant.BackendProfile{Session: "postgres"}); err != nil {
		t.Fatal(err)
	}
	message := channels.InboundEnvelope{
		TenantID: id, BindingID: "binding", Channel: "feishu", ExternalMessageID: id,
		ExternalUserID: "user", ExternalConversationID: "chat", ConversationType: channels.ConversationP2P,
		Content: "hello", ReceivedAt: time.Now(), TraceID: uuid.NewString(), ReplyToken: "reply-to",
	}
	accepted, err := repository.AcceptInbound(ctx, message)
	if err != nil || !accepted {
		t.Fatalf("accept=%v err=%v", accepted, err)
	}
	accepted, err = repository.AcceptInbound(ctx, message)
	if err != nil || accepted {
		t.Fatalf("duplicate accept=%v err=%v", accepted, err)
	}
	dispatch, err := repository.ClaimDispatch(ctx, "integration", 1, time.Second)
	if err != nil || len(dispatch) != 1 {
		t.Fatalf("dispatch=%d err=%v", len(dispatch), err)
	}
	if err := repository.CompleteDispatch(ctx, dispatch[0].ID); err != nil {
		t.Fatal(err)
	}
	out := channels.OutboundEnvelope{
		ID: id + "-reply", TenantID: id, BindingID: "binding", Channel: "feishu",
		Content: "answer", TraceID: message.TraceID,
	}
	if err := repository.CommitAgentResult(ctx, message, out); err != nil {
		t.Fatal(err)
	}
	replies, err := repository.ClaimReplies(ctx, "integration", 1, time.Second)
	if err != nil || len(replies) != 1 {
		t.Fatalf("replies=%d err=%v", len(replies), err)
	}
	if err := repository.CompleteReply(ctx, replies[0].ID); err != nil {
		t.Fatal(err)
	}
}
