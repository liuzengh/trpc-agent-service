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
	runtimePayload, _ := json.Marshal(tenant.RuntimeProfile{TenantID: id, Agent: tenant.AgentProfile{ID: "agent", Version: "1", Instruction: "help"}})
	if err := repository.CreateAgentVersion(ctx, id, "agent", "1", runtimePayload); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PublishAgent(ctx, id, "agent", "1"); err != nil {
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
	if audits, err := repository.ListAudits(ctx, id, 10); err != nil || len(audits) == 0 {
		t.Fatalf("audits=%+v err=%v", audits, err)
	}
}

func TestPostgresRuntimeNotifyAndRollbackIntegration(t *testing.T) {
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
	id := "runtime-integration-" + uuid.NewString()
	base := tenant.Tenant{ID: id, Name: id, Enabled: true, Agent: tenant.AgentProfile{ID: "assistant", Version: "1", Instruction: "version one"}}
	if err := repository.SeedTenants(ctx, []tenant.Tenant{base}); err != nil {
		t.Fatal(err)
	}

	changes, err := repository.WatchRuntimeChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewCachedRuntimeResolver(repository, time.Minute)
	resolverDone := make(chan error, 1)
	go func() { resolverDone <- resolver.Run(ctx) }()
	if err := resolver.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	initial, err := resolver.Resolve(ctx, id)
	if err != nil || initial.PublishedVersion != "1" || initial.Revision != 1 {
		t.Fatalf("initial runtime=%+v err=%v", initial, err)
	}

	next := tenant.RuntimeProfileFromTenant(base)
	next.Agent.Version = "2"
	next.Agent.Instruction = "version two"
	payload, _ := json.Marshal(next)
	if err := repository.CreateAgentVersion(ctx, id, base.Agent.ID, "2", payload); err != nil {
		t.Fatal(err)
	}
	app, err := repository.PublishAgent(ctx, id, base.Agent.ID, "2")
	if err != nil {
		t.Fatal(err)
	}
	if app.PublishedVersion != "2" || app.Revision != 2 {
		t.Fatalf("published app=%+v", app)
	}
	select {
	case change := <-changes:
		if change.TenantID != id || change.Version != "2" || change.Revision != 2 {
			t.Fatalf("notification=%+v", change)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime notification not received")
	}
	waitRuntimeVersion(t, ctx, resolver, id, "2", 2)

	rolledBack, err := repository.PublishAgent(ctx, id, base.Agent.ID, "1")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.PublishedVersion != "1" || rolledBack.Revision != 3 {
		t.Fatalf("rollback app=%+v", rolledBack)
	}
	waitRuntimeVersion(t, ctx, resolver, id, "1", 3)
	cancel()
	if err := <-resolverDone; err != nil {
		t.Fatal(err)
	}
}

func waitRuntimeVersion(t *testing.T, ctx context.Context, resolver *CachedRuntimeResolver, tenantID, version string, revision int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		profile, err := resolver.Resolve(ctx, tenantID)
		if err == nil && profile.PublishedVersion == version && profile.Revision == revision {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime did not change to %s/%d: %+v err=%v", version, revision, profile, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
