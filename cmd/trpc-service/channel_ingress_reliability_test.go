package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
)

type retryingIngressProducer struct {
	failures int
	calls    int
	events   []messaging.Envelope
}

func (p *retryingIngressProducer) Publish(_ context.Context, envelope messaging.Envelope) error {
	p.calls++
	if p.calls <= p.failures {
		return errors.New("temporary Kafka failure")
	}
	p.events = append(p.events, envelope)
	return nil
}

func reliableIngressFixture(t *testing.T, producer messaging.Producer) *channelIngress {
	t.Helper()
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	if _, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: config.ChannelTelegram, BindingID: "support-bot", AccessPolicy: config.ChannelAccessPublic,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	state := storage.NewMemoryStateStore()
	identities := identity.NewMemoryIdentityStore()
	if err := identities.UpsertTenant(ctx, "tenant-a", "Support"); err != nil {
		t.Fatal(err)
	}
	controls := &channelControlHandler{
		approvals: &approvalFlowBroker{},
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return &connectorTestSender{}, nil
		},
	}
	manifests, err := messaging.NewExecutionManifestCodec("ingress-test", []byte("ingress-test-manifest-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &channelIngress{
		repository: repository, producer: producer, sessions: state, identities: identities, controls: controls,
		artifacts: connectorTestArtifactProvider{service: artifactinmemory.NewService()}, manifests: manifests,
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return &connectorTestSender{}, nil
		},
	}
}

func TestChannelIngressPublishReliablyRetriesTransientFailureWithoutDuplicatingDelivery(t *testing.T) {
	t.Parallel()
	producer := &retryingIngressProducer{failures: 1}
	ingress := reliableIngressFixture(t, producer)
	inbound := channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Telegram, ConversationID: "customer-1", SenderID: "customer-1",
		ConversationScope: channels.ConversationDirect, Text: "查询订单", ReceivedAt: time.Now().UTC(),
	}
	started := time.Now()
	if err := ingress.publishReliably(context.Background(), "support-bot", inbound); err != nil {
		t.Fatalf("publishReliably() error = %v", err)
	}
	if producer.calls != 2 || len(producer.events) != 1 {
		t.Fatalf("producer calls/events = %d/%d", producer.calls, len(producer.events))
	}
	if time.Since(started) < 90*time.Millisecond {
		t.Fatalf("retry returned without backoff: %v", time.Since(started))
	}
	got := producer.events[0]
	payload, err := messaging.DecodeInboundPayload(got)
	if err != nil {
		t.Fatalf("DecodeInboundPayload() error = %v", err)
	}
	if got.EventID != "message-1" || got.TenantID != "tenant-a" || payload.AppCode != "support" || payload.BindingID != "support-bot" || payload.Inbound.MessageID != "message-1" {
		t.Fatalf("published envelope/payload = %+v / %+v", got, payload)
	}
}

func TestChannelIngressPublishReliablyStopsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()
	producer := &retryingIngressProducer{failures: 100}
	ingress := reliableIngressFixture(t, producer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ingress.publishReliably(ctx, "support-bot", channels.InboundMessage{
		MessageID: "message-cancel", Channel: channels.Telegram, ConversationID: "customer-1", SenderID: "customer-1",
		ConversationScope: channels.ConversationDirect, Text: "查询订单", ReceivedAt: time.Now().UTC(),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publishReliably() error = %v", err)
	}
	if producer.calls > 1 || len(producer.events) != 0 {
		t.Fatalf("canceled producer calls/events = %d/%d", producer.calls, len(producer.events))
	}
}

func TestChannelIngressPublishReliablyAcknowledgesPolicyRejectionWithoutRetry(t *testing.T) {
	t.Parallel()
	state := storage.NewMemoryStateStore()
	resolver := newSessionIdentityResolver{platformUserID: "platform-member", role: identity.RoleMember}
	repository := tenant.NewMemoryRepository()
	if _, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-bot", AccessPolicy: config.ChannelAccessPublic}},
	}); err != nil {
		t.Fatal(err)
	}
	ingress := &channelIngress{repository: repository, sessions: state, identities: resolver}
	started := time.Now()
	err := ingress.publishReliably(context.Background(), "support-bot", channels.InboundMessage{
		MessageID: "group-new-member", Channel: channels.Telegram, ConversationID: "group-1", SenderID: "member-1",
		ConversationScope: channels.ConversationGroup, TriggerType: channels.TriggerCommand, Text: "/new",
	})
	if err != nil {
		t.Fatalf("publishReliably(policy rejection) = %v, want acknowledged nil", err)
	}
	if time.Since(started) >= 90*time.Millisecond {
		t.Fatalf("policy rejection entered retry backoff: %v", time.Since(started))
	}
	if pending, listErr := state.ListPendingOutbox(context.Background(), "tenant-a", 10); listErr != nil || len(pending) != 0 {
		t.Fatalf("policy rejection outbox = %#v, %v", pending, listErr)
	}
}
