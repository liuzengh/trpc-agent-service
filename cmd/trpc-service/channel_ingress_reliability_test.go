package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
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
	ingress, _, _ := reliableIngressFixtureForChannel(t, producer, channels.Telegram, "support-bot")
	return ingress
}

func reliableIngressFixtureForChannel(t *testing.T, producer messaging.Producer, channel channels.Channel, bindingID string) (*channelIngress, *connectorTestSender, agentartifact.Service) {
	t.Helper()
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	if _, err := repository.Publish(ctx, config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: string(channel), BindingID: bindingID, AccessPolicy: config.ChannelAccessPublic,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	state := storage.NewMemoryStateStore()
	identities := identity.NewMemoryIdentityStore()
	if err := identities.UpsertTenant(ctx, "tenant-a", "Support"); err != nil {
		t.Fatal(err)
	}
	sender := &connectorTestSender{}
	controls := &channelControlHandler{
		approvals: &approvalFlowBroker{},
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}
	manifests, err := messaging.NewExecutionManifestCodec("ingress-test", []byte("ingress-test-manifest-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	pending, err := messaging.NewRedisPendingAttachmentStore(redisClient)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := artifactinmemory.NewService()
	return &channelIngress{
		repository: repository, producer: producer, sessions: state, identities: identities, controls: controls,
		artifacts: connectorTestArtifactProvider{service: artifacts}, pendingFiles: pending, manifests: manifests,
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}, sender, artifacts
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

func TestChannelIngressCombinesBareAttachmentWithFollowingTextForEnterpriseIM(t *testing.T) {
	for _, test := range []struct {
		name      string
		channel   channels.Channel
		bindingID string
	}{
		{name: "feishu", channel: channels.Feishu, bindingID: "feishu-main"},
		{name: "wecom", channel: channels.WeCom, bindingID: "wecom-main"},
	} {
		t.Run(test.name, func(t *testing.T) {
			producer := &retryingIngressProducer{failures: 1}
			ingress, sender, artifacts := reliableIngressFixtureForChannel(t, producer, test.channel, test.bindingID)
			attachment := channels.InboundMessage{
				MessageID: "attachment-1", Channel: test.channel, ConversationID: "customer-1", SenderID: "customer-1",
				ConversationScope: channels.ConversationDirect, ReceivedAt: time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC),
				ReceivedFiles: []channels.ReceivedFile{{Name: "proof.txt", MimeType: "text/plain", Data: []byte("evidence")}},
			}
			if err := ingress.publishReliably(context.Background(), test.bindingID, attachment); err != nil {
				t.Fatalf("bare attachment error = %v", err)
			}
			if producer.calls != 0 || len(producer.events) != 0 {
				t.Fatalf("bare attachment reached Kafka: calls/events=%d/%d", producer.calls, len(producer.events))
			}
			if len(sender.replies) != 1 || sender.replies[0].Text != "已收到附件，请继续发送你的问题。" {
				t.Fatalf("pending attachment acknowledgement = %#v", sender.replies)
			}

			followUp := channels.InboundMessage{
				MessageID: "text-1", Channel: test.channel, ConversationID: "customer-1", SenderID: "customer-1",
				ConversationScope: channels.ConversationDirect, Text: "请根据刚才的文件处理", ReceivedAt: attachment.ReceivedAt.Add(time.Second),
			}
			if err := ingress.publishReliably(context.Background(), test.bindingID, followUp); err != nil {
				t.Fatalf("follow-up text error = %v", err)
			}
			if producer.calls != 2 || len(producer.events) != 1 {
				t.Fatalf("retry calls/events = %d/%d, want 2/1", producer.calls, len(producer.events))
			}
			payload, err := messaging.DecodeInboundPayload(producer.events[0])
			if err != nil {
				t.Fatal(err)
			}
			if payload.Inbound.Text != followUp.Text || len(payload.Inbound.Files) != 1 || payload.Inbound.Files[0].Name != "proof.txt" {
				t.Fatalf("combined inbound = %#v", payload.Inbound)
			}
			version := payload.Inbound.Files[0].Version
			stored, err := artifacts.LoadArtifact(context.Background(), agentartifact.SessionInfo{
				AppName: "tenant-a/support", UserID: payload.Inbound.SubjectID, SessionID: producer.events[0].SessionKey,
			}, payload.Inbound.Files[0].ArtifactName, &version)
			if err != nil || stored == nil || string(stored.Data) != "evidence" {
				t.Fatalf("combined artifact = %#v, %v", stored, err)
			}
		})
	}
}

func TestChannelIngressDoesNotLeakGroupAttachmentAcrossSenders(t *testing.T) {
	producer := &retryingIngressProducer{}
	ingress, sender, _ := reliableIngressFixtureForChannel(t, producer, channels.Feishu, "feishu-main")
	receivedAt := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	attachment := channels.InboundMessage{
		MessageID: "alice-file", Channel: channels.Feishu, ConversationID: "group-1", SenderID: "alice",
		ConversationScope: channels.ConversationGroup, ReceivedAt: receivedAt,
		ReceivedFiles: []channels.ReceivedFile{{Name: "alice.txt", MimeType: "text/plain", Data: []byte("alice-only")}},
	}
	if err := ingress.publishReliably(context.Background(), "feishu-main", attachment); err != nil {
		t.Fatal(err)
	}
	if len(sender.replies) != 0 {
		t.Fatalf("group bare attachment emitted acknowledgement: %#v", sender.replies)
	}
	if err := ingress.publishReliably(context.Background(), "feishu-main", channels.InboundMessage{
		MessageID: "bob-text", Channel: channels.Feishu, ConversationID: "group-1", SenderID: "bob",
		ConversationScope: channels.ConversationGroup, TriggerType: channels.TriggerMention, Text: "Bob 的问题", ReceivedAt: receivedAt.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := ingress.publishReliably(context.Background(), "feishu-main", channels.InboundMessage{
		MessageID: "alice-text", Channel: channels.Feishu, ConversationID: "group-1", SenderID: "alice",
		ConversationScope: channels.ConversationGroup, TriggerType: channels.TriggerMention, Text: "Alice 的问题", ReceivedAt: receivedAt.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if len(producer.events) != 2 {
		t.Fatalf("published events = %d, want 2", len(producer.events))
	}
	bobPayload, err := messaging.DecodeInboundPayload(producer.events[0])
	if err != nil {
		t.Fatal(err)
	}
	alicePayload, err := messaging.DecodeInboundPayload(producer.events[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(bobPayload.Inbound.Files) != 0 {
		t.Fatalf("bob received alice attachment: %#v", bobPayload.Inbound.Files)
	}
	if len(alicePayload.Inbound.Files) != 1 || alicePayload.Inbound.Files[0].Name != "alice.txt" {
		t.Fatalf("alice attachment missing: %#v", alicePayload.Inbound.Files)
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
