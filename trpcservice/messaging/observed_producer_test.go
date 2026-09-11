package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type observedDelegateProducer struct {
	envelopes []Envelope
	err       error
}

func (p *observedDelegateProducer) Publish(_ context.Context, envelope Envelope) error {
	p.envelopes = append(p.envelopes, envelope)
	return p.err
}

type recordingInboundObserver struct {
	attributes []metrics.InboundAttributes
	finished   []error
}

func (o *recordingInboundObserver) StartInbound(ctx context.Context, attributes metrics.InboundAttributes) (context.Context, func(error)) {
	o.attributes = append(o.attributes, attributes)
	return ctx, func(err error) { o.finished = append(o.finished, err) }
}

func TestObservedProducerRecordsAcceptedInboundWithoutChangingPublish(t *testing.T) {
	codec := testExecutionManifestCodec(t)
	envelope, err := NewInboundEnvelope(
		tenant.ReleaseSelection{Snapshot: tenant.Snapshot{Config: testInboundConfig(channels.Telegram)}, Variant: tenant.ReleaseStable},
		"telegram-main", "tenant-a/support/session/session-a",
		channels.InboundMessage{MessageID: "message-a", Channel: channels.Telegram, ConversationID: "chat-a", SenderID: "user-a", Text: "hello"}, codec,
	)
	if err != nil {
		t.Fatal(err)
	}
	delegate := &observedDelegateProducer{}
	observer := &recordingInboundObserver{}
	producer, err := NewObservedProducer(delegate, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if len(delegate.envelopes) != 1 || len(observer.attributes) != 1 || len(observer.finished) != 1 {
		t.Fatalf("publish=%d observe=%d finish=%d", len(delegate.envelopes), len(observer.attributes), len(observer.finished))
	}
	got := observer.attributes[0]
	if got.TenantID != "tenant-a" || got.AppCode != "support" || got.Channel != "telegram" {
		t.Fatalf("inbound attributes = %#v", got)
	}
}

func TestObservedProducerRecordsPublishFailure(t *testing.T) {
	envelope := validInboundEnvelopeForObservedProducer(t, channels.Web)
	want := errors.New("Kafka unavailable")
	delegate := &observedDelegateProducer{err: want}
	observer := &recordingInboundObserver{}
	producer, err := NewObservedProducer(delegate, observer)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Publish(context.Background(), envelope); !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(observer.finished) != 1 || !errors.Is(observer.finished[0], want) {
		t.Fatalf("finish errors = %#v", observer.finished)
	}
}

func validInboundEnvelopeForObservedProducer(t *testing.T, channel channels.Channel) Envelope {
	t.Helper()
	codec := testExecutionManifestCodec(t)
	binding := "web-console"
	if channel != channels.Web {
		binding = string(channel) + "-main"
	}
	envelope, err := NewInboundEnvelope(
		tenant.ReleaseSelection{Snapshot: tenant.Snapshot{Config: testInboundConfig(channel)}, Variant: tenant.ReleaseStable},
		binding, "tenant-a/support/session/session-a",
		channels.InboundMessage{MessageID: "message-a", Channel: channel, ConversationID: "chat-a", SenderID: "user-a", Text: "hello"}, codec,
	)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func testInboundConfig(channel channels.Channel) config.TenantConfig {
	binding := "web-console"
	if channel != channels.Web {
		binding = string(channel) + "-main"
	}
	return config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{Type: string(channel), BindingID: binding}},
	}
}

var _ metrics.InboundObserver = (*recordingInboundObserver)(nil)
