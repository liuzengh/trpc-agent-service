package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewInboundEnvelopeWithContextCarriesTraceParent(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("test").Start(context.Background(), "http.server.request")
	defer span.End()
	envelope, err := NewInboundEnvelopeWithContext(ctx, tenant.ReleaseSelection{Snapshot: tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", ConfigVersion: 1}}, Variant: tenant.ReleaseStable}, "telegram-a", "tenant-a/support/session/session-1", channels.InboundMessage{
		MessageID: "message-1", ProviderRequestID: "provider-request-1", Channel: channels.Telegram,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello", ReceivedAt: time.Now(),
	}, testExecutionManifestCodec(t))
	if err != nil {
		t.Fatalf("NewInboundEnvelopeWithContext() error = %v", err)
	}
	if envelope.TraceParent == "" {
		t.Fatal("Kafka envelope must carry the HTTP traceparent")
	}
	if envelope.Manifest == nil {
		t.Fatal("Kafka inbound envelope must carry a signed execution manifest")
	}
	payload, err := DecodeInboundPayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Inbound.ProviderRequestID != "provider-request-1" {
		t.Fatalf("provider request ID = %q", payload.Inbound.ProviderRequestID)
	}
}
