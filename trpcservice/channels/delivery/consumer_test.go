package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type replyQueueStub struct {
	reclaimed      []channel.ReplyDelivery
	acked          []channel.ReplyDelivery
	consumeStarted chan struct{}
}

func (q *replyQueueStub) ConsumeReplies(ctx context.Context, _ channel.ReplyDestination, _ channel.ReplyConsumerOptions, _ func(context.Context, channel.ReplyDelivery) error) error {
	if q.consumeStarted != nil {
		close(q.consumeStarted)
	}
	<-ctx.Done()
	return ctx.Err()
}
func (q *replyQueueStub) AckReply(_ context.Context, _ channel.ReplyDestination, delivery channel.ReplyDelivery) error {
	q.acked = append(q.acked, delivery)
	return nil
}
func (q *replyQueueStub) ReclaimReplies(context.Context, channel.ReplyDestination, channel.ReplyConsumerOptions) ([]channel.ReplyDelivery, error) {
	return append([]channel.ReplyDelivery(nil), q.reclaimed...), nil
}

type eventDelivererStub struct {
	calls int
	err   error
}

func (s *eventDelivererStub) Deliver(context.Context, channel.ReplyEvent) error {
	s.calls++
	return s.err
}

type traceCapturingDeliverer struct{ traceParent string }

func (s *traceCapturingDeliverer) Deliver(ctx context.Context, _ channel.ReplyEvent) error {
	s.traceParent = telemetry.EffectiveTraceParent(ctx, "")
	return nil
}

type traceCapturingProvider struct {
	operation telemetry.Operation
	parent    oteltrace.SpanContext
}

func (p *traceCapturingProvider) StartSpan(ctx context.Context, operation telemetry.Operation, _ ...telemetry.Attribute) (context.Context, telemetry.Span) {
	p.operation = operation
	p.parent = oteltrace.SpanContextFromContext(ctx)
	return ctx, traceCapturingSpan{}
}
func (*traceCapturingProvider) Counter(telemetry.MetricDescriptor) telemetry.Counter {
	return telemetry.Noop().Counter(telemetry.MetricOperationTotal)
}
func (*traceCapturingProvider) Histogram(telemetry.MetricDescriptor) telemetry.Histogram {
	return telemetry.Noop().Histogram(telemetry.MetricOperationDuration)
}
func (*traceCapturingProvider) Logger(telemetry.Component) telemetry.Logger {
	return telemetry.Noop().Logger(telemetry.ComponentChannelDelivery)
}
func (*traceCapturingProvider) Shutdown(context.Context) error { return nil }

type traceCapturingSpan struct{}

func (traceCapturingSpan) End(error) {}

func TestConsumerACKsOnlyAfterDurableDeliverySuccess(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination, Event: channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ContentRef: "result://request"}}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &eventDelivererStub{}
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1"}
	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || deliverer.calls != 1 || len(queue.acked) != 1 {
		t.Fatalf("count=%d calls=%d acked=%d err=%v", count, deliverer.calls, len(queue.acked), err)
	}
}

func TestConsumerLeavesRetryableDeliveryPending(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination, Event: channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ContentRef: "result://request"}}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &eventDelivererStub{err: runtime.ErrBackendUnavailable}
	var reported error
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1", OnDeliveryError: func(_ channel.ReplyDelivery, err error) { reported = err }}
	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || len(queue.acked) != 0 || !errors.Is(reported, runtime.ErrBackendUnavailable) {
		t.Fatalf("count=%d acked=%d reported=%v err=%v", count, len(queue.acked), reported, err)
	}
}

func TestConsumerACKsDurablyRecordedTerminalFailure(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &eventDelivererStub{err: TerminalError{Err: errors.New("recipient blocked")}}
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1"}
	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || len(queue.acked) != 1 {
		t.Fatalf("count=%d acked=%d err=%v", count, len(queue.acked), err)
	}
}

func TestConsumerRejectsCrossBindingDelivery(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	wrong := destination
	wrong.ChannelBindingID = "other"
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{{ID: "1-0", Destination: wrong}}}
	consumer := Consumer{Queue: queue, Deliverer: &eventDelivererStub{}, Destination: destination, ConsumerID: "adapter-1"}
	count, err := consumer.ReclaimOnce(context.Background())
	if count != 0 || !errors.Is(err, runtime.ErrTenantScope) || len(queue.acked) != 0 {
		t.Fatalf("count=%d acked=%d err=%v", count, len(queue.acked), err)
	}
}

func TestConsumerRestoresDurableTraceParentBeforeIMDelivery(t *testing.T) {
	const traceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination, Event: channel.ReplyEvent{
		SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ContentRef: "result://request", TraceParent: traceParent,
	}}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &traceCapturingDeliverer{}
	provider := &traceCapturingProvider{}
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1", Telemetry: provider}

	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || len(queue.acked) != 1 {
		t.Fatalf("count=%d acked=%d err=%v", count, len(queue.acked), err)
	}
	if provider.operation != telemetry.OperationChannelDeliver {
		t.Fatalf("operation=%q", provider.operation)
	}
	if got := provider.parent.TraceID().String(); got != "0123456789abcdef0123456789abcdef" || !provider.parent.IsRemote() {
		t.Fatalf("restored parent=%s remote=%t", got, provider.parent.IsRemote())
	}
	if deliverer.traceParent != traceParent {
		t.Fatalf("adapter traceparent=%q want=%q", deliverer.traceParent, traceParent)
	}
}

func TestConsumerRunCancelsBothQueueLoops(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	queue := &replyQueueStub{consumeStarted: make(chan struct{})}
	consumer := Consumer{
		Queue:           queue,
		Deliverer:       &eventDelivererStub{},
		Destination:     destination,
		ConsumerID:      "adapter-1",
		ReclaimInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	<-queue.consumeStarted
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v, want context cancellation", err)
	}
}

func TestConsumerRejectsIncompleteConfiguration(t *testing.T) {
	consumer := Consumer{Queue: &replyQueueStub{}, Deliverer: &eventDelivererStub{}}
	if _, err := consumer.ReclaimOnce(context.Background()); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("ReclaimOnce error=%v", err)
	}
}
