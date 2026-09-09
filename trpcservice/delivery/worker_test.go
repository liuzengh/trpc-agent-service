package delivery

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func TestOutboxDeliveryContinuesDurableTraceWithoutPayloadLeak(t *testing.T) {
	oldProvider := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	defer func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
	}()
	telemetry, err := servicemetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	log := &eventLog{}
	store := &workerStore{log: log}
	router, err := NewRouter(Route{Binding: BindingKey{TenantID: "tenant", BindingID: "wecom"}, Sender: senderFunc(func(context.Context, gateway.OutboundMessage) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(store, router, limiterFunc(func(context.Context, gateway.OutboundMessage) error { return nil }), telemetry, WorkerConfig{Owner: "worker", ClaimTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const traceID = "0102030405060708090a0b0c0d0e0f10"
	canary := "secret-and-message-canary"
	message := gateway.OutboundMessage{TenantID: "tenant", AppID: "app", BindingID: "wecom", OutboxID: "outbox", Text: canary, TraceID: canary, TraceContext: map[string]string{
		"traceparent": "00-" + traceID + "-0102030405060708-01",
		"tracestate":  "vendor=" + canary,
		"baggage":     "authorization=" + canary,
	}}
	worker.deliver(context.Background(), Claim{Message: message, Owner: "worker", ClaimToken: "token", Attempt: 1, Status: StatusClaimed, LeaseUntil: time.Now().Add(time.Hour)})
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "outbox.deliver" || spans[0].SpanContext.TraceID().String() != traceID {
		t.Fatalf("delivery spans=%+v", spans)
	}
	if spans[0].SpanContext.TraceState().Len() != 0 {
		t.Fatal("untrusted tracestate reached exported span")
	}
	for _, attr := range spans[0].Attributes {
		if strings.Contains(attr.Value.Emit(), canary) {
			t.Fatalf("span attribute %q leaked canary", attr.Key)
		}
	}
}

func (log *eventLog) add(event string) {
	log.mu.Lock()
	log.events = append(log.events, event)
	log.mu.Unlock()
}

type workerStore struct {
	log             *eventLog
	failRetryable   bool
	failStatus      Status
	markedSent      bool
	markedUncertain bool
}

func (store *workerStore) ClaimReady(context.Context, []BindingKey, string, time.Duration, int) ([]Claim, error) {
	return nil, nil
}
func (store *workerStore) Renew(_ context.Context, claim Claim, _ time.Duration) (Claim, error) {
	store.log.add("renew")
	return claim, nil
}
func (store *workerStore) BeginSend(context.Context, Claim) error {
	store.log.add("begin")
	return nil
}
func (store *workerStore) MarkSent(context.Context, Claim) error {
	store.log.add("sent")
	store.markedSent = true
	return nil
}
func (store *workerStore) Fail(_ context.Context, _ Claim, _ error, _ time.Time, retryable bool) (Status, error) {
	store.log.add("fail")
	store.failRetryable = retryable
	store.failStatus = StatusRetry
	return StatusRetry, nil
}
func (store *workerStore) MarkUncertain(context.Context, Claim, error) error {
	store.log.add("uncertain")
	store.markedUncertain = true
	return nil
}

type senderFunc func(context.Context, gateway.OutboundMessage) error

func (send senderFunc) SendText(ctx context.Context, message gateway.OutboundMessage) error {
	return send(ctx, message)
}

type limiterFunc func(context.Context, gateway.OutboundMessage) error

func (limit limiterFunc) Wait(ctx context.Context, message gateway.OutboundMessage) error {
	return limit(ctx, message)
}

type classifiedError struct{ canRetry bool }

func (err classifiedError) Error() string           { return "provider rejected" }
func (err classifiedError) DeliveryRetryable() bool { return err.canRetry }

func TestWorkerDeliveryTransitions(t *testing.T) {
	tests := []struct {
		name              string
		sendErr           error
		wantEvents        []string
		wantSent          bool
		wantUncertain     bool
		wantFailRetryable bool
	}{
		{name: "success", wantEvents: []string{"limit", "begin", "send", "sent"}, wantSent: true},
		{name: "retryable provider error", sendErr: classifiedError{canRetry: true}, wantEvents: []string{"limit", "begin", "send", "fail"}, wantFailRetryable: true},
		{name: "permanent provider error", sendErr: classifiedError{canRetry: false}, wantEvents: []string{"limit", "begin", "send", "fail"}},
		{name: "unknown provider outcome", sendErr: &channels.UncertainError{Cause: errors.New("timeout")}, wantEvents: []string{"limit", "begin", "send", "uncertain"}, wantUncertain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &eventLog{}
			store := &workerStore{log: log}
			message := gateway.OutboundMessage{TenantID: "tenant", AppID: "app", BindingID: "wecom", OutboxID: "outbox", Text: "hello"}
			router, err := NewRouter(Route{Binding: BindingKey{TenantID: "tenant", BindingID: "wecom"}, Sender: senderFunc(func(context.Context, gateway.OutboundMessage) error {
				log.add("send")
				return test.sendErr
			})})
			if err != nil {
				t.Fatal(err)
			}
			worker, err := NewWorker(store, router, limiterFunc(func(context.Context, gateway.OutboundMessage) error {
				log.add("limit")
				return nil
			}), nil, WorkerConfig{Owner: "worker", ClaimTTL: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			worker.deliver(context.Background(), Claim{Message: message, Owner: "worker", ClaimToken: "token", Attempt: 1, Status: StatusClaimed, LeaseUntil: time.Now().Add(time.Hour)})
			if !reflect.DeepEqual(log.events, test.wantEvents) {
				t.Fatalf("events=%v want=%v", log.events, test.wantEvents)
			}
			if store.markedSent != test.wantSent || store.markedUncertain != test.wantUncertain || store.failRetryable != test.wantFailRetryable {
				t.Fatalf("sent=%v uncertain=%v retryable=%v", store.markedSent, store.markedUncertain, store.failRetryable)
			}
		})
	}
}

func TestRouterRejectsDuplicateTenantBinding(t *testing.T) {
	sender := senderFunc(func(context.Context, gateway.OutboundMessage) error { return nil })
	key := BindingKey{TenantID: "tenant", BindingID: "binding"}
	if _, err := NewRouter(Route{Binding: key, Sender: sender}, Route{Binding: key, Sender: sender}); err == nil {
		t.Fatal("duplicate route accepted")
	}
}

type evalSequence struct {
	mu      sync.Mutex
	results []any
	calls   int
}

func (eval *evalSequence) Eval(context.Context, string, []string, ...any) (any, error) {
	eval.mu.Lock()
	defer eval.mu.Unlock()
	result := eval.results[min(eval.calls, len(eval.results)-1)]
	eval.calls++
	return result, nil
}

func TestRedisLimiterWaitsForSharedWindow(t *testing.T) {
	eval := &evalSequence{results: []any{int64(1), int64(0)}}
	limiter := &RedisFixedWindowLimiter{Redis: eval, Limit: 1, Window: time.Millisecond}
	if err := limiter.Wait(context.Background(), gateway.OutboundMessage{TenantID: "tenant", BindingID: "wecom"}); err != nil {
		t.Fatal(err)
	}
	if eval.calls != 2 {
		t.Fatalf("calls=%d", eval.calls)
	}
}
