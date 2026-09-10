package otel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gootel "go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
)

func TestProviderExportsTraceAndMetricsWithPersistedParent(t *testing.T) {
	traces, metrics := &traceSink{}, &metricSink{}
	provider, err := newProvider(context.Background(), testConfig("http://collector:4318"), traces, metrics)
	if err != nil {
		t.Fatal(err)
	}
	const traceID = "0123456789abcdef0123456789abcdef"
	ctx, finish := telemetry.StartOperation(context.Background(), provider,
		"00-"+traceID+"-0123456789abcdef-01", telemetry.OperationGatewaySubmit,
		telemetry.ComponentAttribute(telemetry.ComponentGateway),
		telemetry.ComponentAttribute(telemetry.Component("tenant-secret")))
	if got := oteltrace.SpanContextFromContext(ctx).TraceID().String(); got != traceID {
		t.Fatalf("trace id=%s", got)
	}
	finish(nil)
	time.Sleep(30 * time.Millisecond)
	traces.mu.Lock()
	if len(traces.spans) != 1 {
		traces.mu.Unlock()
		t.Fatalf("exported spans=%d", len(traces.spans))
	}
	for _, item := range traces.spans[0].Attributes() {
		if item.Value.AsString() == "tenant-secret" {
			traces.mu.Unlock()
			t.Fatal("invalid attribute escaped whitelist")
		}
	}
	traces.mu.Unlock()
	if metrics.exports.Load() == 0 {
		t.Fatal("operation metrics were not exported")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatal("shutdown is not idempotent:", err)
	}
}

func TestOperationChainPropagatesOneTraceAcrossAsyncBoundaries(t *testing.T) {
	traces, metrics := &traceSink{}, &metricSink{}
	provider, err := newProvider(context.Background(), testConfig("http://collector:4318"), traces, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	ctx, finishGateway := telemetry.StartOperation(context.Background(), provider, "", telemetry.OperationGatewaySubmit,
		telemetry.ComponentAttribute(telemetry.ComponentGateway))
	gatewayParent := telemetry.EffectiveTraceParent(ctx, "")
	traceID := oteltrace.SpanContextFromContext(ctx).TraceID()
	finishGateway(nil)

	ctx, finishWorker := telemetry.StartOperation(context.Background(), provider, gatewayParent, telemetry.OperationWorkerExecute,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
	workerParent := telemetry.EffectiveTraceParent(ctx, "")
	finishWorker(nil)
	ctx, finishReply := telemetry.StartOperation(context.Background(), provider, workerParent, telemetry.OperationRelayReply,
		telemetry.ComponentAttribute(telemetry.ComponentBusinessRelay))
	replyParent := telemetry.EffectiveTraceParent(ctx, "")
	finishReply(nil)
	ctx, finishDelivery := telemetry.StartOperation(context.Background(), provider, replyParent, telemetry.OperationChannelDeliver,
		telemetry.ComponentAttribute(telemetry.ComponentChannelDelivery))
	finishDelivery(nil)

	time.Sleep(30 * time.Millisecond)
	traces.mu.Lock()
	defer traces.mu.Unlock()
	if len(traces.spans) != 4 {
		t.Fatalf("exported spans=%d", len(traces.spans))
	}
	parents := map[string]oteltrace.SpanID{}
	spanIDs := map[string]oteltrace.SpanID{}
	for _, span := range traces.spans {
		if span.SpanContext().TraceID() != traceID {
			t.Fatalf("operation %s left trace %s", span.Name(), span.SpanContext().TraceID())
		}
		parents[span.Name()] = span.Parent().SpanID()
		spanIDs[span.Name()] = span.SpanContext().SpanID()
	}
	if parents[string(telemetry.OperationWorkerExecute)] != spanIDs[string(telemetry.OperationGatewaySubmit)] ||
		parents[string(telemetry.OperationRelayReply)] != spanIDs[string(telemetry.OperationWorkerExecute)] ||
		parents[string(telemetry.OperationChannelDeliver)] != spanIDs[string(telemetry.OperationRelayReply)] {
		t.Fatalf("broken parent chain parents=%v spans=%v", parents, spanIDs)
	}
}

func TestFrameworkMetricsUseServicePipeline(t *testing.T) {
	previousTracerProvider := gootel.GetTracerProvider()
	previousMeterProvider := gootel.GetMeterProvider()
	previousPropagator := gootel.GetTextMapPropagator()
	previousFrameworkMeterProvider := agentmetric.GetMeterProvider()
	t.Cleanup(func() {
		gootel.SetTracerProvider(previousTracerProvider)
		gootel.SetMeterProvider(previousMeterProvider)
		gootel.SetTextMapPropagator(previousPropagator)
		if err := agentmetric.InitMeterProvider(previousFrameworkMeterProvider); err != nil {
			t.Errorf("restore framework meter provider: %v", err)
		}
	})

	traces, metrics := &traceSink{}, &metricSink{}
	provider, err := newProvider(context.Background(), testConfig("http://collector:4318"), traces, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := installFrameworkTelemetry(provider); err != nil {
		t.Fatal(err)
	}
	if agentmetric.GetMeterProvider() != provider.metrics {
		t.Fatal("framework metric provider does not match service provider")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}

}

func TestProviderRejectsUnsafeEndpointAndDoesNotDialOnOperation(t *testing.T) {
	config := testConfig("http://collector:4318")
	config.AllowInsecure = false
	if _, err := New(context.Background(), config); err == nil {
		t.Fatal("insecure endpoint accepted without explicit opt-in")
	}
	config = testConfig("http://127.0.0.1:1")
	config.BatchTimeout = time.Hour
	provider, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, finish := telemetry.StartOperation(context.Background(), provider, "", telemetry.OperationWorkerExecute,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
	finish(nil)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("business operation blocked on collector for %s", elapsed)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = provider.Shutdown(shutdownCtx)
}

func testConfig(endpoint string) Config {
	return Config{Endpoint: endpoint, ServiceName: "trpc-agent-service", ServiceVersion: "test", Role: "test",
		AllowInsecure: true, BatchTimeout: 5 * time.Millisecond, ExportTimeout: 200 * time.Millisecond,
		MetricInterval: 10 * time.Millisecond, MaxQueueSize: 16, MaxExportBatchSize: 8}
}

type traceSink struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (s *traceSink) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	s.mu.Lock()
	s.spans = append(s.spans, spans...)
	s.mu.Unlock()
	return nil
}
func (*traceSink) Shutdown(context.Context) error { return nil }

type metricSink struct{ exports atomic.Int64 }

func (*metricSink) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(kind)
}
func (*metricSink) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}
func (s *metricSink) Export(context.Context, *metricdata.ResourceMetrics) error {
	s.exports.Add(1)
	return nil
}
func (*metricSink) ForceFlush(context.Context) error { return nil }
func (*metricSink) Shutdown(context.Context) error   { return nil }
