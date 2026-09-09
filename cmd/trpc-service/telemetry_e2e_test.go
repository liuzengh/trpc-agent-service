package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkmetricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// telemetryRuntimeForTest composes a runtime whose spans land in an in-memory
// recorder and whose metrics land in a manual reader.
// composeTestTelemetry wraps the test providers into a production runtime
// handle through the assembly seam.
func composeTestTelemetry(provider *sdktrace.TracerProvider, meterProvider *sdkmetric.MeterProvider) *telemetry.Runtime {
	return telemetry.NewRuntimeForTest(provider, meterProvider)
}

func webServerForTest(t *testing.T, runtimeValue *productionRuntime, f *productionTestFixture) *web.Server {
	t.Helper()
	server := web.NewServer(platform.NewMemoryStore(), platform.Runner{Store: platform.NewMemoryStore(), Responder: platform.EchoResponder{}})
	server.Resolver = runtimeValue.resolver
	server.AsyncIngress = runtimeValue.ingress
	server.Readiness = runtimeValue.readiness
	runtimeValue.beginDrainingHook = server.BeginDraining
	return server
}

func httptestRequest(method, path string, body []byte) *http.Request {
	return httptest.NewRequest(method, path, bytes.NewReader(body))
}

func TestTelemetryEndToEndWebhookChain(t *testing.T) {
	spanRecorder := sdktracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	metricReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(context.Background())
		_ = provider.Shutdown(context.Background())
	})

	f := newProductionTestFixture(t)
	factory := &deterministicAgentFactory{result: "telemetry synthetic reply", started: make(chan agent.AgentInput, 4)}
	larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 4)}
	telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 4)}
	hooks := newProductionRuntimeHooks()

	// Compose the production telemetry runtime from the test providers.
	telemetryRuntime := composeTestTelemetry(provider, meterProvider)
	dependencies := productionAssemblyDependencies{
		agentFactory:   factory,
		larkSender:     larkSender,
		telegramSender: telegramSender,
		Telemetry:      telemetryRuntime,
		dispatcherStartHook: func() {
			close(hooks.dispatcherEntered)
			<-hooks.dispatcherRelease
		},
		completionHook:      func() { hooks.completion <- struct{}{} },
		outboxCompletedHook: func() { hooks.outboxCompleted <- struct{}{} },
	}
	runtimeValue, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{factory: factory}, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	seedProductionRows(t, f, runtimeValue.pool)
	t.Cleanup(func() {
		hooks.releaseDispatcher()
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runtimeValue.Stop(stopCtx); err != nil {
			t.Logf("runtime stop failed: %v", err)
		}
	})
	store := webServerForTest(t, runtimeValue, f)
	middleware := runtimeValue.TelemetryWebMiddleware()
	if middleware == nil {
		t.Fatal("telemetry middleware missing")
	}
	store.SetTelemetryMiddleware(middleware)

	startResult := startProductionRuntime(t, runtimeValue, hooks)
	larkEventID := "lark-telemetry-" + f.tenantID
	body, timestamp, nonce := larkProductionEvent(t, f, larkEventID, "lark-message-"+f.tenantID)
	request := requestWithBody(http.MethodPost, "/webhook/lark/"+f.larkExternalID, body)
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", larkProductionSignature(timestamp, nonce, os.Getenv("P009GC_LARK_ENCRYPT_KEY"), body))
	recorder := serveProductionWebhook(t, store, "/webhook/lark/"+f.larkExternalID, request, body)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"accepted":true`) {
		t.Fatalf("webhook accepted failed: code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	awaitInput(t, factory.started, "telemetry runner")
	awaitSignal(t, hooks.completion, "telemetry atomic completion")
	finishProductionStart(t, startResult, hooks)
	awaitSender(t, larkSender)
	awaitSignal(t, hooks.outboxCompleted, "telemetry outbox completion")

	// Business invariants are unchanged by telemetry.
	if factory.Calls() != 1 || larkSender.Calls() != 1 || telegramSender.Calls() != 0 {
		t.Fatalf("business invariants changed: runner=%d lark=%d telegram=%d", factory.Calls(), larkSender.Calls(), telegramSender.Calls())
	}

	// Collect spans and verify the cross-boundary correlation chain.
	provider.ForceFlush(context.Background())
	ended := spanRecorder.Ended()
	required := []string{
		"http /webhook/{channel}",
		"queue queue.enqueue",
		"worker execution attempt",
	}
	var names []string
	for _, span := range ended {
		names = append(names, fmt.Sprintf("%q", span.Name()))
	}
	for _, name := range required {
		if !spansContain(ended, name) {
			t.Fatalf("missing required span %q; ended=%d names=%v", name, len(ended), names)
		}
	}

	// Metrics: drain into a metrics collection and verify low-cardinality
	// instruments exist with allowlisted attributes only.
	var collected sdkmetricdata.ResourceMetrics
	if err := metricReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	if len(collected.ScopeMetrics) == 0 {
		t.Fatal("no metrics exported")
	}
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if !strings.HasPrefix(metric.Name, "trpcagent.") {
				t.Fatalf("unexpected metric namespace: %s", metric.Name)
			}
			assertAllowlistedMetric(t, metric)
		}
	}
}

func requestWithBody(method, path string, body []byte) *http.Request {
	request := httptestRequest(method, path, body)
	return request
}

func spansContain(spans []sdktrace.ReadOnlySpan, names ...string) bool {
	for _, name := range names {
		found := false
		for _, span := range spans {
			if span.Name() == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func assertAllowlistedMetric(t *testing.T, metric sdkmetricdata.Metrics) {
	t.Helper()
	allowed := map[string]bool{
		"component": true, "operation": true, "outcome": true, "error_category": true,
		"channel": true, "method": true, "route": true, "status_class": true,
		"backend_kind": true, "worker_kind": true, "filtered_reason": true,
		"candidates": true, "hydrated": true, "scanned": true, "enqueued": true,
		"attempt": true, "job_fingerprint": true, "execution_fingerprint": true,
		"worker_fingerprint": true, "context_state": true,
	}
	check := func(attrs attribute.Set) {
		for _, kv := range attrs.ToSlice() {
			if !allowed[string(kv.Key)] {
				t.Fatalf("non-allowlisted metric attribute %q on %s", string(kv.Key), metric.Name)
			}
		}
	}
	if metric.Data != nil {
		switch data := metric.Data.(type) {
		case sdkmetricdata.Sum[int64]:
			for _, point := range data.DataPoints {
				check(point.Attributes)
			}
		case sdkmetricdata.Sum[float64]:
			for _, point := range data.DataPoints {
				check(point.Attributes)
			}
		case sdkmetricdata.Histogram[float64]:
			for _, point := range data.DataPoints {
				check(point.Attributes)
			}
		case sdkmetricdata.Gauge[int64]:
			for _, point := range data.DataPoints {
				check(point.Attributes)
			}
		}
	}
}
