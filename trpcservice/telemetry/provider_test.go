package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNoopProviderIsFailOpen(t *testing.T) {
	p, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := p.Start(context.Background(), "test")
	if ctx == nil || span == nil {
		t.Fatal("missing span")
	}
	span.End()
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOTLPHTTPExportsTraceAndMetric(t *testing.T) {
	var traces, metrics atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("content type=%q", r.Header.Get("Content-Type"))
		}
		switch r.URL.Path {
		case "/v1/traces":
			traces.Add(1)
		case "/v1/metrics":
			metrics.Add(1)
		default:
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	p, err := New(context.Background(), Config{Enabled: true, Endpoint: server.URL, ServiceName: "test", SampleRatio: 1, SpanBatchTimeout: 10 * time.Millisecond, MetricInterval: 10 * time.Millisecond, ExportTimeout: time.Second, MetricExportTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := p.Start(context.Background(), "gateway.submit")
	p.Metrics.Request(ctx, "tenant", "demo", "succeeded")
	span.End()
	time.Sleep(50 * time.Millisecond)
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if traces.Load() == 0 || metrics.Load() == 0 {
		t.Fatalf("trace requests=%d metric requests=%d", traces.Load(), metrics.Load())
	}
}

func TestInvalidEndpointRejected(t *testing.T) {
	if _, err := New(context.Background(), Config{Enabled: true, Endpoint: "relative"}); err == nil {
		t.Fatal("expected endpoint error")
	}
}

func TestBoundedMetricViewDropsOnlyFrameworkMetrics(t *testing.T) {
	stream, matched := boundedMetricView(sdkmetric.Instrument{Scope: instrumentation.Scope{Name: "trpc_agent_go.internal.chat"}})
	if !matched {
		t.Fatal("framework metric scope was not matched")
	}
	if _, ok := stream.Aggregation.(sdkmetric.AggregationDrop); !ok {
		t.Fatalf("aggregation = %T, want AggregationDrop", stream.Aggregation)
	}
	if _, matched = boundedMetricView(sdkmetric.Instrument{Scope: instrumentation.Scope{Name: "trpc-agent-service"}}); matched {
		t.Fatal("platform metric scope was dropped")
	}
}
