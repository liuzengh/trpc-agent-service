package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var (
	promOnce       sync.Once
	metricsHandler http.Handler
)

// requireProm installs the Prometheus-backed provider once, so every test
// that reads recorded values back is self-contained and order-independent.
func requireProm(t *testing.T) {
	t.Helper()
	promOnce.Do(func() {
		h, err := InitMetrics()
		if err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}
		metricsHandler = h
	})
	if metricsHandler == nil {
		t.Fatal("metrics handler not installed")
	}
}

// InitMetrics installs a Prometheus-backed provider; the package instruments
// created at load time must then record through it, and the returned handler
// must expose them.
func TestInitMetrics(t *testing.T) {
	requireProm(t)
	h := metricsHandler

	ctx := context.Background()
	InboundTotal.Add(ctx, 1)
	DedupDroppedTotal.Add(ctx, 2)
	ProcessDuration.Record(ctx, 12.5)
	ProcessErrorTotal.Add(ctx, 1)
	OutboundTotal.Add(ctx, 3)
	TokensTotal.Add(ctx, 10)
	EndToEndDuration.Record(ctx, 100)
	GatewayRejectedTotal.Add(ctx, 1)
	SendRateLimitedTotal.Add(ctx, 1)
	LLMCallDuration.Record(ctx, 42)
	ToolCallDuration.Record(ctx, 7)
	SessionStoreDuration.Record(ctx, 3)
	CostUSDTotal.Add(ctx, 0.5)

	if got := gaugeOrCounter(t, "im_inbound_total", nil); got != 1 {
		t.Fatalf("im_inbound_total = %v, want 1", got)
	}
	if got := gaugeOrCounter(t, "im_dedup_dropped_total", nil); got != 2 {
		t.Fatalf("im_dedup_dropped_total = %v, want 2", got)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "im_inbound_total") {
		t.Fatal("/metrics must expose im_inbound_total")
	}
	if !strings.Contains(rec.Body.String(), "worker_process_duration") {
		t.Fatal("/metrics must expose worker_process_duration")
	}
	for _, name := range []string{
		"llm_call_duration", "tool_call_duration", "session_store_duration", "llm_cost_usd_total",
	} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Fatalf("/metrics must expose %s", name)
		}
	}
}

// findSample returns the sample value of the named metric carrying at least
// the given labels; ok=false when absent (the exporter adds otel_scope_*
// labels to every sample, so matching is by subset).
func findSample(name string, labels map[string]string) (float64, bool) {
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0, false
	}
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue(), true
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue(), true
			}
			if h := m.GetHistogram(); h != nil {
				return h.GetSampleSum(), true
			}
		}
	}
	return 0, false
}

func gaugeOrCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := findSample(name, labels)
	if !ok {
		t.Fatalf("metric %s with labels %v not found", name, labels)
	}
	return v
}

func metricExists(name string, labels map[string]string) bool {
	_, ok := findSample(name, labels)
	return ok
}

// fakeStreamStats is a StreamStats stub whose canned results exercise the
// collector branches: ok, pending error, and the group-less target.
type fakeStreamStats struct {
	mu          sync.Mutex
	lenCalls    int
	pendingCall int
}

func (f *fakeStreamStats) Len(_ context.Context, stream string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lenCalls++
	switch stream {
	case "stream:inbound":
		return 3, nil
	case "stream:outbound":
		return 1, nil
	case "stream:deadletter":
		return 7, nil
	}
	return 0, errors.New("unexpected stream " + stream)
}

func (f *fakeStreamStats) Pending(_ context.Context, stream, group string) (int64, time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pendingCall++
	switch {
	case stream == "stream:inbound" && group == "workers":
		return 2, 5 * time.Second, nil
	case stream == "stream:outbound" && group == "senders":
		return 0, 0, errors.New("pending unavailable")
	case stream == "stream:outbound" && group == "senders-ws":
		return 4, 10 * time.Second, nil
	}
	return 0, 0, errors.New("unexpected target " + stream + "/" + group)
}

func (f *fakeStreamStats) callCount() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lenCalls, f.pendingCall
}

// brokenStreamStats fails every query: the collector must log and skip
// instead of recording or panicking.
type brokenStreamStats struct {
	mu sync.Mutex
	n  int
}

func (b *brokenStreamStats) Len(context.Context, string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.n++
	return 0, errors.New("redis down")
}

func (b *brokenStreamStats) Pending(context.Context, string, string) (int64, time.Duration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.n++
	return 0, 0, errors.New("redis down")
}

func (b *brokenStreamStats) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}

// The collector polls the queue gauges and shuts down on context cancel.
func TestStartStreamCollector(t *testing.T) {
	requireProm(t)

	// Pass 1: every query fails — no gauge may appear.
	broken := &brokenStreamStats{}
	ctx, cancel := context.WithCancel(context.Background())
	StartStreamCollector(ctx, broken, 20*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if broken.calls() >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if broken.calls() < 4 {
		t.Fatalf("collector did not poll all targets: %d calls", broken.calls())
	}
	cancel()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(60 * time.Millisecond)
		if broken.calls() == 0 {
			continue
		}
		break
	}
	if metricExists("stream_length", map[string]string{"stream": "stream:inbound"}) {
		t.Fatal("failed queries must not record gauges")
	}

	// Pass 2: canned values — every branch records what the alerts read.
	stats := &fakeStreamStats{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	StartStreamCollector(ctx2, stats, 20*time.Millisecond)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l, p := stats.callCount(); l >= 8 && p >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if l, p := stats.callCount(); l < 8 || p < 3 {
		t.Fatalf("collector did not complete a pass: len=%d pending=%d", l, p)
	}

	if got := gaugeOrCounter(t, "stream_length", map[string]string{"stream": "stream:inbound"}); got != 3 {
		t.Fatalf("stream_length[inbound] = %v, want 3", got)
	}
	if got := gaugeOrCounter(t, "stream_length", map[string]string{"stream": "stream:outbound"}); got != 1 {
		t.Fatalf("stream_length[outbound] = %v, want 1", got)
	}
	if got := gaugeOrCounter(t, "stream_length", map[string]string{"stream": "stream:deadletter"}); got != 7 {
		t.Fatalf("stream_length[deadletter] = %v, want 7", got)
	}
	if got := gaugeOrCounter(t, "stream_pending", map[string]string{"stream": "stream:inbound", "group": "workers"}); got != 2 {
		t.Fatalf("stream_pending[workers] = %v, want 2", got)
	}
	if got := gaugeOrCounter(t, "stream_pending", map[string]string{"stream": "stream:outbound", "group": "senders-ws"}); got != 4 {
		t.Fatalf("stream_pending[senders-ws] = %v, want 4", got)
	}
	if got := gaugeOrCounter(t, "stream_oldest_pending_seconds", map[string]string{"stream": "stream:outbound", "group": "senders-ws"}); got != 10 {
		t.Fatalf("stream_oldest_pending_seconds[senders-ws] = %v, want 10", got)
	}

	// The collector must exit when the context is canceled (no goroutine leak).
	cancel2()
	l, p := stats.callCount()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(60 * time.Millisecond) // > 2 intervals
		if l2, p2 := stats.callCount(); l2 == l && p2 == p {
			return // no new polls after cancel: goroutine exited
		}
	}
	t.Fatal("collector kept polling after context cancel")
}

// An interval <= 0 falls back to the default cadence without panicking.
func TestStartStreamCollectorDefaultInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	StartStreamCollector(ctx, &fakeStreamStats{}, 0)
	cancel()
}

// InjectTraceparent writes the ctx span context into the carrier;
// ExtractTraceparent restores it into a fresh context. The W3C propagator is
// installed the same way InitTracing installs it (self-contained when run via
// -run filters).
func TestInjectExtractTraceparent(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("metrics-test").Start(context.Background(), "op")
	want := oteltrace.SpanContextFromContext(ctx)
	if !want.IsValid() {
		t.Fatal("test span context invalid")
	}

	carrier := propagation.MapCarrier{}
	InjectTraceparent(ctx, carrier)
	span.End()

	got := carrier.Get("traceparent")
	if got == "" {
		t.Fatal("traceparent not injected")
	}
	if !strings.HasPrefix(got, "00-"+want.TraceID().String()+"-") {
		t.Fatalf("unexpected traceparent %q, want trace id %s", got, want.TraceID())
	}

	restored := ExtractTraceparent(context.Background(), carrier)
	sc := oteltrace.SpanContextFromContext(restored)
	if !sc.IsValid() || sc.TraceID() != want.TraceID() {
		t.Fatalf("extracted span context invalid: %v", sc)
	}

	// An empty carrier yields an invalid (no-op) span context, not a panic.
	if sc := oteltrace.SpanContextFromContext(ExtractTraceparent(context.Background(), propagation.MapCarrier{})); sc.IsValid() {
		t.Fatal("empty carrier must not produce a valid span context")
	}
}

// InitTracing installs the global OTLP provider and propagator; shutdown must
// return promptly once the context is canceled (no span backlog, unreachable
// collector must not hang the test).
func TestInitTracing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	shutdown, err := InitTracing(ctx)
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown must not be nil")
	}

	// The W3C propagator is now installed: injecting without a span is a
	// no-op (empty traceparent), not a panic.
	carrier := propagation.MapCarrier{}
	InjectTraceparent(ctx, carrier)
	if carrier.Get("traceparent") != "" {
		t.Fatalf("inject without a span must stay empty, got %q", carrier.Get("traceparent"))
	}

	cancel()
	done := make(chan error, 1)
	go func() { done <- shutdown() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not return within 15s")
	}
}
