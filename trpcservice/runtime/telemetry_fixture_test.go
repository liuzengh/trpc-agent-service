package runtime

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

type runtimeTelemetryProvider struct {
	spans   []*runtimeTelemetrySpan
	metrics []runtimeTelemetryMetric
}

func (p *runtimeTelemetryProvider) Tracer(string) observability.Tracer {
	return runtimeTelemetryTracer{provider: p}
}

func (p *runtimeTelemetryProvider) Meter(string) observability.Meter {
	return runtimeTelemetryMeter{provider: p}
}

func (p *runtimeTelemetryProvider) Logger() observability.Logger { return runtimeTelemetryLogger{} }

func (p *runtimeTelemetryProvider) Shutdown(context.Context) error { return nil }

type runtimeTelemetryTracer struct{ provider *runtimeTelemetryProvider }

func (t runtimeTelemetryTracer) Start(ctx context.Context, name string, attrs ...observability.Attribute) (context.Context, observability.Span) {
	span := &runtimeTelemetrySpan{name: name, attrs: append([]observability.Attribute(nil), attrs...)}
	t.provider.spans = append(t.provider.spans, span)
	return ctx, span
}

type runtimeTelemetrySpan struct {
	name          string
	attrs         []observability.Attribute
	status        observability.Status
	recordedError error
	ended         bool
}

func (s *runtimeTelemetrySpan) End() { s.ended = true }

func (s *runtimeTelemetrySpan) SetAttributes(attrs ...observability.Attribute) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *runtimeTelemetrySpan) SetStatus(status observability.Status, _ string) { s.status = status }

func (s *runtimeTelemetrySpan) RecordError(err error) { s.recordedError = err }

type runtimeTelemetryMeter struct{ provider *runtimeTelemetryProvider }

func (m runtimeTelemetryMeter) Counter(name string) observability.Counter {
	return runtimeTelemetryCounter{provider: m.provider, name: name}
}

func (m runtimeTelemetryMeter) Histogram(name string) observability.Histogram {
	return runtimeTelemetryHistogram{provider: m.provider, name: name}
}

func (m runtimeTelemetryMeter) UpDownCounter(string) observability.UpDownCounter {
	return runtimeTelemetryUpDownCounter{}
}

type runtimeTelemetryMetric struct {
	name  string
	value float64
	attrs []observability.Attribute
}

type runtimeTelemetryCounter struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (c runtimeTelemetryCounter) Add(_ context.Context, value int64, attrs ...observability.Attribute) {
	c.provider.metrics = append(c.provider.metrics, runtimeTelemetryMetric{name: c.name, value: float64(value), attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryHistogram struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (h runtimeTelemetryHistogram) Record(_ context.Context, value float64, attrs ...observability.Attribute) {
	h.provider.metrics = append(h.provider.metrics, runtimeTelemetryMetric{name: h.name, value: value, attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryUpDownCounter struct{}

func (runtimeTelemetryUpDownCounter) Add(context.Context, int64, ...observability.Attribute) {}

type runtimeTelemetryLogger struct{}

func (runtimeTelemetryLogger) Log(context.Context, observability.Level, string, ...observability.Attribute) {
}

func assertTelemetrySpan(t *testing.T, provider *runtimeTelemetryProvider, operation string, status observability.Status, wantError bool) {
	t.Helper()
	if len(provider.spans) == 0 {
		t.Fatal("no telemetry span recorded")
	}
	span := provider.spans[len(provider.spans)-1]
	if span.name != operation || span.status != status || !span.ended || (span.recordedError != nil) != wantError {
		t.Fatalf("span = %+v, want operation=%q status=%q error=%v", span, operation, status, wantError)
	}
}

func assertTelemetryMetric(t *testing.T, provider *runtimeTelemetryProvider, name string, wantValue float64, wantLabels map[string]string) {
	t.Helper()
	for _, metric := range provider.metrics {
		if metric.name == name && (wantValue < 0 || metric.value == wantValue) && telemetryLabelsEqual(metric.attrs, wantLabels) {
			return
		}
	}
	t.Fatalf("metric %q with value %v and labels %#v not found in %#v", name, wantValue, wantLabels, provider.metrics)
}

func telemetryLabelsEqual(attrs []observability.Attribute, want map[string]string) bool {
	if len(attrs) != len(want) {
		return false
	}
	for _, attr := range attrs {
		expected, ok := want[attr.Key]
		if !ok || expected != attr.Value {
			return false
		}
	}
	return true
}
