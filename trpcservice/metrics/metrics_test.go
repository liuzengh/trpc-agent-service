package metrics

import (
	"context"
	"testing"
	"time"

	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// TestRecorderInstruments records through a manual reader and asserts the
// aggregated data points, proving names and label wiring without a collector.
func TestRecorderInstruments(t *testing.T) {
	reader := metricsdk.NewManualReader()
	provider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	rec, err := NewRecorder(provider.Meter("test"))
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}

	rec.Message("demo", "webchat", "ok")
	rec.Message("demo", "webchat", "guardrail")
	rec.GuardrailBlock("demo", "input", "keyword")
	rec.ModelLatency("demo", 120*time.Millisecond)
	rec.Tokens("demo", "prompt", 10)
	rec.Tokens("demo", "completion", 5)
	rec.Tokens("demo", "completion", 0) // zero adds ignored
	rec.SendError("demo", "wecom")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	sums := map[string]int64{}
	histograms := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				var total int64
				for _, dp := range d.DataPoints {
					total += dp.Value
				}
				sums[m.Name] = total
			case metricdata.Histogram[float64]:
				histograms += len(d.DataPoints)
			}
		}
	}
	want := map[string]int64{
		"trpcservice.messages":         2,
		"trpcservice.guardrail.blocks": 1,
		"trpcservice.model.tokens":     15,
		"trpcservice.im.send_errors":   1,
	}
	for name, v := range want {
		if sums[name] != v {
			t.Fatalf("%s = %d, want %d", name, sums[name], v)
		}
	}
	if histograms != 1 {
		t.Fatalf("latency histogram points = %d, want 1", histograms)
	}
}

func TestNilRecorderNoop(t *testing.T) {
	var r *Recorder
	r.Message("t", "c", "ok")
	r.GuardrailBlock("t", "input", "keyword")
	r.ModelLatency("t", time.Second)
	r.Tokens("t", "prompt", 3)
	r.SendError("t", "c")
}

// TestSetupOff keeps the noop path honest: setup must succeed, instruments
// must be usable, and shutdown must be callable when nothing is exported.
func TestSetupOff(t *testing.T) {
	rec, shutdown, err := Setup(config.TelemetryConfig{
		Traces:  config.ExporterConfig{Exporter: config.ExporterOff},
		Metrics: config.MetricsExporterConfig{Exporter: config.ExporterOff},
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if rec == nil {
		t.Fatal("recorder must exist even with exporters off")
	}
	rec.Message("t", "c", "ok") // noop meter, must not panic
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
