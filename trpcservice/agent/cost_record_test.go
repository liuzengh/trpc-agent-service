package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// clearPricing resets the pricing table (atomic.Value cannot store an
// untyped nil, so an empty map stands in for "unset").
func clearPricing() { SetModelPricing(map[string][2]float64{}) }

// usageEvent is a non-final event carrying token usage, like the intermediate
// events of a failed attempt that still burned tokens.
func usageEvent(prompt, completion int) *event.Event {
	return &event.Event{Response: &model.Response{
		Usage: &model.Usage{PromptTokens: prompt, CompletionTokens: completion},
	}}
}

// CostUSD prices known models and tracks unknown ones as 0 (token counts are
// recorded separately, so the spend can be re-derived once a price exists).
func TestCostUSD(t *testing.T) {
	SetModelPricing(map[string][2]float64{"m1": {2, 8}})
	t.Cleanup(clearPricing)

	if got := CostUSD("m1", 1_000_000, 500_000); got != 6 {
		t.Fatalf("CostUSD(m1) = %v, want 6", got)
	}
	if got := CostUSD("unknown", 1_000_000, 1_000_000); got != 0 {
		t.Fatalf("unknown model must cost-track as 0, got %v", got)
	}
}

// recordCostUSD meters into whatever provider is installed; the call must be
// safe for known, unknown and unpriced models alike.
func TestRecordCostUSD(t *testing.T) {
	SetModelPricing(map[string][2]float64{"m1": {2, 8}})
	t.Cleanup(clearPricing)

	ctx := context.Background()
	recordCostUSD(ctx, "t1", "m1", 100, 50)
	recordCostUSD(ctx, "t1", "unknown", 100, 50)
	clearPricing()
	recordCostUSD(ctx, "t1", "m1", 100, 50) // no pricing table at all
}

var (
	testMeterOnce sync.Once
	testReader    *sdkmetric.ManualReader
)

// requireTestMeter installs an SDK meter provider with a manual reader once
// for the whole test run, like metrics_test.go's requireProm. It is not
// restored afterwards: the otel global delegates init-time instruments (such
// as metrics.CostUSDTotal) to the provider of the FIRST SetMeterProvider only,
// so swapping providers per test would silently re-route later recordings.
// Assertions therefore read deltas off the cumulative sums.
func requireTestMeter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	testMeterOnce.Do(func() {
		testReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testReader)))
	})
	return testReader
}

// costSum reads back the aggregated llm_cost_usd_total for the given
// tenant/model attributes; 0 means no matching data point was recorded.
func costSum(t *testing.T, reader *sdkmetric.ManualReader, tenantID, modelName string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "llm_cost_usd_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				t.Fatalf("llm_cost_usd_total data = %T, want metricdata.Sum[float64]", m.Data)
			}
			var total float64
			for _, dp := range sum.DataPoints {
				tid, _ := dp.Attributes.Value(attribute.Key("tenant_id"))
				mdl, _ := dp.Attributes.Value(attribute.Key("model"))
				if tid.AsString() == tenantID && mdl.AsString() == modelName {
					total += dp.Value
				}
			}
			return total
		}
	}
	return 0
}

// The success path of Process records the cost exactly once, tagged with the
// tenant and model, from the usage accumulated across all attempts (here: a
// failed attempt's tokens plus the successful retry's).
func TestProcessRecordsCostOnSuccess(t *testing.T) {
	SetModelPricing(map[string][2]float64{"test-model": {1, 1}})
	t.Cleanup(clearPricing)
	reader := requireTestMeter(t)
	before := costSum(t, reader, "t1", "test-model")

	r := &fakeRunner{run: func(ctx context.Context, call int) (<-chan *event.Event, error) {
		if call == 1 {
			// A failed attempt that still burned tokens.
			return scriptedEvents(usageEvent(10, 5), errorEvent("boom"))(ctx, call)
		}
		return scriptedEvents(finalEvent("ok"))(ctx, call)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	// Both attempts' usage is what the cost record is computed from.
	if out.PromptTokens != 20 || out.CompletionTokens != 10 {
		t.Fatalf("usage must accumulate across attempts: %+v", out)
	}
	// Exactly one record: the delta equals a single billing of 20+10 tokens.
	if got, want := costSum(t, reader, "t1", "test-model")-before, CostUSD("test-model", 20, 10); got != want {
		t.Fatalf("llm_cost_usd_total[t1,test-model] delta = %v, want %v (one record)", got, want)
	}
}

// A terminal failure with usage still records the cost once: the failed
// attempts burned priced tokens that TokensTotal already counted.
func TestProcessRecordsCostOnTerminalFailure(t *testing.T) {
	SetModelPricing(map[string][2]float64{"test-model": {1, 1}})
	t.Cleanup(clearPricing)
	reader := requireTestMeter(t)
	before := costSum(t, reader, "t1", "test-model")

	r := &fakeRunner{run: func(ctx context.Context, call int) (<-chan *event.Event, error) {
		return scriptedEvents(usageEvent(10, 5), errorEvent("boom"))(ctx, call)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	if _, err := p.Process(context.Background(), testMsg("hi")); err == nil {
		t.Fatal("expected failure")
	}
	// One record covering both attempts' usage (10+10 prompt, 5+5 completion).
	if got, want := costSum(t, reader, "t1", "test-model")-before, CostUSD("test-model", 20, 10); got != want {
		t.Fatalf("llm_cost_usd_total[t1,test-model] delta = %v, want %v (one record)", got, want)
	}
}

// A failure that never produced usage records nothing.
func TestProcessSkipsCostOnFailure(t *testing.T) {
	SetModelPricing(map[string][2]float64{"test-model": {1, 1}})
	t.Cleanup(clearPricing)
	reader := requireTestMeter(t)
	before := costSum(t, reader, "t1", "test-model")

	p := newRunnerProcessor(&fakeRunner{run: scriptedEvents(errorEvent("boom"))}, "test-model", time.Second, 0)
	if _, err := p.Process(context.Background(), testMsg("hi")); err == nil {
		t.Fatal("expected failure")
	}
	if got := costSum(t, reader, "t1", "test-model") - before; got != 0 {
		t.Fatalf("llm_cost_usd_total[t1,test-model] delta = %v, want 0 (no usage, no record)", got)
	}
}
