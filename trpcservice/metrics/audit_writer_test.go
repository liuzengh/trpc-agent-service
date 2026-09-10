package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

func TestAuditWriterRecordsOnlyAcceptedCostDelta(t *testing.T) {
	provider := &auditMetricProvider{}
	delegate := &auditAppendWriter{}
	writer := WrapAuditWriter(delegate, provider)
	input, err := usageEvent("accepted")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if provider.cost != 5 || provider.usage != 5 || provider.tokens != 7 {
		t.Fatalf("accepted telemetry = cost %d usage %d tokens %d", provider.cost, provider.usage, provider.tokens)
	}

	delegate.duplicate = true
	duplicate, err := usageEvent("duplicate")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(context.Background(), duplicate); err != nil {
		t.Fatal(err)
	}
	delegate.err = errors.New("append failed")
	failed, err := usageEvent("failed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(context.Background(), failed); err == nil {
		t.Fatal("append failure should be preserved")
	}
	if provider.cost != 5 || provider.usage != 5 || provider.tokens != 7 {
		t.Fatalf("duplicate/failure changed telemetry = cost %d usage %d tokens %d", provider.cost, provider.usage, provider.tokens)
	}
}

func TestAuditWriterSeparatesModelAndToolCost(t *testing.T) {
	provider := &auditMetricProvider{}
	delegate := &auditAppendWriter{}
	writer := WrapAuditWriter(delegate, provider)
	event, err := usageEvent("both")
	if err != nil {
		t.Fatal(err)
	}
	event.Cost.ToolCostMinor = int64Ptr(4)
	if _, err := writer.Append(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if provider.cost != 9 || provider.usage != 9 || provider.tokens != 7 {
		t.Fatalf("split telemetry = cost %d usage %d tokens %d", provider.cost, provider.usage, provider.tokens)
	}
	if provider.costComponents["model"] != 5 || provider.costComponents["tool"] != 4 {
		t.Fatalf("cost components = %#v", provider.costComponents)
	}
}

func usageEvent(id string) (audit.Event, error) {
	input, output, modelCost := int64(3), int64(4), int64(5)
	value := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: id, EventType: audit.EventExecutionCompleted, TenantID: "tenant-a", Channel: "api", AgentAppID: "app-a", OccurredAt: time.Now().UTC(), Cost: &audit.Usage{InputTokens: &input, OutputTokens: &output, ModelCostMinor: &modelCost, Currency: "USD", Provider: "openai", Model: "gpt-4o-mini", ExecutionResult: audit.ResultSuccess}}
	if err := value.Validate(); err != nil {
		return audit.Event{}, err
	}
	return value, nil
}

func int64Ptr(value int64) *int64 { return &value }

type auditAppendWriter struct {
	duplicate bool
	err       error
}

func (writer *auditAppendWriter) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	if writer.err != nil {
		return audit.AppendResult{}, writer.err
	}
	return audit.AppendResult{Event: event, Duplicate: writer.duplicate}, nil
}

type auditMetricProvider struct {
	cost           int64
	usage          int64
	tokens         int64
	costComponents map[string]int64
}

func (provider *auditMetricProvider) Tracer(string) observability.Tracer { return auditNoopTracer{} }
func (provider *auditMetricProvider) Meter(string) observability.Meter {
	if provider.costComponents == nil {
		provider.costComponents = map[string]int64{}
	}
	return auditMetricMeter{provider: provider}
}
func (provider *auditMetricProvider) Logger() observability.Logger   { return auditNoopLogger{} }
func (provider *auditMetricProvider) Shutdown(context.Context) error { return nil }

type auditMetricMeter struct{ provider *auditMetricProvider }

func (meter auditMetricMeter) Counter(name string) observability.Counter {
	return auditMetricCounter{name: name, provider: meter.provider}
}
func (meter auditMetricMeter) Histogram(string) observability.Histogram {
	return auditNoopHistogram{}
}
func (meter auditMetricMeter) UpDownCounter(string) observability.UpDownCounter {
	return auditNoopUpDownCounter{}
}

type auditMetricCounter struct {
	name     string
	provider *auditMetricProvider
}

func (counter auditMetricCounter) Add(_ context.Context, value int64, attrs ...observability.Attribute) {
	switch counter.name {
	case CostMinorTotal:
		counter.provider.cost += value
	case UsageCostTotal:
		counter.provider.usage += value
	case TokensTotal:
		counter.provider.tokens += value
	}
	component := ""
	for _, attr := range attrs {
		if attr.Key == "component" {
			component = attr.Value
		}
	}
	if component != "" && counter.name == CostMinorTotal {
		counter.provider.costComponents[component] += value
	}
}

type auditNoopTracer struct{}

func (auditNoopTracer) Start(ctx context.Context, _ string, _ ...observability.Attribute) (context.Context, observability.Span) {
	return ctx, auditNoopSpan{}
}

type auditNoopSpan struct{}

func (auditNoopSpan) End()                                     {}
func (auditNoopSpan) SetAttributes(...observability.Attribute) {}
func (auditNoopSpan) SetStatus(observability.Status, string)   {}
func (auditNoopSpan) RecordError(error)                        {}

type auditNoopHistogram struct{}

func (auditNoopHistogram) Record(context.Context, float64, ...observability.Attribute) {}

type auditNoopUpDownCounter struct{}

func (auditNoopUpDownCounter) Add(context.Context, int64, ...observability.Attribute) {}

type auditNoopLogger struct{}

func (auditNoopLogger) Log(context.Context, observability.Level, string, ...observability.Attribute) {
}
