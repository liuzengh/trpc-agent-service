package postgres

import (
	"context"
	"sync"
	"testing"

	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type histogramRecord struct {
	value      float64
	attributes map[string]string
}

type recordingHistogram struct {
	mu      sync.Mutex
	records []histogramRecord
}

func (h *recordingHistogram) Record(_ context.Context, value float64, attributes ...telemetry.Attribute) {
	record := histogramRecord{value: value, attributes: make(map[string]string, len(attributes))}
	for _, attribute := range attributes {
		record.attributes[attribute.Key()] = attribute.Value()
	}
	h.mu.Lock()
	h.records = append(h.records, record)
	h.mu.Unlock()
}

type recordingProvider struct {
	telemetry.Provider
	histogram *recordingHistogram
	spans     *recordingSpans
}

type recordingSpans struct {
	mu         sync.Mutex
	operations []telemetry.Operation
}

func (p recordingProvider) StartSpan(ctx context.Context, operation telemetry.Operation, attributes ...telemetry.Attribute) (context.Context, telemetry.Span) {
	if p.spans != nil {
		p.spans.mu.Lock()
		p.spans.operations = append(p.spans.operations, operation)
		p.spans.mu.Unlock()
	}
	return p.Provider.StartSpan(ctx, operation, attributes...)
}

func (p recordingProvider) Histogram(descriptor telemetry.MetricDescriptor) telemetry.Histogram {
	if descriptor == telemetry.MetricSessionBackendDuration {
		return p.histogram
	}
	return p.Provider.Histogram(descriptor)
}

func TestOpenForRunRecordsFixedBackendLatencyLabels(t *testing.T) {
	histogram := &recordingHistogram{}
	store := NewWithTelemetry(nil, recordingProvider{Provider: telemetry.Noop(), histogram: histogram})
	if _, err := store.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{}); err == nil {
		t.Fatal("invalid request unexpectedly succeeded")
	}
	histogram.mu.Lock()
	defer histogram.mu.Unlock()
	if len(histogram.records) != 1 {
		t.Fatalf("records=%d", len(histogram.records))
	}
	record := histogram.records[0]
	if record.value < 0 || record.attributes["destination"] != "postgres" || record.attributes["operation"] != "session.open" || record.attributes["outcome"] != "error" {
		t.Fatalf("record=%#v", record)
	}
	if _, exists := record.attributes["tenant_id"]; exists {
		t.Fatalf("tenant label must not be emitted: %#v", record.attributes)
	}
}

func TestOpenForRunStartsSessionOperation(t *testing.T) {
	spans := &recordingSpans{}
	store := NewWithTelemetry(nil, recordingProvider{Provider: telemetry.Noop(), histogram: &recordingHistogram{}, spans: spans})
	if _, err := store.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{}); err == nil {
		t.Fatal("invalid request unexpectedly succeeded")
	}
	spans.mu.Lock()
	defer spans.mu.Unlock()
	if len(spans.operations) != 1 || spans.operations[0] != telemetry.OperationSessionOpen {
		t.Fatalf("operations=%v", spans.operations)
	}
}
