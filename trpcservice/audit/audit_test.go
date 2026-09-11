package audit

import (
	"context"
	"testing"
)

func TestMemoryWriterRedactsSensitiveDetails(t *testing.T) {
	writer := NewMemoryWriter()
	if err := writer.Record(context.Background(), Event{
		TenantID: "tenant", Decision: "allow",
		Details: map[string]any{"api_key": "secret", "safe": "value"},
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	event := writer.Events()[0]
	if event.Details["api_key"] != "[REDACTED]" || event.Details["safe"] != "value" {
		t.Fatalf("details=%+v", event.Details)
	}
}

func TestMemoryWriterQueryIsTenantScoped(t *testing.T) {
	writer := NewMemoryWriter()
	_ = writer.Record(context.Background(), Event{TenantID: "tenant-a", Decision: "allow"})
	_ = writer.Record(context.Background(), Event{TenantID: "tenant-b", Decision: "deny"})
	events, err := writer.Query(context.Background(), Query{TenantID: "tenant-a", Limit: 10})
	if err != nil || len(events) != 1 || events[0].TenantID != "tenant-a" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}
