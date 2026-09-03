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
