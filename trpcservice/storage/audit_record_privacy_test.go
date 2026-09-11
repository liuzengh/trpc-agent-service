package storage

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func TestRecordAuditStoresDigestedDetail(t *testing.T) {
	store, err := NewMemoryStateStoreWithAuditKey([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewMemoryStateStoreWithAuditKey() error = %v", err)
	}
	if err := store.RecordAudit(context.Background(), AuditEvent{
		TenantID: "tenant-a", TraceID: "trace-a", Action: "login", Result: "failed",
		Detail: "customer alice@example.com entered Authorization=Bearer super-secret",
	}); err != nil {
		t.Fatalf("RecordAudit() error = %v", err)
	}
	events, err := store.ListAudit(context.Background(), "tenant-a", "trace-a")
	if err != nil || len(events) != 1 {
		t.Fatalf("ListAudit() = %d, %v, want 1, nil", len(events), err)
	}
	if strings.Contains(events[0].Detail, "alice@example.com") || strings.Contains(events[0].Detail, "super-secret") {
		t.Fatalf("audit detail leaks private input: %q", events[0].Detail)
	}
	if !regexp.MustCompile(`^hmac-sha256:[0-9a-f]{64}$`).MatchString(events[0].Detail) {
		t.Fatalf("audit detail = %q, want HMAC digest", events[0].Detail)
	}
}
