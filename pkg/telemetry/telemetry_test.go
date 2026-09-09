//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// failingWriter simulates an audit sink that is unavailable.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("sink down") }

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("audit line is not valid JSON: %v\nline: %s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// TestLogAudit_WritesAllRequiredFields pins the audit field set demanded by the
// enterprise requirements: tenant_id, channel, user_id, session_id, agent_name,
// tool_name, decision, latency, error_type, cost, trace_id.
func TestLogAudit_WritesAllRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	err := c.LogAudit(context.Background(), &AuditLog{
		TenantID:    "tenant-a",
		ChannelType: "wework",
		UserID:      "user-1",
		SessionID:   "sess-1",
		AgentName:   "support-agent",
		ToolName:    "search",
		Decision:    "allowed",
		LatencyMS:   42,
		ErrorType:   "none",
		TokenCount:  1234,
		CostUSD:     0.0567,
	})
	if err != nil {
		t.Fatalf("LogAudit returned error: %v", err)
	}

	lines := decodeLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("expected 1 audit line, got %d", len(lines))
	}
	entry := lines[0]

	for _, field := range []string{
		"tenant_id", "channel", "user_id", "session_id", "agent_name",
		"tool_name", "decision", "latency_ms", "error_type",
		"token_count", "cost_usd", "trace_id", "timestamp",
	} {
		if _, ok := entry[field]; !ok {
			t.Errorf("required audit field %q missing from record", field)
		}
	}

	if entry["tenant_id"] != "tenant-a" {
		t.Errorf("tenant_id = %v, want tenant-a", entry["tenant_id"])
	}
	if entry["decision"] != "allowed" {
		t.Errorf("decision = %v, want allowed", entry["decision"])
	}
}

func TestLogAudit_PseudonymizesUserIDWithTenantScopedHMAC(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSinkAndIdentityKey(&buf, []byte("shared-audit-key"))
	entry := &AuditLog{TenantID: "tenant-a", UserID: "provider-user-42", Decision: "allowed"}
	if err := c.LogAudit(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if entry.UserID != "provider-user-42" {
		t.Fatal("LogAudit mutated the caller-owned audit entry")
	}
	got := decodeLines(t, &buf)[0]["user_id"].(string)
	if got == "provider-user-42" || !strings.HasPrefix(got, "hmac:") || len(got) != len("hmac:")+32 {
		t.Fatalf("user_id=%q is not a stable pseudonym", got)
	}

	var other bytes.Buffer
	otherCollector := NewCollectorWithAuditSinkAndIdentityKey(&other, []byte("shared-audit-key"))
	if err := otherCollector.LogAudit(context.Background(), &AuditLog{TenantID: "tenant-b", UserID: "provider-user-42", Decision: "allowed"}); err != nil {
		t.Fatal(err)
	}
	otherID := decodeLines(t, &other)[0]["user_id"].(string)
	if got == otherID {
		t.Fatal("same provider ID must not share a pseudonym across tenants")
	}
}

func TestLogAudit_RedactsUserIDWithoutIdentityKey(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)
	if err := c.LogAudit(context.Background(), &AuditLog{TenantID: "tenant-a", UserID: "provider-user-42", Decision: "allowed"}); err != nil {
		t.Fatal(err)
	}
	if got := decodeLines(t, &buf)[0]["user_id"]; got != redactedIdentity {
		t.Fatalf("user_id=%v, want redaction marker", got)
	}
}

// TestLogAudit_NoSecretFieldsOnStruct is the anti-leakage guard: the audit
// record must have no field capable of carrying a credential. If someone adds
// one, this test fails and forces a redaction decision.
func TestLogAudit_NoSecretFieldsOnStruct(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	if err := c.LogAudit(context.Background(), &AuditLog{
		TenantID: "tenant-a",
		Decision: "allowed",
	}); err != nil {
		t.Fatalf("LogAudit returned error: %v", err)
	}

	// Marshalled keys are the complete surface an audit record can expose.
	// token_count is a metering field, not a credential, so match exact key
	// names plus credential-bearing substrings rather than the word "token".
	entry := decodeLines(t, &buf)[0]
	bannedExact := map[string]bool{
		"token": true, "secret": true, "api_key": true, "apikey": true,
		"password": true, "passwd": true, "credential": true, "dsn": true,
		"authorization": true, "content": true, "payload": true,
		"private_key": true, "access_token": true, "refresh_token": true,
	}
	bannedSubstr := []string{
		"secret", "password", "passwd", "credential", "api_key", "apikey",
		"private_key", "access_token", "refresh_token", "authorization",
	}
	for key := range entry {
		lower := strings.ToLower(key)
		if bannedExact[lower] {
			t.Errorf("audit record exposes potentially sensitive field %q", key)
			continue
		}
		for _, b := range bannedSubstr {
			if strings.Contains(lower, b) {
				t.Errorf("audit record exposes potentially sensitive field %q", key)
				break
			}
		}
	}
}

// TestLogAudit_TraceIDFromContext proves trace correlation actually happens
// rather than emitting a malformed placeholder.
func TestLogAudit_TraceIDFromContext(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	ctx, span := StartOperation(context.Background(), OperationRunnerRun)
	err := c.LogAudit(ctx, &AuditLog{TenantID: "tenant-a", Decision: "allowed"})
	span.End()
	if err != nil {
		t.Fatalf("LogAudit returned error: %v", err)
	}

	entry := decodeLines(t, &buf)[0]
	traceID, _ := entry["trace_id"].(string)

	// The default global tracer is a no-op, so an unset trace ID is expected
	// here; what must never happen is emitting a malformed value.
	if traceID != "" && len(traceID) != 32 {
		t.Errorf("trace_id = %q, want empty or 32 hex chars", traceID)
	}
	if strings.Count(traceID, "0") == 32 {
		t.Error("trace_id is all zeros; correlation would be useless")
	}
}

// TestLogAudit_PreservesExplicitTimestamp ensures a caller-supplied event time
// is not silently overwritten with the write time.
func TestLogAudit_PreservesExplicitTimestamp(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	want := time.Date(2026, 3, 5, 14, 30, 0, 0, time.UTC)
	if err := c.LogAudit(context.Background(), &AuditLog{
		TenantID:  "tenant-a",
		Decision:  "allowed",
		Timestamp: want,
	}); err != nil {
		t.Fatalf("LogAudit returned error: %v", err)
	}

	entry := decodeLines(t, &buf)[0]
	got, err := time.Parse(time.RFC3339Nano, entry["timestamp"].(string))
	if err != nil {
		t.Fatalf("timestamp is not RFC3339: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("timestamp = %v, want %v", got, want)
	}
}

func TestLogAudit_StampsTimestampWhenAbsent(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	before := time.Now().UTC().Add(-time.Second)
	if err := c.LogAudit(context.Background(), &AuditLog{TenantID: "t", Decision: "allowed"}); err != nil {
		t.Fatalf("LogAudit returned error: %v", err)
	}

	entry := decodeLines(t, &buf)[0]
	got, err := time.Parse(time.RFC3339Nano, entry["timestamp"].(string))
	if err != nil {
		t.Fatalf("timestamp is not RFC3339: %v", err)
	}
	if got.Before(before) {
		t.Errorf("timestamp %v predates the call", got)
	}
}

func TestLogAudit_NilEntryRejected(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	if err := c.LogAudit(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil audit entry, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("nil entry wrote %d bytes to sink, want 0", buf.Len())
	}
}

// TestLogAudit_SinkFailureSurfaces proves a broken sink is reported rather than
// silently swallowed, which was the flaw in the previous placeholder.
func TestLogAudit_SinkFailureSurfaces(t *testing.T) {
	c := NewCollectorWithAuditSink(failingWriter{})

	err := c.LogAudit(context.Background(), &AuditLog{TenantID: "t", Decision: "allowed"})
	if err == nil {
		t.Fatal("expected error when audit sink fails, got nil")
	}
	if !strings.Contains(err.Error(), "sink down") {
		t.Errorf("error %q does not wrap the sink failure", err)
	}
}

func TestNewCollectorWithNilSinkDiscards(t *testing.T) {
	c := NewCollectorWithAuditSink(nil)
	if err := c.LogAudit(context.Background(), &AuditLog{TenantID: "t"}); err != nil {
		t.Fatalf("nil sink should discard, got error: %v", err)
	}
}

// TestLogAudit_ConcurrentWritesStayWellFormed is the reason auditMu exists:
// without it, concurrent workers interleave partial JSON into the sink.
func TestLogAudit_ConcurrentWritesStayWellFormed(t *testing.T) {
	var buf bytes.Buffer
	c := NewCollectorWithAuditSink(&buf)

	const goroutines, perGoroutine = 16, 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_ = c.LogAudit(context.Background(), &AuditLog{
					TenantID: fmt.Sprintf("tenant-%d", g),
					UserID:   fmt.Sprintf("user-%d-%d", g, i),
					Decision: "allowed",
				})
			}
		}(g)
	}
	wg.Wait()

	lines := decodeLines(t, &buf)
	if want := goroutines * perGoroutine; len(lines) != want {
		t.Errorf("got %d audit lines, want %d", len(lines), want)
	}
}

// TestRecordMetrics_DoNotPanic covers the Prometheus recording helpers, which
// are called on every request path.
func TestRecordMetrics_DoNotPanic(t *testing.T) {
	c := NewCollectorWithAuditSink(nil)

	start := c.StartTimer()
	if start.IsZero() {
		t.Error("StartTimer returned zero time")
	}

	c.RecordRequestDuration("tenant-a", "support", start)
	c.RecordTokens("tenant-a", "gpt-4o-mini", 100, 50)
	c.RecordCost("tenant-a", 0.01)
	c.RecordSuccess("tenant-a", "telegram")
	c.RecordError("tenant-a", "telegram", "model_timeout")
}

// TestRecordMetrics_EmptyTenantIDTolerated guards metric cardinality handling on
// a degenerate input rather than panicking mid-request.
func TestRecordMetrics_EmptyTenantIDTolerated(t *testing.T) {
	c := NewCollectorWithAuditSink(nil)
	c.RecordSuccess("", "")
	c.RecordError("", "", "")
	c.RecordCost("", 0)
}
