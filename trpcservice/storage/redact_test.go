package storage

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func TestRedactStripsCredentialForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"bearer", "Authorization=Bearer abc123", "Authorization=[REDACTED]"},
		{"bearer lowercase", "authorization = bearer xyz-999", "authorization = [REDACTED]"},
		{"api key", "api_key=secret-value", "api_key=[REDACTED]"},
		{"api-key spaced", "api-key = another-secret", "api-key = [REDACTED]"},
		{"token", "token=abcdef", "token=[REDACTED]"},
		{"multiple in one line", "api_key=a token=b", "api_key=[REDACTED] token=[REDACTED]"},
		{"plain text unchanged", "order shipped", "order shipped"},
		{"empty unchanged", "", ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := Redact(testCase.input); got != testCase.want {
				t.Fatalf("Redact(%q) = %q, want %q", testCase.input, got, testCase.want)
			}
		})
	}
}

// TestRecordExecutionStoresDigestedAuditDetail verifies conversation content
// never enters durable audit storage as reversible text.
func TestRecordExecutionStoresRedactedAuditDetail(t *testing.T) {
	t.Parallel()

	store, err := NewMemoryStateStoreWithAuditKey([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewMemoryStateStoreWithAuditKey() error = %v", err)
	}
	_, err = store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID:      "tenant-a",
		AppCode:       "support",
		SessionKey:    "tenant-a/support/telegram/chat-1",
		MessageID:     "update-42",
		Channel:       "telegram",
		BindingID:     "telegram-bot",
		TraceID:       "trace-42",
		Action:        "agent.reply",
		Result:        "success",
		AuditDetail:   "Authorization=Bearer real-secret-value token=abc123",
		OutboxType:    "agent.reply.completed",
		OutboxPayload: []byte(`{"message_id":"update-42"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	audits, err := store.ListAudit(context.Background(), "tenant-a", "trace-42")
	if err != nil || len(audits) != 1 {
		t.Fatalf("ListAudit() = %d events, error = %v, want 1", len(audits), err)
	}
	detail := audits[0].Detail
	if strings.Contains(detail, "real-secret-value") || strings.Contains(detail, "abc123") {
		t.Fatalf("audit detail leaks credentials: %q", detail)
	}
	if !regexp.MustCompile(`^hmac-sha256:[0-9a-f]{64}$`).MatchString(detail) {
		t.Fatalf("audit detail %q is not an HMAC-SHA256 digest", detail)
	}
}

func TestAuditContentDigestIsStableAndKeyed(t *testing.T) {
	t.Parallel()
	first, err := NewAuditContentDigester([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewAuditContentDigester() error = %v", err)
	}
	second, err := NewAuditContentDigester([]byte("abcdef0123456789abcdef0123456789"))
	if err != nil {
		t.Fatalf("NewAuditContentDigester() second error = %v", err)
	}
	if first.Digest("customer message") != first.Digest("customer message") {
		t.Fatal("same content and key produced different digests")
	}
	if first.Digest("customer message") == second.Digest("customer message") {
		t.Fatal("different keys produced the same digest")
	}
	if _, err := NewAuditContentDigester([]byte("too-short")); err == nil {
		t.Fatal("short audit HMAC key error = nil")
	}
}
