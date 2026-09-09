package log

import (
	"bytes"
	"testing"
)

func TestRedactorRemovesSecretsAndSensitivePatterns(t *testing.T) {
	redactor := NewRedactor([]string{"bot-secret", "postgres://user:password@db/service"}, []string{"customer-id-42"})
	got := redactor.Redact("token=bot-secret dsn=postgres://user:password@db/service subject=customer-id-42")
	if got != "token=[REDACTED] dsn=[REDACTED] subject=[REDACTED]" {
		t.Fatalf("redacted = %q", got)
	}
	if redactor.Redact("safe diagnostic") != "safe diagnostic" {
		t.Fatal("safe diagnostic was modified")
	}
}

func TestRedactingWriterFiltersProductionLogOutput(t *testing.T) {
	var destination bytes.Buffer
	writer := NewRedactingWriter(&destination, NewRedactor([]string{"stage5-secret"}, nil))
	input := []byte("startup failed for stage5-secret\n")
	if count, err := writer.Write(input); err != nil || count != len(input) {
		t.Fatalf("write = %d, %v", count, err)
	}
	if destination.String() != "startup failed for [REDACTED]\n" {
		t.Fatalf("output = %q", destination.String())
	}
}
