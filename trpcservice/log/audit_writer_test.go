package log

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLAuditWriterFlushesRedactedBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	writer, err := NewMySQLAuditWriter(db, NewRedactor(), AuditWriterConfig{
		BufferSize:    4,
		BatchSize:     1,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMySQLAuditWriter() error = %v", err)
	}
	mock.ExpectExec("INSERT INTO audit_log").
		WithArgs(
			"tenant-a", "webui", redactedValue, "session-a", "agent-a",
			"search", "executed", int64(25), "", 0.5,
			"trace-a", "request-a", sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	writer.Write(context.Background(), AuditRecord{
		TenantID: "tenant-a", Channel: "webui", UserID: "token=secret",
		SessionID: "session-a", AgentName: "agent-a", ToolName: "search",
		Decision: "executed", Latency: 25 * time.Millisecond, Cost: 0.5,
		TraceID: "trace-a", RequestID: "request-a",
	})
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
	writer.Write(context.Background(), AuditRecord{})
	if writer.Dropped() != 1 {
		t.Fatalf("Dropped() = %d, want 1", writer.Dropped())
	}
}

func TestSharedRedactor(t *testing.T) {
	redactor := NewRedactor(`account-[0-9]+`, `[invalid`)
	got := redactor.Redact("account-123 Bearer abc sk-12345678 password=hunter2")
	want := redactedValue + " " + redactedValue + " " + redactedValue + " " + redactedValue
	if got != want {
		t.Fatalf("Redact() = %q, want %q", got, want)
	}
}

func TestAuditWriterValidation(t *testing.T) {
	if _, err := NewMySQLAuditWriter(nil, nil, AuditWriterConfig{}); err == nil {
		t.Fatal("NewMySQLAuditWriter() accepted nil database")
	}
}
