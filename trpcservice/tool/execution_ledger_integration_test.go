package tool

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

func postgresExecutionLedger(t *testing.T) (*PostgresExecutionLedger, *sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	if _, err := database.ExecContext(context.Background(), `
GRANT USAGE ON SCHEMA public TO trpc_tenant;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trpc_tenant;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trpc_tenant`); err != nil {
		t.Fatalf("grant tenant role access: %v", err)
	}
	tenantID := "support-tool-" + uuid.NewString()
	if _, err := database.ExecContext(context.Background(), "INSERT INTO tenants (id) VALUES ($1)", tenantID); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	t.Cleanup(func() { _, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID) })
	ledger, err := NewPostgresExecutionLedger(database, []byte("support-tool-ledger-key-material-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	return ledger, database, tenantID
}

func postgresToolRequest(tenantID, requestID string, arguments string) ExecutionRequest {
	return ExecutionRequest{
		TenantID: tenantID, RequestID: requestID, ToolName: "support.lookup_order",
		Arguments: []byte(arguments), TraceID: "trace-" + requestID, LeaseTTL: time.Minute,
	}
}

func TestPostgresExecutionLedgerLifecycleAndReplay(t *testing.T) {
	ledger, _, tenantID := postgresExecutionLedger(t)
	ctx := context.Background()
	request := postgresToolRequest(tenantID, "request-complete", `{"order_id":"42"}`)
	first, err := ledger.Begin(ctx, request)
	if err != nil || !first.Created || first.Status != ExecutionRunning || first.IdempotencyKey == "" {
		t.Fatalf("Begin() = %#v, %v", first, err)
	}
	running, err := ledger.Begin(ctx, request)
	if err != nil || running.Created || running.Status != ExecutionRunning || running.IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("Begin(running replay) = %#v, %v", running, err)
	}
	result := map[string]any{"status": "found", "count": float64(1)}
	if err := ledger.Complete(ctx, tenantID, first.IdempotencyKey, result); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	replay, err := ledger.Begin(ctx, request)
	if err != nil || replay.Created || replay.Status != ExecutionCompleted {
		t.Fatalf("Begin(completed replay) = %#v, %v", replay, err)
	}
	decoded, ok := replay.Result.(map[string]any)
	if !ok || decoded["status"] != "found" || decoded["count"] != float64(1) {
		t.Fatalf("replayed result = %#v", replay.Result)
	}
	records, err := ledger.ListExecutions(ctx, tenantID, request.RequestID)
	if err != nil || len(records) != 1 {
		t.Fatalf("ListExecutions() = %#v, %v", records, err)
	}
	if records[0].Status != ExecutionCompleted || records[0].CompletedAt == nil || !strings.HasPrefix(records[0].ToolCallID, "implicit-") {
		t.Fatalf("execution record = %#v", records[0])
	}
}

func TestPostgresExecutionLedgerFailedIntentCanRestart(t *testing.T) {
	ledger, _, tenantID := postgresExecutionLedger(t)
	ctx := context.Background()
	request := postgresToolRequest(tenantID, "request-retry", `{"order_id":"43"}`)
	first, err := ledger.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(ctx, tenantID, first.IdempotencyKey, "remote_rejected"); err != nil {
		t.Fatal(err)
	}
	request.ToolCallID = "retry-call"
	restarted, err := ledger.Begin(ctx, request)
	if err != nil || !restarted.Created || restarted.Status != ExecutionRunning {
		t.Fatalf("Begin(after failure) = %#v, %v", restarted, err)
	}
	if restarted.IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("idempotency key changed: %q != %q", restarted.IdempotencyKey, first.IdempotencyKey)
	}
	if err := ledger.Complete(ctx, tenantID, restarted.IdempotencyKey, "ok"); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresExecutionLedgerOutcomeUnknownIsTerminal(t *testing.T) {
	ledger, _, tenantID := postgresExecutionLedger(t)
	ctx := context.Background()
	request := postgresToolRequest(tenantID, "request-unknown", `{"order_id":"44"}`)
	first, err := ledger.Begin(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkOutcomeUnknown(ctx, tenantID, first.IdempotencyKey, "connection_lost"); err != nil {
		t.Fatal(err)
	}
	replay, err := ledger.Begin(ctx, request)
	if err != nil || replay.Created || replay.Status != ExecutionOutcomeUnknown {
		t.Fatalf("Begin(outcome unknown) = %#v, %v", replay, err)
	}
	if err := ledger.Complete(ctx, tenantID, first.IdempotencyKey, "late"); !errors.Is(err, ErrToolOutcomeUnknown) {
		t.Fatalf("Complete(terminal) error = %v", err)
	}
}

func TestPostgresExecutionLedgerExpiresRunningLeaseToOutcomeUnknown(t *testing.T) {
	ledger, _, tenantID := postgresExecutionLedger(t)
	now := time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	ledger.now = func() time.Time { return now }
	request := postgresToolRequest(tenantID, "request-expired", `{"order_id":"45"}`)
	request.LeaseTTL = time.Second
	first, err := ledger.Begin(context.Background(), request)
	if err != nil || !first.Created {
		t.Fatalf("Begin() = %#v, %v", first, err)
	}
	now = now.Add(2 * time.Second)
	replay, err := ledger.Begin(context.Background(), request)
	if err != nil || replay.Status != ExecutionOutcomeUnknown || replay.Created {
		t.Fatalf("Begin(expired) = %#v, %v", replay, err)
	}
}

func TestPostgresExecutionLedgerValidatesPublicOperations(t *testing.T) {
	ledger, _, tenantID := postgresExecutionLedger(t)
	ctx := context.Background()
	for name, request := range map[string]ExecutionRequest{
		"tenant":  {RequestID: "request", ToolName: "support.lookup", TraceID: "trace", LeaseTTL: time.Second},
		"request": {TenantID: tenantID, ToolName: "support.lookup", TraceID: "trace", LeaseTTL: time.Second},
		"tool":    {TenantID: tenantID, RequestID: "request", TraceID: "trace", LeaseTTL: time.Second},
		"trace":   {TenantID: tenantID, RequestID: "request", ToolName: "support.lookup", LeaseTTL: time.Second},
		"lease":   {TenantID: tenantID, RequestID: "request", ToolName: "support.lookup", TraceID: "trace"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ledger.Begin(ctx, request); err == nil {
				t.Fatal("Begin() accepted invalid request")
			}
		})
	}
	if _, err := ledger.ListExecutions(ctx, "", "request"); err == nil {
		t.Fatal("ListExecutions() accepted empty tenant")
	}
	if _, err := ledger.ListExecutions(ctx, tenantID, ""); err == nil {
		t.Fatal("ListExecutions() accepted empty request")
	}
	if err := ledger.Fail(ctx, "", "key", "failed"); err == nil {
		t.Fatal("Fail() accepted empty tenant")
	}
	if err := ledger.MarkOutcomeUnknown(ctx, tenantID, "", "unknown"); err == nil {
		t.Fatal("MarkOutcomeUnknown() accepted empty key")
	}
	if err := ledger.Complete(ctx, tenantID, "missing-key", "result"); !errors.Is(err, ErrToolOutcomeUnknown) {
		t.Fatalf("Complete(missing) error = %v", err)
	}
	if err := ledger.Complete(ctx, tenantID, "missing-key", func() {}); err == nil || !strings.Contains(err.Error(), "encode tool result") {
		t.Fatalf("Complete(unencodable) error = %v", err)
	}
}

func TestPostgresExecutionLedgerFailsClosedOnCorruptCompletedResult(t *testing.T) {
	tests := []struct {
		name       string
		ciphertext func(*PostgresExecutionLedger) []byte
		want       string
	}{
		{name: "empty", ciphertext: func(*PostgresExecutionLedger) []byte { return []byte{} }, want: "result is unavailable"},
		{name: "truncated", ciphertext: func(*PostgresExecutionLedger) []byte { return []byte{0x01} }, want: "decrypt completed tool result"},
		{name: "invalid json", ciphertext: func(ledger *PostgresExecutionLedger) []byte {
			ciphertext, err := encryptToolResult(ledger.key, []byte("not-json"))
			if err != nil {
				t.Fatalf("encrypt invalid fixture: %v", err)
			}
			return ciphertext
		}, want: "decode completed tool result"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger, database, tenantID := postgresExecutionLedger(t)
			request := postgresToolRequest(tenantID, "request-corrupt-"+test.name, `{"order_id":"46"}`)
			first, err := ledger.Begin(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledger.Complete(context.Background(), tenantID, first.IdempotencyKey, map[string]any{"status": "ok"}); err != nil {
				t.Fatal(err)
			}
			if _, err := database.ExecContext(context.Background(), `
UPDATE tool_executions SET result_ciphertext=$3
WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, first.IdempotencyKey, test.ciphertext(ledger)); err != nil {
				t.Fatalf("corrupt completed result: %v", err)
			}
			if _, err := ledger.Begin(context.Background(), request); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Begin(corrupt result) error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestPostgresExecutionLedgerPropagatesClosedDatabaseErrors(t *testing.T) {
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err := NewPostgresExecutionLedger(database, []byte("support-tool-ledger-key-material-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	request := postgresToolRequest("tenant-a", "request-closed", `{}`)
	if _, err := ledger.Begin(context.Background(), request); err == nil || !strings.Contains(err.Error(), "begin tool execution transaction") {
		t.Fatalf("Begin(closed DB) error = %v", err)
	}
	if _, err := ledger.ListExecutions(context.Background(), "tenant-a", "request-closed"); err == nil || !strings.Contains(err.Error(), "begin tool execution list") {
		t.Fatalf("ListExecutions(closed DB) error = %v", err)
	}
	if err := ledger.Fail(context.Background(), "tenant-a", "key", "failed"); err == nil || !strings.Contains(err.Error(), "begin tool execution completion") {
		t.Fatalf("Fail(closed DB) error = %v", err)
	}
}
