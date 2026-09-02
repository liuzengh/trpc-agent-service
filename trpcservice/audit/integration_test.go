//go:build integration

package audit

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const auditSchema = `CREATE TABLE IF NOT EXISTS audit_logs (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    audit_id    VARCHAR(36)  NOT NULL,
    tenant_id   VARCHAR(36)  NOT NULL,
    channel     VARCHAR(32)  NOT NULL,
    user_id     VARCHAR(64)  NOT NULL,
    session_id  VARCHAR(128) NOT NULL,
    agent_name  VARCHAR(128) NULL,
    tool_name   VARCHAR(128) NULL,
    decision    VARCHAR(32)  NULL,
    latency_ms  INT          NULL,
    error_type  VARCHAR(64)  NULL,
    cost        DECIMAL(12,6) NULL,
    trace_id    VARCHAR(64)  NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_audit_id (audit_id),
    KEY idx_audit_tenant_time (tenant_id, created_at),
    KEY idx_audit_trace (trace_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

func TestMySQLRecorderAsyncFlush(t *testing.T) {
	ctx := context.Background()

	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(auditSchema); err != nil {
		t.Fatalf("create audit_logs: %v", err)
	}

	r := NewMySQL(db)
	const n = 10
	for i := 0; i < n; i++ {
		r.Record(Entry{
			TenantID:  "t1",
			Channel:   "wecom",
			UserID:    "u1",
			SessionID: "s1",
			AgentName: "support-bot",
			ToolName:  "echo",
			Decision:  DecisionExecuted,
			Latency:   120 * time.Millisecond,
			Cost:      0.0042,
			TraceID:   "trace-1",
		})
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Close drains and flushes synchronously, so all rows must be present.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE tenant_id = 't1'").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Errorf("audit rows = %d, want %d", count, n)
	}

	// Spot-check one row's dimensions survive the round-trip.
	var agent, decision string
	var latencyMs int
	if err := db.QueryRow(
		"SELECT agent_name, decision, latency_ms FROM audit_logs WHERE trace_id = 'trace-1' LIMIT 1",
	).Scan(&agent, &decision, &latencyMs); err != nil {
		t.Fatalf("spot check: %v", err)
	}
	if agent != "support-bot" || decision != "executed" || latencyMs != 120 {
		t.Errorf("row = agent %q decision %q latency %d", agent, decision, latencyMs)
	}

	// The read side: List must return the flushed rows for a tenant in
	// newest-first order with the dimensions a front-end table needs.
	logs, err := r.List(ctx, "t1", 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != n {
		t.Errorf("List rows = %d, want %d", len(logs), n)
	}
	if logs[0].LatencyMS != 120 || logs[0].Decision != DecisionExecuted || logs[0].TenantID != "t1" {
		t.Errorf("List[0] = %+v, want latency 120/executed/t1", logs[0])
	}
	if logs[0].CreatedAt.IsZero() {
		t.Error("List[0].CreatedAt must be populated")
	}
	if logs[0].ErrorType != "" || logs[0].Cost == 0 {
		t.Errorf("List[0] dimensions = %+v, want cost present / no error", logs[0])
	}

	// Empty tenant filter returns every tenant's rows.
	all, err := r.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) < n {
		t.Errorf("List(all) rows = %d, want >= %d", len(all), n)
	}
}
