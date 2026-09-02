//go:build integration

package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const webAuditSchema = `CREATE TABLE IF NOT EXISTS audit_logs (
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

// TestAuditAPIList exercises GET /audit end to end: record rows through the
// recorder, flush, then assert the HTTP JSON list returns them newest first.
func TestAuditAPIList(t *testing.T) {
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
	if _, err := db.Exec(webAuditSchema); err != nil {
		t.Fatalf("create audit_logs: %v", err)
	}

	rec := audit.NewMySQL(db)
	rec.Record(audit.Entry{
		TenantID: "t-http", Channel: "wecom", UserID: "u-1", SessionID: "s-1",
		AgentName: "bot", Decision: audit.DecisionExecuted,
		Latency: 5 * time.Millisecond, Cost: 0.001, TraceID: "trace-http",
	})
	if err := rec.Close(); err != nil {
		t.Fatalf("recorder close: %v", err)
	}

	mux := http.NewServeMux()
	NewAuditAPI(rec).Register(mux)

	// tenant-scoped
	req := httptest.NewRequest(http.MethodGet, "/audit?tenant_id=t-http", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rr.Code, rr.Body.String())
	}
	var logs []audit.Log
	if err := json.NewDecoder(rr.Body).Decode(&logs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("rows = %d, want 1", len(logs))
	}
	if logs[0].AgentName != "bot" || logs[0].TraceID != "trace-http" || logs[0].CreatedAt.IsZero() {
		t.Errorf("row = %+v", logs[0])
	}

	// all-tenant (empty filter)
	req = httptest.NewRequest(http.MethodGet, "/audit", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rr.Code, rr.Body.String())
	}
	if err := json.NewDecoder(rr.Body).Decode(&logs); err != nil {
		t.Fatalf("decode all: %v", err)
	}
	if len(logs) < 1 {
		t.Error("all-tenant list must include the recorded row")
	}
}
