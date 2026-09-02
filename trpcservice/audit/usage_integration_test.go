//go:build integration

package audit

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

const usageSchema = `CREATE TABLE IF NOT EXISTS usage_records (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    record_id  VARCHAR(36)  NOT NULL,
    tenant_id  VARCHAR(36)  NOT NULL,
    agent_id   VARCHAR(36)  NULL,
    dimension  VARCHAR(32)  NOT NULL,
    amount     DECIMAL(20,6) NOT NULL,
    meta       JSON         NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_usage_record (record_id),
    KEY idx_usage_tenant_dim (tenant_id, dimension, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

func newUsageRecorderForTest(t *testing.T) *MySQLRecorder {
	t.Helper()
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(usageSchema); err != nil {
		t.Fatalf("create usage_records: %v", err)
	}
	return NewMySQL(db)
}

func TestMySQLUsageMeteringIdempotentAndAggregated(t *testing.T) {
	ctx := context.Background()
	r := newUsageRecorderForTest(t)

	// write token usage for two agents in two dimensions
	for i := 0; i < 2; i++ { // second is idempotent-ignored
		if err := r.RecordUsage(ctx, UsageEntry{RecordID: "m1:token", TenantID: "t1", AgentID: "a1", Dimension: "token", Amount: 150}); err != nil {
			t.Fatalf("RecordUsage: %v", err)
		}
	}
	if err := r.RecordUsage(ctx, UsageEntry{RecordID: "m2:token", TenantID: "t1", AgentID: "a2", Dimension: "token", Amount: 50}); err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}
	if err := r.RecordUsage(ctx, UsageEntry{RecordID: "m3:tool", TenantID: "t1", AgentID: "a1", Dimension: "tool", Amount: 3}); err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}

	// summary per dimension
	sum, err := r.UsageSummary(ctx, UsageQuery{TenantID: "t1"})
	if err != nil {
		t.Fatalf("UsageSummary: %v", err)
	}
	if len(sum) != 2 {
		t.Fatalf("summary = %+v, want 2 dimensions", sum)
	}
	totals := map[string]float64{}
	for _, s := range sum {
		totals[s.Dimension] = s.Total
	}
	if totals["token"] != 200 { // 150 (idempotent) + 50
		t.Errorf("token total = %v, want 200", totals["token"])
	}
	if totals["tool"] != 3 {
		t.Errorf("tool total = %v, want 3", totals["tool"])
	}

	// dimension filter
	sum, err = r.UsageSummary(ctx, UsageQuery{TenantID: "t1", Dimension: "tool"})
	if err != nil {
		t.Fatalf("UsageSummary(dim): %v", err)
	}
	if len(sum) != 1 || sum[0].Dimension != "tool" || sum[0].Count != 1 {
		t.Errorf("tool summary = %+v, want single tool row count 1", sum)
	}

	// rows (detail) newest first, tenant filter
	rows, err := r.UsageRows(ctx, UsageQuery{TenantID: "t1", Limit: 10})
	if err != nil {
		t.Fatalf("UsageRows: %v", err)
	}
	if len(rows) != 3 { // m1:token, m2:token, m3:tool (idempotent dedup)
		t.Errorf("rows = %d, want 3", len(rows))
	}
}
