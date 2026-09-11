//go:build integration

package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

const webUsageSchema = `CREATE TABLE IF NOT EXISTS usage_records (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    record_id  VARCHAR(36)  NOT NULL,
    tenant_id  VARCHAR(36)  NOT NULL,
    agent_id   VARCHAR(36)  NULL,
    member_id  VARCHAR(64)  NULL,
    dimension  VARCHAR(32)  NOT NULL,
    amount     DECIMAL(20,6) NOT NULL,
    meta       JSON         NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_usage_record (record_id),
    KEY idx_usage_tenant_dim (tenant_id, dimension, created_at),
    KEY idx_usage_member_dim (tenant_id, member_id, dimension, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

// TestUsageAPIGet exercises GET /usage end to end: a recorded token amount
// shows up in both the per-dimension summary and the detail rows.
func TestUsageAPIGet(t *testing.T) {
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
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(webUsageSchema); err != nil {
		t.Fatalf("create usage_records: %v", err)
	}

	rec := audit.NewMySQL(db)
	if err := rec.RecordUsage(ctx, audit.UsageEntry{
		RecordID: "m1:token", TenantID: "t-http", AgentID: "a-1", Dimension: "token", Amount: 128,
	}); err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}

	mux := http.NewServeMux()
	NewUsageAPI(rec).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	owner := clientAs(ownerClaims())
	resp := getAs(t, owner, srv.URL+"/usage?tenant_id=t-http")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body usageResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Summary) != 1 || body.Summary[0].Dimension != "token" || body.Summary[0].Total != 128 {
		t.Errorf("summary = %+v, want single token total 128", body.Summary)
	}
	if len(body.Rows) != 1 || body.Rows[0].Amount != 128 {
		t.Errorf("rows = %+v, want single 128 row", body.Rows)
	}
}

// TestUsageAPIMemberSeesOwnUsageOnly is the row-level rule for metering: a
// plain member's query is pinned to the member dimension, so it can neither see
// the tenant aggregate nor widen the view with ?member_id=.
func TestUsageAPIMemberSeesOwnUsageOnly(t *testing.T) {
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
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(webUsageSchema); err != nil {
		t.Fatalf("create usage_records: %v", err)
	}

	rec := audit.NewMySQL(db)
	for _, e := range []audit.UsageEntry{
		{RecordID: "alice:token", TenantID: "t-mem", Dimension: "token", Amount: 100, MemberID: "alice"},
		{RecordID: "bob:token", TenantID: "t-mem", Dimension: "token", Amount: 900, MemberID: "bob"},
		{RecordID: "tenant:token", TenantID: "t-mem", Dimension: "token", Amount: 50},
	} {
		if err := rec.RecordUsage(ctx, e); err != nil {
			t.Fatalf("RecordUsage %s: %v", e.RecordID, err)
		}
	}

	mux := http.NewServeMux()
	NewUsageAPI(rec).Register(mux)
	srv := httptest.NewServer(asClaims(mux))
	defer srv.Close()

	// Alice sees her own 100 tokens only, even when she asks for bob's.
	alice := clientAs(memberClaims("t-mem", "alice"))
	resp := getAs(t, alice, srv.URL+"/usage?member_id=bob")
	var own usageResponse
	if err := json.NewDecoder(resp.Body).Decode(&own); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(own.Summary) != 1 || own.Summary[0].Total != 100 {
		t.Errorf("member summary = %+v, want own 100 tokens", own.Summary)
	}
	if len(own.Rows) != 1 || own.Rows[0].MemberID != "alice" {
		t.Errorf("member rows = %+v, want alice's row", own.Rows)
	}

	// The tenant admin sees the whole tenant, including the unattributed row.
	admin := clientAs(adminClaims("t-mem"))
	resp = getAs(t, admin, srv.URL+"/usage")
	var all usageResponse
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(all.Summary) != 1 || all.Summary[0].Total != 1050 {
		t.Errorf("admin summary = %+v, want tenant total 1050", all.Summary)
	}
}
