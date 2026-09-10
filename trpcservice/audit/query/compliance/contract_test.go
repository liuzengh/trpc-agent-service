package compliance

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query/contracttest"
)

func TestComplianceAuditQueryContract(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("TRPC_MIGRATION_TEST=1 is required")
	}
	dsn := os.Getenv("TRPC_AUDIT_COMPLIANCE_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_AUDIT_COMPLIANCE_TEST_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	seed := func(events ...audit.Event) {
		for _, event := range events {
			digest, err := audit.Digest(event)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(context.Background(), `INSERT INTO compliance.audit_event(
tenant_id,audit_id,schema_version,event_json,event_digest,occurred_at) VALUES($1,$2,$3,$4,$5,$6)`,
				event.TenantID, event.AuditID, event.SchemaVersion, raw, digest, event.OccurredAt); err != nil {
				t.Fatal(err)
			}
		}
	}
	contracttest.Suite(t, store, seed)
}
