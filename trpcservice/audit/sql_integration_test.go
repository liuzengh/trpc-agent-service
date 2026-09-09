package audit_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func TestSQLRetentionWorkerUsesPublishedTenantPolicy(t *testing.T) {
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}
	tenantID := fmt.Sprintf("audit-retention-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM audit_logs WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM config_versions WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM tenants WHERE tenant_id=$1`, tenantID)
	})
	payload := []byte(fmt.Sprintf(`schema_version: 1
tenants:
  - tenant_id: %s
    name: audit retention test
    enabled: true
    config_version: 1
    audit: {enabled: true, retention_days: 1, store_content: false}
    apps:
      - app_id: assistant
        name: assistant
        enabled: true
        config: {instruction: test}
        model: {provider: mock, name: mock}
        tools: {allow: [echo]}
        channels:
          - {binding_id: test-http, type: http, provider_account_id: test, enabled: true}
        storage:
          session: {type: inmemory}
          memory: {type: inmemory}
          summary: {type: inmemory}
          artifact: {type: inmemory}
          knowledge: {type: inmemory}
          audit: {type: inmemory}
`, tenantID))
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (tenant_id,name,enabled,current_config_version) VALUES ($1,'audit retention test',TRUE,1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO config_versions (tenant_id,version,config_yaml,config_sha256,created_by) VALUES ($1,1,$2,'fixture','test')`, tenantID, payload); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	store := &audit.SQLStore{DB: db}
	for _, record := range []audit.Record{
		{TenantID: tenantID, Decision: "allow", TraceID: "old", ConfigVersion: 1, PolicyVersion: 1, CreatedAt: now.Add(-48 * time.Hour)},
		{TenantID: tenantID, Decision: "allow", TraceID: "new", ConfigVersion: 1, PolicyVersion: 1, CreatedAt: now.Add(-time.Hour)},
	} {
		if err := store.Append(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	result := make(chan error, 1)
	worker := &audit.RetentionWorker{Store: store, Policies: &audit.SQLPolicySource{DB: db, TenantID: tenantID}, Interval: time.Hour, Now: func() time.Time { return now }, OnResult: func(deleted int64, err error) {
		if err == nil && deleted != 1 {
			err = fmt.Errorf("deleted=%d", deleted)
		}
		result <- err
	}}
	workerCtx, stopWorker := context.WithCancel(ctx)
	if err := worker.Start(workerCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopWorker()
	if err := worker.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs WHERE tenant_id=$1`, tenantID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("remaining=%d err=%v", count, err)
	}
	var configVersion, policyVersion uint64
	if err := db.QueryRowContext(ctx, `SELECT config_version,policy_version FROM audit_logs WHERE tenant_id=$1`, tenantID).Scan(&configVersion, &policyVersion); err != nil || configVersion != 1 || policyVersion != 1 {
		t.Fatalf("audit versions config=%d policy=%d err=%v", configVersion, policyVersion, err)
	}
}
