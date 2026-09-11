package storage

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestTenantScopedFrameworkPostgresClientEnforcesApplicationRLS(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	for _, id := range []string{"scope-a-existing", "scope-b-existing", "scope-a-exec", "scope-a-tx"} {
		_, _ = admin.ExecContext(ctx, `DELETE FROM memories WHERE memory_id=$1`, id)
	}
	if _, err := admin.ExecContext(ctx, `
INSERT INTO memories (memory_id, app_name, user_id, memory_data)
VALUES
    ('scope-a-existing','tenant-a/support','customer-1','{}'::jsonb),
    ('scope-b-existing','tenant-b/support','customer-2','{}'::jsonb)`); err != nil {
		t.Fatalf("seed memories: %v", err)
	}
	t.Cleanup(func() {
		for _, id := range []string{"scope-a-existing", "scope-b-existing", "scope-a-exec", "scope-a-tx"} {
			_, _ = admin.ExecContext(context.Background(), `DELETE FROM memories WHERE memory_id=$1`, id)
		}
	})

	delegate, err := newMigrationManagedFrameworkClient(ctx, dsn, "memories")
	if err != nil {
		t.Fatal(err)
	}
	client := &tenantScopedFrameworkPostgresClient{
		delegate: delegate,
		scope: frameworkPostgresScope{
			component: "memory", tenantID: "tenant-a", appName: "tenant-a/support", enforceTenantScope: true,
		},
	}
	t.Cleanup(func() { _ = client.Close() })

	result, err := client.ExecContext(ctx, `
INSERT INTO memories (memory_id, app_name, user_id, memory_data)
VALUES ('scope-a-exec','tenant-a/support','customer-1','{}'::jsonb)`)
	if err != nil {
		t.Fatalf("tenant-scoped ExecContext: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("ExecContext rows = %d, %v", affected, err)
	}

	var visible []string
	if err := client.Query(ctx, func(rows *sql.Rows) error {
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			visible = append(visible, id)
		}
		return rows.Err()
	}, `SELECT memory_id FROM memories WHERE memory_id LIKE 'scope-%' ORDER BY memory_id`); err != nil {
		t.Fatalf("tenant-scoped Query: %v", err)
	}
	if len(visible) != 2 || visible[0] != "scope-a-exec" || visible[1] != "scope-a-existing" {
		t.Fatalf("visible memories = %v, want only tenant-a/support rows", visible)
	}

	if err := client.Transaction(ctx, func(tx *sql.Tx) error {
		var currentUser, appName string
		if err := tx.QueryRowContext(ctx, `SELECT current_user, current_setting('app.app_name', true)`).Scan(&currentUser, &appName); err != nil {
			return err
		}
		if currentUser != "trpc_tenant" || appName != "tenant-a/support" {
			t.Fatalf("transaction scope = user:%q app:%q", currentUser, appName)
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO memories (memory_id, app_name, user_id, memory_data)
VALUES ('scope-a-tx','tenant-a/support','customer-1','{}'::jsonb)`)
		return err
	}); err != nil {
		t.Fatalf("tenant-scoped Transaction: %v", err)
	}

	var tenantACount, tenantBCount int
	if err := admin.QueryRowContext(ctx, `SELECT COUNT(*) FROM memories WHERE memory_id IN ('scope-a-existing','scope-a-exec','scope-a-tx')`).Scan(&tenantACount); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRowContext(ctx, `SELECT COUNT(*) FROM memories WHERE memory_id='scope-b-existing'`).Scan(&tenantBCount); err != nil {
		t.Fatal(err)
	}
	if tenantACount != 3 || tenantBCount != 1 {
		t.Fatalf("persisted scoped memories = tenant-a:%d tenant-b:%d", tenantACount, tenantBCount)
	}
}
