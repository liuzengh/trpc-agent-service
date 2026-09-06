package permissions

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func TestPolicyRenderingFailsClosed(t *testing.T) {
	for _, bad := range []string{"public", "x; DROP SCHEMA public", "unsafe-secret-canary"} {
		if _, err := SQL(bad, "test"); err == nil || strings.Contains(err.Error(), "secret-canary") {
			t.Fatal("unsafe SQL identifiers accepted or leaked")
		}
	}
	sqlText, err := SQL("agent_platform", "trpc")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"GRANT ALL", "DEFAULT PRIVILEGES", "PASSWORD", " WITH GRANT OPTION", "GRANT DELETE"} {
		if strings.Contains(sqlText, bad) {
			t.Fatal("overbroad SQL policy")
		}
	}
	acl, err := Redis("trpc", "platform")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"+@all", "+@write", "~*", " on ", "nopass", "+flushdb", "+scan"} {
		if strings.Contains(acl, bad) {
			t.Fatal("unsafe Redis default")
		}
	}
	if !strings.Contains(acl, ":channel-poll:coord:session:") || !strings.Contains(acl, ":idempotency:message:") {
		t.Fatal("Redis namespaces do not match implementation")
	}
}

func TestPostgresRolePermissionsIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open test database")
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("perm_test_%x", time.Now().UnixNano())
	createdRoles := false
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create isolated schema")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error("cleanup isolated permission schema")
		}
		if createdRoles {
			for _, role := range roles {
				if _, err := db.ExecContext(cleanup, "DROP ROLE "+schema+"_"+role); err != nil {
					t.Error("cleanup test-created role")
				}
			}
		}
	}()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse test database URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal("open scoped database")
	}
	defer func() { _ = scoped.Close() }()
	if err := database.Migrate(ctx, scoped); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, scoped, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	policy, _ := SQL(schema, schema)
	if _, err := scoped.ExecContext(ctx, policy); err != nil {
		_, _ = scoped.ExecContext(context.Background(), "ROLLBACK")
		t.Fatal("apply isolated role policy: ", err)
	}
	createdRoles = true
	for _, tc := range []struct {
		role, statement string
		allow           bool
	}{
		{"gateway", "SELECT tenant_id FROM tenant", true},
		{"gateway", "UPDATE agent_app SET status='active' WHERE false", false},
		{"gateway", "UPDATE channel_delivery_attempt SET status='sent' WHERE false", false},
		{"worker", "UPDATE agent_run SET status='running' WHERE false", true},
		{"worker", "INSERT INTO agent_revision SELECT * FROM agent_revision WHERE false", false},
		{"worker", "UPDATE outbound_message SET status='sent' WHERE false", false},
		{"relay", "UPDATE queue_outbox SET status='published' WHERE false", true},
		{"relay", "SELECT payload FROM inbound_message", false},
		{"sender", "UPDATE channel_delivery_attempt SET status='unknown' WHERE false", true},
		{"sender", "SELECT payload FROM inbound_message", false},
		{"admin", "UPDATE agent_app SET status='active' WHERE false", true},
		{"admin", "INSERT INTO work_item SELECT * FROM work_item WHERE false", false},
		{"jobs", "UPDATE backend_binding SET version=version WHERE false", true},
		{"jobs", "UPDATE agent_app SET status='active' WHERE false", false},
		{"gateway", "DELETE FROM audit_log WHERE false", false},
		{"admin", "DELETE FROM audit_log WHERE false", false},
		{"worker", "CREATE TABLE unwanted(id int)", false},
	} {
		t.Run(tc.role+"/"+tc.statement, func(t *testing.T) {
			tx, err := scoped.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+schema+"_"+tc.role); err != nil {
				t.Fatal(err)
			}
			_, err = tx.ExecContext(ctx, tc.statement)
			if (err == nil) != tc.allow {
				t.Fatalf("allowed=%t actual error=%v", tc.allow, err)
			}
		})
	}
}
