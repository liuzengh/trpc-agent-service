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
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
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
	journal, _ := gateway.NewPostgresJournal(scoped)
	accepted, err := journal.Accept(ctx, gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "metrics-test", UserID: "user", SessionID: "session", ChatType: "direct", Text: "synthetic message"})
	if err != nil {
		t.Fatal(err)
	}
	source := metrics.SQLBacklogSource(scoped)
	snapshot, err := source(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := func(stage, state string) int64 {
		for _, row := range snapshot {
			if row.TenantID == "tutorial-tenant" && row.Stage == stage && row.State == state {
				return row.Items
			}
		}
		return -1
	}
	if count("queue", "pending") != 1 || count("run", "queued") != 1 || count("delivery", "unknown") != 0 {
		t.Fatal("backlog view counts are incorrect")
	}
	if _, err := scoped.ExecContext(ctx, `UPDATE queue_outbox SET status='published' WHERE payload->>'request_id'=$1`, accepted.RequestID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = source(ctx)
	if err != nil || count("queue", "pending") != 0 {
		t.Fatal("drained backlog did not emit explicit zero")
	}
	for _, tc := range []struct {
		role, statement string
		allow           bool
	}{
		{"admin", "SELECT connection_id FROM backend_connection", true},
		{"admin", "INSERT INTO backend_connection SELECT * FROM backend_connection WHERE false", true},
		{"admin", "UPDATE backend_connection SET display_name='changed' WHERE false", false},
		{"worker", "SELECT connection_id FROM backend_connection", false},
		{"worker", "SELECT script FROM skill_bundle", true},
		{"worker", "UPDATE skill_bundle SET status='approved' WHERE false", false},
		{"admin", "UPDATE skill_bundle SET status='approved' WHERE false", true},
		{"admin", "SELECT document_id FROM knowledge_document", true},
		{"admin", "INSERT INTO knowledge_document SELECT * FROM knowledge_document WHERE false", true},
		{"worker", "SELECT document_id FROM knowledge_document", false},
		{"jobs", "UPDATE knowledge_document SET state='ready' WHERE false", false},
		{"jobs", "SELECT ciphertext FROM channel_credential", true},
		{"gateway", "SELECT script FROM skill_bundle", false},
		{"gateway", "SELECT tenant_id FROM tenant", true},
		{"gateway", "SELECT items FROM platform_backlog", true},
		{"relay", "SELECT items FROM platform_backlog", false},
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
		{"jobs", "UPDATE knowledge_sync SET state=state WHERE false", true},
		{"admin", "UPDATE knowledge_sync SET state=state WHERE false", false},
		{"worker", "UPDATE knowledge_sync SET state=state WHERE false", false},
		{"admin", "UPDATE tenant SET version=version WHERE false", false},
		{"admin", `SELECT platform_tenant_policy_update('tutorial-tenant',-1,'{}','{}','test','')`, true},
		{"gateway", `SELECT platform_tenant_policy_update('tutorial-tenant',-1,'{}','{}','test','')`, false},
		{"gateway", "DELETE FROM audit_log WHERE false", false},
		{"gateway", "SELECT audit_id FROM audit_log", false},
		{"gateway", "SELECT platform_audit_prune('tutorial-tenant',1)", false},
		{"admin", "SELECT platform_audit_prune('tutorial-tenant',1)", false},
		{"jobs", "SELECT platform_audit_prune('tutorial-tenant',1)", true},
		{"gateway", `SELECT platform_audit_append('{"audit_id":"audit-permission-test","tenant_id":"tutorial-tenant","occurred_at":"2026-01-01T00:00:00Z","decision":"test"}'::jsonb)`, true},
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
	repo, err := controlplane.NewPostgresRepository(scoped)
	if err != nil {
		t.Fatal(err)
	}
	uploads := platformskill.NewStore(repo)
	if err := database.InTransaction(ctx, scoped, func(ctx context.Context) error {
		if _, err := database.Transaction(ctx, scoped).ExecContext(ctx, "SET LOCAL ROLE "+schema+"_admin"); err != nil {
			return err
		}
		d, err := uploads.Upload(ctx, "tutorial-tenant", "permission-admin", platformskill.Upload{Name: "permission-skill", Version: "1", Markdown: "---\nname: permission-skill\ndescription: Permission fixture\n---\nRead the input.\n", Script: "echo sandbox"})
		if err != nil {
			return err
		}
		_, err = uploads.Review(ctx, "tutorial-tenant", d.Name, d.Version, "approved", "permission-admin", d.Revision)
		return err
	}); err != nil {
		t.Fatal("least-privilege admin cannot manage uploaded Skill", err)
	}
}
