package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAuditProjectionPostgres(t *testing.T) {
	raw := os.Getenv("CONTROL_AUDIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated Control audit PostgreSQL fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, sql := range []string{
		`CREATE TABLE agents(tenant_id text,id text,created_by text,created_at timestamptz)`,
		`CREATE TABLE agent_versions(tenant_id text,id text,published_by text,published_at timestamptz)`,
		`CREATE TABLE runtime_profiles(tenant_id text,id text,created_by text,created_at timestamptz)`,
		`CREATE TABLE runtime_profile_revisions(tenant_id text,id text,published_by text,published_at timestamptz)`,
		`CREATE TABLE deployments(tenant_id text,id text,created_by text,created_at timestamptz)`,
		`CREATE TABLE deployment_revisions(tenant_id text,id text,published_by text,published_at timestamptz)`,
		`CREATE TABLE channel_accounts(tenant_id text,id text,created_by text,created_at timestamptz)`,
		`CREATE TABLE channel_bindings(tenant_id text,id text,created_by text,created_at timestamptz)`,
	} {
		if _, err = pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	if _, err = pool.Exec(ctx, `INSERT INTO agents VALUES('tenant','agent','user-a',$1),('other','hidden','user-b',$1)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO deployment_revisions VALUES('tenant','revision','user-c',$1)`, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	page, err := NewAuditReader(pool).List(ctx, "tenant", 0, 25)
	if err != nil || page.Total != 2 || len(page.Events) != 2 {
		t.Fatal(page, err)
	}
	if page.Events[0].ResourceID != "revision" || page.Events[0].ActorID != "user-c" || page.Events[1].ResourceID != "agent" || page.Events[1].Source != "control" {
		t.Fatal(page)
	}
}
