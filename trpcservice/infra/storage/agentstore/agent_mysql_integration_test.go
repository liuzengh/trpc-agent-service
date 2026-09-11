//go:build integration

package agentstore

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

// TestMySQLAgentVersions exercises the SQL the version lifecycle depends on:
// publishing freezes an immutable version and switching the current pointer is
// atomic. (The asset-level canary release was removed: platform rollout is a
// deployment concern, so agent versions only move through Publish/Rollback.)
func TestMySQLAgentVersions(t *testing.T) {
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts("../../../../deployments/mysql/init/003_agents.sql"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := NewMySQLManager(db, nil)
	if err := mgr.Create(ctx, agent.Agent{ID: "a1", TenantID: "t1", Name: "helper"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	v1, err := mgr.Publish(ctx, "a1", agent.RuntimeProfile{SystemPrompt: "first"})
	if err != nil || v1 != 1 {
		t.Fatalf("publish v1: version=%d err=%v", v1, err)
	}
	v2, err := mgr.Publish(ctx, "a1", agent.RuntimeProfile{SystemPrompt: "second"})
	if err != nil || v2 != 2 {
		t.Fatalf("publish v2: version=%d err=%v", v2, err)
	}

	// The current profile is v2...
	cur, err := mgr.Resolve(ctx, "a1")
	if err != nil || cur.SystemPrompt != "second" {
		t.Fatalf("resolve current = %q (%v), want second", cur.SystemPrompt, err)
	}

	// ...and rolling back to v1 switches the pointer without touching history.
	if err := mgr.Rollback(ctx, "a1", 1); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	back, err := mgr.Resolve(ctx, "a1")
	if err != nil || back.SystemPrompt != "first" {
		t.Fatalf("resolve after rollback = %q (%v), want first", back.SystemPrompt, err)
	}
	ag, err := mgr.Get(ctx, "a1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ag.CurrentVersion != 1 {
		t.Errorf("current_version = %d after rollback, want 1", ag.CurrentVersion)
	}
	vs, err := mgr.Versions(ctx, "a1")
	if err != nil || len(vs) != 2 {
		t.Fatalf("versions = %+v (%v), want both frozen versions", vs, err)
	}
}
