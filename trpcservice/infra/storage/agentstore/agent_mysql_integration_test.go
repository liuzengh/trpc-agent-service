//go:build integration

package agentstore

import (
	"context"
	"errors"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

// TestMySQLAgentVersionsAndGrayRelease exercises the SQL the canary release
// depends on: publishing freezes versions, ResolveVersion reads a non-current
// one, and the gray column round-trips (including clearing it).
func TestMySQLAgentVersionsAndGrayRelease(t *testing.T) {
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

	// The current profile is v2; the canary needs v1's frozen profile.
	cur, err := mgr.Resolve(ctx, "a1")
	if err != nil || cur.SystemPrompt != "second" {
		t.Fatalf("resolve current = %q (%v), want second", cur.SystemPrompt, err)
	}
	old, err := mgr.ResolveVersion(ctx, "a1", 1)
	if err != nil || old.SystemPrompt != "first" {
		t.Fatalf("resolve v1 = %q (%v), want first", old.SystemPrompt, err)
	}
	if _, err := mgr.ResolveVersion(ctx, "a1", 9); !errors.Is(err, agent.ErrNotFound) {
		t.Errorf("unknown version err = %v, want ErrNotFound", err)
	}

	// Install, read back and clear the release.
	if err := mgr.SetGray(ctx, "a1", &agent.GrayRelease{Version: 1, Percent: 25}); err != nil {
		t.Fatalf("set gray: %v", err)
	}
	ag, err := mgr.Get(ctx, "a1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ag.Gray == nil || ag.Gray.Version != 1 || ag.Gray.Percent != 25 {
		t.Fatalf("gray = %+v, want {1 25}", ag.Gray)
	}

	// A 100% release sends every session to v1 through the manager.
	if err := mgr.SetGray(ctx, "a1", &agent.GrayRelease{Version: 1, Percent: 100}); err != nil {
		t.Fatalf("set gray 100: %v", err)
	}
	p, version, err := mgr.ResolveForSession(ctx, "a1", "session-1")
	if err != nil || version != 1 || p.SystemPrompt != "first" {
		t.Fatalf("gray resolve = v%d %q (%v), want v1 first", version, p.SystemPrompt, err)
	}

	if err := mgr.ClearGray(ctx, "a1"); err != nil {
		t.Fatalf("clear gray: %v", err)
	}
	ag, err = mgr.Get(ctx, "a1")
	if err != nil {
		t.Fatalf("get after clear: %v", err)
	}
	if ag.Gray != nil {
		t.Errorf("gray = %+v after clearing, want nil", ag.Gray)
	}
	if _, version, _ := mgr.ResolveForSession(ctx, "a1", "session-1"); version != 2 {
		t.Errorf("version = %d after clearing, want 2 (current)", version)
	}
}
