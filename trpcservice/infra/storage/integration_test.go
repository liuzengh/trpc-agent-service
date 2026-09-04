//go:build integration

package storage

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestSessionsMySQLTenantIsolation(t *testing.T) {
	ctx := context.Background()

	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}

	s, err := NewSessions(SessionConfig{Backend: BackendMySQL, MySQLDSN: dsn})
	if err != nil {
		t.Fatalf("NewSessions mysql: %v", err)
	}
	assertSessionIsolation(t, s)
}

func TestSessionsRedisTenantIsolation(t *testing.T) {
	ctx := context.Background()

	c, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}

	s, err := NewSessions(SessionConfig{Backend: BackendRedis, RedisURL: url})
	if err != nil {
		t.Fatalf("NewSessions redis: %v", err)
	}
	assertSessionIsolation(t, s)
}

func TestMemoriesRedisTenantIsolation(t *testing.T) {
	ctx := context.Background()

	c, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}

	m, err := NewMemories(MemoryConfig{Backend: BackendRedis, RedisURL: url})
	if err != nil {
		t.Fatalf("NewMemories redis: %v", err)
	}

	if err := m.Add(ctx, "tenant-a", "user-1", "prefers dark mode", nil); err != nil {
		t.Fatalf("add tenant-a: %v", err)
	}
	entries, err := m.Read(ctx, "tenant-a", "user-1", 10)
	if err != nil {
		t.Fatalf("read tenant-a: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("tenant-a entries = %d, want 1", len(entries))
	}

	entries, err = m.Read(ctx, "tenant-b", "user-1", 10)
	if err != nil {
		t.Fatalf("read tenant-b: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("tenant-b entries = %d, want 0 (isolated)", len(entries))
	}
}

// assertSessionIsolation verifies that a session created under one tenant is
// invisible under another tenant with the same user/session ids.
func assertSessionIsolation(t *testing.T, s *Sessions) {
	t.Helper()
	ctx := context.Background()

	if _, err := s.Create(ctx, "tenant-a", "user-1", "sess-1",
		session.StateMap{"k": []byte("v")}); err != nil {
		t.Fatalf("create tenant-a: %v", err)
	}

	a, err := s.Get(ctx, "tenant-a", "user-1", "sess-1")
	if err != nil {
		t.Fatalf("get tenant-a: %v", err)
	}
	if a == nil {
		t.Fatal("tenant-a session should exist")
	}
	if got := string(a.State["k"]); got != "v" {
		t.Errorf("tenant-a state[k] = %q, want %q", got, "v")
	}

	b, err := s.Get(ctx, "tenant-b", "user-1", "sess-1")
	if err != nil {
		t.Fatalf("get tenant-b: %v", err)
	}
	if b != nil {
		t.Error("tenant-b must not resolve tenant-a's session")
	}
}
