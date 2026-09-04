package storage

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestSessionsTenantIsolation(t *testing.T) {
	ctx := context.Background()
	s, err := NewSessions(SessionConfig{Backend: BackendInMemory})
	if err != nil {
		t.Fatalf("NewSessions: %v", err)
	}

	// Tenant A creates a session with identifiable state.
	if _, err := s.Create(ctx, "tenant-a", "user-1", "sess-1",
		session.StateMap{"k": []byte("v")}); err != nil {
		t.Fatalf("create tenant-a: %v", err)
	}

	// Tenant A sees its own session and state.
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

	// Tenant B resolves the same user/session ids but a different AppName,
	// so it must resolve to nothing.
	b, err := s.Get(ctx, "tenant-b", "user-1", "sess-1")
	if err != nil {
		t.Fatalf("get tenant-b: %v", err)
	}
	if b != nil {
		t.Error("tenant-b must not resolve tenant-a's session")
	}
}

func TestSessionsDelete(t *testing.T) {
	ctx := context.Background()
	s, err := NewSessions(SessionConfig{Backend: BackendInMemory})
	if err != nil {
		t.Fatalf("NewSessions: %v", err)
	}

	if _, err := s.Create(ctx, "t", "u", "s", session.StateMap{"k": []byte("v")}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.Delete(ctx, "t", "u", "s"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, err := s.Get(ctx, "t", "u", "s")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if got != nil {
		t.Error("session should be gone after delete")
	}
}
