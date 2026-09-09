package storage

import (
	"context"
	"testing"
)

// NewPGLazy must return a usable pool without pinging at construction: the
// first query dials. A malformed DSN fails at parse time.
func TestNewPGLazy(t *testing.T) {
	ctx := context.Background()
	pool, err := NewPGLazy(ctx, testPGDSN)
	if err != nil {
		t.Fatalf("lazy pool construction must not fail on a good DSN: %v", err)
	}
	defer pool.Close()

	var one int
	if err := pool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("lazy pool must dial on first use (one=%d err=%v)", one, err)
	}

	if _, err := NewPGLazy(ctx, "not-a-dsn"); err == nil {
		t.Fatal("a malformed DSN must fail at parse time")
	}
}

// NewPG fails fast (within dialTimeout) when the server is unreachable.
func TestNewPGPingFailure(t *testing.T) {
	if _, err := NewPG(context.Background(), "postgres://trpc:trpc-dev-only@localhost:1/trpc?sslmode=disable"); err == nil {
		t.Fatal("NewPG against an unreachable server must fail")
	}
}
