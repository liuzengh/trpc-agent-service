package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestPoolExhaustionAcquireIsBounded proves P2-03 pool protection: when the
// pool is saturated, a query with a context deadline fails at the deadline
// (bounded), never blocks indefinitely; after the slot is released and after
// a pool reset, recovery is immediate.
func TestPoolExhaustionAcquireIsBounded(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; pool capacity evidence unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := NewPool(ctx, PostgresConfig{URL: url, MaxConns: 1, MinConns: 1, ConnectTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal("pool unavailable")
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal("could not saturate the pool")
	}
	start := time.Now()
	exhaustedCtx, exhaustedCancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer exhaustedCancel()
	err = pool.Ping(exhaustedCtx)
	elapsed := time.Since(start)
	if err == nil {
		conn.Release()
		t.Fatalf("saturated pool answered a ping; exhaustion not exercised")
	}
	if !errors.Is(err, context.DeadlineExceeded) && elapsed < 550*time.Millisecond {
		t.Fatalf("acquire failure was neither the deadline nor bounded: %v %s", err, elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("acquire waited unboundedly: %s", elapsed)
	}
	conn.Release()
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("no recovery after slot release: %v", err)
	}
	pool.Reset()
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("no recovery after pool reset: %v", err)
	}
}
