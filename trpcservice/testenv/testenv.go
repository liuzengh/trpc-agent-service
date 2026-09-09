// Package testenv holds what the integration tests share: the infrastructure
// dependencies are gated through environment variables — CI services and dev
// shells set them explicitly, so every skip message names the variable that
// enables the test, and a CI run with skips > 0 fails the build instead of
// silently shrugging past the platform's core paths.
package testenv

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// RedisAddr is the test Redis address (TRPC_TEST_REDIS_ADDR).
func RedisAddr() string { return getenv("TRPC_TEST_REDIS_ADDR", "localhost:6380") }

// PGDSN is the test PostgreSQL DSN (TRPC_TEST_PG_DSN).
func PGDSN() string {
	return getenv("TRPC_TEST_PG_DSN", "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
}

// S3Endpoint is the test MinIO endpoint (TRPC_TEST_S3_ENDPOINT).
func S3Endpoint() string { return getenv("TRPC_TEST_S3_ENDPOINT", "localhost:9000") }

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Redis connects for an integration test and registers cleanup; it skips with
// the enabling variable named when the server is unreachable.
func Redis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis unavailable (%v) — set TRPC_TEST_REDIS_ADDR (default %s), skipping integration test", err, RedisAddr())
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// PG connects for an integration test and registers cleanup; it skips with
// the enabling variable named when the server is unreachable.
func PG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, PGDSN())
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skipf("postgres unavailable (%v) — set TRPC_TEST_PG_DSN (default %s), skipping integration test", err, PGDSN())
	}
	t.Cleanup(pool.Close)
	return pool
}

// CaptureLogs redirects the service logger into a pipe at the given level and
// returns a function yielding everything written to it. plog builds its core
// against os.Stderr when Init runs, so the swap has to precede the Init;
// stopping restores os.Stderr and rebuilds the logger at the package default
// (info, console) so later tests are unaffected. The stop is idempotent and
// also registered as a cleanup: a test that fails before calling it must not
// leave the pipe installed as the process stderr.
func CaptureLogs(t *testing.T, level string) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	plog.Init(level, false)

	var (
		once sync.Once
		out  string
	)
	stop := func() string {
		once.Do(func() {
			plog.Sync()
			os.Stderr = old
			plog.Init("info", true)
			_ = w.Close()
			b, _ := io.ReadAll(r)
			_ = r.Close()
			out = string(b)
		})
		return out
	}
	t.Cleanup(func() { stop() })
	return stop
}
