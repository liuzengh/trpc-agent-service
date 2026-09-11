package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

// deliveryDatabase supplies two independent connection pools sharing one fresh
// schema. The fixture deliberately depends on no prospective Delivery API.
func deliveryDatabase(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DATABASE_URL for real PostgreSQL Delivery tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var id [8]byte
	if _, err = rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"gateway_delivery_" + hex.EncodeToString(id[:])}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	newPool := func() *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
		cfg.MaxConns = 20
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	first, second := newPool(), newPool()
	if err = migrations.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	return first, second
}

// waitDeliveryBlocked proves the competitor reached PostgreSQL's row/advisory
// lock, rather than assuming a goroutine has reached SQL after sleeping.
func waitDeliveryBlocked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blockerPID int32) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, blockerPID).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("competitor did not wait on the held database lock")
		case <-ticker.C:
		}
	}
}
