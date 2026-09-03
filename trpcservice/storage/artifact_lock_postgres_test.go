package storage

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestArtifactDistributedLockPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	router := &ArtifactRouter{lockDB: db}
	var active atomic.Int64
	var maximum atomic.Int64
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := router.withDistributedLock(context.Background(), "same-artifact", func() error {
				current := active.Add(1)
				for {
					previous := maximum.Load()
					if current <= previous || maximum.CompareAndSwap(previous, current) {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
				active.Add(-1)
				return nil
			}); err != nil {
				t.Errorf("distributed lock: %v", err)
			}
		}()
	}
	group.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent holders=%d", maximum.Load())
	}
}
