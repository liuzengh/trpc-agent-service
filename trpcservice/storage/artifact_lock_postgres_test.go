package storage

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/artifact"

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
	// Use the actual composite key. A plain fixture string hid PostgreSQL's
	// rejection of the NUL delimiters in real user/session/artifact keys.
	key := router.artifactLockKey(artifact.SessionInfo{
		AppName: "t/test-tenant/a/test-app", UserID: "test-user", SessionID: "test-session",
	}, "att_test")
	var active atomic.Int64
	var maximum atomic.Int64
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := router.withDistributedLock(context.Background(), key, func() error {
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
