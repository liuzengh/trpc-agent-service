package backend

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/artifact"

	"github.com/cyl6/trpc-agent-service/migrations"
)

func TestPostgresObjectArtifactIntegrationConcurrentRevisionLedger(t *testing.T) {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok || strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not configured")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tenantID := "artifact-test-" + uuid.NewString()
	appName := tenantID + ":assistant"
	servicePool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewPostgresObjectArtifact(ctx, servicePool, newFakeBlobStore(), tenantID, appName, 0)
	if err != nil {
		servicePool.Close()
		t.Fatal(err)
	}
	defer service.Close()
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		cleanupTx, cleanupErr := admin.Begin(cleanupCtx)
		if cleanupErr != nil {
			return
		}
		defer func() { _ = cleanupTx.Rollback(cleanupCtx) }()
		_, _ = cleanupTx.Exec(cleanupCtx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID)
		_, _ = cleanupTx.Exec(cleanupCtx, `DELETE FROM artifact_blob_versions WHERE tenant_id=$1`, tenantID)
		_ = cleanupTx.Commit(cleanupCtx)
	})

	info := artifact.SessionInfo{AppName: appName, UserID: "user-a", SessionID: "session-a"}
	const count = 16
	versions := make(chan int, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			version, err := service.SaveArtifact(ctx, info, "parallel.bin", &artifact.Artifact{Data: []byte(fmt.Sprintf("blob-%d", i))})
			versions <- version
			errs <- err
		}(i)
	}
	wg.Wait()
	close(versions)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got := make([]int, 0, count)
	for version := range versions {
		got = append(got, version)
	}
	sort.Ints(got)
	for i := range got {
		if got[i] != i {
			t.Fatalf("database revisions = %v", got)
		}
	}
}
