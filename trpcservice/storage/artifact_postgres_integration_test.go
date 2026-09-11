package storage

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestPostgresArtifactServiceImplementsFrameworkVersionedScope(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer database.Close()

	service, err := NewPostgresArtifactService(database)
	if err != nil {
		t.Fatalf("NewPostgresArtifactService() error = %v", err)
	}
	ctx := context.Background()
	info := agentartifact.SessionInfo{AppName: "artifact-test/app", UserID: "user-1", SessionID: "session-1"}
	other := agentartifact.SessionInfo{AppName: "artifact-other/app", UserID: "user-1", SessionID: "session-1"}
	for _, tenantID := range []string{"artifact-test", "artifact-other"} {
		if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed tenant %q: %v", tenantID, err)
		}
		if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1, 'app', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed application %q: %v", tenantID, err)
		}
		if _, err := database.ExecContext(ctx, "DELETE FROM artifacts WHERE tenant_id=$1 AND app_code='app'", tenantID); err != nil {
			t.Fatalf("clear artifacts %q: %v", tenantID, err)
		}
	}

	version0, err := service.SaveArtifact(ctx, info, "report.txt", &agentartifact.Artifact{Data: []byte("v0"), MimeType: "text/plain", Name: "report"})
	if err != nil || version0 != 0 {
		t.Fatalf("SaveArtifact(v0) = %d, %v; want 0, nil", version0, err)
	}
	version1, err := service.SaveArtifact(ctx, info, "report.txt", &agentartifact.Artifact{Data: []byte("v1"), MimeType: "text/plain", Name: "report"})
	if err != nil || version1 != 1 {
		t.Fatalf("SaveArtifact(v1) = %d, %v; want 1, nil", version1, err)
	}
	if version, err := service.SaveArtifact(ctx, other, "report.txt", &agentartifact.Artifact{Data: []byte("other"), MimeType: "text/plain"}); err != nil || version != 0 {
		t.Fatalf("SaveArtifact(other tenant) = %d, %v; want 0, nil", version, err)
	}

	latest, err := service.LoadArtifact(ctx, info, "report.txt", nil)
	if err != nil || latest == nil || string(latest.Data) != "v1" || latest.MimeType != "text/plain" || latest.Name != "report" {
		t.Fatalf("LoadArtifact(latest) = %#v, %v", latest, err)
	}
	old, err := service.LoadArtifact(ctx, info, "report.txt", &version0)
	if err != nil || old == nil || string(old.Data) != "v0" {
		t.Fatalf("LoadArtifact(v0) = %#v, %v", old, err)
	}
	versions, err := service.ListVersions(ctx, info, "report.txt")
	if err != nil || !reflect.DeepEqual(versions, []int{0, 1}) {
		t.Fatalf("ListVersions() = %v, %v; want [0 1], nil", versions, err)
	}
	keys, err := service.ListArtifactKeys(ctx, info)
	if err != nil || !reflect.DeepEqual(keys, []string{"report.txt"}) {
		t.Fatalf("ListArtifactKeys() = %v, %v; want [report.txt], nil", keys, err)
	}
	otherLatest, err := service.LoadArtifact(ctx, other, "report.txt", nil)
	if err != nil || otherLatest == nil || string(otherLatest.Data) != "other" {
		t.Fatalf("LoadArtifact(other tenant) = %#v, %v", otherLatest, err)
	}

	if err := service.DeleteArtifact(ctx, info, "report.txt"); err != nil {
		t.Fatalf("DeleteArtifact() error = %v", err)
	}
	deleted, err := service.LoadArtifact(ctx, info, "report.txt", nil)
	if err != nil || deleted != nil {
		t.Fatalf("LoadArtifact(deleted) = %#v, %v; want nil, nil", deleted, err)
	}
}

func TestPostgresArtifactServiceSerializesConcurrentVersions(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer database.Close()
	service, err := NewPostgresArtifactService(database)
	if err != nil {
		t.Fatalf("NewPostgresArtifactService() error = %v", err)
	}
	ctx := context.Background()
	if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ('artifact-concurrent') ON CONFLICT (id) DO NOTHING"); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ('artifact-concurrent', 'app', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING"); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM artifacts WHERE tenant_id='artifact-concurrent' AND app_code='app'"); err != nil {
		t.Fatalf("clear artifacts: %v", err)
	}
	info := agentartifact.SessionInfo{AppName: "artifact-concurrent/app", UserID: "user-1", SessionID: "session-1"}
	const writers = 8
	versions := make(chan int, writers)
	errorsCh := make(chan error, writers)
	var group sync.WaitGroup
	for index := 0; index < writers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			version, err := service.SaveArtifact(ctx, info, "parallel.txt", &agentartifact.Artifact{Data: []byte("x"), MimeType: "text/plain"})
			if err != nil {
				errorsCh <- err
				return
			}
			versions <- version
		}()
	}
	group.Wait()
	close(versions)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent SaveArtifact() error = %v", err)
	}
	got := make([]int, 0, writers)
	for version := range versions {
		got = append(got, version)
	}
	sort.Ints(got)
	want := []int{0, 1, 2, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("concurrent versions = %v, want %v", got, want)
	}
}
