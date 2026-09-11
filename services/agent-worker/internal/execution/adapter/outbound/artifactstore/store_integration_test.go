package artifactstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/minio/minio-go/v7"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func fixture(t *testing.T) (*Store, *pgxpool.Pool, datav1.Snapshot, Credentials) {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("ARTIFACT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL and MinIO fixture required")
	}
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err = migrations.Apply(ctx, admin); err != nil {
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, "CREATE ROLE artifact_fixture_runtime LOGIN PASSWORD 'fixture-runtime-password'; GRANT USAGE ON SCHEMA public TO artifact_fixture_runtime; GRANT SELECT,INSERT,UPDATE,DELETE ON worker_artifact_files,worker_artifact_versions TO artifact_fixture_runtime")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = "artifact_fixture_runtime"
	cfg.ConnConfig.Password = "fixture-runtime-password"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "artifact", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 10000, MaxConcurrency: 8, MaxBytes: 4096}, S3: &datav1.S3Target{Endpoint: os.Getenv("ARTIFACT_TEST_S3_ENDPOINT"), Bucket: "artifact-fixture", Region: "us-east-1", PathStyle: true, Versioning: "disabled"}}
	secret := Credentials{AccessKeyID: "fixture-access-key", SecretAccessKey: "fixture-secret-key"}
	s, err := Open(ctx, pool, b, secret, "tenant", artifact.SessionInfo{AppName: "app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: b.S3.Region}); err != nil {
		t.Fatal(err)
	}
	return s, admin, b, secret
}
func TestArtifactS3Postgres(t *testing.T) {
	s, admin, b, secret := fixture(t)
	ctx := context.Background()
	scope := s.scope
	value := &artifact.Artifact{Data: []byte{0, 1, 2, 255}, MimeType: "application/octet-stream", Name: "binary", URL: "https://display.invalid/file"}
	if version, err := s.SaveArtifact(ctx, scope, "../binary", value); err != nil || version != 0 {
		t.Fatalf("save %d %v", version, err)
	}
	loaded, err := s.LoadArtifact(ctx, scope, "../binary", nil)
	if err != nil || !reflect.DeepEqual(loaded, value) {
		t.Fatalf("load %v", err)
	}
	loaded.Data[0] = 9
	again, err := s.LoadArtifact(ctx, scope, "../binary", nil)
	if err != nil || again.Data[0] != 0 {
		t.Fatalf("alias %v", err)
	}
	if versions, err := s.ListVersions(ctx, scope, "../binary"); err != nil || !reflect.DeepEqual(versions, []int{0}) {
		t.Fatal(versions, err)
	}
	var wg sync.WaitGroup
	got := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := s.SaveArtifact(ctx, scope, "../binary", value)
			if e != nil {
				t.Error(e)
				return
			}
			got <- v
		}()
	}
	wg.Wait()
	close(got)
	versions := []int{}
	for v := range got {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	if !reflect.DeepEqual(versions, []int{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatal(versions)
	}
	zero := 0
	if _, err = s.LoadArtifact(ctx, scope, "../binary", &zero); err != nil {
		t.Fatal(err)
	}
	wrong := scope
	wrong.UserID = "other"
	if _, err = s.LoadArtifact(ctx, wrong, "../binary", nil); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	for _, change := range []func(*datav1.Snapshot, *artifact.SessionInfo){
		func(b *datav1.Snapshot, _ *artifact.SessionInfo) { b.BackendRevision++ },
		func(b *datav1.Snapshot, _ *artifact.SessionInfo) { b.TenantID = "other" },
		func(_ *datav1.Snapshot, s *artifact.SessionInfo) { s.UserID = "other" },
		func(_ *datav1.Snapshot, s *artifact.SessionInfo) { s.AppName = "other" },
		func(_ *datav1.Snapshot, s *artifact.SessionInfo) { s.SessionID = "other" },
	} {
		other, sc := b, scope
		change(&other, &sc)
		isolated, e := Open(ctx, s.pool, other, secret, other.TenantID, sc)
		if e != nil {
			t.Fatal(e)
		}
		keys, e := isolated.ListArtifactKeys(ctx, sc)
		isolated.Close()
		if e != nil || len(keys) != 0 {
			t.Fatalf("isolation %v %v", keys, e)
		}
	}
	if _, err = Open(ctx, s.pool, b, secret, "wrong", scope); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	// Upload failure never produces visible metadata.
	bad := b
	target := *b.S3
	bad.S3 = &target
	bad.S3.Bucket = "missing-bucket"
	unavailable, err := Open(ctx, s.pool, bad, secret, "tenant", scope)
	if err != nil {
		t.Fatal(err)
	}
	defer unavailable.Close()
	if _, err = unavailable.SaveArtifact(ctx, scope, "failed", value); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if keys, err := unavailable.ListArtifactKeys(ctx, scope); err != nil || len(keys) != 0 {
		t.Fatal(keys, err)
	}
	// PG failure occurs after S3 upload; transaction rollback prevents success.
	if _, err = admin.Exec(ctx, "REVOKE INSERT ON worker_artifact_versions FROM artifact_fixture_runtime"); err != nil {
		t.Fatal(err)
	}
	_, saveErr := s.SaveArtifact(ctx, scope, "pg-failed", value)
	_, restoreErr := admin.Exec(ctx, "GRANT INSERT ON worker_artifact_versions TO artifact_fixture_runtime")
	if !errors.Is(saveErr, ErrUnavailable) || restoreErr != nil {
		t.Fatal(saveErr, restoreErr)
	}
	if got, err := s.LoadArtifact(ctx, scope, "pg-failed", nil); err != nil || got != nil {
		t.Fatal(got, err)
	}
	// The failed transaction did not consume the next successful version.
	if v, err := s.SaveArtifact(ctx, scope, "pg-failed", value); err != nil || v != 0 {
		t.Fatal(v, err)
	}
	// Content corruption is detected even when replacement length is identical.
	var key string
	if err = admin.QueryRow(ctx, "SELECT object_key FROM worker_artifact_versions WHERE file_id=$1 AND version=0", hash(s.scopeID, "pg-failed")).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.PutObject(ctx, s.bucket, key, bytes.NewReader([]byte{9, 9, 9, 9}), 4, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadArtifact(ctx, scope, "pg-failed", nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err = s.client.PutObject(ctx, s.bucket, key, strings.NewReader("short"), 5, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadArtifact(ctx, scope, "pg-failed", nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if err = s.DeleteArtifact(ctx, scope, "../binary"); err != nil {
		t.Fatal(err)
	}
	if v, err := s.ListVersions(ctx, scope, "../binary"); err != nil || len(v) != 0 {
		t.Fatal(v, err)
	}
	if out, err := s.LoadArtifact(ctx, scope, "../binary", &zero); err != nil || out != nil {
		t.Fatal(out, err)
	}
	if v, err := s.SaveArtifact(ctx, scope, "../binary", value); err != nil || v != 9 {
		t.Fatal(v, err)
	}
	if _, err = s.SaveArtifact(ctx, scope, "large", &artifact.Artifact{Data: make([]byte, 4097), MimeType: "text/plain"}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.SaveArtifact(canceled, scope, "cancel", value); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	deadline, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	if _, err = s.LoadArtifact(deadline, scope, "../binary", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err = s.DeleteArtifact(ctx, scope, "absent"); err != nil {
		t.Fatal(err)
	}
	// Real wire failures use a bounded fixed endpoint and never leak diagnostics.
	var requests atomic.Int32
	failureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte("fixture-secret-key"))
	}))
	targetFailure := b
	copyTarget := *b.S3
	targetFailure.S3 = &copyTarget
	targetFailure.S3.Endpoint = failureServer.URL
	failing, e := Open(ctx, s.pool, targetFailure, secret, "tenant", scope)
	if e != nil {
		t.Fatal(e)
	}
	_, e = failing.SaveArtifact(ctx, scope, "wire-fail", value)
	failing.Close()
	failureServer.Close()
	if !errors.Is(e, ErrUnavailable) || strings.Contains(e.Error(), secret.SecretAccessKey) || requests.Load() != 1 {
		t.Fatalf("redacted single request %v requests=%d", e, requests.Load())
	}
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	targetFailure.S3.Endpoint = blocked.URL
	targetFailure.Limits.TimeoutMS = 40
	timed, e := Open(ctx, s.pool, targetFailure, secret, "tenant", scope)
	if e != nil {
		t.Fatal(e)
	}
	started := time.Now()
	_, e = timed.SaveArtifact(ctx, scope, "timeout", value)
	timed.Close()
	blocked.Close()
	if !errors.Is(e, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatalf("timeout %v", e)
	}
	tlsServer := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted TLS reached handler") }))
	tlsServer.Config.ErrorLog = log.New(io.Discard, "", 0)
	tlsServer.StartTLS()
	targetFailure.S3.Endpoint = tlsServer.URL
	targetFailure.Limits.TimeoutMS = 10000
	untrusted, e := Open(ctx, s.pool, targetFailure, secret, "tenant", scope)
	if e != nil {
		t.Fatal(e)
	}
	_, e = untrusted.SaveArtifact(ctx, scope, "tls", value)
	untrusted.Close()
	tlsServer.Close()
	if !errors.Is(e, ErrUnavailable) {
		t.Fatalf("TLS trust %v", e)
	}
	s.Close()
	if _, err = s.ListArtifactKeys(ctx, scope); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	t.Log("ARTIFACT_STORE=PASS bytes=true versions=true scope=true backend_digest=true summary_independent=true s3_failure_no_metadata=true pg_failure_no_success=true logical_delete=true")
}
