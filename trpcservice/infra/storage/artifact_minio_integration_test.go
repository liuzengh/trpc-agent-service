//go:build integration

package storage

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// newMinioForTest starts a throwaway MinIO server and returns the artifact
// service bound to it.
func newMinioForTest(t *testing.T) *MinioArtifactService {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		Cmd:          []string{"server", "/data"},
		Env:          map[string]string{"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"},
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("API:").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("minio run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("minio port: %v", err)
	}
	endpoint := net.JoinHostPort("127.0.0.1", port.Port())

	svc, err := NewMinioArtifactService(ctx, endpoint, "minioadmin", "minioadmin", "test-artifacts", false)
	if err != nil {
		t.Fatalf("NewMinioArtifactService: %v", err)
	}
	return svc
}

func TestMinioArtifactVersionedLifecycle(t *testing.T) {
	ctx := context.Background()
	svc := newMinioForTest(t)
	si := artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s1"}

	// first save -> revision 0
	rev, err := svc.SaveArtifact(ctx, si, "report.md",
		&artifact.Artifact{Data: []byte("v0"), MimeType: "text/markdown"})
	if err != nil {
		t.Fatalf("save v0: %v", err)
	}
	if rev != 0 {
		t.Errorf("first revision = %d, want 0", rev)
	}
	// second save -> revision 1, previous version still there
	rev, err = svc.SaveArtifact(ctx, si, "report.md",
		&artifact.Artifact{Data: []byte("v1 content"), MimeType: "text/markdown"})
	if err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if rev != 1 {
		t.Errorf("second revision = %d, want 1", rev)
	}

	// load latest = v1
	latest, err := svc.LoadArtifact(ctx, si, "report.md", nil)
	if err != nil {
		t.Fatalf("load latest: %v", err)
	}
	if latest == nil || string(latest.Data) != "v1 content" {
		t.Fatalf("latest = %+v, want v1 content", latest)
	}
	if latest.MimeType != "text/markdown" {
		t.Errorf("mime = %q, want text/markdown", latest.MimeType)
	}

	// load an explicit old version = v0
	v0 := 0
	old, err := svc.LoadArtifact(ctx, si, "report.md", &v0)
	if err != nil {
		t.Fatalf("load v0: %v", err)
	}
	if old == nil || string(old.Data) != "v0" {
		t.Errorf("v0 = %+v, want v0", old)
	}

	// versions are contiguous and ordered
	versions, err := svc.ListVersions(ctx, si, "report.md")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Errorf("versions = %v, want [0 1]", versions)
	}

	// keys list the session file once
	keys, err := svc.ListArtifactKeys(ctx, si)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "report.md" {
		t.Errorf("keys = %v, want [report.md]", keys)
	}

	// missing artifact -> (nil, nil)
	missing, err := svc.LoadArtifact(ctx, si, "nope.txt", nil)
	if err != nil || missing != nil {
		t.Errorf("missing load = (%v, %v), want (nil, nil)", missing, err)
	}

	// delete wipes every revision
	if err := svc.DeleteArtifact(ctx, si, "report.md"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after, err := svc.LoadArtifact(ctx, si, "report.md", nil)
	if err != nil || after != nil {
		t.Errorf("load after delete = (%v, %v), want (nil, nil)", after, err)
	}
}

func TestMinioArtifactBinaryRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	svc := newMinioForTest(t)

	blob := bytes.Repeat([]byte{0x00, 0xff, 0x7b, 0x80}, 4096) // non-utf8 binary
	si := artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s1"}
	if _, err := svc.SaveArtifact(ctx, si, "plot.png", &artifact.Artifact{Data: blob, MimeType: "image/png"}); err != nil {
		t.Fatalf("save binary: %v", err)
	}
	got, err := svc.LoadArtifact(ctx, si, "plot.png", nil)
	if err != nil || got == nil {
		t.Fatalf("load binary: (%v, %v)", got, err)
	}
	if !bytes.Equal(got.Data, blob) {
		t.Error("binary artifact data corrupted in round trip")
	}

	// user-namespaced artifact (persistent across sessions)
	if _, err := svc.SaveArtifact(ctx, artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s1"},
		"user:avatar.png", &artifact.Artifact{Data: []byte("avatar"), MimeType: "image/png"}); err != nil {
		t.Fatalf("save user artifact: %v", err)
	}
	// a different session of the same user still lists the user namespace file
	otherSI := artifact.SessionInfo{AppName: "acme", UserID: "u1", SessionID: "s-other"}
	keys, err := svc.ListArtifactKeys(ctx, otherSI)
	if err != nil {
		t.Fatalf("list other session: %v", err)
	}
	if len(keys) != 1 || keys[0] != "user:avatar.png" {
		t.Errorf("other session keys = %v, want [user:avatar.png]", keys)
	}

	// tenant isolation: another tenant sees nothing
	otherTenant := artifact.SessionInfo{AppName: "rival", UserID: "u1", SessionID: "s1"}
	tKeys, err := svc.ListArtifactKeys(ctx, otherTenant)
	if err != nil {
		t.Fatalf("list other tenant: %v", err)
	}
	if len(tKeys) != 0 {
		t.Errorf("other tenant keys = %v, want none", tKeys)
	}
}

func TestMinioArtifactServiceValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := NewMinioArtifactService(ctx, "", "k", "s", "b", false); err == nil {
		t.Error("empty endpoint must error")
	}
	if _, err := NewMinioArtifactService(ctx, "localhost:9000", "", "s", "b", false); err == nil {
		t.Error("empty access key must error")
	}
	if _, err := NewMinioArtifactService(ctx, "localhost:9000", "k", "", "b", false); err == nil {
		t.Error("empty secret key must error")
	}
	if _, err := NewMinioArtifactService(ctx, "localhost:9000", "k", "s", "", false); err == nil {
		t.Error("empty bucket must error")
	}
}
