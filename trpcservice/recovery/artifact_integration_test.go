package recovery_test

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The integration target is created here with tmpfs and a random loopback
// port. Do not read .env or use the user's Compose MinIO/buckets/credentials.
func TestIsolatedArtifactS3Contracts(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("TEST_RECOVERY_DOCKER not enabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	tag := "artifact-" + time.Now().UTC().Format("20060102T150405.000000000")
	cid := docker(t, ctx, "run", "-d", "--pull=never", "--label", "trpc-agent.artifact-test="+tag,
		"--tmpfs", "/data:rw", "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin", "minio/minio:RELEASE.2025-07-23T15-54-02Z", "server", "/data")
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(cid) {
		t.Fatal("invalid isolated artifact container ID")
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		label, err := exec.CommandContext(cleanupCtx, "docker", "inspect", "--format", `{{index .Config.Labels "trpc-agent.artifact-test"}}`, cid).Output()
		if err != nil || strings.TrimSpace(string(label)) != tag {
			t.Error("cannot verify container ownership; retained for inspection")
			return
		}
		if err := exec.CommandContext(cleanupCtx, "docker", "rm", "-f", cid).Run(); err != nil {
			t.Error("cannot remove owned artifact test container")
		} else {
			t.Log("removed only the owned synthetic MinIO container and objects")
		}
	})
	addr := docker(t, ctx, "port", cid, "9000/tcp")
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("isolated MinIO must bind loopback")
	}
	endpoint := "http://" + addr
	client := &http.Client{Timeout: time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/minio/health/ready", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("isolated MinIO startup timed out")
		case <-time.After(100 * time.Millisecond):
		}
	}
	s3client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")})
	if _, err := s3client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("trpc-agent-artifacts")}); err != nil {
		t.Fatal("cannot create isolated artifact fixture bucket: ", err)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-race", "-count=1", "./trpcservice/storage", "-run", "^TestArtifactRouterS3Integration$")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "TEST_S3_ENDPOINT="+endpoint, "TEST_RECOVERY_DOCKER=0", "TEST_POSTGRES_URL=", "TEST_REDIS_URL=", "TEST_QDRANT_HOST=")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated artifact contracts failed: %v\n%s", err, output)
	}
	t.Log("artifact S3 save, reopen, immutable retry and session boundary verified")
}
