package recovery_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestIsolatedKnowledgeMigration(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("isolated Docker checks disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	tag := fmt.Sprintf("knowledge-%x", time.Now().UnixNano())
	cid := docker(t, ctx, "run", "-d", "--pull=never", "--label", "trpc-agent.knowledge-test="+tag, "--tmpfs", "/qdrant/storage:rw", "-p", "127.0.0.1::6334", "qdrant/qdrant:v1.15.4")
	if !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(cid) {
		t.Fatal("invalid isolated container ID")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		label, err := exec.CommandContext(cleanup, "docker", "inspect", "--format", `{{index .Config.Labels "trpc-agent.knowledge-test"}}`, cid).Output()
		if err != nil || strings.TrimSpace(string(label)) != tag {
			t.Error("test ownership not verified; container retained")
			return
		}
		if err := exec.CommandContext(cleanup, "docker", "rm", "-f", cid).Run(); err != nil {
			t.Error("isolated vector cleanup failed")
		} else {
			t.Log("removed only owned synthetic Qdrant container and vectors")
		}
	})
	addr := docker(t, ctx, "port", cid, "6334/tcp")
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("test Qdrant must bind loopback")
	}
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("isolated Qdrant startup timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-race", "-count=1", "./trpcservice/storage", "-run", "TestKnowledge(MigrationQdrantIntegration|RouterRemoteQdrantIntegration)")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "TEST_QDRANT_HOST="+host, "TEST_QDRANT_PORT="+port, "TEST_POSTGRES_URL=", "TEST_REDIS_URL=", "TEST_S3_ENDPOINT=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated vector migration: %v\n%s", err, out)
	}
	t.Log(string(out))
}
