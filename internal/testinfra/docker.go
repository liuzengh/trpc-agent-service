// Package testinfra contains test-only infrastructure helpers. It is not used by
// production storage or serving code.
package testinfra

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type DockerLab struct {
	t          *testing.T
	mu         sync.Mutex
	prefix     string
	network    string
	redisID    string
	proxyID    string
	postgresID string
	password   string
	cleaned    bool
}

func NewDockerLab(t *testing.T) *DockerLab {
	t.Helper()
	lab := &DockerLab{t: t, prefix: fmt.Sprintf("p006-fi-%d", time.Now().UnixNano())}
	lab.password = "p006_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	t.Cleanup(func() { lab.Cleanup() })
	return lab
}

func (l *DockerLab) run(ctx context.Context, args ...string) string {
	l.t.Helper()
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		l.t.Fatalf("docker %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

func (l *DockerLab) try(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", args...)
	_, err := cmd.Output()
	return err
}

func (l *DockerLab) Start(ctx context.Context) {
	l.t.Helper()
	if err := l.try(ctx, "version"); err != nil {
		l.t.Skipf("blocked: Docker is unavailable: %v", err)
	}
	l.network = l.run(ctx, "network", "create", l.prefix+"-net")
	l.redisID = l.run(ctx, "run", "-d", "--name", l.prefix+"-redis", "--network", l.network, "--network-alias", "redis", "redis:7-alpine")
	l.proxyID = l.run(ctx, "run", "-d", "--name", l.prefix+"-redis-proxy", "--network", l.network, "-p", "127.0.0.1::6379", "alpine/socat", "TCP-LISTEN:6379,fork,reuseaddr", "TCP:redis:6379")
	l.postgresID = l.run(ctx, "run", "-d", "--name", l.prefix+"-postgres", "--network", l.network,
		"-e", "POSTGRES_USER=trpc_test", "-e", "POSTGRES_PASSWORD="+l.password, "-e", "POSTGRES_DB=trpc_agent_test",
		"-p", "127.0.0.1::5432", "postgres:16-alpine")
}

func (l *DockerLab) ContainerIDs() (redisID, postgresID string) { return l.redisID, l.postgresID }
func (l *DockerLab) ProxyID() string                            { return l.proxyID }
func (l *DockerLab) NetworkID() string                          { return l.network }

func (l *DockerLab) port(ctx context.Context, id, containerPort string) int {
	value := l.run(ctx, "port", id, containerPort)
	parts := strings.Split(value, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		l.t.Fatalf("invalid mapped port %q: %v", value, err)
	}
	return port
}

func (l *DockerLab) RedisURL(ctx context.Context) string {
	return fmt.Sprintf("redis://127.0.0.1:%d/15", l.port(ctx, l.proxyID, "6379/tcp"))
}
func (l *DockerLab) RedisManagementPing(ctx context.Context) error {
	return l.try(ctx, "exec", l.redisID, "redis-cli", "ping")
}
func (l *DockerLab) PostgresURL(ctx context.Context) string {
	return fmt.Sprintf("postgres://trpc_test:%s@127.0.0.1:%d/trpc_agent_test?sslmode=disable", l.password, l.port(ctx, l.postgresID, "5432/tcp"))
}

func (l *DockerLab) Healthy(ctx context.Context) {
	if err := l.try(ctx, "exec", l.redisID, "redis-cli", "ping"); err != nil {
		l.t.Fatalf("Redis is not healthy: %v", err)
	}
	if err := l.try(ctx, "exec", l.postgresID, "pg_isready", "-U", "trpc_test", "-d", "trpc_agent_test"); err != nil {
		l.t.Fatalf("PostgreSQL is not healthy: %v", err)
	}
}

func (l *DockerLab) WaitHealthy(ctx context.Context) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		if l.try(ctx, "exec", l.redisID, "redis-cli", "ping") == nil && l.try(ctx, "inspect", "--format", "{{.State.Running}}", l.proxyID) == nil && l.try(ctx, "exec", l.postgresID, "pg_isready", "-U", "trpc_test", "-d", "trpc_agent_test") == nil {
			return
		}
		if time.Now().After(deadline) {
			l.t.Fatalf("isolated services did not become healthy; redis logs:\n%s\npostgres logs:\n%s", l.LogsSummary(ctx, l.redisID), l.LogsSummary(ctx, l.postgresID))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (l *DockerLab) DisconnectRedis(ctx context.Context) {
	l.run(ctx, "network", "disconnect", l.network, l.redisID)
}
func (l *DockerLab) ReconnectRedis(ctx context.Context) {
	l.run(ctx, "network", "connect", "--alias", "redis", l.network, l.redisID)
}
func (l *DockerLab) StopRedis(ctx context.Context)       { l.run(ctx, "stop", l.redisID) }
func (l *DockerLab) RestartRedis(ctx context.Context)    { l.run(ctx, "start", l.redisID) }
func (l *DockerLab) StopPostgres(ctx context.Context)    { l.run(ctx, "stop", l.postgresID) }
func (l *DockerLab) RestartPostgres(ctx context.Context) { l.run(ctx, "start", l.postgresID) }

func (l *DockerLab) LogsSummary(ctx context.Context, id string) string {
	cmd := exec.CommandContext(ctx, "docker", "logs", "--tail", "40", id)
	out, _ := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if len(text) > 8192 {
		text = text[len(text)-8192:]
	}
	return text
}

func (l *DockerLab) Cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cleaned {
		return
	}
	l.cleaned = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, id := range []string{l.proxyID, l.redisID, l.postgresID} {
		if id != "" {
			_ = l.try(ctx, "rm", "-f", id)
		}
	}
	if l.network != "" {
		_ = l.try(ctx, "network", "rm", l.network)
	}
}
