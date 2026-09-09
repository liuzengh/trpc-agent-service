// Package testinfra contains test-only infrastructure helpers. It is not used by
// production storage or serving code.
package testinfra

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type DockerLab struct {
	t          *testing.T
	mu         sync.Mutex
	owner      string
	prefix     string
	network    string
	redisID    string
	volume     string
	proxyID    string
	postgresID string
	password   string
	cleaned    bool
}

func NewDockerLab(t *testing.T) *DockerLab {
	t.Helper()
	lab := &DockerLab{t: t, owner: "caelumc", prefix: fmt.Sprintf("p006-fi-%d", time.Now().UnixNano())}
	lab.password = "p006_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	t.Cleanup(func() { lab.Cleanup() })
	return lab
}

func (l *DockerLab) run(ctx context.Context, args ...string) string {
	l.t.Helper()
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		l.t.Fatalf("docker operation failed: category=command_failed argc=%d", len(args))
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
		l.t.Skip("blocked: Docker is unavailable")
	}
	l.network = l.run(ctx, "network", "create", "--label", "p009.redis.owner="+l.owner, "--label", "p009.redis.run="+l.prefix, l.prefix+"-net")
	l.volume = l.run(ctx, "volume", "create", "--label", "p009.redis.owner="+l.owner, "--label", "p009.redis.run="+l.prefix, l.prefix+"-redis-data")
	l.redisID = l.run(ctx, "run", "-d", "--name", l.prefix+"-redis", "--label", "p009.redis.owner="+l.owner, "--label", "p009.redis.run="+l.prefix, "--network", l.network, "--network-alias", "redis", "-v", l.volume+":/data", "redis:7-alpine")
	l.proxyID = l.run(ctx, "run", "-d", "--name", l.prefix+"-redis-proxy", "--label", "p009.redis.owner="+l.owner, "--label", "p009.redis.run="+l.prefix, "--network", l.network, "-p", "127.0.0.1::6379", "alpine/socat", "TCP-LISTEN:6379,fork,reuseaddr", "TCP:redis:6379")
	l.postgresID = l.run(ctx, "run", "-d", "--name", l.prefix+"-postgres", "--label", "p009.redis.owner="+l.owner, "--label", "p009.redis.run="+l.prefix, "--network", l.network, "-e", "POSTGRES_USER=trpc_test", "-e", "POSTGRES_PASSWORD="+l.password, "-e", "POSTGRES_DB=trpc_agent_test", "-p", "127.0.0.1::5432", "postgres:16-alpine")
}
func (l *DockerLab) OwnershipVerified(ctx context.Context) bool {
	return l.ownerInspect(ctx, "container", l.redisID) && l.ownerInspect(ctx, "container", l.proxyID) && l.ownerInspect(ctx, "container", l.postgresID) && l.ownerInspect(ctx, "network", l.network) && l.ownerInspect(ctx, "volume", l.volume)
}
func (l *DockerLab) ownerInspect(ctx context.Context, kind, id string) bool {
	if id == "" {
		return false
	}
	format := "{{ index .Config.Labels \"p009.redis.owner\" }}"
	if kind != "container" {
		format = "{{ index .Labels \"p009.redis.owner\" }}"
	}
	cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", format, id)
	out, err := cmd.Output()
	return err == nil && strings.TrimSpace(string(out)) == l.owner
}
func (l *DockerLab) RemainingResources(ctx context.Context) bool {
	for _, args := range [][]string{{"ps", "-aq", "--filter", "label=p009.redis.owner=" + l.owner, "--filter", "label=p009.redis.run=" + l.prefix}, {"network", "ls", "-q", "--filter", "label=p009.redis.owner=" + l.owner, "--filter", "label=p009.redis.run=" + l.prefix}, {"volume", "ls", "-q", "--filter", "label=p009.redis.owner=" + l.owner, "--filter", "label=p009.redis.run=" + l.prefix}} {
		cmd := exec.CommandContext(ctx, "docker", args...)
		out, err := cmd.Output()
		if err != nil || strings.TrimSpace(string(out)) != "" {
			return true
		}
	}
	return false
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
		if l.try(ctx, "exec", l.redisID, "redis-cli", "ping") == nil && l.try(ctx, "inspect", "--format", "{{.State.Running}}", l.proxyID) == nil && l.try(ctx, "exec", l.postgresID, "pg_isready", "-U", "trpc_test", "-d", "trpc_agent_test") == nil && l.hostPortReachable(ctx, l.proxyID, "6379/tcp") && l.postgresHostHealthy(ctx) {
			return
		}
		if time.Now().After(deadline) {
			l.t.Fatal("isolated services did not become healthy: category=dependency_not_ready")
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
	ownershipVerified := l.OwnershipVerified(ctx)
	for _, id := range []string{l.proxyID, l.redisID, l.postgresID} {
		if id != "" && l.ownerInspect(ctx, "container", id) {
			_ = l.try(ctx, "rm", "-f", id)
		}
	}
	if l.volume != "" && l.ownerInspect(ctx, "volume", l.volume) {
		_ = l.try(ctx, "volume", "rm", l.volume)
	}
	if l.network != "" && l.ownerInspect(ctx, "network", l.network) {
		_ = l.try(ctx, "network", "rm", l.network)
	}
	l.t.Logf("redis_failover stage=cleanup category=owner_scoped_cleanup success=%t duration_ms=0 retries=0 ownership_verified=%t", !l.RemainingResources(ctx), ownershipVerified)
}

func (l *DockerLab) hostPortReachable(ctx context.Context, id, containerPort string) bool {
	command := exec.CommandContext(ctx, "docker", "port", id, containerPort)
	output, err := command.Output()
	value := string(output)
	if err != nil {
		return false
	}
	parts := strings.Split(strings.TrimSpace(value), ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil || port < 1 {
		return false
	}
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	connection, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	return connection.Close() == nil
}

func (l *DockerLab) postgresHostHealthy(ctx context.Context) bool {
	value, err := l.tryPort(ctx, l.postgresID, "5432/tcp")
	if err != nil {
		return false
	}
	parts := strings.Split(strings.TrimSpace(value), ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil || port < 1 {
		return false
	}
	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://trpc_test:%s@127.0.0.1:%d/trpc_agent_test?sslmode=disable", l.password, port))
	if err != nil {
		return false
	}
	config.ConnectTimeout = 500 * time.Millisecond
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return false
	}
	defer connection.Close(ctx)
	return connection.Ping(ctx) == nil
}

func (l *DockerLab) tryPort(ctx context.Context, id, containerPort string) (string, error) {
	command := exec.CommandContext(ctx, "docker", "port", id, containerPort)
	output, err := command.Output()
	return string(output), err
}
