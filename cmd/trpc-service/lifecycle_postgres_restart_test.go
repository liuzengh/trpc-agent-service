package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	postgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

type gdPostgresContainer struct {
	prefix       string
	network      string
	container    string
	volume       string
	host         string
	password     string
	port         int
	url          string
	ownsNetwork  bool
	networkOwner string
}

func dockerGDAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").Run() == nil
}

func gdDockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("isolated G-D Docker operation failed")
	}
	return strings.TrimSpace(string(output))
}

func gdDockerTry(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func newGDPostgresContainer(t *testing.T) *gdPostgresContainer {
	t.Helper()
	if !dockerGDAvailable() {
		t.Skip("PostgreSQL restart evidence SKIP: Docker is unavailable")
	}
	prefix := "g-d-e-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	network := strings.TrimSpace(os.Getenv("G_D_POSTGRES_NETWORK"))
	networkOwner := strings.TrimSpace(os.Getenv("G_D_POSTGRES_OWNER"))
	ownsNetwork := network == ""
	if ownsNetwork {
		network = prefix + "-net"
		networkOwner = prefix
	}
	host := strings.TrimSpace(os.Getenv("G_D_POSTGRES_HOST"))
	if !ownsNetwork {
		if networkOwner == "" {
			t.Skip("PostgreSQL restart evidence SKIP: shared network owner is unavailable")
		}
		inspectedOwner, inspectErr := gdDockerTry("network", "inspect", "--format", "{{index .Labels \"g-d-e-owner\"}}", network)
		if inspectErr != nil || inspectedOwner != networkOwner {
			t.Skip("PostgreSQL restart evidence SKIP: shared network ownership is not provable")
		}
		host = "postgres"
	} else if host == "" {
		host = "127.0.0.1"
	}
	fixture := &gdPostgresContainer{prefix: prefix, network: network, container: prefix + "-postgres", volume: prefix + "-data", host: host, ownsNetwork: ownsNetwork, networkOwner: networkOwner}
	fixture.password = "p" + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	t.Cleanup(func() {
		owner, inspectErr := gdDockerTry("inspect", "--format", "{{index .Config.Labels \"g-d-e-owner\"}}", fixture.container)
		if inspectErr == nil && owner == fixture.prefix {
			_, _ = gdDockerTry("rm", "-f", fixture.container)
		}
		volumeOwner, volumeErr := gdDockerTry("volume", "inspect", "--format", "{{index .Labels \"g-d-e-owner\"}}", fixture.volume)
		if volumeErr == nil && volumeOwner == fixture.prefix {
			_, _ = gdDockerTry("volume", "rm", fixture.volume)
		}
		if fixture.ownsNetwork {
			networkOwner, networkErr := gdDockerTry("network", "inspect", "--format", "{{index .Labels \"g-d-e-owner\"}}", fixture.network)
			if networkErr == nil && networkOwner == fixture.networkOwner {
				_, _ = gdDockerTry("network", "rm", fixture.network)
			}
		}
	})
	if fixture.ownsNetwork {
		gdDockerCommand(t, "network", "create", "--label", "g-d-e-owner="+fixture.prefix, fixture.network)
	}
	gdDockerCommand(t, "volume", "create", "--label", "g-d-e-owner="+fixture.prefix, fixture.volume)
	runArgs := []string{"run", "-d", "--name", fixture.container, "--network", fixture.network, "--network-alias", "postgres", "--label", "g-d-e-owner=" + fixture.prefix,
		"-e", "POSTGRES_USER=gd_user", "-e", "POSTGRES_PASSWORD=" + fixture.password, "-e", "POSTGRES_DB=gd_db"}
	runArgs = append(runArgs, "--mount", "source="+fixture.volume+",target=/var/lib/postgresql/data", "postgres:16-alpine")
	if fixture.ownsNetwork {
		fixture.port = gdRunPostgresWithFixedLoopbackPort(t, runArgs)
	} else {
		gdDockerCommand(t, runArgs...)
	}
	owner, err := gdDockerTry("inspect", "--format", "{{index .Config.Labels \"g-d-e-owner\"}}", fixture.container)
	if err != nil || owner != fixture.prefix {
		t.Fatalf("G-D container ownership inspection failed")
	}
	networkLabel, err := gdDockerTry("network", "inspect", "--format", "{{index .Labels \"g-d-e-owner\"}}", fixture.network)
	if err != nil || networkLabel != fixture.networkOwner {
		t.Fatalf("G-D network ownership inspection failed")
	}
	volumeOwner, err := gdDockerTry("volume", "inspect", "--format", "{{index .Labels \"g-d-e-owner\"}}", fixture.volume)
	if err != nil || volumeOwner != fixture.prefix {
		t.Fatalf("G-D volume ownership inspection failed")
	}
	networkID := gdDockerCommand(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}", fixture.container)
	if networkID == "" {
		t.Fatalf("G-D container network attachment was not inspectable")
	}
	if !fixture.ownsNetwork {
		aliases, aliasErr := gdDockerTry("inspect", "--format", "{{range .NetworkSettings.Networks}}{{range .Aliases}}{{println .}}{{end}}{{end}}", fixture.container)
		if aliasErr != nil || !strings.Contains("\n"+aliases+"\n", "\npostgres\n") {
			t.Fatalf("G-D shared network postgres alias was not inspectable")
		}
		published, portErr := gdDockerTry("inspect", "--format", "{{json .NetworkSettings.Ports}}", fixture.container)
		if portErr != nil || strings.Contains(published, "HostPort") {
			t.Fatalf("G-D shared network unexpectedly published PostgreSQL port")
		}
	}
	if !fixture.ownsNetwork {
		fixture.port = 5432
	} else if fixture.port == 0 {
		t.Fatal("G-D fixed PostgreSQL loopback port was not assigned")
	}
	fixture.url = fmt.Sprintf("postgres://gd_user:%s@%s:%d/gd_db?sslmode=disable", fixture.password, fixture.host, fixture.port)
	return fixture
}

func gdRunPostgresWithFixedLoopbackPort(t *testing.T, runArgs []string) int {
	t.Helper()
	for attempt := 0; attempt < 5; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("G-D loopback port allocation failed")
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		args := append([]string(nil), runArgs[:len(runArgs)-1]...)
		args = append(args, "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), runArgs[len(runArgs)-1])
		if _, err := gdDockerTry(args...); err == nil {
			return port
		}
	}
	t.Fatalf("G-D PostgreSQL fixed loopback port could not be published")
	return 0
}

func (f *gdPostgresContainer) waitHealthy(t *testing.T) {
	t.Helper()
	gdWaitFor(t, 90*time.Second, func(context.Context) (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := exec.CommandContext(ctx, "docker", "exec", f.container, "pg_isready", "-U", "gd_user", "-d", "gd_db").Run()
		return err == nil, err
	})
}

func (f *gdPostgresContainer) stop(t *testing.T) {
	t.Helper()
	gdDockerCommand(t, "stop", "--time", "1", f.container)
}
func (f *gdPostgresContainer) start(t *testing.T) {
	t.Helper()
	gdDockerCommand(t, "start", f.container)
}

func (f *gdPostgresContainer) verifyRunnerTopology(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	backoff := 25 * time.Millisecond
	lastCategory := "not_started"
	for time.Now().Before(deadline) {
		ctx, cancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		reachable := true
		if !f.ownsNetwork {
			if _, err := net.DefaultResolver.LookupHost(ctx, f.host); err != nil {
				lastCategory = "dns_unavailable"
				reachable = false
			}
		}
		if reachable {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(f.host, strconv.Itoa(f.port)))
			if err != nil {
				lastCategory = "tcp_unreachable"
				reachable = false
			} else {
				_ = conn.Close()
			}
		}
		cancel()
		if reachable {
			return
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	if !f.ownsNetwork {
		t.Skipf("PostgreSQL restart evidence SKIP: shared-network topology category=%s", lastCategory)
	}
	t.Fatalf("G-D PostgreSQL owned endpoint category=%s", lastCategory)
}

func newGDDockerProductionFixture(t *testing.T, container *gdPostgresContainer) *productionTestFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var base *pgxpool.Pool
	deadline := time.Now().Add(30 * time.Second)
	backoff := 25 * time.Millisecond
	for time.Now().Before(deadline) {
		candidate, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: container.url, MaxConns: 4, MinConns: 1})
		if err == nil {
			base = candidate
			break
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	if base == nil {
		t.Fatalf("isolated G-D PostgreSQL did not become usable")
	}
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	fixture := &productionTestFixture{
		base:              base,
		schema:            "g_d_" + suffix,
		tenantID:          "g-d-tenant-" + suffix,
		agentID:           "g-d-agent-" + suffix,
		larkBindingID:     "g-d-lark-binding-" + suffix,
		telegramBindingID: "g-d-telegram-binding-" + suffix,
		larkExternalID:    "g-d-lark-external-" + suffix,
		telegramExternal:  "g-d-telegram-external-" + suffix,
		telegramUpdateID:  time.Now().UTC().UnixNano(),
		larkAppID:         "cli_g_d_" + suffix,
		ownerID:           "g-d-owner-" + suffix,
	}
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{fixture.schema}.Sanitize()); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	t.Setenv("TEST_DATABASE_URL", container.url)
	t.Setenv("DATABASE_URL", container.url)
	fixture.runtimeURL = ensureProductionRuntimeRole(t, container.url, fixture.schema)
	t.Setenv("DATABASE_RUNTIME_URL", fixture.runtimeURL)
	t.Setenv("DATABASE_SCHEMA", fixture.schema)
	t.Setenv("MIGRATIONS_DIR", filepath.Join(gdRepoRoot(t), "migrations"))
	t.Setenv("MODEL", "synthetic-model")
	t.Setenv("MODEL_CONFIG_REF", "synthetic-model-config")
	t.Setenv("TOOL_POLICY_REF", "synthetic-tool-policy")
	t.Setenv("ASYNC_OWNER_ID", fixture.ownerID)
	t.Setenv("BOOTSTRAP_TENANT_ID", fixture.tenantID)
	t.Setenv("BOOTSTRAP_TENANT_NAME", "synthetic tenant")
	t.Setenv("BOOTSTRAP_AGENT_APP_ID", fixture.agentID)
	t.Setenv("BOOTSTRAP_AGENT_NAME", "synthetic agent")
	t.Setenv("BOOTSTRAP_AGENT_VERSION", "1")
	t.Setenv("BOOTSTRAP_LARK_ENABLED", "true")
	t.Setenv("BOOTSTRAP_TELEGRAM_ENABLED", "true")
	t.Setenv("BOOTSTRAP_LARK_APP_ID", fixture.larkAppID)
	t.Setenv("BOOTSTRAP_LARK_BINDING_ID", fixture.larkBindingID)
	t.Setenv("BOOTSTRAP_LARK_EXTERNAL_APP_ID", fixture.larkExternalID)
	t.Setenv("BOOTSTRAP_LARK_RECEIVER_ID_TYPE", "chat_id")
	t.Setenv("BOOTSTRAP_TELEGRAM_BINDING_ID", fixture.telegramBindingID)
	t.Setenv("BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID", fixture.telegramExternal)
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_APP_SECRET_REF", "P009GC_LARK_APP_SECRET", productionSyntheticValue("a"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_VERIFY_TOKEN_REF", "P009GC_LARK_VERIFY_TOKEN", productionSyntheticValue("b"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_ENCRYPT_KEY_REF", "P009GC_LARK_ENCRYPT_KEY", productionSyntheticValue("c")) //gitleaks:allow synthetic test value is generated at runtime
	setProductionSyntheticSecret(t, "BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF", "P009GC_TELEGRAM_BOT_TOKEN", productionSyntheticValue("d"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF", "P009GC_TELEGRAM_WEBHOOK_SECRET", productionSyntheticValue("e"))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := base.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{fixture.schema}.Sanitize()+" CASCADE"); err != nil {
			base.Close()
			cancel()
			return
		}
		base.Close()
		cancel()
	})
	return fixture
}

func gdHTTPStatus(t *testing.T, addr, path string) (int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return 0, err
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

func gdWaitHTTPStatus(t *testing.T, addr, path string, status int, timeout time.Duration) {
	t.Helper()
	gdWaitFor(t, timeout, func(ctx context.Context) (bool, error) {
		got, err := gdHTTPStatus(t, addr, path)
		if err != nil {
			return false, err
		}
		if got != status {
			return false, fmt.Errorf("HTTP status=%d", got)
		}
		return true, nil
	})
}

var (
	errGDOutageWebhookFactsObserved = errors.New("outage webhook durable facts observed")
)

type gdRecoveryStage string

const (
	gdStagePoolConfig          gdRecoveryStage = "pool_config"
	gdStagePoolCreate          gdRecoveryStage = "pool_create"
	gdStagePoolPing            gdRecoveryStage = "pool_ping"
	gdStageSelectOne           gdRecoveryStage = "select_one"
	gdStageDeadline            gdRecoveryStage = "deadline"
	gdStageOutageWebhook       gdRecoveryStage = "outage_webhook"
	gdStageHealthz             gdRecoveryStage = "healthz"
	gdStageQueueOutboxRecovery gdRecoveryStage = "queue_outbox_recovery"
)

type gdRecoveryFailure struct {
	stage             gdRecoveryStage
	category          string
	cause             error
	deadlineExhausted bool
}

func (e *gdRecoveryFailure) Error() string {
	return fmt.Sprintf("PostgreSQL recovery stage=%s category=%s deadline_exhausted=%t", e.stage, e.category, e.deadlineExhausted)
}

func (e *gdRecoveryFailure) Unwrap() error { return e.cause }

func gdSafeErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "not_found"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "connection_reset"
	}
	if errors.Is(err, syscall.ECONNABORTED) {
		return "connection_aborted"
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return "connect_error"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "postgres_error"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "unexpected eof"):
		return "unexpected_eof"
	case strings.Contains(message, "connection refused"):
		return "connection_refused"
	case strings.Contains(message, "connection reset"):
		return "connection_reset"
	case strings.Contains(message, "connection aborted"):
		return "connection_aborted"
	default:
		return "other_error"
	}
}

func gdExpectedPostgresInterruption(err error) bool {
	if err == nil {
		return false
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	message := strings.ToLower(err.Error())
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) || strings.Contains(message, "unexpected eof") || strings.Contains(message, "connection refused") || strings.Contains(message, "connection reset") || strings.Contains(message, "connection aborted")
}

func gdLogRecoveryStage(t *testing.T, stage gdRecoveryStage, result, category string) {
	t.Helper()
	if category == "" {
		t.Logf("PostgreSQL recovery stage=%s result=%s", stage, result)
		return
	}
	t.Logf("PostgreSQL recovery stage=%s result=%s category=%s", stage, result, category)
}

func gdLogRecoveryProfile(t *testing.T, profile, searchPath, minConns, connectTimeout string) {
	t.Helper()
	t.Logf("PostgreSQL recovery profile=%s search_path=%s min_conns=%s connect_timeout=%s context_cancellation=enabled", profile, searchPath, minConns, connectTimeout)
}

func gdFailureForStage(stage gdRecoveryStage, err error) *gdRecoveryFailure {
	if err == nil {
		return &gdRecoveryFailure{stage: stage, category: "unknown"}
	}
	return &gdRecoveryFailure{stage: stage, category: gdSafeErrorCategory(err), cause: err}
}

func gdRecoveryAttemptContext(deadline time.Time) (context.Context, context.CancelFunc, bool) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, func() {}, false
	}
	attemptTimeout := 2 * time.Second
	if remaining < attemptTimeout {
		attemptTimeout = remaining
	}
	ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
	return ctx, cancel, true
}

func gdWaitRecoveryBackoff(deadline time.Time, backoff time.Duration) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	if backoff > remaining {
		backoff = remaining
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	<-timer.C
	return true
}

func gdPGXPoolNewAndPing(ctx context.Context, url string) (*pgxpool.Pool, *gdRecoveryFailure) {
	if _, err := pgxpool.ParseConfig(url); err != nil {
		return nil, gdFailureForStage(gdStagePoolConfig, err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, gdFailureForStage(gdStagePoolCreate, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, gdFailureForStage(gdStagePoolPing, err)
	}
	return pool, nil
}

func gdPostgresNewPool(ctx context.Context, url, searchPath string) (*pgxpool.Pool, *gdRecoveryFailure) {
	if _, err := pgxpool.ParseConfig(url); err != nil {
		return nil, gdFailureForStage(gdStagePoolConfig, err)
	}
	pool, err := postgres.NewPool(ctx, postgres.PostgresConfig{
		URL: url, SearchPath: searchPath, MaxConns: 4, MinConns: 1, ConnectTimeout: 2 * time.Second,
	})
	if err == nil {
		return pool, nil
	}
	// postgres.NewPool exposes safe, stable operation prefixes; only those
	// prefixes are used to identify its internal operation without retaining
	// or printing the wrapped connection error.
	stage := gdStagePoolCreate
	switch {
	case strings.HasPrefix(err.Error(), "postgres: invalid connection configuration:"):
		stage = gdStagePoolConfig
	case strings.HasPrefix(err.Error(), "postgres: ping:"):
		stage = gdStagePoolPing
	case strings.HasPrefix(err.Error(), "postgres: create pool:"):
		stage = gdStagePoolCreate
	}
	return nil, gdFailureForStage(stage, err)
}

func gdFreshPoolRecovers(t *testing.T, f *productionTestFixture, container *gdPostgresContainer, timeout time.Duration) (*pgxpool.Pool, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	backoff := 25 * time.Millisecond
	gdLogRecoveryProfile(t, "pgxpool.New", "none", "driver_default", "driver_default")
	gdLogRecoveryProfile(t, "postgres.NewPool", "none", "1", "2s")
	gdLogRecoveryProfile(t, "postgres.NewPool", "production_schema_present", "1", "2s")
	var lastFailure *gdRecoveryFailure
	for attempt := 0; attempt < 40 && time.Now().Before(deadline); attempt++ {
		ctx, cancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		candidate, failure := gdPGXPoolNewAndPing(ctx, container.url)
		cancel()
		if failure != nil {
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			if !gdExpectedPostgresInterruption(failure.cause) {
				return nil, failure
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			if backoff < 500*time.Millisecond {
				backoff *= 2
				if backoff > 500*time.Millisecond {
					backoff = 500 * time.Millisecond
				}
			}
			continue
		}
		gdLogRecoveryStage(t, gdStagePoolConfig, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolCreate, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolPing, "PASS", "")
		candidate.Close()

		ctx, cancel, ok = gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		candidate, failure = gdPostgresNewPool(ctx, container.url, "")
		cancel()
		if failure != nil {
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			if !gdExpectedPostgresInterruption(failure.cause) {
				return nil, failure
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			continue
		}
		gdLogRecoveryStage(t, gdStagePoolConfig, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolCreate, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolPing, "PASS", "")
		candidate.Close()

		ctx, cancel, ok = gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		candidate, failure = gdPostgresNewPool(ctx, container.url, f.schema)
		cancel()
		if failure != nil {
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			if !gdExpectedPostgresInterruption(failure.cause) {
				return nil, failure
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			continue
		}
		gdLogRecoveryStage(t, gdStagePoolConfig, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolCreate, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolPing, "PASS", "")
		queryCtx, queryCancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			candidate.Close()
			break
		}
		var value int
		queryErr := candidate.QueryRow(queryCtx, "SELECT 1").Scan(&value)
		queryCancel()
		if queryErr != nil {
			candidate.Close()
			failure = gdFailureForStage(gdStageSelectOne, queryErr)
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			if !gdExpectedPostgresInterruption(queryErr) {
				return nil, failure
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			continue
		}
		if value != 1 {
			candidate.Close()
			failure = &gdRecoveryFailure{stage: gdStageSelectOne, category: "unexpected_value"}
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			return nil, failure
		}
		gdLogRecoveryStage(t, gdStageSelectOne, "PASS", "")
		return candidate, nil
	}
	if lastFailure != nil {
		lastFailure.deadlineExhausted = true
		gdLogRecoveryStage(t, lastFailure.stage, "FAIL", lastFailure.category)
		gdLogRecoveryStage(t, gdStageDeadline, "EXHAUSTED", lastFailure.category)
		return nil, lastFailure
	}
	failure := &gdRecoveryFailure{stage: gdStageDeadline, category: "not_started", deadlineExhausted: true}
	gdLogRecoveryStage(t, failure.stage, "EXHAUSTED", failure.category)
	return nil, failure
}

func gdWaitHTTPStatusAfterRecovery(t *testing.T, addr, path string, status int, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	backoff := 25 * time.Millisecond
	lastCategory := "not_started"
	for time.Now().Before(deadline) {
		got, err := gdHTTPStatus(t, addr, path)
		if err == nil && got == status {
			gdLogRecoveryStage(t, gdStageHealthz, "PASS", "")
			return nil
		}
		if err != nil {
			lastCategory = gdSafeErrorCategory(err)
		} else {
			lastCategory = "unexpected_status"
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	failure := &gdRecoveryFailure{stage: gdStageHealthz, category: "deadline_exceeded", cause: errors.New(lastCategory)}
	gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
	return failure
}

func gdWaitQueueOutboxRecovery(t *testing.T, pool *pgxpool.Pool, f *productionTestFixture, jobID, executionID string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	backoff := 25 * time.Millisecond
	var queryCategory string
	var lastQueueStatus, lastOutboxStatus string
	var lastExecutions, lastOutboxes int
	for time.Now().Before(deadline) {
		ctx, cancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		var queueStatus, outboxStatus string
		var executions, outboxes int
		err := pool.QueryRow(ctx, "SELECT status FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&queueStatus)
		if err == nil {
			err = pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1 AND execution_id=$2", f.tenantID, executionID).Scan(&executions)
		}
		if err == nil {
			err = pool.QueryRow(ctx, "SELECT status FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, "reply-"+executionID).Scan(&outboxStatus)
		}
		if err == nil {
			err = pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1", f.tenantID).Scan(&outboxes)
		}
		cancel()
		lastQueueStatus, lastOutboxStatus = queueStatus, outboxStatus
		lastExecutions, lastOutboxes = executions, outboxes
		if err != nil {
			queryCategory = gdSafeErrorCategory(err)
		} else if queueStatus != "acked" || executions != 1 || outboxes != 1 || outboxStatus != "completed" {
			queryCategory = "durable_facts_not_ready"
		}
		if err == nil && queueStatus == "acked" && executions == 1 && outboxes == 1 && outboxStatus == "completed" {
			gdLogRecoveryStage(t, gdStageQueueOutboxRecovery, "PASS", "")
			return nil
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	t.Logf("PostgreSQL recovery queue_outbox_state queue_status=%s outbox_status=%s execution_count=%d outbox_count=%d category=%s", lastQueueStatus, lastOutboxStatus, lastExecutions, lastOutboxes, queryCategory)
	gdLogRecoveryStage(t, gdStageQueueOutboxRecovery, "FAIL", "deadline_exceeded")
	failure := &gdRecoveryFailure{stage: gdStageQueueOutboxRecovery, category: "deadline_exceeded"}
	gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
	return failure
}

type gdWebhookFactCounts struct {
	claims   int
	jobs     int
	results  int
	outboxes int
}

func gdReadWebhookFactCounts(ctx context.Context, pool *pgxpool.Pool, f *productionTestFixture, eventID, jobID, executionID string) (gdWebhookFactCounts, error) {
	var counts gdWebhookFactCounts
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "message_dedup")+" WHERE tenant_id=$1 AND external_message_id=$2", f.tenantID, eventID).Scan(&counts.claims); err != nil {
		return counts, err
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&counts.jobs); err != nil {
		return counts, err
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1 AND execution_id=$2", f.tenantID, executionID).Scan(&counts.results); err != nil {
		return counts, err
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND aggregate_id=$2", f.tenantID, executionID).Scan(&counts.outboxes); err != nil {
		return counts, err
	}
	return counts, nil
}

func gdObserveNoWebhookFacts(t *testing.T, pool *pgxpool.Pool, f *productionTestFixture, eventID, jobID, executionID string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	backoff := 25 * time.Millisecond
	for time.Now().Before(deadline) {
		ctx, cancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		counts, err := gdReadWebhookFactCounts(ctx, pool, f, eventID, jobID, executionID)
		cancel()
		if err != nil {
			return err
		}
		if counts.claims != 0 || counts.jobs != 0 || counts.results != 0 || counts.outboxes != 0 {
			return errGDOutageWebhookFactsObserved
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	return nil
}

func gdVerifyFreshPoolProfile(t *testing.T, profile, url, searchPath string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	backoff := 25 * time.Millisecond
	var lastFailure *gdRecoveryFailure
	for time.Now().Before(deadline) {
		gdLogRecoveryProfile(t, profile, searchPath, "1", "2s")
		ctx, cancel, ok := gdRecoveryAttemptContext(deadline)
		if !ok {
			break
		}
		var pool *pgxpool.Pool
		var failure *gdRecoveryFailure
		if profile == "pgxpool.New" {
			pool, failure = gdPGXPoolNewAndPing(ctx, url)
		} else {
			pool, failure = gdPostgresNewPool(ctx, url, searchPath)
		}
		cancel()
		if failure != nil {
			lastFailure = failure
			gdLogRecoveryStage(t, failure.stage, "FAIL", failure.category)
			if !gdExpectedPostgresInterruption(failure.cause) {
				t.Fatal(failure)
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			if backoff < 500*time.Millisecond {
				backoff *= 2
				if backoff > 500*time.Millisecond {
					backoff = 500 * time.Millisecond
				}
			}
			continue
		}
		gdLogRecoveryStage(t, gdStagePoolConfig, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolCreate, "PASS", "")
		gdLogRecoveryStage(t, gdStagePoolPing, "PASS", "")
		queryCtx, queryCancel, queryOK := gdRecoveryAttemptContext(deadline)
		if !queryOK {
			pool.Close()
			break
		}
		var value int
		queryErr := pool.QueryRow(queryCtx, "SELECT 1").Scan(&value)
		queryCancel()
		if queryErr != nil {
			pool.Close()
			lastFailure = gdFailureForStage(gdStageSelectOne, queryErr)
			gdLogRecoveryStage(t, lastFailure.stage, "FAIL", lastFailure.category)
			if !gdExpectedPostgresInterruption(queryErr) {
				t.Fatal(lastFailure)
			}
			if !gdWaitRecoveryBackoff(deadline, backoff) {
				break
			}
			continue
		}
		if value != 1 {
			pool.Close()
			t.Fatal(&gdRecoveryFailure{stage: gdStageSelectOne, category: "unexpected_value"})
		}
		gdLogRecoveryStage(t, gdStageSelectOne, "PASS", "")
		pool.Close()
		return
	}
	if lastFailure == nil {
		lastFailure = &gdRecoveryFailure{stage: gdStageDeadline, category: "not_started"}
	}
	lastFailure.deadlineExhausted = true
	gdLogRecoveryStage(t, lastFailure.stage, "FAIL", lastFailure.category)
	gdLogRecoveryStage(t, gdStageDeadline, "EXHAUSTED", lastFailure.category)
	t.Fatal(lastFailure)
}

func TestGDPostgreSQLHostMappedEndpointSurvivesRestart(t *testing.T) {
	container := newGDPostgresContainer(t)
	if !container.ownsNetwork {
		t.Skip("G-D host-mapped endpoint test SKIP: shared network uses alias-only topology")
	}
	container.waitHealthy(t)
	container.verifyRunnerTopology(t)
	gdVerifyFreshPoolProfile(t, "pgxpool.New", container.url, "")
	gdVerifyFreshPoolProfile(t, "postgres.NewPool", container.url, "")
	container.stop(t)
	container.start(t)
	container.waitHealthy(t)
	container.verifyRunnerTopology(t)
	gdVerifyFreshPoolProfile(t, "pgxpool.New", container.url, "")
	gdVerifyFreshPoolProfile(t, "postgres.NewPool", container.url, "")
}

func TestGDPostgreSQLOutageAndRestart(t *testing.T) {
	if strings.TrimSpace(os.Getenv("G_D_POSTGRES_NETWORK")) == "" {
		t.Skip("PostgreSQL restart evidence SKIP: shared-network/no-host-port runner topology is unavailable")
	}
	container := newGDPostgresContainer(t)
	container.waitHealthy(t)
	container.verifyRunnerTopology(t)
	f := newGDDockerProductionFixture(t, container)
	model := newGDModelServer(true)
	defer model.Close()
	provider := newGDProviderServer(false)
	defer provider.Close()
	binary := buildGDServiceBinary(t)
	pool := openGDSchemaPool(t, f)
	addr := gdFreeHTTPAddr(t)
	process := startGDProcess(t, binary, f, addr, model.server.URL, provider.server.URL, f.ownerID)
	defer stopGDProcess(t, process)
	gdWaitReady(t, process, addr, 30*time.Second)
	seedProductionRows(t, f, pool)

	eventID := "g-d-outage-event-" + f.tenantID
	body, timestamp, nonce := larkProductionEvent(t, f, eventID, "g-d-outage-message-"+f.tenantID)
	webhook := gdPostLarkWebhook(t, addr, f, body, timestamp, nonce)
	if !webhook.completeResponse || webhook.status != http.StatusAccepted {
		t.Fatalf("outage setup webhook status=%d complete_response=%t category=%s elapsed=%s", webhook.status, webhook.completeResponse, webhook.category, webhook.elapsed)
	}
	gdWaitCounter(t, &model.calls, 1, 15*time.Second)
	key := storage.DedupKey{TenantID: f.tenantID, Channel: lark.Channel, BindingID: f.larkBindingID, ExternalMessageID: eventID}
	jobID, executionID := gdIngressJobIdentity(key)
	inFlight := gdWaitQueueInFlight(t, pool, f, jobID)
	staleDelivery := queue.Delivery{
		DeliveryID: inFlight.deliveryID,
		Job: queue.AgentJob{
			JobID:  jobID,
			Tenant: queue.TenantContextDTO{TenantID: f.tenantID},
		},
	}

	container.stop(t)
	gdWaitHTTPStatus(t, addr, "/livez", http.StatusOK, 5*time.Second)
	gdWaitHTTPStatus(t, addr, "/healthz", http.StatusServiceUnavailable, 5*time.Second)
	newEventID := "g-d-outage-new-event-" + f.tenantID
	newBody, newTimestamp, newNonce := larkProductionEvent(t, f, newEventID, "g-d-outage-new-message-"+f.tenantID)
	newKey := storage.DedupKey{TenantID: f.tenantID, Channel: lark.Channel, BindingID: f.larkBindingID, ExternalMessageID: newEventID}
	newJobID, newExecutionID := gdIngressJobIdentity(newKey)
	outageWebhook := gdPostLarkWebhook(t, addr, f, newBody, newTimestamp, newNonce)
	if !outageWebhook.completeResponse {
		gdLogRecoveryStage(t, gdStageOutageWebhook, "FAIL", outageWebhook.category)
		model.Release()
		pool.Close()
		container.start(t)
		container.waitHealthy(t)
		diagnosticPool, recoveryErr := gdFreshPoolRecovers(t, f, container, 20*time.Second)
		if recoveryErr != nil {
			t.Fatalf("outage webhook category=%s had no terminal response; post-restart observation failed: %v", outageWebhook.category, recoveryErr)
		}
		defer diagnosticPool.Close()
		if observationErr := gdObserveNoWebhookFacts(t, diagnosticPool, f, newEventID, newJobID, newExecutionID, gdOutageWebhookDeadline); observationErr != nil {
			if errors.Is(observationErr, errGDOutageWebhookFactsObserved) {
				t.Fatalf("outage webhook created durable facts after client failure")
			}
			t.Fatalf("outage webhook category=%s had no terminal response; post-restart durable-fact observation failed category=%s", outageWebhook.category, gdSafeErrorCategory(observationErr))
		}
		t.Fatalf("outage webhook category=%s had no terminal response; no bounded post-restart durable-fact result", outageWebhook.category)
	}
	if outageWebhook.status != http.StatusServiceUnavailable {
		gdLogRecoveryStage(t, gdStageOutageWebhook, "FAIL", outageWebhook.category)
		t.Fatalf("database outage webhook status=%d category=%s elapsed=%s; recovery chain not continued", outageWebhook.status, outageWebhook.category, outageWebhook.elapsed)
	}
	gdLogRecoveryStage(t, gdStageOutageWebhook, "PASS", outageWebhook.category)

	pool.Close()
	container.start(t)
	container.waitHealthy(t)
	freshPool, err := gdFreshPoolRecovers(t, f, container, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool = freshPool
	t.Cleanup(pool.Close)
	if err := gdWaitHTTPStatusAfterRecovery(t, addr, "/healthz", http.StatusOK, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	tenantContext := queue.TenantContextDTO{TenantID: f.tenantID}
	recoveryQueue, err := queue.NewPostgresQueue(pool, queue.PostgresQueueConfig{PollInterval: 25 * time.Millisecond})
	if err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	gdWaitDeliveryExpired(t, pool, f, jobID)
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	reclaimed, err := recoveryQueue.Receive(recoveryCtx, 2*time.Second)
	recoveryCancel()
	workerReclaimed := false
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
		}
		stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, stateErr := gdReadQueueState(stateCtx, pool, f, jobID)
		stateCancel()
		if stateErr != nil {
			t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, stateErr))
		}
		t.Logf("PostgreSQL recovery queue_reclaim_fallback status=%s has_delivery=%t old_delivery=%t", state.status, state.deliveryID != "", state.deliveryID == staleDelivery.DeliveryID)
		if state.status != "in_flight" || state.deliveryID == "" || state.deliveryID == staleDelivery.DeliveryID {
			t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, fmt.Errorf("reclaim did not produce a new active delivery")))
		}
		reclaimed = queue.Delivery{
			DeliveryID: state.deliveryID,
			Job:        queue.AgentJob{JobID: jobID, Tenant: queue.TenantContextDTO{TenantID: f.tenantID}},
		}
		workerReclaimed = true
		t.Log("PostgreSQL recovery queue_reclaim_owner=production_worker")
	}
	if reclaimed.Job.Tenant.TenantID != tenantContext.TenantID || reclaimed.DeliveryID == staleDelivery.DeliveryID {
		t.Fatalf("reclaimed delivery did not change owner token")
	}
	fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 5*time.Second)
	fenceErr := recoveryQueue.Ack(fenceCtx, staleDelivery)
	fenceCancel()
	if !errors.Is(fenceErr, queue.ErrDeliveryFinished) && !errors.Is(fenceErr, queue.ErrDeliveryExpired) {
		t.Fatalf("stale delivery Ack was not fenced")
	}
	if !workerReclaimed {
		nackCtx, nackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		nackErr := recoveryQueue.Nack(nackCtx, reclaimed, queue.NackOptions{Requeue: true, Reason: "g-d-recovery-fence"})
		nackCancel()
		if nackErr != nil {
			t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, nackErr))
		}
	}
	gdLogRecoveryStage(t, gdStageQueueOutboxRecovery, "PASS", "stale_delivery_fenced")
	model.Release()
	if err := gdWaitQueueOutboxRecovery(t, pool, f, jobID, executionID, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	var outageClaims, outageJobs, outageResults, outageOutboxes int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "message_dedup")+" WHERE tenant_id=$1 AND external_message_id=$2", f.tenantID, newEventID).Scan(&outageClaims); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, newJobID).Scan(&outageJobs); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1 AND execution_id=$2", f.tenantID, newExecutionID).Scan(&outageResults); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND aggregate_id=$2", f.tenantID, newExecutionID).Scan(&outageOutboxes); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if outageClaims != 0 || outageJobs != 0 || outageResults != 0 || outageOutboxes != 0 {
		t.Fatalf("outage webhook created durable facts claims=%d jobs=%d results=%d outboxes=%d", outageClaims, outageJobs, outageResults, outageOutboxes)
	}
	var jobs, results, outboxes int
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1", f.tenantID).Scan(&jobs); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1", f.tenantID).Scan(&results); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1", f.tenantID).Scan(&outboxes); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	if jobs != 1 || results != 1 || outboxes != 1 || provider.calls.Load() != 1 {
		t.Fatalf("restart facts jobs=%d results=%d outboxes=%d provider_calls=%d", jobs, results, outboxes, provider.calls.Load())
	}
	var queueTenant, queueExecution, queueStatus string
	if err := pool.QueryRow(ctx, "SELECT tenant_id, execution_id, status FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&queueTenant, &queueExecution, &queueStatus); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	var resultTenant, resultJob, resultSession string
	if err := pool.QueryRow(ctx, "SELECT tenant_id, job_id, session_id FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1 AND execution_id=$2", f.tenantID, executionID).Scan(&resultTenant, &resultJob, &resultSession); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	var outboxTenant, outboxAggregate, payloadTenant, payloadJob, payloadExecution, payloadBinding, payloadChannel, payloadDestinationType, payloadDestination, payloadThread string
	if err := pool.QueryRow(ctx, "SELECT tenant_id, aggregate_id, payload->>'tenant_id', payload->>'job_id', payload->>'execution_id', payload->>'binding_id', payload->>'channel', payload->>'destination_type', payload->>'destination_id', COALESCE(payload->>'message_thread_id', '') FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, "reply-"+executionID).Scan(&outboxTenant, &outboxAggregate, &payloadTenant, &payloadJob, &payloadExecution, &payloadBinding, &payloadChannel, &payloadDestinationType, &payloadDestination, &payloadThread); err != nil {
		t.Fatal(gdFailureForStage(gdStageQueueOutboxRecovery, err))
	}
	expectedSession := channels.SessionID(f.tenantID, lark.Channel, "ou_"+f.tenantID, "oc_"+f.tenantID)
	if queueTenant != f.tenantID || queueExecution != executionID || queueStatus != "acked" || resultTenant != f.tenantID || resultJob != jobID || resultSession != expectedSession || outboxTenant != f.tenantID || outboxAggregate != executionID || payloadTenant != f.tenantID || payloadJob != jobID || payloadExecution != executionID || payloadBinding != f.larkBindingID || payloadChannel != lark.Channel || payloadDestinationType != channels.DestinationTypeChat || payloadDestination != "oc_"+f.tenantID || payloadThread != "" {
		t.Fatalf("restart durable identity validation failed")
	}
	stopGDProcess(t, process)
	gdAssertSafeOutput(t, process.output(), os.Getenv("TEST_DATABASE_URL"), os.Getenv("P009GC_LARK_APP_SECRET"), os.Getenv("P009GC_TELEGRAM_BOT_TOKEN"))
}
