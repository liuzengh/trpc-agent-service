package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
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
	publishHost := "127.0.0.1"
	if host != "127.0.0.1" && host != "localhost" {
		publishHost = "0.0.0.0"
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
	if fixture.ownsNetwork {
		runArgs = append(runArgs, "-p", publishHost+"::5432")
	}
	runArgs = append(runArgs, "--mount", "source="+fixture.volume+",target=/var/lib/postgresql/data", "postgres:16-alpine")
	gdDockerCommand(t, runArgs...)
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
		fixture.port = 5432
		fixture.url = fmt.Sprintf("postgres://gd_user:%s@%s:5432/gd_db?sslmode=disable", fixture.password, fixture.host)
		return fixture
	}
	mapped := gdDockerCommand(t, "port", fixture.container, "5432/tcp")
	port := 0
	for _, line := range strings.Split(mapped, "\n") {
		if index := strings.LastIndex(line, ":"); index >= 0 {
			if value, parseErr := strconv.Atoi(strings.TrimSpace(line[index+1:])); parseErr == nil {
				port = value
				break
			}
		}
	}
	if port == 0 {
		t.Fatalf("G-D PostgreSQL mapped port was not inspectable")
	}
	fixture.port = port
	fixture.url = fmt.Sprintf("postgres://gd_user:%s@%s:%d/gd_db?sslmode=disable", fixture.password, fixture.host, port)
	return fixture
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

func newGDDockerProductionFixture(t *testing.T, container *gdPostgresContainer) *productionTestFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var base *pgxpool.Pool
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		candidate, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: container.url, MaxConns: 4, MinConns: 1})
		if err == nil {
			base = candidate
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
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

var errGDPostgresReconnectUnavailable = errors.New("controlled PostgreSQL reconnect did not converge")

func gdExpectedPostgresInterruption(err error) bool {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) || strings.Contains(strings.ToLower(err.Error()), "unexpected eof")
}

func gdPoolRecovers(pool *pgxpool.Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	sawExpectedInterruption := false
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var value int
		err := pool.QueryRow(ctx, "SELECT 1").Scan(&value)
		cancel()
		if err == nil && value == 1 {
			return nil
		}
		if err != nil && !gdExpectedPostgresInterruption(err) {
			chain := make([]string, 0, 4)
			for current := err; current != nil && len(chain) < cap(chain); current = errors.Unwrap(current) {
				chain = append(chain, fmt.Sprintf("%T", current))
			}
			return fmt.Errorf("PostgreSQL reconnect returned an unexpected error: %s", strings.Join(chain, " -> "))
		}
		if err != nil {
			sawExpectedInterruption = true
		}
		remaining := time.Until(deadline)
		if remaining > 100*time.Millisecond {
			remaining = 100 * time.Millisecond
		}
		timer := time.NewTimer(remaining)
		<-timer.C
	}
	if sawExpectedInterruption {
		return errGDPostgresReconnectUnavailable
	}
	return errors.New("PostgreSQL reconnect did not become ready")
}

func TestGDPostgreSQLOutageAndRestart(t *testing.T) {
	container := newGDPostgresContainer(t)
	container.waitHealthy(t)
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
	if status := gdPostLarkWebhook(t, addr, f, body, timestamp, nonce); status != http.StatusAccepted {
		t.Fatalf("outage setup webhook status=%d", status)
	}
	gdWaitCounter(t, &model.calls, 1, 15*time.Second)
	key := storage.DedupKey{TenantID: f.tenantID, Channel: lark.Channel, BindingID: f.larkBindingID, ExternalMessageID: eventID}
	jobID, executionID := gdIngressJobIdentity(key)
	gdWaitQueueInFlight(t, pool, f, jobID)

	container.stop(t)
	gdWaitHTTPStatus(t, addr, "/livez", http.StatusOK, 5*time.Second)
	gdWaitHTTPStatus(t, addr, "/healthz", http.StatusServiceUnavailable, 5*time.Second)
	newBody, newTimestamp, newNonce := larkProductionEvent(t, f, "g-d-outage-new-event-"+f.tenantID, "g-d-outage-new-message-"+f.tenantID)
	if status := gdPostLarkWebhook(t, addr, f, newBody, newTimestamp, newNonce); status == http.StatusAccepted {
		t.Fatal("database outage webhook was accepted")
	}

	model.Release()
	pool.Reset()
	container.start(t)
	container.waitHealthy(t)
	if err := gdPoolRecovers(pool, 20*time.Second); err != nil {
		if errors.Is(err, errGDPostgresReconnectUnavailable) {
			t.Skip("PostgreSQL restart evidence SKIP: controlled reconnect returned unexpected EOF")
		}
		t.Fatal(err)
	}
	gdWaitHTTPStatus(t, addr, "/healthz", http.StatusOK, 20*time.Second)
	gdWaitQueueCompletion(t, pool, f, jobID, executionID)
	var jobs, results, outboxes int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1", f.tenantID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1", f.tenantID).Scan(&results); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1", f.tenantID).Scan(&outboxes); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || results != 1 || outboxes != 1 || provider.calls.Load() != 1 {
		t.Fatalf("restart facts jobs=%d results=%d outboxes=%d provider_calls=%d", jobs, results, outboxes, provider.calls.Load())
	}
	stopGDProcess(t, process)
	gdAssertSafeOutput(t, process.output(), os.Getenv("TEST_DATABASE_URL"), os.Getenv("P009GC_LARK_APP_SECRET"), os.Getenv("P009GC_TELEGRAM_BOT_TOKEN"))
}
