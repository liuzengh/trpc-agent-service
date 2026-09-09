package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/coordination"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
)

const serviceOutputLimit = 32 << 10

type boundedOutput struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		b.data = append(b.data, p[:remaining]...)
	}
	return len(p), nil
}

func (b *boundedOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

type serviceProcess struct {
	cmd    *exec.Cmd
	stdout boundedOutput
	stderr boundedOutput
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

func (p *serviceProcess) start() error {
	if err := p.cmd.Start(); err != nil {
		return err
	}
	go func() {
		err := p.cmd.Wait()
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	}()
	return nil
}

func (p *serviceProcess) waitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *serviceProcess) output() string {
	return p.stdout.String() + "\n" + p.stderr.String()
}

func (p *serviceProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	if p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			_ = p.cmd.Process.Kill()
		}
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Fatal("service process did not exit after kill")
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
}

func buildServiceBinary(t *testing.T, ctx context.Context) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "trpc-service-test")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/trpc-service")
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build cmd/trpc-service: %v\n%s", err, output)
	}
	return binary
}

func freeHTTPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// ensureP006RuntimeRole provisions the schema-scoped NOBYPASSRLS runtime role
// used by the real command gate. The owner connection only creates the role.
func ensureP006RuntimeRole(t *testing.T, databaseURL, schema string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := NewPool(ctx, PostgresConfig{URL: databaseURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatalf("runtime role admin pool: %v", err)
	}
	roleName := "trpc_rt_" + schema
	if len(roleName) > 60 {
		roleName = roleName[:60]
	}
	password := "p201rls" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := EnsureTenantRuntimeRole(ctx, admin, schema, roleName, password); err != nil {
		admin.Close()
		t.Fatalf("ensure runtime role: %v", err)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatalf("runtime role url: %v", err)
	}
	parsed.User = url.UserPassword(roleName, password)
	runtimeURL := parsed.String()
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName))
		admin.Close()
	})
	return runtimeURL
}

func startService(t *testing.T, ctx context.Context, binary, databaseURL, schema, migrationsDir string) *serviceProcess {
	t.Helper()
	process := &serviceProcess{
		cmd:    exec.CommandContext(ctx, binary),
		done:   make(chan struct{}),
		stdout: boundedOutput{limit: serviceOutputLimit},
		stderr: boundedOutput{limit: serviceOutputLimit},
	}
	process.cmd.Dir = repoRoot(t)
	runtimeURL := ensureP006RuntimeRole(t, databaseURL, schema)
	process.cmd.Env = append(os.Environ(),
		"DATABASE_URL="+databaseURL,
		"DATABASE_RUNTIME_URL="+runtimeURL,
		"DATABASE_SCHEMA="+schema,
		"MIGRATIONS_DIR="+migrationsDir,
		"HTTP_ADDR="+freeHTTPAddr(t),
		"DEFAULT_TENANT=p006-command",
	)
	process.cmd.Stdout = &process.stdout
	process.cmd.Stderr = &process.stderr
	if err := process.start(); err != nil {
		t.Fatalf("start cmd/trpc-service: %v", err)
	}
	return process
}

func waitForHealth(t *testing.T, process *serviceProcess, addr string, timeout time.Duration) (bool, error) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			return false, process.waitErr()
		default:
		}
		response, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return true, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false, context.DeadlineExceeded
}

func waitForExit(t *testing.T, process *serviceProcess, timeout time.Duration) (error, bool) {
	t.Helper()
	select {
	case <-process.done:
		return process.waitErr(), true
	case <-time.After(timeout):
		return nil, false
	}
}

func copyMigrations(t *testing.T, source string) string {
	t.Helper()
	destination := filepath.Join(t.TempDir(), "migrations")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(destination, entry.Name()), contents, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return destination
}

func quoteSchema(schema string) string { return pgx.Identifier{schema}.Sanitize() }

func createCommandSchema(t *testing.T, ctx context.Context, url, schema string) *pgxpool.Pool {
	t.Helper()
	admin, err := NewPool(ctx, PostgresConfig{URL: url, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoteSchema(schema)); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	admin.Close()
	schemaPool, err := NewPool(ctx, PostgresConfig{URL: url, SearchPath: schema, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	return schemaPool
}

func runReadyService(t *testing.T, ctx context.Context, binary, url, schema, migrations string) {
	t.Helper()
	addr := freeHTTPAddr(t)
	process := &serviceProcess{
		cmd:    exec.CommandContext(ctx, binary),
		done:   make(chan struct{}),
		stdout: boundedOutput{limit: serviceOutputLimit},
		stderr: boundedOutput{limit: serviceOutputLimit},
	}
	process.cmd.Dir = repoRoot(t)
	runtimeURL := ensureP006RuntimeRole(t, url, schema)
	process.cmd.Env = append(os.Environ(),
		"DATABASE_URL="+url, "DATABASE_RUNTIME_URL="+runtimeURL, "DATABASE_SCHEMA="+schema, "MIGRATIONS_DIR="+migrations, "HTTP_ADDR="+addr,
		// The service fails closed before serving unless the full production
		// bootstrap family is present. Channels stay disabled, so the secret
		// references only need to be well-formed and no network is dialed.
		"DEFAULT_TENANT=p006-command", "DEFAULT_AGENT_APP_ID=p006-agent",
		"MODEL_PROVIDER=runner", "MODEL=openai",
		"MODEL_BASE_URL=http://127.0.0.1:1", "MODEL_NAME=p006-model", "MODEL_API_KEY=p006-key", "MODEL_CONFIG_VERSION=1",
		"BOOTSTRAP_AGENT_VERSION=1",
		"BOOTSTRAP_TENANT_ID=p006-command", "BOOTSTRAP_TENANT_NAME=p006-command",
		"BOOTSTRAP_AGENT_APP_ID=p006-agent", "BOOTSTRAP_AGENT_NAME=p006-agent",
		"BOOTSTRAP_LARK_BINDING_ID=p006-lark", "BOOTSTRAP_LARK_EXTERNAL_APP_ID=p006-lark-app",
		"BOOTSTRAP_LARK_APP_ID=p006-lark-app-id", "BOOTSTRAP_LARK_RECEIVER_ID_TYPE=chat_id",
		"BOOTSTRAP_LARK_APP_SECRET_REF=env://P006_LARK_APP_SECRET",
		"BOOTSTRAP_LARK_VERIFY_TOKEN_REF=env://P006_LARK_VERIFY_TOKEN",
		"BOOTSTRAP_LARK_ENCRYPT_KEY_REF=env://P006_LARK_ENCRYPT_KEY",
		"BOOTSTRAP_TELEGRAM_BINDING_ID=p006-telegram", "BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID=p006-telegram-bot",
		"BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF=env://P006_TELEGRAM_TOKEN",
		"BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF=env://P006_TELEGRAM_HOOK",
		"ASYNC_OWNER_ID=p006-owner",
		"BOOTSTRAP_OBJECT_BACKEND=none",
	)
	process.cmd.Stdout = &process.stdout
	process.cmd.Stderr = &process.stderr
	if err := process.start(); err != nil {
		t.Fatal(err)
	}
	defer process.stop(t)
	ready, err := waitForHealth(t, process, addr, 30*time.Second)
	if err != nil || !ready {
		t.Fatalf("real command did not become ready: ready=%v err=%v output=%s", ready, err, process.output())
	}
}

func assertFailureOutputIsRedacted(t *testing.T, output, databaseURL string) {
	t.Helper()
	if strings.Contains(output, databaseURL) || strings.Contains(output, "trpc_test:") {
		t.Fatalf("startup output contains a complete or credential-bearing DSN: %q", output)
	}
	if strings.Contains(strings.ToLower(output), "123456") || strings.Contains(strings.ToLower(output), "token=") {
		t.Fatalf("startup output contains sensitive material: %q", output)
	}
}

func TestCommandFailsClosedOnMigrationMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	lab := testinfra.NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	binary := buildServiceBinary(t, ctx)
	source := filepath.Join(repoRoot(t), "migrations")
	databaseURL := lab.PostgresURL(ctx)

	tests := []struct {
		name          string
		prepare       func(t *testing.T, pool *pgxpool.Pool, schema, migrations string)
		prepareBefore bool
		shouldPass    bool
	}{
		{name: "normal migration", shouldPass: true},
		{name: "checksum mismatch", prepare: func(t *testing.T, pool *pgxpool.Pool, schema, migrations string) {
			contents, err := os.ReadFile(filepath.Join(migrations, "000002_coordination.up.sql"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(migrations, "000002_coordination.up.sql"), append(contents, []byte("\n-- checksum mismatch fixture\n")...), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unknown future version", prepare: func(t *testing.T, pool *pgxpool.Pool, schema, migrations string) {
			_, err := pool.Exec(context.Background(), "INSERT INTO schema_migration(version,name,checksum) VALUES($1,$2,$3)", int64(999), "future", "future-checksum")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing version", prepare: func(t *testing.T, pool *pgxpool.Pool, schema, migrations string) {
			if _, err := pool.Exec(context.Background(), "DELETE FROM schema_migration WHERE version=1"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "migration apply failure", prepareBefore: true, prepare: func(t *testing.T, pool *pgxpool.Pool, schema, migrations string) {
			if err := os.WriteFile(filepath.Join(migrations, "000002_coordination.up.sql"), []byte("THIS IS NOT VALID SQL;\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := fmt.Sprintf("p006_cmd_%d", time.Now().UnixNano())
			pool := createCommandSchema(t, ctx, databaseURL, schema)
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cleanupCancel()
				_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
				pool.Close()
			})
			migrations := copyMigrations(t, source)
			if test.prepareBefore && test.prepare != nil {
				test.prepare(t, pool, schema, migrations)
			}
			if !test.shouldPass && !test.prepareBefore {
				runReadyService(t, ctx, binary, databaseURL, schema, migrations)
			}
			if !test.prepareBefore && test.prepare != nil {
				test.prepare(t, pool, schema, migrations)
			}
			if test.shouldPass {
				runReadyService(t, ctx, binary, databaseURL, schema, migrations)
				return
			}
			addr := freeHTTPAddr(t)
			process := &serviceProcess{
				cmd: exec.CommandContext(ctx, binary), done: make(chan struct{}),
				stdout: boundedOutput{limit: serviceOutputLimit}, stderr: boundedOutput{limit: serviceOutputLimit},
			}
			process.cmd.Dir = repoRoot(t)
			runtimeURL := ensureP006RuntimeRole(t, databaseURL, schema)
			process.cmd.Env = append(os.Environ(), "DATABASE_URL="+databaseURL, "DATABASE_RUNTIME_URL="+runtimeURL, "DATABASE_SCHEMA="+schema, "MIGRATIONS_DIR="+migrations, "HTTP_ADDR="+addr)
			process.cmd.Stdout, process.cmd.Stderr = &process.stdout, &process.stderr
			if err := process.start(); err != nil {
				t.Fatal(err)
			}
			defer process.stop(t)
			ready, _ := waitForHealth(t, process, addr, 20*time.Second)
			if ready {
				t.Fatalf("migration failure unexpectedly exposed HTTP 200: %s", process.output())
			}
			err, exited := waitForExit(t, process, 20*time.Second)
			if !exited {
				process.stop(t)
				t.Fatalf("migration failure left the command running; output=%s", process.output())
			}
			if err == nil {
				t.Fatalf("migration failure exited successfully; output=%s", process.output())
			}
			assertFailureOutputIsRedacted(t, process.output(), databaseURL)
		})
	}
}

func postgresPrimaryFailover(f *dockerFailoverFixture, primaryClaims storage.ClaimStore, primaryLeases storage.LeaseStore, authority storage.EpochAuthority, secondary storage.LeaseStore, secondaryClaims storage.ClaimStore) *coordination.FailoverStore {
	return coordination.NewFailoverStoreWithAuthority(
		coordination.Endpoint{Name: storage.BackendPostgres, Claims: primaryClaims, Leases: primaryLeases},
		coordination.Endpoint{Name: storage.BackendRedis, Claims: secondaryClaims, Leases: secondary, Probe: f.backend.Ping},
		authority, f.tc.TenantID, f.resource,
		coordination.Config{FailureThreshold: 1, QuarantineWindow: 0, Now: time.Now},
	)
}

func TestRealFailoverStoreAfterPostgresRestart(t *testing.T) {
	f := newDockerFailoverFixture(t)
	primary := coordination.NewFailoverStoreWithAuthority(
		coordination.Endpoint{Name: storage.BackendPostgres, Claims: f.pg, Leases: f.pg},
		coordination.Endpoint{Name: storage.BackendRedis, Claims: f.redis, Leases: f.redis, Probe: f.backend.Ping},
		f.pg, f.tc.TenantID, f.resource, coordination.Config{FailureThreshold: 1, QuarantineWindow: 0, Now: time.Now},
	)
	if err := primary.Initialize(f.ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := primary.Acquire(f.ctx, f.tc, f.resource, "postgres-old-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.DedupKey{TenantID: f.tc.TenantID, Channel: f.tc.Channel, BindingID: f.tc.BindingID, ExternalMessageID: "postgres-restart-claim"}
	claim, err := primary.Claim(f.ctx, f.tc, key, 5*time.Second, "postgres-old-owner")
	if err != nil {
		t.Fatal(err)
	}
	oldGuard := storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}
	before := primary.Snapshot()
	if before.Epoch == 0 || before.ActiveBackend != storage.BackendPostgres {
		t.Fatalf("invalid pre-restart failover state: %+v", before)
	}

	f.lab.StopPostgres(f.ctx)
	failureCtx, failureCancel := context.WithTimeout(f.ctx, 8*time.Second)
	_, acquireErr := primary.Acquire(failureCtx, f.tc, f.resource, "failed-owner", time.Second)
	failureCancel()
	if acquireErr == nil || errors.Is(acquireErr, storage.ErrEpochRejected) || errors.Is(acquireErr, storage.ErrFenceRejected) {
		t.Fatalf("PostgreSQL outage was not classified as a backend failure: %v", acquireErr)
	}
	if primary.State() != coordination.Quarantined {
		t.Fatalf("PostgreSQL outage did not quarantine failover store: %+v", primary.Snapshot())
	}

	failureChecks := []struct {
		name string
		call func(context.Context) error
	}{
		{name: "get epoch", call: func(ctx context.Context) error { _, e := f.pg.GetEpoch(ctx, f.tc.TenantID, f.resource); return e }},
		{name: "bump epoch", call: func(ctx context.Context) error { _, e := f.pg.BumpEpoch(ctx, f.tc.TenantID, f.resource); return e }},
		{name: "lease renew", call: func(ctx context.Context) error { _, e := f.pg.Renew(ctx, f.tc, lease, time.Second); return e }},
		{name: "lease release", call: func(ctx context.Context) error { return f.pg.Release(ctx, f.tc, lease) }},
		{name: "lease validate", call: func(ctx context.Context) error { return f.pg.Validate(ctx, f.tc, lease) }},
		{name: "claim complete", call: func(ctx context.Context) error {
			return f.pg.Complete(ctx, f.tc, key, claim.OwnerID, "should-not-commit", oldGuard)
		}},
		{name: "claim fail", call: func(ctx context.Context) error { return f.pg.Fail(ctx, f.tc, key, claim.OwnerID, oldGuard, true) }},
	}
	for _, check := range failureChecks {
		check := check
		t.Run("outage/"+check.name, func(t *testing.T) {
			checkCtx, checkCancel := context.WithTimeout(f.ctx, 3*time.Second)
			defer checkCancel()
			if err := check.call(checkCtx); err == nil {
				t.Fatal("operation unexpectedly succeeded while PostgreSQL was stopped")
			}
		})
	}

	f.lab.RestartPostgres(f.ctx)
	f.lab.WaitHealthy(f.ctx)
	oldPoolRecovered := waitForPoolHealthy(f.pool, f.ctx, 15*time.Second)
	t.Logf("old pgxpool automatic recovery=%v", oldPoolRecovered)

	var authority storage.EpochAuthority = f.pg
	var recoveredPG *CoordinationStore = f.pg
	var recoveredPool *pgxpool.Pool
	if !oldPoolRecovered {
		cfg := PostgresConfig{URL: f.lab.PostgresURL(f.ctx), MaxConns: 8, MinConns: 1, SearchPath: f.schema, AllowDestructiveDown: true, ConnectTimeout: 500 * time.Millisecond}
		recoveredPool = newRecoveredPostgresPool(t, f.ctx, cfg, 20*time.Second)
		var err error
		t.Cleanup(recoveredPool.Close)
		recoveredPG, err = NewCoordinationStore(recoveredPool)
		if err != nil {
			t.Fatal(err)
		}
		authority = recoveredPG
	}
	checkMigrationMetadata(t, f.ctx, recoveredPoolOr(f.pool, recoveredPool), f.schema)

	var recovered *coordination.FailoverStore
	if oldPoolRecovered {
		recovered = primary
		recoveredEpoch, err := recovered.AutomaticSwitch(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if recoveredEpoch != before.Epoch+1 {
			t.Fatalf("recovery switch epoch=%d before=%d", recoveredEpoch, before.Epoch)
		}
	} else {
		// A pgxpool that does not reconnect is replaced explicitly. The new
		// authority is bumped once before the replacement store is created.
		recoveredEpoch, err := authority.BumpEpoch(f.ctx, f.tc.TenantID, f.resource)
		if err != nil {
			t.Fatal(err)
		}
		if recoveredEpoch != before.Epoch+1 {
			t.Fatalf("explicit recovery epoch=%d before=%d", recoveredEpoch, before.Epoch)
		}
		recoveredRedis := redisstore.NewStoreWithEpochAuthority(f.backend, authority)
		recovered = postgresPrimaryFailover(f, recoveredPG, recoveredPG, authority, recoveredRedis, recoveredRedis)
		if err := recovered.Initialize(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if recovered.Epoch() != before.Epoch+1 {
		t.Fatalf("recovered state regressed epoch: %+v", recovered.Snapshot())
	}
	if err := recovered.Complete(f.ctx, f.tc, key, claim.OwnerID, "stale", oldGuard); !errors.Is(err, storage.ErrEpochRejected) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old claim guard after recovery=%v", err)
	}
	if err := recovered.Validate(f.ctx, f.tc, lease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old lease after recovery=%v", err)
	}
	newLease, err := recovered.Acquire(f.ctx, f.tc, f.resource, "postgres-new-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch < lease.Epoch || (newLease.Epoch == lease.Epoch && newLease.FenceToken <= lease.FenceToken) {
		t.Fatalf("new lease did not advance fencing identity: old=%+v new=%+v", lease, newLease)
	}
	if err := recovered.Validate(f.ctx, f.tc, newLease); err != nil {
		t.Fatalf("new authority lease validation=%v", err)
	}
}

func newRecoveredPostgresPool(t *testing.T, ctx context.Context, cfg PostgresConfig, timeout time.Duration) *pgxpool.Pool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		pool, err := NewPool(ctx, cfg)
		if err == nil {
			return pool
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("recreated PostgreSQL pool did not become reachable: %v", lastErr)
	return nil
}

func waitForPoolHealthy(pool *pgxpool.Pool, ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func recoveredPoolOr(oldPool, recoveredPool *pgxpool.Pool) *pgxpool.Pool {
	if recoveredPool != nil {
		return recoveredPool
	}
	return oldPool
}

func checkMigrationMetadata(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	migrations, err := loadMigrations(os.DirFS(filepath.Join(repoRoot(t), "migrations")))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migration").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(migrations) {
		t.Fatalf("migration metadata count=%d, expected %d successful migrations", count, len(migrations))
	}
}

type countedEpochAuthority struct {
	storage.EpochAuthority
	bumps atomic.Int32
}

func (a *countedEpochAuthority) BumpEpoch(ctx context.Context, tenantID, resource string) (storage.Epoch, error) {
	a.bumps.Add(1)
	return a.EpochAuthority.BumpEpoch(ctx, tenantID, resource)
}

func (a *countedEpochAuthority) BumpCount() int { return int(a.bumps.Load()) }

type recoveryEvent struct {
	operation string
	err       error
	snapshot  coordination.Snapshot
	lease     storage.Lease
	claim     storage.Claim
}

type recoveryEvents struct {
	mu    sync.Mutex
	items []recoveryEvent
}

func (e *recoveryEvents) add(item recoveryEvent) {
	e.mu.Lock()
	e.items = append(e.items, item)
	e.mu.Unlock()
}

func (e *recoveryEvents) snapshot() []recoveryEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]recoveryEvent(nil), e.items...)
}

func TestRealFaultRecoveryNoDoubleOwner(t *testing.T) {
	f := newDockerFailoverFixture(t)
	authority := &countedEpochAuthority{EpochAuthority: f.pg}
	store := coordination.NewFailoverStoreWithAuthority(
		coordination.Endpoint{Name: storage.BackendRedis, Claims: f.redis, Leases: f.redis, Probe: f.backend.Ping},
		coordination.Endpoint{Name: storage.BackendPostgres, Claims: f.pg, Leases: f.pg},
		authority, f.tc.TenantID, f.resource,
		coordination.Config{FailureThreshold: 1, QuarantineWindow: 0, Now: time.Now},
	)
	if err := store.Initialize(f.ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(f.ctx, f.tc, f.resource, "old-primary-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.DedupKey{TenantID: f.tc.TenantID, Channel: f.tc.Channel, BindingID: f.tc.BindingID, ExternalMessageID: "fault-recovery-claim"}
	claim, err := store.Claim(f.ctx, f.tc, key, 5*time.Second, "old-primary-owner")
	if err != nil {
		t.Fatal(err)
	}
	oldGuard := storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}
	before := store.Snapshot()
	events := &recoveryEvents{}

	f.lab.DisconnectRedis(f.ctx)
	faultCtx, faultCancel := context.WithTimeout(f.ctx, 20*time.Second)
	var workers sync.WaitGroup
	for i, operation := range []string{"renew", "validate", "secondary-acquire", "claim-acquire", "complete", "fail"} {
		workers.Add(1)
		go func(i int, operation string) {
			defer workers.Done()
			for {
				select {
				case <-faultCtx.Done():
					return
				default:
				}
				callCtx, cancel := context.WithTimeout(faultCtx, 400*time.Millisecond)
				item := recoveryEvent{operation: fmt.Sprintf("fault/%d/%s", i, operation)}
				switch operation {
				case "renew":
					item.lease, item.err = store.Renew(callCtx, f.tc, lease, time.Second)
				case "validate":
					item.err = store.Validate(callCtx, f.tc, lease)
				case "secondary-acquire":
					item.lease, item.err = store.Acquire(callCtx, f.tc, f.resource, "secondary-contender", time.Second)
				case "claim-acquire":
					item.claim, item.err = store.Claim(callCtx, f.tc, key, time.Second, "fault-contender")
				case "complete":
					item.err = store.Complete(callCtx, f.tc, key, claim.OwnerID, "fault-write", oldGuard)
				case "fail":
					item.err = store.Fail(callCtx, f.tc, key, claim.OwnerID, oldGuard, true)
				}
				cancel()
				item.snapshot = store.Snapshot()
				events.add(item)
				time.Sleep(time.Duration(5+i) * time.Millisecond)
			}
		}(i, operation)
	}
	deadline := time.Now().Add(10 * time.Second)
	for store.State() != coordination.Quarantined && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if store.State() != coordination.Quarantined {
		faultCancel()
		workers.Wait()
		t.Fatalf("fault did not quarantine primary: %+v", store.Snapshot())
	}
	faultCancel()
	workers.Wait()

	var switchSuccesses atomic.Int32
	var switchWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		switchWG.Add(1)
		go func() {
			defer switchWG.Done()
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			epoch, switchErr := store.AutomaticSwitch(ctx)
			cancel()
			if switchErr == nil {
				switchSuccesses.Add(1)
			}
			events.add(recoveryEvent{operation: "switch", err: switchErr, snapshot: store.Snapshot(), lease: storage.Lease{Epoch: epoch}})
		}()
	}
	switchWG.Wait()
	if switchSuccesses.Load() != 1 || store.State() != coordination.ActiveSecondary {
		t.Fatalf("switch was not exactly once: successes=%d state=%+v", switchSuccesses.Load(), store.Snapshot())
	}
	secondaryLease, err := store.Acquire(f.ctx, f.tc, f.resource, "secondary-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryLease.Backend != storage.BackendPostgres || secondaryLease.Epoch != before.Epoch+1 || (secondaryLease.Epoch == lease.Epoch && secondaryLease.FenceToken <= lease.FenceToken) {
		t.Fatalf("secondary lease did not advance fencing identity: secondary=%+v old=%+v", secondaryLease, lease)
	}
	if err := store.Validate(f.ctx, f.tc, lease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old primary lease validation=%v", err)
	}

	f.lab.ReconnectRedis(f.ctx)
	f.lab.WaitHealthy(f.ctx)
	var probeSuccesses atomic.Int32
	var recoverSuccesses atomic.Int32
	var transitionWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		transitionWG.Add(1)
		go func() {
			defer transitionWG.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
				if probeErr := store.Probe(ctx); probeErr == nil {
					probeSuccesses.Add(1)
				}
				cancel()
				events.add(recoveryEvent{operation: "probe", snapshot: store.Snapshot()})
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		transitionWG.Add(1)
		go func() {
			defer transitionWG.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
				if _, recoverErr := store.Recover(ctx); recoverErr == nil {
					recoverSuccesses.Add(1)
				}
				cancel()
				events.add(recoveryEvent{operation: "recover", snapshot: store.Snapshot()})
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	transitionWG.Wait()
	if probeSuccesses.Load() != 1 || recoverSuccesses.Load() != 1 || store.State() != coordination.ActivePrimary {
		t.Fatalf("probe/recover was not exactly once: probes=%d recovers=%d state=%+v", probeSuccesses.Load(), recoverSuccesses.Load(), store.Snapshot())
	}
	if authority.BumpCount() != 2 {
		t.Fatalf("authority bump count=%d, expected exactly switch plus recovery", authority.BumpCount())
	}
	if store.Epoch() != before.Epoch+2 {
		t.Fatalf("epoch did not advance exactly twice: before=%d after=%d", before.Epoch, store.Epoch())
	}
	// Redis keeps the old physical lease until its TTL expires. Epoch fencing
	// makes it invalid for business writes immediately, while this wait ensures
	// a replacement Redis owner is not installed beside the stale key.
	if wait := time.Until(lease.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		select {
		case <-time.After(wait):
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
	newLease, err := store.Acquire(f.ctx, f.tc, f.resource, "recovered-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Backend != storage.BackendRedis || newLease.Epoch != before.Epoch+2 || (newLease.Epoch == secondaryLease.Epoch && newLease.FenceToken <= secondaryLease.FenceToken) {
		t.Fatalf("recovered lease did not advance fencing identity: recovered=%+v secondary=%+v", newLease, secondaryLease)
	}
	if err := store.Validate(f.ctx, f.tc, secondaryLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("secondary lease validation after recovery=%v", err)
	}
	if err := store.Complete(f.ctx, f.tc, key, claim.OwnerID, "stale", oldGuard); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old claim completion after recovery=%v", err)
	}

	for _, item := range events.snapshot() {
		if item.snapshot.Epoch < before.Epoch {
			t.Fatalf("epoch regressed in event %s: %+v", item.operation, item.snapshot)
		}
		if item.err != nil && (errors.Is(item.err, storage.ErrFenceRejected) && strings.Contains(item.operation, "fault/")) {
			t.Fatalf("fault operation was incorrectly classified as a fence rejection: %s: %v", item.operation, item.err)
		}
	}
	if owner, epoch, token := persistedPostgresLease(t, f.ctx, f.pool, f.schema, f.tc.TenantID, f.resource); owner != "secondary-owner" || epoch != before.Epoch+1 || token != secondaryLease.FenceToken {
		t.Fatalf("persisted PostgreSQL secondary owner=%s epoch=%d token=%d lease=%+v", owner, epoch, token, secondaryLease)
	}
}

func persistedPostgresLease(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema, tenantID, resource string) (string, storage.Epoch, uint64) {
	t.Helper()
	var owner string
	var epoch storage.Epoch
	var token uint64
	if err := pool.QueryRow(ctx, "SELECT owner_id,epoch,fencing_token FROM session_lease WHERE tenant_id=$1 AND session_id=$2", tenantID, resource).Scan(&owner, &epoch, &token); err != nil {
		t.Fatal(err)
	}
	return owner, epoch, token
}

var _ = atomic.Int32{}
