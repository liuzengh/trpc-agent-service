//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// The P1-09 deployment gates exercise the real Docker/Compose lifecycle:
// config validation, image build and security properties, the standalone
// migration step, readiness/liveness semantics, bounded SIGTERM shutdown,
// SIGKILL restart recovery, dependency outage behavior and the migration
// mismatch fail-closed gate. Everything is owner-scoped with unique run ids
// and cleaned up unconditionally, including on failure.

const composePollInterval = 500 * time.Millisecond

func composeDockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker binary unavailable; deployment gates UNAVAILABLE")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "info", "--format", "ok").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ok") {
		t.Skip("docker daemon unavailable; deployment gates UNAVAILABLE")
	}
}

func composeRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func tailString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[len(value)-limit:]
}

var (
	composeImageMu  sync.Mutex
	composeImageTag string
)

// composeAppImage builds the release image once per test process and removes
// it on cleanup. The build is bounded; a failure fails the gate instead of
// being retried until success.
func composeAppImage(t *testing.T) string {
	t.Helper()
	composeDockerAvailable(t)
	composeImageMu.Lock()
	defer composeImageMu.Unlock()
	if composeImageTag != "" {
		return composeImageTag
	}
	tag := fmt.Sprintf("p109-build-%d:app", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "docker", "build", "-t", tag, composeRepoRoot(t))
	output, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("docker build failed: %v\n%s", err, tailString(string(output), 2000))
	}
	composeImageTag = tag
	return tag
}

// TestMain removes the shared image after all tests in the binary finish;
// per-test cleanup would delete it while later tests still reference the tag.
func TestMain(m *testing.M) {
	code := m.Run()
	if composeImageTag != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		_ = exec.CommandContext(ctx, "docker", "rmi", "-f", composeImageTag).Run()
		cancel()
	}
	os.Exit(code)
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free loopback port unavailable")
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

type composeRun struct {
	id      string
	project string
	dir     string
	root    string
	appPort int
	pgPort  int
	canary  string
}

// newComposeRun creates an owner-scoped per-run secret and env file outside
// the repository and registers unconditional teardown.
func newComposeRun(t *testing.T, image string) *composeRun {
	t.Helper()
	composeDockerAvailable(t)
	run := &composeRun{
		id:      fmt.Sprintf("%d", time.Now().UnixNano()),
		root:    composeRepoRoot(t),
		appPort: freeLocalPort(t),
		pgPort:  freeLocalPort(t),
		canary:  fmt.Sprintf("p109-canary-%d", time.Now().UnixNano()),
	}
	run.dir = filepath.Join(os.TempDir(), "p109-deploy-"+run.id)
	if err := os.MkdirAll(run.dir, 0o700); err != nil {
		t.Fatalf("run directory unavailable")
	}
	secretDir := filepath.Join(run.dir, "secrets")
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatalf("secret directory unavailable")
	}
	secret := filepath.Join(secretDir, "pg_password")
	if err := os.WriteFile(secret, []byte(run.canary), 0o600); err != nil {
		t.Fatalf("secret file unavailable")
	}
	runtimeSecret := filepath.Join(secretDir, "pg_runtime_password")
	if err := os.WriteFile(runtimeSecret, []byte(run.canary+"-runtime"), 0o600); err != nil {
		t.Fatalf("runtime secret file unavailable")
	}
	env := fmt.Sprintf("P109_RUN_ID=%s\nP109_SECRET_DIR=%s\nP109_APP_PORT=%d\nP109_PG_PORT=%d\nP109_APP_IMAGE=%s\n",
		run.id, secretDir, run.appPort, run.pgPort, image)
	if err := os.WriteFile(filepath.Join(run.dir, "env"), []byte(env), 0o600); err != nil {
		t.Fatalf("env file unavailable")
	}
	t.Cleanup(run.teardown)
	return run
}

func (r *composeRun) compose(t *testing.T, timeout time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "--project-directory", r.root, "--env-file", filepath.Join(r.dir, "env")}, args...)
	output, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v failed: %v\n%s", args, err, tailString(string(output), 2000))
	}
	return string(output)
}

func (r *composeRun) tryCompose(timeout time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "--project-directory", r.root, "--env-file", filepath.Join(r.dir, "env")}, args...)
	output, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	return string(output), err
}

// teardown brings the whole project down with volumes and verifies the
// owner-scoped resource set is empty. Cleanup must never panic the test
// binary, so leftovers are recorded on stderr instead of failing.
func (r *composeRun) teardown() {
	if r == nil || r.id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	args := []string{"compose", "--project-directory", r.root, "--env-file", filepath.Join(r.dir, "env"),
		"down", "-v", "--remove-orphans", "--timeout", "10"}
	_ = exec.CommandContext(ctx, "docker", args...).Run()
	for _, check := range [][]string{
		{"ps", "-aq", "--filter", "label=run_id=" + r.id},
		{"volume", "ls", "-q", "--filter", "label=run_id=" + r.id},
		{"network", "ls", "-q", "--filter", "label=run_id=" + r.id},
	} {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := exec.CommandContext(probeCtx, "docker", check...).CombinedOutput()
		probeCancel()
		if err != nil || strings.TrimSpace(string(out)) != "" {
			fmt.Fprintf(os.Stderr, "p109 teardown leftover: %v -> %s\n", check, strings.TrimSpace(string(out)))
		}
	}
}

func (r *composeRun) probeHealth(path string) int {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", r.appPort, path))
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func pollUntil(t *testing.T, deadline time.Duration, done func() bool) {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	tick := time.NewTicker(composePollInterval)
	defer tick.Stop()
	for {
		if done() {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("bounded deadline exceeded while polling deployment state")
		case <-tick.C:
		}
	}
}

func (r *composeRun) waitForHealth(t *testing.T, path string, want int, deadline time.Duration) {
	t.Helper()
	pollUntil(t, deadline, func() bool { return r.probeHealth(path) == want })
}

// waitForHealthAbsent proves the endpoint stops returning the expected ready
// status within the bounded window; any other code or connection failure
// counts as absent.
func (r *composeRun) waitForHealthAbsent(t *testing.T, path string, forbidden int, deadline time.Duration) {
	t.Helper()
	pollUntil(t, deadline, func() bool { return r.probeHealth(path) != forbidden })
}

func (r *composeRun) composeExec(t *testing.T, timeout time.Duration, service string, command ...string) string {
	t.Helper()
	args := append([]string{"exec", "-T", service}, command...)
	return r.compose(t, timeout, args...)
}

func (r *composeRun) psqlScalar(t *testing.T, sql string) string {
	t.Helper()
	out := r.composeExec(t, 60*time.Second, "postgres", "psql", "-U", "trpc_app", "-d", "trpc_agent", "-tAc", sql)
	return strings.TrimSpace(out)
}

func (r *composeRun) waitPostgresReady(t *testing.T, deadline time.Duration) {
	t.Helper()
	pollUntil(t, deadline, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		args := []string{"compose", "--project-directory", r.root, "--env-file", filepath.Join(r.dir, "env"),
			"exec", "-T", "postgres", "pg_isready", "-U", "trpc_app", "-d", "trpc_agent"}
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return err == nil && strings.Contains(string(out), "accepting connections")
	})
}

func (r *composeRun) serviceState(t *testing.T, service string) (string, int) {
	t.Helper()
	id := strings.TrimSpace(r.compose(t, 30*time.Second, "ps", "--all", "-q", service))
	if id == "" {
		return "absent", -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	state, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", id).CombinedOutput()
	if err != nil {
		return "unknown", -1
	}
	fields := strings.Fields(string(state))
	if len(fields) != 2 {
		return "unknown", -1
	}
	var code int
	if _, err := fmt.Sscanf(fields[1], "%d", &code); err != nil {
		return fields[0], -1
	}
	return fields[0], code
}

func (r *composeRun) waitServiceExited(t *testing.T, service string, deadline time.Duration) int {
	t.Helper()
	var exit int
	pollUntil(t, deadline, func() bool {
		status, code := r.serviceState(t, service)
		if status != "exited" {
			return false
		}
		exit = code
		return true
	})
	return exit
}

func (r *composeRun) serviceLogs(t *testing.T, service string) string {
	t.Helper()
	return r.compose(t, 60*time.Second, "logs", service)
}

func TestComposeConfigValidation(t *testing.T) {
	run := newComposeRun(t, "p109-deploy-app:local")
	output, err := run.tryCompose(90*time.Second, "config")
	if err != nil {
		t.Fatalf("compose config invalid: %v\n%s", err, tailString(output, 2000))
	}
	if strings.Contains(output, run.canary) {
		t.Fatal("rendered compose config leaked the secret value")
	}
	if strings.Contains(output, ":latest") {
		t.Fatal("compose config uses an unpinned latest image")
	}
	services := map[string]bool{}
	for _, name := range strings.Fields(run.compose(t, 60*time.Second, "config", "--services")) {
		services[name] = true
	}
	for _, want := range []string{"app", "migrate", "postgres", "redis"} {
		if !services[want] {
			t.Fatalf("default profile missing service %s", want)
		}
	}
	if services["collector"] {
		t.Fatal("collector must not be part of the default profile")
	}
	profiled := run.compose(t, 60*time.Second, "--profile", "telemetry", "config", "--services")
	if !strings.Contains(profiled, "collector") {
		t.Fatal("telemetry profile must add the local collector")
	}
	if !strings.Contains(output, "127.0.0.1:") {
		t.Fatalf("published ports are not loopback-only:\n%s", tailString(output, 1500))
	}
}

func TestComposeImageSecurity(t *testing.T) {
	image := composeAppImage(t)
	inspect := func(format string) string {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", format, image).CombinedOutput()
		if err != nil {
			t.Fatalf("image inspect failed: %v\n%s", err, tailString(string(out), 1000))
		}
		return string(out)
	}
	if user := strings.TrimSpace(inspect("{{.Config.User}}")); user == "" || user == "0" || user == "root" {
		t.Fatalf("runtime image must not run as root: user=%q", user)
	}
	if entrypoint := inspect("{{json .Config.Entrypoint}}"); !strings.Contains(entrypoint, "trpc-service") {
		t.Fatalf("unexpected entrypoint: %s", entrypoint)
	}
	// The runtime stage must not contain builder artifacts or env files, and
	// the P1-08 publication migration must ship with the release.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	check := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint", "sh", image,
		"-c", "test ! -e /src && test ! -d /usr/local/go && test ! -e /app/.env && test ! -e /home/app/.env && test -f /app/migrations/000009_p1_08_config_publication.up.sql")
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("runtime image content check failed: %v\n%s", err, tailString(string(out), 1000))
	}
	// Build history must not embed a secret assignment or the per-run canary.
	run := newComposeRun(t, image)
	historyCtx, historyCancel := context.WithTimeout(context.Background(), time.Minute)
	defer historyCancel()
	history, err := exec.CommandContext(historyCtx, "docker", "history", "--no-trunc", "--format", "{{.CreatedBy}}", image).CombinedOutput()
	if err != nil {
		t.Fatalf("image history unavailable: %v", err)
	}
	if strings.Contains(string(history), run.canary) || strings.Contains(string(history), "pg_password=") {
		t.Fatal("image history contains a secret")
	}
}

func TestComposeLifecycleAndRecovery(t *testing.T) {
	image := composeAppImage(t)
	run := newComposeRun(t, image)

	// 1. Dependency cold start with real health checks.
	run.compose(t, 2*time.Minute, "up", "-d", "postgres", "redis")
	run.waitPostgresReady(t, 2*time.Minute)
	pollUntil(t, time.Minute, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		args := []string{"compose", "--project-directory", run.root, "--env-file", filepath.Join(run.dir, "env"),
			"exec", "-T", "redis", "redis-cli", "ping"}
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return err == nil && strings.Contains(string(out), "PONG")
	})

	// 2. The standalone migration step applies versions 1..11 and exits 0.
	run.compose(t, 2*time.Minute, "up", "-d", "migrate")
	if exit := run.waitServiceExited(t, "migrate", 2*time.Minute); exit != 0 {
		t.Fatalf("migration step exit=%d", exit)
	}
	if logs := run.serviceLogs(t, "migrate"); !strings.Contains(logs, "current_version=11") {
		t.Fatalf("migration step did not reach version 11: %s", tailString(logs, 800))
	}

	// 3. The application becomes ready only after the migration succeeded.
	run.compose(t, 2*time.Minute, "up", "-d", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)
	if code := run.probeHealth("/livez"); code != 200 {
		t.Fatalf("livez status=%d want=200", code)
	}

	// 3b. The serving process must run as the non-root app user even though
	// the secret-reading wrapper starts as root.
	uid := strings.TrimSpace(run.composeExec(t, 30*time.Second, "app", "sh", "-c",
		"grep '^Uid' /proc/1/status | tr -s '\\t' ' ' | cut -d' ' -f2"))
	if uid == "" || uid == "0" {
		t.Fatalf("serving process must not run as root: uid=%q", uid)
	}

	// 4. Durable facts after a cold start.
	if got := run.psqlScalar(t, "SELECT count(*) FROM schema_migration"); got != "11" {
		t.Fatalf("schema_migration versions=%s want=11", got)
	}
	for _, table := range []string{"job_queue", "execution_result", "outbox_message", "tenant", "agent_app", "tenant_config_rollout", "tenant_config_operation"} {
		if got := run.psqlScalar(t, fmt.Sprintf("SELECT count(*) FROM information_schema.tables WHERE table_name='%s'", table)); got != "1" {
			t.Fatalf("durable table %s missing", table)
		}
	}
	tenantMarker := run.psqlScalar(t, "SELECT count(*) FROM tenant")
	if tenantMarker == "" {
		t.Fatalf("tenant marker unavailable")
	}

	// 5. SIGTERM triggers a bounded clean shutdown.
	stopStart := time.Now()
	run.compose(t, time.Minute, "stop", "--timeout", "30", "app")
	if elapsed := time.Since(stopStart); elapsed > 45*time.Second {
		t.Fatalf("SIGTERM shutdown exceeded the bounded window: %s", elapsed)
	}
	if status, exit := run.serviceState(t, "app"); status != "exited" || exit != 0 {
		t.Fatalf("post-SIGTERM state=%s exit=%d", status, exit)
	}
	if logs := run.serviceLogs(t, "app"); !strings.Contains(logs, "service stopped status=clean") {
		t.Fatalf("SIGTERM shutdown was not clean:\n%s", tailString(logs, 800))
	}

	// 6. Restart returns the deployment to ready.
	run.compose(t, time.Minute, "up", "-d", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)

	// 7. SIGKILL: docker kill deliberately bypasses the restart policy, so
	// the killed state is deterministic. The operator-level restart then
	// recovers the deployment and durable facts are unchanged. (A true
	// in-container crash restart via the restart policy is covered by the
	// migration-mismatch restart loop below, which exercises the same
	// unless-stopped policy.)
	appContainer := strings.TrimSpace(run.compose(t, 30*time.Second, "ps", "--all", "-q", "app"))
	if appContainer == "" {
		t.Fatalf("app container id unavailable")
	}
	killCtx, killCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer killCancel()
	if out, err := exec.CommandContext(killCtx, "docker", "kill", "--signal=KILL", appContainer).CombinedOutput(); err != nil {
		t.Fatalf("SIGKILL failed: %v\n%s", err, tailString(string(out), 500))
	}
	pollUntil(t, time.Minute, func() bool {
		status, code := run.serviceState(t, "app")
		if status != "exited" {
			return false
		}
		if code != 137 {
			t.Fatalf("SIGKILL exit=%d want=137", code)
		}
		return true
	})
	run.compose(t, time.Minute, "start", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)
	if got := run.psqlScalar(t, "SELECT count(*) FROM schema_migration"); got != "11" {
		t.Fatalf("schema_migration versions after SIGKILL=%s want=11", got)
	}
	if got := run.psqlScalar(t, "SELECT count(*) FROM tenant"); got != tenantMarker {
		t.Fatalf("tenant rows changed after SIGKILL: %s want %s", got, tenantMarker)
	}

	// 8. PostgreSQL outage: readiness fails closed, liveness stays
	// independent, and recovery returns the deployment to ready.
	run.compose(t, time.Minute, "stop", "postgres")
	run.waitForHealthAbsent(t, "/healthz", 200, 30*time.Second)
	if code := run.probeHealth("/livez"); code != 200 {
		t.Fatalf("livez must not depend on the dependency outage: status=%d", code)
	}
	run.compose(t, time.Minute, "start", "postgres")
	run.waitPostgresReady(t, 2*time.Minute)
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)

	// 9. Migration mismatch must fail closed end to end. The standalone
	// migration gate rejects the tampered checksum (stable exit code 4), so
	// compose refuses to bring the application up at all; if any application
	// process is already running it must never report ready.
	originalChecksum := run.psqlScalar(t, "SELECT checksum FROM schema_migration WHERE version=2")
	if originalChecksum == "" {
		t.Fatalf("original checksum unavailable")
	}
	run.psqlScalar(t, "UPDATE schema_migration SET checksum='p109-tampered-gate' WHERE version=2")
	run.compose(t, time.Minute, "stop", "app")
	output, upErr := run.tryCompose(2*time.Minute, "up", "-d", "--force-recreate", "app")
	if upErr == nil {
		pollUntil(t, 30*time.Second, func() bool {
			if run.probeHealth("/healthz") == 200 {
				t.Fatalf("application became ready during a migration checksum mismatch")
			}
			return strings.Contains(run.serviceLogs(t, "app"), "startup_failure")
		})
	} else if !strings.Contains(output, "didn't complete successfully") {
		t.Fatalf("unexpected compose failure during mismatch:\n%s", tailString(output, 800))
	}
	if logs := run.serviceLogs(t, "migrate"); !strings.Contains(logs, "category=checksum_mismatch") {
		t.Fatalf("migration gate did not reject the tampered checksum:\n%s", tailString(logs, 800))
	}
	run.psqlScalar(t, fmt.Sprintf("UPDATE schema_migration SET checksum='%s' WHERE version=2", originalChecksum))
	run.compose(t, 2*time.Minute, "up", "-d", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)

	// 10. Owner-scoped resources must be zero while the stack is down.
	run.compose(t, 2*time.Minute, "down", "-v", "--remove-orphans")
	for _, check := range [][]string{
		{"ps", "-aq", "--filter", "label=run_id=" + run.id},
		{"volume", "ls", "-q", "--filter", "label=run_id=" + run.id},
		{"network", "ls", "-q", "--filter", "label=run_id=" + run.id},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := exec.CommandContext(ctx, "docker", check...).CombinedOutput()
		cancel()
		if err != nil || strings.TrimSpace(string(out)) != "" {
			t.Fatalf("owner resources remain after teardown: %v -> %s", check, strings.TrimSpace(string(out)))
		}
	}
}
