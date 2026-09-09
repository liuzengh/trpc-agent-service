//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// recoveryBinary builds the command once per test process.
func recoveryBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "trpc-recovery")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/trpc-recovery")
	build.Dir = repoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build trpc-recovery: %v\n%s", err, output)
	}
	return binary
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "../.."))
}

// runRecovery executes the binary with the given env and extra args and
// returns the exit code plus stdout/stderr.
func runRecovery(t *testing.T, binary string, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Args = append([]string{binary}, args...)
	cmd.Env = os.Environ()
	cmd.Dir = repoRoot(t)
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run trpc-recovery: %v", err)
		}
		exit = exitErr.ExitCode()
	}
	return exit, stdout.String(), stderr.String()
}

func clientImage(t *testing.T) string {
	t.Helper()
	image := "postgres:16-alpine"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	inspect := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image)
	out, err := inspect.Output()
	if err != nil {
		pull := exec.CommandContext(ctx, "docker", "pull", image)
		if output, pullErr := pull.CombinedOutput(); pullErr != nil {
			t.Fatalf("pull pinned client image: bytes=%d", len(output))
		}
		if out, err = inspect.Output(); err != nil {
			t.Fatalf("resolve client digest")
		}
	}
	digest := strings.TrimSpace(string(out))
	if !strings.Contains(digest, "@sha256:") {
		t.Fatalf("digest malformed")
	}
	return digest
}

// recoveryLabEnv boots the owner-scoped laboratory (source database with
// migrations and a provisioned runtime role) for the binary drills.
type recoveryLabEnv struct {
	ownerURL   string
	runtimeURL string
	ownerPool  *pgxpool.Pool
	image      string
	migrations string
}

// runtimeDSN rebuilds the DSN with the runtime role credentials.
func runtimeDSN(t *testing.T, ownerURL string) string {
	t.Helper()
	parsed, err := url.Parse(ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword("trpc_runtime", "p202-runtime-password")
	return parsed.String()
}

// seedTenantRow inserts a bare tenant row.
func seedTenantRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ($1,$2,'active',1)`, tenant, tenant); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
}

// seedBinaryIdentityRows inserts the FK parents (agent_app + binding).
func seedBinaryIdentityRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ($1,'bin-app','bin-app')`, tenant); err != nil {
		t.Fatalf("seed app: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ($1,'lark','bin-binding','bin-ext')`, tenant); err != nil {
		t.Fatalf("seed binding: %v", err)
	}
}

// seedBinaryFacts seeds one valid session event chain via the runtime role
// tenant protocol (same protocol as the production write path).
func seedBinaryFacts(ctx context.Context, t *testing.T, runtimeURL, tenant string) {
	t.Helper()
	pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: runtimeURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("runtime pool")
	}
	defer pool.Close()
	err = tenantctx.WithTenantContext(ctx, pool, tenant, "binary seed", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user, state, state_version, summary_version, last_event_seq)
			VALUES ($1,'bin-session','bin-app',1,'lark','bin-binding','bin-chat','bin-user','active',1,0,3)`, tenant); err != nil {
			return err
		}
		for i, eventType := range []string{"user.received", "agent.started", "assistant.completed"} {
			if _, err := tx.Exec(ctx, `INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, attempt, payload)
				VALUES ($1,'bin-session',$2,$3,$4,1,'{}'::jsonb)`, tenant, fmt.Sprintf("bin-evt-%d", i+1), i+1, eventType); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed facts: %v", err)
	}
}

// assertSafeOutput asserts that no DSN, no password and no tenant
// identifier leaked into stdout/stderr.
func assertSafeOutput(t *testing.T, output, ownerURL string) {
	t.Helper()
	forbidden := []string{"postgres://", "postgresql://", "trpc_test", "p202-runtime-password", "p202-bin-tenant", "p006_"}
	for _, needle := range forbidden {
		if strings.Contains(output, needle) {
			t.Fatalf("recovery output leaked sensitive content (needle length %d)", len(needle))
		}
	}
}

// createTargetDB creates an isolated target database with migrations and a
// runtime role, returning its DSN.
func createTargetDB(ctx context.Context, t *testing.T, ownerURL, migrationsDir string) string {
	t.Helper()
	name := fmt.Sprintf("p202_bin_target_%d", time.Now().UnixNano())
	cfg, err := pgx.ParseConfig(ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("bootstrap connection")
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{name}.Sanitize())); err != nil {
		conn.Close(ctx)
		t.Fatalf("create target: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	targetURL := strings.Replace(ownerURL, "/trpc_agent_test", "/"+name, 1)
	migrator, err := pgstore.NewMigrator(ctx, pgstore.PostgresConfig{URL: targetURL}, os.DirFS(migrationsDir))
	if err != nil {
		t.Fatal("target migrator")
	}
	if err := migrator.Up(ctx); err != nil {
		migrator.Close()
		t.Fatalf("target migrations: %v", err)
	}
	migrator.Close()
	admin, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: targetURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("target admin pool")
	}
	defer admin.Close()
	if err := pgstore.EnsureTenantRuntimeRole(ctx, admin, "public", "trpc_runtime", "p202-runtime-password"); err != nil {
		t.Fatalf("target runtime role: %v", err)
	}
	if err := pgstore.GrantRuntimeSchemaPrivileges(ctx, admin, "public", "trpc_runtime"); err != nil {
		t.Fatalf("target runtime grants: %v", err)
	}
	return targetURL
}

// TestRecoveryBinaryEndToEnd drives the actual trpc-recovery binary through
// backup -> verify -> restore -> replay against real PostgreSQL 16 with the
// pinned client image, then pins the failure exit codes and the
// no-secret-stderr contract.
func TestRecoveryBinaryEndToEnd(t *testing.T) {
	binary := recoveryBinary(t)
	image := clientImage(t)
	migrationsDir := filepath.Clean(filepath.Join(repoRoot(t), "migrations"))

	// Laboratory: fresh PostgreSQL 16 + migrations + runtime role.
	lab := testinfra.NewDockerLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	ownerURL := lab.PostgresURL(ctx)
	migrator, err := pgstore.NewMigrator(ctx, pgstore.PostgresConfig{URL: ownerURL}, os.DirFS(migrationsDir))
	if err != nil {
		t.Fatal("migrator")
	}
	if err := migrator.Up(ctx); err != nil {
		migrator.Close()
		t.Fatalf("migrations: %v", err)
	}
	migrator.Close()
	admin, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: ownerURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("admin pool")
	}
	defer admin.Close()
	if err := pgstore.EnsureTenantRuntimeRole(ctx, admin, "public", "trpc_runtime", "p202-runtime-password"); err != nil {
		t.Fatalf("runtime role: %v", err)
	}
	if err := pgstore.GrantRuntimeSchemaPrivileges(ctx, admin, "public", "trpc_runtime"); err != nil {
		t.Fatalf("runtime grants: %v", err)
	}
	runtimeURL := runtimeDSN(t, ownerURL)
	// Seed one tenant with a valid event chain through the tenant protocol.
	seedTenantRow(ctx, t, admin, "p202-bin-tenant")
	seedBinaryIdentityRows(ctx, t, admin, "p202-bin-tenant")
	seedBinaryFacts(ctx, t, runtimeURL, "p202-bin-tenant")

	outputDir := t.TempDir()
	base := map[string]string{
		"P202_SOURCE_URL":     ownerURL,
		"P202_OUTPUT_DIR":     outputDir,
		"P202_MIGRATIONS_DIR": migrationsDir,
		"P202_CLIENT_IMAGE":   image,
		"P202_TIMEOUT":        "180",
	}

	// 1. backup: exit 0, safe output.
	exit, stdout, stderr := runRecovery(t, binary, base, "backup")
	if exit != 0 {
		t.Fatalf("backup exit=%d stderr=%s", exit, stderr)
	}
	var backupSummary struct {
		BackupDir     string `json:"backup_dir"`
		ArchiveSHA    string `json:"archive_sha256"`
		RowsTotal     int64  `json:"rows_total"`
		PostgresMajor int    `json:"postgres_major"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &backupSummary); err != nil {
		t.Fatalf("backup summary parse: %v (%s)", err, stdout)
	}
	if backupSummary.BackupDir == "" || backupSummary.RowsTotal == 0 || backupSummary.PostgresMajor != 16 {
		t.Fatalf("backup summary incomplete: %s", stdout)
	}
	assertSafeOutput(t, stdout+stderr, ownerURL)

	// 2. verify: exit 0.
	verifyEnv := map[string]string{
		"P202_BACKUP_DIR":     backupSummary.BackupDir,
		"P202_MIGRATIONS_DIR": migrationsDir,
		"P202_CLIENT_IMAGE":   image,
	}
	exit, stdout, stderr = runRecovery(t, binary, verifyEnv, "verify")
	if exit != 0 {
		t.Fatalf("verify exit=%d stderr=%s", exit, stderr)
	}
	assertSafeOutput(t, stdout+stderr, ownerURL)

	// 3. restore into a fresh isolated target database: exit 0.
	targetURL := createTargetDB(ctx, t, ownerURL, migrationsDir)
	restoreEnv := map[string]string{
		"P202_BACKUP_DIR":     backupSummary.BackupDir,
		"P202_TARGET_URL":     targetURL,
		"P202_MIGRATIONS_DIR": migrationsDir,
		"P202_CLIENT_IMAGE":   image,
	}
	exit, stdout, stderr = runRecovery(t, binary, restoreEnv, "restore")
	if exit != 0 {
		t.Fatalf("restore exit=%d stderr=%s", exit, stderr)
	}
	assertSafeOutput(t, stdout+stderr, ownerURL)

	// 4. replay dry-run on the target: exit 0 with a digest.
	replayEnv := map[string]string{
		"P202_TARGET_URL":         targetURL,
		"P202_REPLAY_RUNTIME_URL": runtimeDSN(t, targetURL),
	}
	exit, stdout, stderr = runRecovery(t, binary, replayEnv, "replay")
	if exit != 0 {
		t.Fatalf("replay exit=%d stderr=%s", exit, stderr)
	}
	var replaySummary struct {
		Digest   string `json:"digest"`
		Sessions int64  `json:"sessions"`
		Events   int64  `json:"events"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &replaySummary); err != nil {
		t.Fatalf("replay summary parse: %v (%s)", err, stdout)
	}
	if len(replaySummary.Digest) != 64 || replaySummary.Events != 3 {
		t.Fatalf("replay summary unexpected: %s", stdout)
	}
	assertSafeOutput(t, stdout+stderr, ownerURL)

	// 5. repair without the operator gate: stable refusal.
	repairEnv := map[string]string{
		"P202_TARGET_URL":         targetURL,
		"P202_REPLAY_RUNTIME_URL": runtimeDSN(t, targetURL),
	}
	exit, _, stderr = runRecovery(t, binary, repairEnv, "replay", "--mode", "repair")
	if exit != 2 {
		t.Fatalf("ungated repair exit=%d stderr=%s", exit, stderr)
	}
	// 6. restore into the live source: refused.
	restoreEnv["P202_TARGET_URL"] = ownerURL
	exit, _, stderr = runRecovery(t, binary, restoreEnv, "restore")
	if exit != 5 {
		t.Fatalf("same-source restore exit=%d stderr=%s", exit, stderr)
	}
	// 7. unknown subcommand and missing config: stable exit 2.
	exit, _, _ = runRecovery(t, binary, map[string]string{}, "nonsense-subcommand")
	if exit != 2 {
		t.Fatalf("unknown subcommand exit=%d", exit)
	}
	exit, _, _ = runRecovery(t, binary, map[string]string{}, "verify")
	if exit != 2 {
		t.Fatalf("missing config exit=%d", exit)
	}
	// 8. backup against an unreachable database: exit 3.
	unreachable := map[string]string{
		"P202_SOURCE_URL":     "postgres://trpc_test:pw@127.0.0.1:1/none?sslmode=disable",
		"P202_OUTPUT_DIR":     t.TempDir(),
		"P202_MIGRATIONS_DIR": migrationsDir,
		"P202_CLIENT_IMAGE":   image,
		"P202_TIMEOUT":        "5",
	}
	exit, _, stderr = runRecovery(t, binary, unreachable, "backup")
	if exit != 3 {
		t.Fatalf("unreachable source exit=%d stderr=%s", exit, stderr)
	}
	// 9. host client (major 15) against server 16: refused, no archive.
	hostClientEnv := map[string]string{}
	for k, v := range base {
		hostClientEnv[k] = v
	}
	delete(hostClientEnv, "P202_CLIENT_IMAGE")
	exit, _, stderr = runRecovery(t, binary, hostClientEnv, "backup")
	if exit != 8 && exit != 3 {
		t.Fatalf("host client mismatch exit=%d stderr=%s", exit, stderr)
	}
}
