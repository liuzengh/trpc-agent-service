// Command postgres-migration-test runs the destructive migration matrix only
// against a random database that it creates and drops itself.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		return fmt.Errorf("refusing: set TRPC_MIGRATION_TEST=1")
	}
	adminDSN := os.Getenv("TRPC_POSTGRES_ADMIN_DSN")
	if adminDSN == "" {
		return fmt.Errorf("TRPC_POSTGRES_ADMIN_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return err
	}
	defer admin.Close()
	var major int
	if err := admin.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int/10000`).Scan(&major); err != nil {
		return err
	}
	if major != 16 {
		return fmt.Errorf("PostgreSQL 16 is required, found %d", major)
	}
	databaseName, err := randomDatabaseName()
	if err != nil {
		return err
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+databaseName+`"`); err != nil {
		return fmt.Errorf("create test database: %w", err)
	}
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+databaseName+`" WITH (FORCE)`)
	}()
	testDSN, err := testDSNForDatabase(adminDSN, databaseName)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	runner := migrations.NewRunner(db)
	probes, err := migrations.ContractProbes()
	if err != nil {
		return err
	}
	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err != nil {
		return fmt.Errorf("run from repository root: %w", err)
	}
	if err := verifyAcceptanceBaselinePlan(ctx, runner); err != nil {
		return fmt.Errorf("acceptance baseline plan: %w", err)
	}
	if err := verifyUp(ctx, runner, db, probes, repoRoot, testDSN); err != nil {
		return err
	}
	if err := cleanupAuditRetentionTestRole(ctx, db); err != nil {
		return fmt.Errorf("clean audit retention test role: %w", err)
	}
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	empty, err := runner.PublicSchemaEmpty(ctx)
	if err != nil {
		return err
	}
	if !empty {
		return fmt.Errorf("public schema is not empty after down")
	}
	if err := verifyUp(ctx, runner, db, probes, repoRoot, testDSN); err != nil {
		return fmt.Errorf("up-again: %w", err)
	}
	if err := cleanupAuditRetentionTestRole(ctx, db); err != nil {
		return fmt.Errorf("clean audit retention test role after replay: %w", err)
	}
	if err := runArtifactComposeE2E(ctx, repoRoot, testDSN); err != nil {
		return err
	}
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	fmt.Printf("PostgreSQL 16 migration matrix passed for %s\n", databaseName)
	return nil
}

// runArtifactComposeE2E composes the still-alive disposable migrated database
// with real MinIO. It is opt-in (TRPC_ARTIFACT_E2E=1) because the storage
// dependencies only exist in the smoke image. Running it here — instead of as
// a separate `go test` from the smoke shell — is what makes it real: the test
// requires TRPC_POSTGRES_TEST_DSN, which only this command can supply for a
// database that does not exist yet at script start. Without this wiring the
// test silently skipped in every backend-adapter run. When coverage
// attribution is enabled (TRPC_ARTIFACT_E2E_COVERAGE=1) the execution is
// measured against the artifact/object-store adapter seam and gated by
// TRPC_MIN_ARTIFACT_E2E_COVERAGE.
func runArtifactComposeE2E(ctx context.Context, repoRoot, testDSN string) error {
	if os.Getenv("TRPC_ARTIFACT_E2E") != "1" {
		return nil
	}
	arguments := []string{"-run", "^TestComposeArtifactObjectStoreTenantIsolation$"}
	coverageFile := ""
	if os.Getenv("TRPC_ARTIFACT_E2E_COVERAGE") == "1" {
		file, err := os.CreateTemp("", "trpc-artifact-e2e-coverage.XXXXXX")
		if err != nil {
			return fmt.Errorf("create artifact e2e coverage file: %w", err)
		}
		coverageFile = file.Name()
		if err := file.Close(); err != nil {
			return fmt.Errorf("close artifact e2e coverage file: %w", err)
		}
		if os.Getenv("TRPC_ARTIFACT_COVERAGE_OUT") == "" {
			defer os.Remove(coverageFile)
		}
		arguments = append(arguments,
			"-covermode=atomic",
			"-coverpkg="+strings.Join(artifactCoveragePackages(), ","),
			"-coverprofile="+coverageFile,
		)
	}
	arguments = append(arguments, "./trpcservice/integration")
	output, err := goTestWithoutSkips(ctx, repoRoot, append(os.Environ(), "TRPC_POSTGRES_TEST_DSN="+testDSN), arguments...)
	if err != nil {
		return fmt.Errorf("artifact compose e2e: %w\n%s", err, output)
	}
	if coverageFile != "" {
		coverage, err := coverageTotal(ctx, repoRoot, coverageFile)
		if err != nil {
			return err
		}
		fmt.Printf("Artifact object-store e2e coverage: %s\n", coverage)
		if err := assertCoverageFloor("Artifact object store", "TRPC_MIN_ARTIFACT_E2E_COVERAGE", coverage); err != nil {
			return err
		}
		if coverageOut := os.Getenv("TRPC_ARTIFACT_COVERAGE_OUT"); coverageOut != "" {
			if err := persistCoverageProfile(coverageFile, coverageOut); err != nil {
				return fmt.Errorf("persist artifact e2e coverage: %w", err)
			}
		}
	}
	return nil
}

// artifactCoveragePackages deliberately stays explicit. The compose test
// lives in ./trpcservice/integration but the attribution target is the
// artifact/object-store adapter seam it drives.
func artifactCoveragePackages() []string {
	return []string{
		"./trpcservice/storage/artifact/...",
		"./trpcservice/storage/objectstore/s3",
	}
}

// cleanupAuditRetentionTestRole removes global-role state created by the
// PostgreSQL contract suite. Roles are cluster-scoped rather than database-
// scoped, so dropping the disposable database alone cannot clean a temporary
// principal's membership in audit_retention_purger. This belongs to the test
// harness, not to the production migration down path.
func cleanupAuditRetentionTestRole(ctx context.Context, db *sql.DB) error {
	const role = "audit_retention_purger"
	rows, err := db.QueryContext(ctx, `SELECT member_role.rolname
FROM pg_auth_members membership
JOIN pg_roles granted_role ON granted_role.oid = membership.roleid
JOIN pg_roles member_role ON member_role.oid = membership.member
WHERE granted_role.rolname = $1`, role)
	if err != nil {
		return err
	}
	defer rows.Close()
	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			return err
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, member := range members {
		if _, err := db.ExecContext(ctx, "REVOKE "+quoteRoleName(role)+" FROM "+quoteRoleName(member)); err != nil {
			return err
		}
	}
	// Revokes grants made to the migration-created role, which PostgreSQL also
	// counts as dependencies when the role is removed.
	if _, err := db.ExecContext(ctx, "DROP OWNED BY "+quoteRoleName(role)); err != nil {
		return err
	}
	// The role is cluster-scoped and may retain external dependencies. The
	// disposable Docker PostgreSQL cluster is removed after this test, so do
	// not turn an optional role drop into a migration-matrix failure.
	return nil
}

func quoteRoleName(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func verifyAcceptanceBaselinePlan(ctx context.Context, runner *migrations.Runner) error {
	plan, err := runner.Plan(ctx, "000001")
	if err != nil {
		return err
	}
	if plan.Current != migrations.EmptyVersion || plan.Target != "000001" || len(plan.Pending) != 1 || plan.Pending[0] != "000001" {
		return fmt.Errorf("unexpected baseline plan: %+v", plan)
	}
	return nil
}

func verifyUp(ctx context.Context, runner *migrations.Runner, db *sql.DB, probes, repoRoot, dsn string) error {
	if err := runner.Up(ctx); err != nil {
		return err
	}
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("repeated up: %w", err)
	}
	if err := runner.Ready(ctx); err != nil {
		return fmt.Errorf("migration readiness: %w", err)
	}
	if _, err := db.ExecContext(ctx, probes); err != nil {
		return fmt.Errorf("contract probes: %w", err)
	}
	// These PostgreSQL contract packages intentionally use a shared disposable
	// database and some fixtures truncate cross-domain tables. Run them
	// serially; the default package parallelism otherwise creates test-order
	// races that look like random foreign-key/version conflicts.
	contractPackages := postgresContractPackages()
	contractArguments := []string{"-p=1"}
	coverageFile := ""
	if os.Getenv("TRPC_POSTGRES_ADAPTER_COVERAGE") == "1" {
		file, err := os.CreateTemp("", "trpc-postgres-adapter-coverage.XXXXXX")
		if err != nil {
			return fmt.Errorf("create PostgreSQL adapter coverage file: %w", err)
		}
		coverageFile = file.Name()
		if err := file.Close(); err != nil {
			return fmt.Errorf("close PostgreSQL adapter coverage file: %w", err)
		}
		// TRPC_COVERAGE_OUT persists the raw profile for the admission gate to
		// assert a contract-coverage floor; without it the profile stays
		// disposable and only the reported total survives.
		coverageOut := os.Getenv("TRPC_COVERAGE_OUT")
		if coverageOut == "" {
			defer os.Remove(coverageFile)
		}
		contractArguments = append(contractArguments,
			"-covermode=atomic",
			"-coverpkg="+strings.Join(contractPackages, ","),
			"-coverprofile="+coverageFile,
		)
	}
	contractArguments = append(contractArguments, contractPackages...)
	output, err := goTestWithoutSkips(ctx, repoRoot, append(os.Environ(), "TRPC_MIGRATION_TEST=1", "TRPC_POSTGRES_TEST_DSN="+dsn), contractArguments...)
	if err != nil {
		return fmt.Errorf("PostgreSQL repository contracts: %w\n%s", err, output)
	}
	if coverageFile != "" {
		if err := reportPostgresAdapterCoverage(ctx, repoRoot, coverageFile); err != nil {
			return err
		}
		if coverageOut := os.Getenv("TRPC_COVERAGE_OUT"); coverageOut != "" {
			if err := persistCoverageProfile(coverageFile, coverageOut); err != nil {
				return fmt.Errorf("persist PostgreSQL adapter coverage: %w", err)
			}
		}
	}
	if os.Getenv("TRPC_RUNTIME_TEST") == "1" {
		// The package also contains intentionally manual provider-smoke tests.
		// Select the disposable two-worker contract explicitly, so CI can make
		// every test it claims to exercise a no-skip requirement.
		sliceArguments := []string{
			"-run", "^Test(HTTPPostgreSQLRedisTwoWorkerSlice|ComposeTenantMemoryBackendRouting|ComposeRedisMemoryBackfillToPostgres|ComposeRedisMemoryDualWriteToPostgres)$",
		}
		sliceCoverageFile := ""
		if os.Getenv("TRPC_RUNTIME_SLICE_COVERAGE") == "1" {
			file, err := os.CreateTemp("", "trpc-runtime-slice-coverage.XXXXXX")
			if err != nil {
				return fmt.Errorf("create runtime slice coverage file: %w", err)
			}
			sliceCoverageFile = file.Name()
			if err := file.Close(); err != nil {
				return fmt.Errorf("close runtime slice coverage file: %w", err)
			}
			// TRPC_RUNTIME_COVERAGE_OUT persists the raw profile for the
			// admission gate to assert a coordination-core floor; without it
			// the profile stays disposable and only the reported total
			// survives.
			if os.Getenv("TRPC_RUNTIME_COVERAGE_OUT") == "" {
				defer os.Remove(sliceCoverageFile)
			}
			sliceArguments = append(sliceArguments,
				"-covermode=atomic",
				"-coverpkg="+strings.Join(runtimeSliceCoveragePackages(), ","),
				"-coverprofile="+sliceCoverageFile,
			)
		}
		sliceArguments = append(sliceArguments, "./trpcservice/integration")
		output, err = goTestWithoutSkips(ctx, repoRoot, append(os.Environ(), "TRPC_RUNTIME_TEST=1", "TRPC_POSTGRES_TEST_DSN="+dsn), sliceArguments...)
		if err != nil {
			return fmt.Errorf("runtime slice: %w\n%s", err, output)
		}
		if sliceCoverageFile != "" {
			if err := reportRuntimeSliceCoverage(ctx, repoRoot, sliceCoverageFile); err != nil {
				return err
			}
			if coverageOut := os.Getenv("TRPC_RUNTIME_COVERAGE_OUT"); coverageOut != "" {
				if err := persistCoverageProfile(sliceCoverageFile, coverageOut); err != nil {
					return fmt.Errorf("persist runtime slice coverage: %w", err)
				}
			}
		}
	}
	return nil
}

// postgresContractPackages deliberately stays explicit. These packages form
// the PostgreSQL 16 repository-contract matrix and use one disposable schema
// serially, so their coverage is a meaningful adapter-level signal rather
// than a package-local unit-test accounting artifact.
func postgresContractPackages() []string {
	return []string{
		"./trpcservice/agentapp/postgres", "./trpcservice/audit/postgres", "./trpcservice/audit/purgebusiness/postgres",
		"./trpcservice/config/postgres", "./trpcservice/governance/postgres", "./trpcservice/migration/postgres",
		"./trpcservice/migration/knowledgedriver/postgres", "./trpcservice/migration/memorydriver/postgres",
		"./trpcservice/provider/postgres", "./trpcservice/skill/postgres", "./trpcservice/storage/artifact/postgres",
		"./trpcservice/storage/knowledge/postgres", "./trpcservice/storage/messaging/postgres",
		"./trpcservice/storage/session/postgres", "./trpcservice/storage/summary/postgres", "./trpcservice/tenant/postgres",
	}
}

func persistCoverageProfile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	output, err := os.Create(destination)
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		return err
	}
	return output.Close()
}

func reportPostgresAdapterCoverage(ctx context.Context, repoRoot, coverageFile string) error {
	coverage, err := coverageTotal(ctx, repoRoot, coverageFile)
	if err != nil {
		return err
	}
	fmt.Printf("PostgreSQL 16 adapter contract coverage: %s\n", coverage)
	return assertPostgresAdapterCoverageFloor(coverage)
}

func reportRuntimeSliceCoverage(ctx context.Context, repoRoot, coverageFile string) error {
	coverage, err := coverageTotal(ctx, repoRoot, coverageFile)
	if err != nil {
		return err
	}
	fmt.Printf("Runtime slice coordination-core coverage: %s\n", coverage)
	return assertCoverageFloor("Runtime slice coordination core", "TRPC_MIN_RUNTIME_SLICE_COVERAGE", coverage)
}

// coverageTotal parses the "total:" line out of `go tool cover -func`.
func coverageTotal(ctx context.Context, repoRoot, coverageFile string) (string, error) {
	command := exec.CommandContext(ctx, "go", "tool", "cover", "-func="+coverageFile)
	command.Dir = repoRoot
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("read coverage profile: %w", err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "total:") {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				return fields[2], nil
			}
		}
	}
	return "", fmt.Errorf("coverage total is missing")
}

func assertPostgresAdapterCoverageFloor(coverage string) error {
	return assertCoverageFloor("PostgreSQL adapter contract", "TRPC_MIN_POSTGRES_ADAPTER_COVERAGE", coverage)
}

// assertCoverageFloor turns reported coverage into an admission gate when a
// floor is configured. The assertion runs inside the disposable smoke image
// so the comparison uses the pinned toolchain instead of the host runner's
// Go version.
func assertCoverageFloor(label, envVar, coverage string) error {
	floor := strings.TrimSpace(os.Getenv(envVar))
	if floor == "" {
		return nil
	}
	actual, err := strconv.ParseFloat(strings.TrimSuffix(coverage, "%"), 64)
	if err != nil {
		return fmt.Errorf("parse %s coverage %q: %w", label, coverage, err)
	}
	required, err := strconv.ParseFloat(strings.TrimSuffix(floor, "%"), 64)
	if err != nil {
		return fmt.Errorf("parse %s %q: %w", envVar, floor, err)
	}
	if actual+1e-9 < required {
		return fmt.Errorf("%s coverage %.1f%% is below required %.1f%%", label, actual, required)
	}
	return nil
}

// runtimeSliceCoveragePackages deliberately stays explicit. The runtime
// slice drives the cross-node coordination core — Redis relay, PostgreSQL
// session/messaging stores, coordination lease, broker and worker — so its
// coverage is a meaningful distributed-seam signal rather than a package-
// local unit-test accounting artifact.
func runtimeSliceCoveragePackages() []string {
	return []string{
		"./trpcservice/broker",
		"./trpcservice/broker/redis",
		"./trpcservice/coordination",
		"./trpcservice/coordination/redis",
		"./trpcservice/relay",
		"./trpcservice/relay/redis",
		"./trpcservice/storage/messaging/postgres",
		"./trpcservice/storage/session/postgres",
		"./trpcservice/worker",
	}
}

// goTestWithoutSkips makes the disposable contract matrix a real admission
// gate. A conditional skip is useful for a developer without Docker, but in
// this command every dependency has already been provisioned; accepting one
// would turn a missing backend or test setup into a false green build.
func goTestWithoutSkips(ctx context.Context, directory string, environment []string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "go", append([]string{"test", "-count=1", "-json"}, arguments...)...)
	command.Dir = directory
	command.Env = environment
	output, err := command.CombinedOutput()
	if err != nil {
		return output, err
	}
	if bytes.Contains(output, []byte(`"Action":"skip"`)) {
		return output, fmt.Errorf("contract test skipped despite provisioned backend")
	}
	return output, nil
}

func testDSNForDatabase(adminDSN, databaseName string) (string, error) {
	trimmed := strings.TrimSpace(adminDSN)
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.Path = "/" + databaseName
		return parsed.String(), nil
	}
	dsn := trimmed + " dbname=" + quoteConninfoValue(databaseName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	if config.Database != databaseName {
		return "", fmt.Errorf("failed to target test database %q", databaseName)
	}
	return dsn, nil
}

func quoteConninfoValue(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

func randomDatabaseName() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	name := "trpc_agent_service_test_" + hex.EncodeToString(bytes)
	if !strings.HasPrefix(name, "trpc_agent_service_test_") {
		return "", fmt.Errorf("unsafe database name")
	}
	return name, nil
}
