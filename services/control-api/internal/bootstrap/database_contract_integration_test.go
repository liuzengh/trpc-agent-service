package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	sharedpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

type databaseContractFixture struct {
	admin         *pgxpool.Pool
	config        Config
	migrationRole string
	runtimeRole   string
}

func databaseContractEnvironment(t *testing.T) databaseContractFixture {
	t.Helper()
	names := []string{"CONTROL_DB_CONTRACT_ADMIN_URL", "CONTROL_DB_CONTRACT_MIGRATION_URL", "CONTROL_DB_CONTRACT_RUNTIME_URL"}
	values := make([]string, len(names))
	configured := false
	for i, name := range names {
		values[i] = strings.TrimSpace(os.Getenv(name))
		configured = configured || values[i] != ""
	}
	if !configured {
		t.Skip("Control database contract integration environment is not configured")
	}
	for i, value := range values {
		if value == "" {
			t.Fatalf("%s is required when database contract integration is configured", names[i])
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, values[0])
	if err != nil {
		t.Fatal("open contract admin connection")
	}
	t.Cleanup(admin.Close)
	if err = admin.Ping(ctx); err != nil {
		t.Fatal("contract admin connection unavailable")
	}
	migration, err := sharedpostgres.Open(ctx, values[1])
	if err != nil {
		t.Fatal(err)
	}
	defer migration.Close()
	runtime, err := sharedpostgres.Open(ctx, values[2])
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	mt, err := sharedpostgres.InspectTarget(ctx, migration)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := sharedpostgres.InspectTarget(ctx, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err = sharedpostgres.CheckTargets(mt, rt); err != nil {
		t.Fatal(err)
	}
	return databaseContractFixture{admin: admin, config: Config{DatabaseURL: values[2], MigrationDatabaseURL: values[1]}, migrationRole: mt.Role, runtimeRole: rt.Role}
}

func TestV1DatabaseContractControlRuntimeIsolation(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := openDatabase(ctx, fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	target, err := sharedpostgres.InspectTarget(ctx, pool)
	if err != nil || target.Role != fixture.runtimeRole {
		t.Fatal("application did not retain the runtime identity")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin runtime business transaction")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	id := fmt.Sprintf("db_contract_%d", time.Now().UnixNano())
	if _, err = tx.Exec(ctx, `INSERT INTO user_accounts(id,username,normalized_username) VALUES($1,$1,$1)`, id); err != nil {
		t.Fatal("runtime business INSERT denied")
	}
	if _, err = tx.Exec(ctx, `UPDATE user_accounts SET display_name='contract' WHERE id=$1`, id); err != nil {
		t.Fatal("runtime business UPDATE denied")
	}
	var display string
	if err = tx.QueryRow(ctx, `SELECT display_name FROM user_accounts WHERE id=$1`, id).Scan(&display); err != nil || display != "contract" {
		t.Fatal("runtime business SELECT failed")
	}
	if _, err = tx.Exec(ctx, `DELETE FROM user_accounts WHERE id=$1`, id); err != nil {
		t.Fatal("runtime business DELETE denied")
	}
	_ = tx.Rollback(ctx)
	for _, statement := range []string{
		`CREATE TABLE contract_runtime_ddl_forbidden(id int)`,
		`INSERT INTO control_schema_migrations(version) VALUES('forged.sql')`,
		`UPDATE control_schema_migrations SET version=version`,
		`DELETE FROM control_schema_migrations`,
		`TRUNCATE control_schema_migrations`,
	} {
		assertDatabasePermissionDenied(t, ctx, pool, statement)
	}
	var canWrite bool
	if err = pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,'control_schema_migrations','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')`).Scan(&canWrite); err != nil || canWrite {
		t.Fatal("runtime retains mutation rights on migration ledger")
	}
	t.Log("CONTROL_DB_CONTRACT=PASS runtime_dml=yes runtime_ddl=denied migration_ledger_mutation=denied")
}

func TestV1DatabaseContractControlConcurrentFirstMigration(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	schema := fixture.newSchema(t)
	config := fixture.configForSchema(t, schema, schema)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	failures := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool, err := openDatabaseForSchema(ctx, config, schema)
			if err == nil {
				pool.Close()
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	pool, err := sharedpostgres.Open(ctx, config.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Compare the complete embedded migration set, not a baseline-only row count.
	// New main migrations must be applied exactly once under concurrent startup.
	expected, err := fs.Glob(migrations.Files, "*.sql")
	if err != nil || len(expected) == 0 {
		t.Fatal("read embedded migration names")
	}
	rows, err := pool.Query(ctx, `SELECT version FROM control_schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || !slices.Equal(actual, expected) {
		t.Fatalf("concurrent migration ledger: got %v, want %v, err=%v", actual, expected, err)
	}
	assertDatabasePermissionDenied(t, ctx, pool, `INSERT INTO control_schema_migrations(version) VALUES('forged.sql')`)
	t.Logf("CONTROL_CONCURRENT_FIRST_MIGRATION=PASS migrators=6 ledger_rows=%d runtime_ledger_mutation=denied", len(actual))
}

func TestV1DatabaseContractControlRejectsTargetsBeforeDDL(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	a, b := fixture.newSchema(t), fixture.newSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name   string
		config Config
	}{
		{"schema mismatch", fixture.configForSchema(t, a, b)},
		{"public schema", fixture.configForSchema(t, a, "public")},
		{"administrative migration", Config{DatabaseURL: withDatabaseSchema(t, fixture.config.DatabaseURL, a), MigrationDatabaseURL: withDatabaseSchema(t, os.Getenv("CONTROL_DB_CONTRACT_ADMIN_URL"), a)}},
		{"administrative runtime", Config{DatabaseURL: withDatabaseSchema(t, os.Getenv("CONTROL_DB_CONTRACT_ADMIN_URL"), a), MigrationDatabaseURL: withDatabaseSchema(t, fixture.config.MigrationDatabaseURL, a)}},
		{"same role", Config{DatabaseURL: withDatabaseSchema(t, fixture.config.MigrationDatabaseURL, a), MigrationDatabaseURL: withDatabaseSchema(t, fixture.config.MigrationDatabaseURL, a)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := openDatabaseForSchema(ctx, tc.config, a)
			if pool != nil {
				pool.Close()
			}
			if err == nil {
				t.Fatal("invalid target was accepted")
			}
		})
	}
	for _, schema := range []string{a, b} {
		var present bool
		if err := fixture.admin.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, pgx.Identifier{schema, "control_schema_migrations"}.Sanitize()).Scan(&present); err != nil || present {
			t.Fatal("target validation allowed DDL before rejection")
		}
	}
	t.Log("CONTROL_TARGET_PREFLIGHT=PASS schema_mismatch=denied public=denied same_role=denied admin_runtime=denied admin_migration=denied ddl=none")
}

func assertDatabasePermissionDenied(t *testing.T, ctx context.Context, pool *pgxpool.Pool, statement string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin permission probe")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, statement)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatal("expected PostgreSQL insufficient_privilege from runtime role")
	}
}
func (f databaseContractFixture) newSchema(t *testing.T) string {
	t.Helper()
	schema := fmt.Sprintf("control_contract_%d", time.Now().UnixNano())
	name := pgx.Identifier{schema}.Sanitize()
	migration := pgx.Identifier{f.migrationRole}.Sanitize()
	runtime := pgx.Identifier{f.runtimeRole}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.admin.Exec(ctx, "CREATE SCHEMA "+name+" AUTHORIZATION "+migration+"; GRANT USAGE ON SCHEMA "+name+" TO "+runtime+"; ALTER DEFAULT PRIVILEGES FOR ROLE "+migration+" IN SCHEMA "+name+" GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO "+runtime); err != nil {
		t.Fatal("create isolated database contract schema")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := f.admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Error("drop isolated database contract schema")
		}
	})
	return schema
}
func (f databaseContractFixture) configForSchema(t *testing.T, migration, runtime string) Config {
	return Config{MigrationDatabaseURL: withDatabaseSchema(t, f.config.MigrationDatabaseURL, migration), DatabaseURL: withDatabaseSchema(t, f.config.DatabaseURL, runtime)}
}
func withDatabaseSchema(t *testing.T, dsn, schema string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("database contract test requires URL-format connection strings")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestV1DatabaseContractControlRejectsNonOwnerMigratorBeforeDDL(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	schema := fixture.newSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var adminRole string
	if err := fixture.admin.QueryRow(ctx, `SELECT current_user`).Scan(&adminRole); err != nil {
		t.Fatal("inspect test admin role")
	}
	name := pgx.Identifier{schema}.Sanitize()
	if _, err := fixture.admin.Exec(ctx, "ALTER SCHEMA "+name+" OWNER TO "+pgx.Identifier{adminRole}.Sanitize()+"; GRANT USAGE,CREATE ON SCHEMA "+name+" TO "+pgx.Identifier{fixture.migrationRole}.Sanitize()); err != nil {
		t.Fatal("prepare non-owner migration fixture")
	}
	pool, err := openDatabaseForSchema(ctx, fixture.configForSchema(t, schema, schema), schema)
	if pool != nil {
		pool.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "must own") {
		t.Fatal("non-owner migrator was not rejected by ownership preflight")
	}
	var present bool
	if err = fixture.admin.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, pgx.Identifier{schema, "control_schema_migrations"}.Sanitize()).Scan(&present); err != nil || present {
		t.Fatal("non-owner migrator performed DDL")
	}
	t.Log("CONTROL_MIGRATION_OWNER=PASS non_owner_with_create=denied ddl=none")
}

func TestV1DatabaseContractControlRejectsOtherWorkloadBeforeDDL(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	migrationURL, runtimeURL := os.Getenv("GATEWAY_V1_TEST_MIGRATION_DATABASE_URL"), os.Getenv("GATEWAY_V1_TEST_DATABASE_URL")
	if migrationURL == "" && runtimeURL == "" {
		t.Skip("other-workload connection fixture is not configured")
	}
	if migrationURL == "" || runtimeURL == "" {
		t.Fatal("both other-workload connection URLs are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var before bool
	if err := fixture.admin.QueryRow(ctx, `SELECT to_regclass('gateway.control_schema_migrations') IS NOT NULL`).Scan(&before); err != nil || before {
		t.Fatal("other-workload fixture already contains a Control migration ledger")
	}
	pool, err := openDatabase(ctx, Config{MigrationDatabaseURL: migrationURL, DatabaseURL: runtimeURL})
	if pool != nil {
		pool.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "control database requires") {
		t.Fatal("Control did not reject other-workload role/schema binding")
	}
	var after bool
	if err = fixture.admin.QueryRow(ctx, `SELECT to_regclass('gateway.control_schema_migrations') IS NOT NULL`).Scan(&after); err != nil || after {
		t.Fatal("Control performed DDL in the other workload schema")
	}
	t.Log("CONTROL_WORKLOAD_BINDING=PASS gateway_pair=denied ddl=none")
}

func TestV1DatabaseContractControlRejectsAssumedRolesBeforeDDL(t *testing.T) {
	fixture := databaseContractEnvironment(t)
	schema := fixture.newSchema(t)
	normal := fixture.configForSchema(t, schema, schema)
	assumed := func(role string) string {
		u, err := url.Parse(withDatabaseSchema(t, os.Getenv("CONTROL_DB_CONTRACT_ADMIN_URL"), schema))
		if err != nil {
			t.Fatal("parse assumed-role fixture")
		}
		query := u.Query()
		query.Set("options", "-c role="+role)
		u.RawQuery = query.Encode()
		return u.String()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name   string
		config Config
	}{
		{"runtime", Config{MigrationDatabaseURL: normal.MigrationDatabaseURL, DatabaseURL: assumed("control_runtime")}},
		{"migrator", Config{MigrationDatabaseURL: assumed("control_migrator"), DatabaseURL: normal.DatabaseURL}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := openDatabaseForSchema(ctx, tc.config, schema)
			if pool != nil {
				pool.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "authenticate directly") {
				t.Fatal("assumed role was not rejected by authenticated-identity preflight")
			}
		})
	}
	var present bool
	if err := fixture.admin.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, pgx.Identifier{schema, "control_schema_migrations"}.Sanitize()).Scan(&present); err != nil || present {
		t.Fatal("assumed-role connection performed DDL")
	}
	t.Log("CONTROL_DIRECT_ROLE=PASS assumed_runtime=denied assumed_migrator=denied ddl=none")
}
