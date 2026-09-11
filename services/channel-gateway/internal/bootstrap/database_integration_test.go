package bootstrap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

// This test requires a provisioned, disposable V1 database. Unlike historical
// store fixtures it uses deployment role defaults, not a per-connection SET.
func TestGatewayV1DatabaseRoleContract(t *testing.T) {
	runtimeURL := os.Getenv("GATEWAY_V1_TEST_DATABASE_URL")
	migrationURL := os.Getenv("GATEWAY_V1_TEST_MIGRATION_DATABASE_URL")
	if runtimeURL == "" || migrationURL == "" {
		t.Skip("GATEWAY_V1_TEST_DATABASE_URL and GATEWAY_V1_TEST_MIGRATION_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := Config{DatabaseURL: runtimeURL, MigrationDatabaseURL: migrationURL}
	for range 2 {
		pool, err := openDatabase(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer pool.Close()
			target, err := inspectDatabase(ctx, pool)
			if err != nil {
				t.Fatal("inspect runtime target failed")
			}
			if target.schema != "gateway" {
				t.Fatalf("expected gateway schema, got %q", target.schema)
			}
			// Published sibling migrations retain full filenames as identities.
			// Compare the complete embedded set, not a stale single-branch count.
			expected, err := fs.Glob(migrations.Files, "*.sql")
			if err != nil || len(expected) == 0 {
				t.Fatal("read embedded migration names")
			}
			rows, err := pool.Query(ctx, "SELECT version FROM gateway_schema_migrations ORDER BY version")
			if err != nil {
				t.Fatal("read migration ledger")
			}
			actual, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil || !slices.Equal(actual, expected) {
				t.Fatalf("migration ledger readable: got %v, want %v, err=%v", actual, expected, err)
			}
			// A false predicate makes the privilege probes non-mutating even if a
			// broken deployment accidentally grants these statements permission.
			for _, statement := range []string{
				"UPDATE gateway_schema_migrations SET digest=digest WHERE false",
				"DELETE FROM gateway_schema_migrations WHERE false",
				"INSERT INTO gateway_schema_migrations(version,digest) SELECT 'probe','probe' WHERE false",
			} {
				var denied *pgconn.PgError
				if _, err := pool.Exec(ctx, statement); !errors.As(err, &denied) || denied.Code != "42501" {
					t.Fatal("runtime migration ledger write was not denied by privileges")
				}
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal("begin business privilege probe failed")
			}
			defer tx.Rollback(context.Background())
			id := "v1-role-probe-" + time.Now().Format("150405.000000000")
			for _, statement := range []string{
				"INSERT INTO gateway_route_receipts(event_id,digest) VALUES($1,'probe')",
				"SELECT event_id FROM gateway_route_receipts WHERE event_id=$1",
				"UPDATE gateway_route_receipts SET digest='updated' WHERE event_id=$1",
				"DELETE FROM gateway_route_receipts WHERE event_id=$1",
			} {
				if _, err := tx.Exec(ctx, statement, id); err != nil {
					t.Fatal("runtime business DML failed")
				}
			}
			var denied *pgconn.PgError
			if _, err := tx.Exec(ctx, "CREATE TABLE gateway_v1_forbidden_ddl_probe(id int)"); !errors.As(err, &denied) || denied.Code != "42501" {
				t.Fatal("runtime schema DDL was not denied by privileges")
			}
		}()
	}
	// A mistaken same-role configuration is rejected before migration.
	if pool, err := openDatabase(ctx, Config{DatabaseURL: migrationURL, MigrationDatabaseURL: migrationURL}); err == nil {
		pool.Close()
		t.Fatal("same migration/runtime role accepted")
	}
}

func TestGatewayV1RejectsWrongMigrationBeforeDDL(t *testing.T) {
	adminURL := os.Getenv("GATEWAY_V1_TEST_ADMIN_DATABASE_URL")
	if adminURL == "" {
		t.Skip("GATEWAY_V1_TEST_ADMIN_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal("administrator connection failed")
	}
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("gateway_identity_%d", time.Now().UnixNano())
	schemaSQL := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schemaSQL); err != nil {
		t.Fatal("preflight fixture schema creation failed")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+schemaSQL+" CASCADE"); err != nil {
			t.Error("preflight fixture schema cleanup failed")
		}
	})
	u, err := url.Parse(adminURL)
	if err != nil || u.Scheme == "" {
		t.Fatal("administrator fixture requires URL form")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	fixture := fixtureDatabaseConfig(t, u.String())
	c := Config{DatabaseURL: fixture.runtimeURL, MigrationDatabaseURL: fixture.migrationURL}
	assertNoDDL := func(t *testing.T) {
		t.Helper()
		var exists bool
		if err := admin.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", pgx.Identifier{schema, "gateway_schema_migrations"}.Sanitize()).Scan(&exists); err != nil || exists {
			t.Fatalf("rejected preflight created migration ledger: exists=%v err=%v", exists, err)
		}
	}
	t.Run("admin_migrator", func(t *testing.T) {
		bad := c
		bad.MigrationDatabaseURL = u.String()
		if pool, err := openDatabaseForTarget(ctx, bad, fixture.target); err == nil {
			pool.Close()
			t.Fatal("administrator migration role accepted")
		}
		assertNoDDL(t)
	})
	t.Run("admin_startup_role_switch", func(t *testing.T) {
		switched := *u
		q := switched.Query()
		q.Set("role", fixture.target.migrationRole)
		switched.RawQuery = q.Encode()
		bad := c
		bad.MigrationDatabaseURL = switched.String()
		if pool, err := openDatabaseForTarget(ctx, bad, fixture.target); err == nil {
			pool.Close()
			t.Fatal("administrator startup role switch accepted")
		}
		assertNoDDL(t)
	})
	t.Run("nonowner_migrator", func(t *testing.T) {
		var administrator string
		if err := admin.QueryRow(ctx, "SELECT current_user").Scan(&administrator); err != nil {
			t.Fatal("administrator identity inspection failed")
		}
		if _, err := admin.Exec(ctx, "ALTER SCHEMA "+schemaSQL+" OWNER TO "+pgx.Identifier{administrator}.Sanitize()+"; GRANT USAGE, CREATE ON SCHEMA "+schemaSQL+" TO "+pgx.Identifier{fixture.target.migrationRole}.Sanitize()); err != nil {
			t.Fatal("nonowner fixture setup failed")
		}
		if pool, err := openDatabaseForTarget(ctx, c, fixture.target); err == nil {
			pool.Close()
			t.Fatal("nonowner migration role accepted")
		}
		assertNoDDL(t)
		if _, err := admin.Exec(ctx, "ALTER SCHEMA "+schemaSQL+" OWNER TO "+pgx.Identifier{fixture.target.migrationRole}.Sanitize()); err != nil {
			t.Fatal("restore fixture schema ownership failed")
		}
	})
	t.Run("other_workload", func(t *testing.T) {
		// Both identities are otherwise valid ordinary roles on one schema.
		// The production entrypoint still rejects a foreign schema/role pair.
		if pool, err := openDatabase(ctx, c); err == nil {
			pool.Close()
			t.Fatal("production accepted another workload identity")
		}
		assertNoDDL(t)
	})
	pool, err := openDatabaseForTarget(ctx, c, fixture.target)
	if err != nil {
		t.Fatal("ordinary owner/runtime fixture startup failed")
	}
	pool.Close()
}

type fixtureDatabase struct {
	runtimeURL, migrationURL string
	target                   databaseIdentity
}

// fixtureDatabaseConfig makes the administrator a provisioner only. Both App
// connections use ordinary roles, and the migration role owns the test schema.
func fixtureDatabaseConfig(t *testing.T, adminURL string) fixtureDatabase {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal("fixture administrator connection failed")
	}
	defer admin.Close()
	var database, schema, administrator string
	if err = admin.QueryRow(ctx, "SELECT current_database(), current_schema(), current_user").Scan(&database, &schema, &administrator); err != nil {
		t.Fatal("fixture target inspection failed")
	}
	var entropy [16]byte
	if _, err = rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	password := hex.EncodeToString(entropy[:])
	migrator, runtime := "gateway_fixture_m_"+password[:16], "gateway_fixture_r_"+password[:16]
	migrationSQL, runtimeSQL := pgx.Identifier{migrator}.Sanitize(), pgx.Identifier{runtime}.Sanitize()
	schemaSQL, databaseSQL := pgx.Identifier{schema}.Sanitize(), pgx.Identifier{database}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE ROLE "+migrationSQL+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"m'; CREATE ROLE "+runtimeSQL+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"r'"); err != nil {
		t.Fatal("fixture ordinary role creation failed")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		pool, err := pgxpool.New(cleanup, adminURL)
		if err != nil {
			t.Error("fixture role cleanup connection failed")
			return
		}
		defer pool.Close()
		// Preserve the schema for its original fixture cleanup, which may run later.
		for _, statement := range []string{
			"REASSIGN OWNED BY " + migrationSQL + " TO " + pgx.Identifier{administrator}.Sanitize(),
			"DROP OWNED BY " + runtimeSQL + ", " + migrationSQL,
			"DROP ROLE " + runtimeSQL + ", " + migrationSQL,
		} {
			if _, err = pool.Exec(cleanup, statement); err != nil {
				t.Error("fixture role cleanup failed")
				return
			}
		}
	})
	for _, statement := range []string{
		"GRANT CONNECT ON DATABASE " + databaseSQL + " TO " + migrationSQL + ", " + runtimeSQL,
		"ALTER SCHEMA " + schemaSQL + " OWNER TO " + migrationSQL,
		"GRANT USAGE ON SCHEMA " + schemaSQL + " TO " + runtimeSQL,
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + migrationSQL + " IN SCHEMA " + schemaSQL + " GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO " + runtimeSQL,
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + migrationSQL + " IN SCHEMA " + schemaSQL + " GRANT USAGE, SELECT ON SEQUENCES TO " + runtimeSQL,
		"ALTER ROLE " + migrationSQL + " IN DATABASE " + databaseSQL + " SET search_path TO " + schemaSQL,
		"ALTER ROLE " + runtimeSQL + " IN DATABASE " + databaseSQL + " SET search_path TO " + schemaSQL,
	} {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatal("fixture role provisioning failed")
		}
	}
	// Some historical fixtures create/seed tables before constructing App. Move
	// only those fixture tables to the ordinary owner before startup validation.
	rows, err := admin.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN ('r','p')`, schema)
	if err != nil {
		t.Fatal("fixture table inspection failed")
	}
	var tables []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatal("fixture table scan failed")
		}
		tables = append(tables, name)
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal("fixture table inspection failed")
	}
	for _, table := range tables {
		if _, err = admin.Exec(ctx, "ALTER TABLE "+pgx.Identifier{schema, table}.Sanitize()+" OWNER TO "+migrationSQL); err != nil {
			t.Fatal("fixture table ownership transfer failed")
		}
	}
	if _, err = admin.Exec(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "+schemaSQL+" TO "+runtimeSQL); err != nil {
		t.Fatal("fixture runtime existing-table grants failed")
	}
	if _, err = admin.Exec(ctx, "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "+schemaSQL+" TO "+runtimeSQL); err != nil {
		t.Fatal("fixture runtime existing-sequence grants failed")
	}
	makeURL := func(role, password string) string {
		u, err := url.Parse(adminURL)
		if err != nil || u.Scheme == "" {
			t.Fatal("fixture administrator URL must use URL form")
		}
		u.User = url.UserPassword(role, password)
		q := u.Query()
		q.Del("search_path")
		u.RawQuery = q.Encode()
		return u.String()
	}
	return fixtureDatabase{runtimeURL: makeURL(runtime, password+"r"), migrationURL: makeURL(migrator, password+"m"), target: databaseIdentity{schema: schema, migrationRole: migrator, runtimeRole: runtime}}
}
