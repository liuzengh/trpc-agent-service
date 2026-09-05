package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// P2-01 runtime-role RLS evidence: every test connects as the dedicated
// NOBYPASSRLS runtime role and proves the database itself enforces tenant
// isolation on top of the application-level tenant predicates.

const rlsProtectedTables = "tenant,agent_app,channel_binding,user_identity,session,session_event," +
	"message_dedup,memory,summary,artifact,audit_log,outbox_message,dead_letter,agent_release," +
	"tenant_config_version,coordination_epoch,session_lease,execution_result,job_queue," +
	"channel_binding_audit,vector_projection_task,vector_rebuild_run,tenant_config_rollout," +
	"tenant_config_operation"

type rlsFixture struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	schema  string
	tenantA string
	tenantB string
}

func (f *rlsFixture) asTenantA(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	return tenantctx.WithTenantContext(ctx, f.runtime, f.tenantA, "rls tenant a", fn)
}

func (f *rlsFixture) asTenantB(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	return tenantctx.WithTenantContext(ctx, f.runtime, f.tenantB, "rls tenant b", fn)
}

func (f *rlsFixture) seedAudit(ctx context.Context, tenantID, auditID string) error {
	return tenantctx.WithTenantContext(ctx, f.runtime, tenantID, "rls seed audit", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO audit_log (tenant_id, audit_id, trace_id, request_id, execution_id, channel, external_user, decision)
			VALUES ($1,$2,'rls-trace','rls-request','rls-execution','web','rls-user','allow')
			ON CONFLICT (tenant_id, audit_id) DO NOTHING`, tenantID, auditID)
		return err
	})
}

func newRLSFixture(t *testing.T) *rlsFixture {
	t.Helper()
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL is not set; P2-01 runtime-role RLS evidence unavailable")
	}
	_, file, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../migrations"))
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	admin, err := NewPool(ctx, PostgresConfig{URL: adminURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal("RLS admin pool unavailable")
	}
	schema := "p201_rls_" + strings.ToLower(strconv.FormatInt(time.Now().UTC().UnixNano(), 36))
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal("RLS fixture schema unavailable")
	}
	migrator, err := NewMigrator(ctx, PostgresConfig{URL: adminURL, SearchPath: schema, MaxConns: 2, MinConns: 1}, os.DirFS(migrationsDir))
	if err != nil {
		admin.Close()
		t.Fatalf("RLS fixture migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		migrator.Close()
		admin.Close()
		t.Fatalf("RLS fixture migrations: %v", err)
	}
	migrator.Close()
	var where string
	_ = admin.QueryRow(ctx, "SELECT schemaname FROM pg_tables WHERE tablename='tenant'").Scan(&where)
	t.Logf("DIAG tenant table schema=%q", where)

	roleName := "trpc_rt_" + schema
	if len(roleName) > 60 {
		roleName = roleName[:60]
	}
	password := "p201rls" + strings.ToLower(strconv.FormatInt(time.Now().UTC().UnixNano(), 36))
	if err := EnsureTenantRuntimeRole(ctx, admin, schema, roleName, password); err != nil {
		admin.Close()
		t.Fatalf("RLS fixture runtime role: %v", err)
	}
	if err := GrantRuntimeSchemaPrivileges(ctx, admin, schema, roleName); err != nil {
		admin.Close()
		t.Fatalf("RLS fixture runtime grants: %v", err)
	}
	parsed, err := url.Parse(adminURL)
	if err != nil {
		admin.Close()
		t.Fatalf("RLS fixture url: %v", err)
	}
	parsed.User = url.UserPassword(roleName, password)
	runtime, err := NewPool(ctx, PostgresConfig{URL: parsed.String(), SearchPath: schema, MaxConns: 8, MinConns: 1})
	if err != nil {
		admin.Close()
		t.Fatal("RLS runtime pool unavailable")
	}
	var runtimeSchema, sp string
	if diagErr := runtime.QueryRow(ctx, "SELECT COALESCE(current_schema(),''), current_setting('search_path')").Scan(&runtimeSchema, &sp); diagErr != nil {
		t.Logf("DIAG runtime schema probe error: %v", diagErr)
	} else {
		t.Logf("DIAG runtime current_schema=%q search_path=%q", runtimeSchema, sp)
	}
	var usagePriv bool
	_ = admin.QueryRow(ctx, "SELECT has_schema_privilege($1, $2, 'USAGE')", roleName, schema).Scan(&usagePriv)
	var acl string
	_ = admin.QueryRow(ctx, "SELECT COALESCE(nspacl::text,'') FROM pg_namespace WHERE nspname=$1", schema).Scan(&acl)
	t.Logf("DIAG admin-side usage_priv=%t acl=%q", usagePriv, acl)
	granteeRows, granteeErr := admin.Query(ctx, "SELECT grantee, privilege_type FROM information_schema.usage_privileges WHERE object_schema=$1 AND object_type='SCHEMA'", schema)
	if granteeErr != nil {
		t.Logf("DIAG schema_privileges query error: %v", granteeErr)
	} else {
		for granteeRows.Next() {
			var g, pr string
			if scanErr := granteeRows.Scan(&g, &pr); scanErr != nil {
				t.Logf("DIAG schema_privileges scan error: %v", scanErr)
			} else {
				t.Logf("DIAG schema_privileges grantee=%q priv=%q", g, pr)
			}
		}
		granteeRows.Close()
	}
	if err := tenantctx.EnsureRuntimeRoleLimits(ctx, runtime); err != nil {
		runtime.Close()
		admin.Close()
		t.Fatalf("runtime role must be constrained: %v", err)
	}

	tenantA := "p201-tenant-a-" + schema
	tenantB := "p201-tenant-b-" + schema
	fixture := &rlsFixture{admin: admin, runtime: runtime, schema: schema, tenantA: tenantA, tenantB: tenantB}
	for _, tenantID := range []string{tenantA, tenantB} {
		bind := tenantID
		if err := tenantctx.WithTenantContext(ctx, runtime, bind, "rls seed tenant", func(ctx context.Context, tx pgx.Tx) error {
			_, execErr := tx.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version, default_agent_app_id)
				VALUES ($1,'rls-fixture','active',1,NULL)`, bind)
			return execErr
		}); err != nil {
			runtime.Close()
			admin.Close()
			t.Fatalf("RLS fixture seed tenant: %v", err)
		}
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, "DROP SCHEMA "+schema+" CASCADE")
		_, _ = admin.Exec(cctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName))
		runtime.Close()
		admin.Close()
	})
	return fixture
}

func TestRLSPolicyCoverage(t *testing.T) {
	f := newRLSFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, table := range strings.Split(rlsProtectedTables, ",") {
		var relRow, relForce bool
		var polCmd byte
		var polName string
		err := f.admin.QueryRow(ctx, `
			SELECT c.relrowsecurity, c.relforcerowsecurity,
			       COALESCE(p.polcmd, ' '), COALESCE(p.polname, '')
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_policy p ON p.polrelid = c.oid
			WHERE n.nspname = $1 AND c.relname = $2`, f.schema, table).Scan(&relRow, &relForce, &polCmd, &polName)
		if err != nil {
			t.Fatalf("table %s policy state unavailable: %v", table, err)
		}
		if !relRow || !relForce || polCmd != '*' || polName != table+"_tenant_isolation" {
			t.Fatalf("table %s: rowsecurity=%t force=%t polcmd=%q policy=%q", table, relRow, relForce, polCmd, polName)
		}
	}
	var roleSuper, roleBypass bool
	var migrationReadable, schemaCreate bool
	err := f.runtime.QueryRow(ctx, `
		SELECT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
		       (SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user),
		       has_table_privilege(current_user, format('%I.schema_migration', current_schema()), 'SELECT'),
		       has_schema_privilege(current_user, current_schema(), 'CREATE')`).Scan(&roleSuper, &roleBypass, &migrationReadable, &schemaCreate)
	if err != nil {
		t.Fatalf("runtime role introspection: %v", err)
	}
	if roleSuper || roleBypass || migrationReadable || schemaCreate {
		t.Fatalf("runtime role over-privileged: super=%t bypassrls=%t migration_read=%t schema_create=%t", roleSuper, roleBypass, migrationReadable, schemaCreate)
	}
	var ddlDenied bool
	if err := f.runtime.QueryRow(ctx, "CREATE TABLE rls_ddl_probe (id int)").Scan(nil); err != nil {
		ddlDenied = true
	}
	if !ddlDenied {
		t.Fatal("runtime role unexpectedly executed DDL")
	}
}

func TestRLSTwoTenantIsolation(t *testing.T) {
	f := newRLSFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := f.seedAudit(ctx, f.tenantA, "audit-a-1"); err != nil {
		t.Fatal(err)
	}
	if err := f.seedAudit(ctx, f.tenantA, "audit-a-2"); err != nil {
		t.Fatal(err)
	}
	if err := f.seedAudit(ctx, f.tenantB, "audit-b-1"); err != nil {
		t.Fatal(err)
	}

	// Missing tenant context: reads return zero rows.
	var withoutContext int
	if err := f.runtime.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&withoutContext); err != nil {
		t.Fatal(err)
	}
	if withoutContext != 0 {
		t.Fatalf("no-context read saw %d rows, want 0", withoutContext)
	}

	// Tenant A sees exactly its own rows, even when the application predicate
	// is deliberately omitted.
	var aRows int
	if err := f.asTenantA(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&aRows)
	}); err != nil {
		t.Fatal(err)
	}
	if aRows != 2 {
		t.Fatalf("tenant A rows=%d, want 2", aRows)
	}

	// Explicit cross-tenant WHERE under tenant A still returns zero rows.
	var crossRows int
	if err := f.asTenantA(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE tenant_id=$1", f.tenantB).Scan(&crossRows)
	}); err != nil {
		t.Fatal(err)
	}
	if crossRows != 0 {
		t.Fatalf("tenant A saw %d tenant B rows through an explicit lookup", crossRows)
	}

	// WITH CHECK rejects a cross-tenant insert.
	err := f.asTenantA(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, `INSERT INTO audit_log (tenant_id, audit_id, trace_id, request_id, execution_id, channel, external_user, decision)
			VALUES ($1,'audit-x','t','r','e','web','u','allow')`, f.tenantB)
		return execErr
	})
	if !errors.Is(err, storage.ErrTenantMismatch) && err == nil {
		t.Fatal("cross-tenant insert unexpectedly accepted")
	}
	if err != nil && errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("cross-tenant insert error not mapped to a stable category: %v", err)
	}

	// USING hides cross-tenant UPDATE/DELETE.
	if err := f.asTenantA(ctx, func(ctx context.Context, tx pgx.Tx) error {
		command, execErr := tx.Exec(ctx, "UPDATE audit_log SET decision='deny' WHERE audit_id='audit-b-1'")
		if execErr != nil {
			return execErr
		}
		if command.RowsAffected() != 0 {
			t.Fatal("tenant A updated a tenant B row")
		}
		command, execErr = tx.Exec(ctx, "DELETE FROM audit_log WHERE audit_id='audit-b-1'")
		if execErr != nil {
			return execErr
		}
		if command.RowsAffected() != 0 {
			t.Fatal("tenant A deleted a tenant B row")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Tenant B still sees exactly its own row.
	var bRows int
	if err := f.asTenantB(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&bRows)
	}); err != nil {
		t.Fatal(err)
	}
	if bRows != 1 {
		t.Fatalf("tenant B rows=%d, want 1", bRows)
	}
}

func TestRLSConnectionReuseNeverLeaksTenant(t *testing.T) {
	f := newRLSFixture(t)
	if err := f.seedAudit(context.Background(), f.tenantA, "audit-leak-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	assertEmpty := func(stage string) {
		var rows int
		if err := f.runtime.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&rows); err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		if rows != 0 {
			t.Fatalf("%s: tenant context leaked across pool reuse (%d rows)", stage, rows)
		}
	}

	// commit path
	connA, err := f.runtime.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := connA.Conn().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tenantctx.SetTenantContext(ctx, tx, f.tenantA); err != nil {
		t.Fatal(err)
	}
	var seen int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("committed-path rows=%d want=1", seen)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	connA.Release()
	assertEmpty("after commit")

	// rollback path
	connB, err := f.runtime.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := connB.Conn().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tenantctx.SetTenantContext(ctx, tx2, f.tenantB); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	connB.Release()
	assertEmpty("after rollback")

	// cancelled path
	connC, err := f.runtime.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancelFn := context.WithCancel(ctx)
	tx3, err := connC.Conn().Begin(cancelCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tenantctx.SetTenantContext(cancelCtx, tx3, f.tenantA); err != nil {
		t.Fatal(err)
	}
	cancelFn()
	_ = tx3.Rollback(context.Background())
	connC.Release()
	assertEmpty("after cancel")
}

func TestRLSConcurrentTenants(t *testing.T) {
	f := newRLSFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const tenants = 100
	ids := make([]string, tenants)
	for i := range ids {
		ids[i] = fmt.Sprintf("p201-concurrent-%s-%03d", f.schema, i)
		qualified := pgx.Identifier{f.schema}.Sanitize()
		if _, err := f.admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.tenant (tenant_id, name, status, config_version, default_agent_app_id)
			VALUES ($1,'rls-concurrent','active',1,NULL)`, qualified), ids[i]); err != nil {
			t.Fatalf("concurrent tenant seed: %v", err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, tenants)
	for i := 0; i < tenants; i++ {
		id := ids[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			innerCtx, innerCancel := context.WithTimeout(ctx, 120*time.Second)
			defer innerCancel()
			err := tenantctx.WithTenantContext(innerCtx, f.runtime, id, "rls concurrent", func(ctx context.Context, tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `INSERT INTO audit_log (tenant_id, audit_id, trace_id, request_id, execution_id, channel, external_user, decision)
					VALUES ($1,'audit-c','t','r','e','web','u','allow')`, id); err != nil {
					return err
				}
				var rows int
				if err := tx.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&rows); err != nil {
					return err
				}
				if rows != 1 {
					return fmt.Errorf("tenant %s saw %d rows under concurrency", id, rows)
				}
				return nil
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestRLSNarrowGlobalCapabilities(t *testing.T) {
	f := newRLSFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Seed a queued job and a binding for tenant A.
	if err := f.asTenantA(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO job_queue (tenant_id, job_id, execution_id, schema_version, payload)
			VALUES ($1,'job-rls-1','exec-rls-1',1,'{}'::jsonb)`, f.tenantA)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref)
			VALUES ($1,'web','binding-rls-1','p201-external-app',NULL)`, f.tenantA)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Without any tenant context the runtime role can run the fixed claim
	// function and the fixed binding resolver, but cannot read the tables.
	var jobTenant, jobID, executionID string
	var schemaVersion int
	var payload []byte
	var attempt int
	if err := f.runtime.QueryRow(ctx, `
		SELECT tenant_id, job_id, execution_id, schema_version, payload, attempt, leased_until, received_at
		FROM trpc_queue_claim_next(30000000, 'delivery-rls-1')`).Scan(
		&jobTenant, &jobID, &executionID, &schemaVersion, &payload, &attempt, new(time.Time), new(time.Time)); err != nil {
		t.Fatalf("claim function failed for the runtime role: %v", err)
	}
	if jobTenant != f.tenantA || jobID != "job-rls-1" {
		t.Fatalf("claim returned unexpected job: tenant=%q job=%q", jobTenant, jobID)
	}
	var bindings int
	if err := f.runtime.QueryRow(ctx, "SELECT count(*) FROM trpc_binding_resolve('web','p201-external-app')").Scan(&bindings); err != nil {
		t.Fatalf("binding resolve failed for the runtime role: %v", err)
	}
	if bindings != 1 {
		t.Fatalf("binding resolve rows=%d want=1", bindings)
	}
	var tenants int
	if err := f.runtime.QueryRow(ctx, "SELECT count(*) FROM tenant").Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if tenants != 0 {
		t.Fatalf("runtime role enumerated %d tenant rows without a tenant context", tenants)
	}
	var jobs int
	if err := f.runtime.QueryRow(ctx, "SELECT count(*) FROM job_queue").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 {
		t.Fatalf("runtime role enumerated %d queue rows without a tenant context", jobs)
	}
}
