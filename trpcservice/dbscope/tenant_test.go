package dbscope

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type scriptedSQL struct {
	querySQL    string
	queryValues []driver.Value
	queryErr    error
	beginErr    error
	execErrs    []error
	execQueries []string
	execArgs    [][]driver.NamedValue
	rollbacks   int
}

type scriptedConnector struct{ script *scriptedSQL }

func (c scriptedConnector) Connect(context.Context) (driver.Conn, error) {
	return &scriptedConn{script: c.script}, nil
}
func (c scriptedConnector) Driver() driver.Driver { return scriptedDriver{} }

type scriptedDriver struct{}

func (scriptedDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unexpected Open") }

type scriptedConn struct{ script *scriptedSQL }

func (*scriptedConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (*scriptedConn) Close() error { return nil }
func (c *scriptedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *scriptedConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.script.beginErr != nil {
		return nil, c.script.beginErr
	}
	return &scriptedTx{script: c.script}, nil
}
func (c *scriptedConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.script.querySQL = query
	if c.script.queryErr != nil {
		return nil, c.script.queryErr
	}
	return &scriptedRows{values: c.script.queryValues}, nil
}
func (c *scriptedConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.script.execQueries = append(c.script.execQueries, query)
	c.script.execArgs = append(c.script.execArgs, append([]driver.NamedValue(nil), args...))
	index := len(c.script.execQueries) - 1
	if index < len(c.script.execErrs) && c.script.execErrs[index] != nil {
		return nil, c.script.execErrs[index]
	}
	return driver.RowsAffected(1), nil
}

type scriptedTx struct{ script *scriptedSQL }

func (*scriptedTx) Commit() error { return nil }
func (t *scriptedTx) Rollback() error {
	t.script.rollbacks++
	return nil
}

type scriptedRows struct {
	values []driver.Value
	done   bool
}

func (*scriptedRows) Columns() []string {
	return []string{"current_user", "rolsuper", "rolbypassrls", "member"}
}
func (*scriptedRows) Close() error { return nil }
func (r *scriptedRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

func newScriptedDB(script *scriptedSQL) *sql.DB {
	return sql.OpenDB(scriptedConnector{script: script})
}

func TestValidatePlatformRoleCapabilities(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                      string
		superuser, bypass, member bool
		wantErr                   string
	}{
		{name: "managed platform role", bypass: true, member: true},
		{name: "superuser development role", superuser: true, member: true},
		{name: "cannot bypass RLS", member: true, wantErr: "BYPASSRLS"},
		{name: "cannot assume tenant role", bypass: true, wantErr: "member of trpc_tenant"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validatePlatformRoleCapabilities("platform", test.superuser, test.bypass, test.member)
			if test.wantErr == "" && err != nil {
				t.Fatalf("validation error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("validation error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestValidatePlatformRoleQueriesDatabaseAndPropagatesFailures(t *testing.T) {
	t.Parallel()
	if err := ValidatePlatformRole(context.Background(), nil); err == nil {
		t.Fatal("ValidatePlatformRole(nil) succeeded")
	}

	t.Run("valid managed role", func(t *testing.T) {
		script := &scriptedSQL{queryValues: []driver.Value{"platform", false, true, true}}
		database := newScriptedDB(script)
		defer database.Close()
		if err := ValidatePlatformRole(context.Background(), database); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(script.querySQL, "pg_has_role") || !strings.Contains(script.querySQL, "pg_roles") {
			t.Fatalf("role query = %q", script.querySQL)
		}
	})

	t.Run("query failure", func(t *testing.T) {
		wantErr := errors.New("catalog unavailable")
		database := newScriptedDB(&scriptedSQL{queryErr: wantErr})
		defer database.Close()
		err := ValidatePlatformRole(context.Background(), database)
		if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "inspect PostgreSQL platform role") {
			t.Fatalf("ValidatePlatformRole() error = %v", err)
		}
	})
}

func TestBeginTenantTransactionEstablishesRLSContextAndRollsBackOnFailure(t *testing.T) {
	t.Parallel()
	if err := ScopeTenantTransaction(context.Background(), nil, "tenant-a"); err == nil {
		t.Fatal("ScopeTenantTransaction(nil) succeeded")
	}
	if _, err := BeginTenantTransaction(context.Background(), nil, "tenant-a"); err == nil {
		t.Fatal("BeginTenantTransaction(nil) succeeded")
	}
	database := newScriptedDB(&scriptedSQL{})
	if _, err := BeginTenantTransaction(context.Background(), database, " "); err == nil {
		database.Close()
		t.Fatal("BeginTenantTransaction() accepted blank tenant")
	}
	database.Close()

	t.Run("begin failure", func(t *testing.T) {
		wantErr := errors.New("begin unavailable")
		database := newScriptedDB(&scriptedSQL{beginErr: wantErr})
		defer database.Close()
		if _, err := BeginTenantTransaction(context.Background(), database, "tenant-a"); !errors.Is(err, wantErr) {
			t.Fatalf("begin error = %v", err)
		}
	})

	t.Run("set role failure rolls back", func(t *testing.T) {
		wantErr := errors.New("role denied")
		script := &scriptedSQL{execErrs: []error{wantErr}}
		database := newScriptedDB(script)
		defer database.Close()
		if _, err := BeginTenantTransaction(context.Background(), database, "tenant-a"); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "set tenant PostgreSQL role") {
			t.Fatalf("set role error = %v", err)
		}
		if len(script.execQueries) != 1 || script.execQueries[0] != "SET LOCAL ROLE trpc_tenant" || script.rollbacks != 1 {
			t.Fatalf("role failure script = queries=%#v rollbacks=%d", script.execQueries, script.rollbacks)
		}
	})

	t.Run("set tenant failure rolls back", func(t *testing.T) {
		wantErr := errors.New("tenant setting denied")
		script := &scriptedSQL{execErrs: []error{nil, wantErr}}
		database := newScriptedDB(script)
		defer database.Close()
		if _, err := BeginTenantTransaction(context.Background(), database, "tenant-a"); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "set tenant context") {
			t.Fatalf("set tenant error = %v", err)
		}
		if len(script.execQueries) != 2 || script.execQueries[0] != "SET LOCAL ROLE trpc_tenant" || script.execQueries[1] != "SELECT set_config('app.tenant_id', $1, true)" || script.rollbacks != 1 {
			t.Fatalf("tenant failure script = queries=%#v rollbacks=%d", script.execQueries, script.rollbacks)
		}
		if len(script.execArgs[1]) != 1 || script.execArgs[1][0].Value != "tenant-a" {
			t.Fatalf("tenant context args = %#v", script.execArgs[1])
		}
	})

	t.Run("success returns scoped transaction", func(t *testing.T) {
		script := &scriptedSQL{}
		database := newScriptedDB(script)
		defer database.Close()
		tx, err := BeginTenantTransaction(context.Background(), database, "tenant-a")
		if err != nil || tx == nil {
			t.Fatalf("BeginTenantTransaction() = %#v, %v", tx, err)
		}
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Fatal(err)
		}
		if len(script.execQueries) != 2 || script.rollbacks != 1 {
			t.Fatalf("success script = queries=%#v rollbacks=%d", script.execQueries, script.rollbacks)
		}
	})
}
