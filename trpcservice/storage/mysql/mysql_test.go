package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	driver "github.com/go-sql-driver/mysql"
)

func TestMapErrorRedactsMySQLDriverCategories(t *testing.T) {
	notFound := errors.New("not found")
	duplicate := errors.New("duplicate")
	conflict := errors.New("conflict")
	invalid := errors.New("invalid")
	cases := []struct {
		name   string
		number uint16
		want   error
	}{
		{name: "duplicate", number: 1062, want: duplicate},
		{name: "deadlock", number: 1213, want: conflict},
		{name: "lock wait", number: 1205, want: conflict},
		{name: "foreign key", number: 1452, want: invalid},
		{name: "invalid value", number: 1366, want: invalid},
		{name: "check constraint", number: 3819, want: invalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := MapError(context.Background(), &driver.MySQLError{Number: test.number, Message: "secret host detail"}, notFound, duplicate, conflict, invalid); !errors.Is(got, test.want) {
				t.Fatalf("mapped error = %v, want %v", got, test.want)
			}
			if got := MapError(context.Background(), &driver.MySQLError{Number: test.number, Message: "secret host detail"}, notFound, duplicate, conflict, invalid); got.Error() == "secret host detail" {
				t.Fatal("driver detail leaked")
			}
		})
	}
	if got := MapError(context.Background(), sql.ErrNoRows, notFound, duplicate, conflict, invalid); !errors.Is(got, notFound) {
		t.Fatalf("not found mapping = %v", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := MapError(canceled, &driver.MySQLError{Number: 1062}, notFound, duplicate, conflict, invalid); !errors.Is(got, context.Canceled) {
		t.Fatalf("context mapping = %v", got)
	}
}

func TestBeginUsesDatabaseSQLTransactionAndRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	tx, err := Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	Rollback(tx)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeDSNAddsUTCParseTime(t *testing.T) {
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control"); got != "user:password@tcp(localhost:3306)/control?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27" {
		t.Fatalf("normalized DSN = %q", got)
	}
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control?parseTime=true"); got != "user:password@tcp(localhost:3306)/control?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27" {
		t.Fatalf("existing parameter DSN = %q", got)
	}
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control?parseTime=false&loc=Local&time_zone=SYSTEM&charset=utf8mb4"); got != "user:password@tcp(localhost:3306)/control?charset=utf8mb4&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27" {
		t.Fatalf("conflicting parameter DSN = %q", got)
	}
	if normalizeDSN(" ") != "" {
		t.Fatal("blank DSN was accepted")
	}
}

func TestStorageLifecycleAndNullableHelpers(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectPing()
	if err := Ping(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectCommit()
	tx, err := Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	mock.ExpectQuery("SELECT GET_LOCK").WithArgs("test-lock", 3).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
	if err := AcquireLock(context.Background(), conn, "test-lock", 3); err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	connTx, err := BeginConn(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	Rollback(connTx)
	mock.ExpectQuery("SELECT RELEASE_LOCK").WithArgs("test-lock").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
	if err := ReleaseLock(context.Background(), conn, "test-lock"); err != nil {
		t.Fatal(err)
	}
	if got := NullableInt(sql.NullInt64{Int64: 4, Valid: true}); got == nil || *got != 4 || NullableInt(sql.NullInt64{}) != nil {
		t.Fatal("nullable integer conversion failed")
	}
	if got := NullableString(sql.NullString{String: "value", Valid: true}); got == nil || *got != "value" || NullableString(sql.NullString{}) != nil {
		t.Fatal("nullable string conversion failed")
	}
	if NullableText(" ") != nil || NullableText("value") != "value" {
		t.Fatal("nullable text conversion failed")
	}
	previous := time.Now().UTC().Add(time.Second)
	if got := MonotonicNow(previous); got.Before(previous) || !AsUTC(got).Equal(got) {
		t.Fatal("timestamp helpers failed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyApplicationPrivilegesFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		row       any
		queryErr  error
		wantError bool
	}{
		{name: "no DDL privileges", row: 0},
		{name: "DDL privilege", row: 1, wantError: true},
		{name: "missing required DML privilege", row: 1, wantError: true},
		{name: "query failure", queryErr: errors.New("privileges unavailable"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
			mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
			expectation := mock.ExpectQuery("SELECT COUNT").WithArgs("'app'@'%'")
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(test.row))
			}
			if test.row == 0 && test.queryErr == nil {
				mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT USAGE ON *.* TO 'app'@'%'"))
			}
			err = VerifyApplicationPrivileges(context.Background(), db)
			if test.wantError && !errors.Is(err, ErrStorage) {
				t.Fatalf("verification error = %v", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("verification error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if !errors.Is(VerifyApplicationPrivileges(context.Background(), nil), ErrStorage) {
		t.Fatal("nil database was accepted")
	}
	if !errors.Is(VerifyApplicationPrivileges(nil, nil), ErrStorage) {
		t.Fatal("nil context was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if !errors.Is(VerifyApplicationPrivileges(canceled, db), context.Canceled) {
		t.Fatal("canceled context was not preserved")
	}
}

func TestVerifyApplicationPrivilegesQueryEnforcesFullAllowlist(t *testing.T) {
	var privilegeQuery string
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		if expected == "PRIVILEGE_QUERY" {
			privilegeQuery = actual
		}
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT DATABASE").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	mock.ExpectQuery("SELECT CURRENT_USER").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	mock.ExpectQuery("PRIVILEGE_QUERY").WithArgs("'app'@'%'").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT USAGE ON *.* TO 'app'@'%'"))
	if err := VerifyApplicationPrivileges(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"allowed_tables",
		"required_privilege_types",
		"required_privileges",
		"effective_table_privileges",
		"NOT EXISTS",
		"table_schema <> DATABASE()",
		"table_name NOT IN (SELECT table_name FROM allowed_tables)",
		"privilege_type NOT IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')",
		"is_grantable <> 'NO'",
		"information_schema.column_privileges",
		"information_schema.role_table_grants",
		"information_schema.role_column_grants",
		"information_schema.role_routine_grants",
		"information_schema.applicable_roles",
	} {
		if !strings.Contains(privilegeQuery, fragment) {
			t.Fatalf("privilege query missing %q:\n%s", fragment, privilegeQuery)
		}
	}
	if strings.Contains(privilegeQuery, "WHERE privilege_type IN ('ALL PRIVILEGES'") {
		t.Fatal("privilege query still applies the obsolete DDL-only outer filter")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentGranteeIdentityUsesLastAtAndEscapes(t *testing.T) {
	for _, test := range []struct {
		input, want string
	}{
		{input: "app@%", want: "'app'@'%'"},
		{input: "u@v@%", want: "'u@v'@'%'"},
		{input: "o\u0027conn@localhost", want: "'o\u0027\u0027conn'@'localhost'"},
		{input: " app@% ", want: "' app'@'% '"},
	} {
		got, err := currentGranteeIdentity(test.input)
		if err != nil || got != test.want {
			t.Fatalf("currentGranteeIdentity(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	for _, input := range []string{"", "app", "app@"} {
		if _, err := currentGranteeIdentity(input); !errors.Is(err, ErrStorage) {
			t.Fatalf("currentGranteeIdentity(%q) error = %v, want ErrStorage", input, err)
		}
	}
}

func TestVerifyApplicationPrivilegesRejectsDirectRoutineGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	mock.ExpectQuery("SELECT COUNT").WithArgs("'app'@'%'").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT EXECUTE ON PROCEDURE `control_plane`.`unsafe` TO 'app'@'%'").AddRow("GRANT USAGE ON *.* TO 'app'@'%'"))
	if err := VerifyApplicationPrivileges(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("routine grant verification error = %v, want ErrStorage", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyApplicationPrivilegesRejectsDirectProxyGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	mock.ExpectQuery("SELECT COUNT").WithArgs("'app'@'%'").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT PROXY ON ''@'' TO 'app'@'%'"))
	if err := VerifyApplicationPrivileges(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("proxy grant verification error = %v, want ErrStorage", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentUserRedactsFailuresAndRejectsBlankIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		row       any
		queryErr  error
		wantError bool
	}{
		{name: "valid", row: "app@%"},
		{name: "blank", row: "  ", wantError: true},
		{name: "query failure", queryErr: errors.New("identity unavailable"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			expectation := mock.ExpectQuery("SELECT CURRENT_USER\\(\\)")
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow(test.row))
			}
			got, err := CurrentUser(context.Background(), db)
			if test.wantError {
				if !errors.Is(err, ErrStorage) || got != "" {
					t.Fatalf("identity result = %q, err=%v", got, err)
				}
			} else if err != nil || got != "app@%" {
				t.Fatalf("identity result = %q, err=%v", got, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := CurrentUser(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatal("nil database was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := CurrentUser(canceled, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled identity lookup = %v", err)
	}
}

func TestCurrentDatabaseRejectsMissingSchemaAndRedactsFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		row       any
		queryErr  error
		wantError bool
	}{
		{name: "valid", row: "control_plane"},
		{name: "blank", row: "  ", wantError: true},
		{name: "null", row: nil, wantError: true},
		{name: "query failure", queryErr: errors.New("database unavailable"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			expectation := mock.ExpectQuery("SELECT DATABASE\\(\\)")
			if test.queryErr != nil {
				expectation.WillReturnError(test.queryErr)
			} else {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow(test.row))
			}
			got, err := CurrentDatabase(context.Background(), db)
			if test.wantError {
				if !errors.Is(err, ErrStorage) || got != "" {
					t.Fatalf("database result = %q, err=%v", got, err)
				}
			} else if err != nil || got != "control_plane" {
				t.Fatalf("database result = %q, err=%v", got, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := CurrentDatabase(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatal("nil database was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := CurrentDatabase(canceled, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled database lookup = %v", err)
	}
}

func TestStorageRejectsInvalidContextsAndDSNs(t *testing.T) {
	if _, err := Open(context.Background(), "", Options{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("blank DSN error = %v", err)
	}
	if _, err := Open(nil, "dsn", Options{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Open context error = %v", err)
	}
	if err := Ping(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Ping error = %v", err)
	}
	if _, err := Begin(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Begin error = %v", err)
	}
	if _, err := BeginConn(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil BeginConn error = %v", err)
	}
	if err := AcquireLock(context.Background(), nil, "name", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil AcquireLock error = %v", err)
	}
	if err := ReleaseLock(context.Background(), nil, "name"); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil ReleaseLock error = %v", err)
	}
}

func TestJSONCodecRejectsMalformedUnknownAndTrailingValues(t *testing.T) {
	if _, err := EncodeJSON(func() {}); err == nil {
		t.Fatal("unsupported value was encoded")
	}
	payload, err := EncodeJSON(map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]string
	if err := DecodeJSON(payload, &decoded); err != nil || decoded["key"] != "value" {
		t.Fatalf("decoded payload = %#v, err=%v", decoded, err)
	}
	for _, data := range [][]byte{[]byte("not-json"), []byte(`{"known":"value","unknown":true}`), []byte(`{} {}`)} {
		if err := DecodeJSON(data, &map[string]string{}); !errors.Is(err, ErrStorage) {
			t.Fatalf("invalid JSON %q error = %v", data, err)
		}
	}
	var empty map[string]any
	if err := DecodeJSON(nil, &empty); err != nil {
		t.Fatalf("empty JSON error = %v", err)
	}
}

func TestStorageErrorPathsRedactAndPreserveContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := Open(ctx, "user:pass@tcp([::1)/control", Options{MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute}); err == nil {
		t.Fatal("malformed DSN unexpectedly opened")
	}
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectPing().WillReturnError(errors.New("ping failure"))
	if err := Ping(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("ping failure = %v", err)
	}
	mock.ExpectBegin().WillReturnError(errors.New("begin failure"))
	if _, err := Begin(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("begin failure = %v", err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	mock.ExpectBegin().WillReturnError(errors.New("conn begin failure"))
	if _, err := BeginConn(context.Background(), conn); !errors.Is(err, ErrStorage) {
		t.Fatalf("conn begin failure = %v", err)
	}
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(0))
	if err := AcquireLock(context.Background(), conn, "lock", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("unacquired lock = %v", err)
	}
	mock.ExpectQuery("SELECT GET_LOCK").WillReturnError(errors.New("lock query"))
	if err := AcquireLock(context.Background(), conn, "lock", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("lock query failure = %v", err)
	}
	mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(0))
	if err := ReleaseLock(context.Background(), conn, "lock"); !errors.Is(err, ErrStorage) {
		t.Fatalf("unreleased lock = %v", err)
	}
	mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnError(errors.New("release query"))
	if err := ReleaseLock(context.Background(), conn, "lock"); !errors.Is(err, ErrStorage) {
		t.Fatalf("release query failure = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("commit failure"))
	tx, err := Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(context.Background(), tx); !errors.Is(err, ErrStorage) {
		t.Fatalf("commit failure = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err = Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := Commit(canceled, tx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit = %v", err)
	}
	if err := Commit(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil commit = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
