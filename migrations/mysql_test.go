package migrations

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestSplitMySQLStatementsRespectsQuotedSemicolonsAndComments(t *testing.T) {
	script := `-- header;
CREATE TABLE example (value VARCHAR(32) DEFAULT 'a;b');
/* block; comment */
ALTER TABLE example ADD COLUMN quoted VARCHAR(32) DEFAULT "x;y";
`
	statements := splitMySQLStatements(script)
	if len(statements) != 2 {
		t.Fatalf("statement count = %d (%#v)", len(statements), statements)
	}
	if !strings.Contains(statements[0], "CREATE TABLE") || !strings.Contains(statements[0], "'a;b'") {
		t.Fatalf("first statement = %q", statements[0])
	}
	if !strings.Contains(statements[1], "ALTER TABLE") || !strings.Contains(statements[1], `"x;y"`) {
		t.Fatalf("second statement = %q", statements[1])
	}
}

func TestSplitMySQLStatementsRespectsCompoundTriggerBodies(t *testing.T) {
	script := `CREATE TRIGGER example_guard BEFORE INSERT ON example
FOR EACH ROW
BEGIN
  IF NEW.value IS NULL THEN
    SET NEW.value = 'a;b';
  END IF;
END;
CREATE TABLE after_trigger (id INT);`
	statements := splitMySQLStatements(script)
	if len(statements) != 2 || !strings.Contains(statements[0], "END IF;") || !strings.Contains(statements[1], "CREATE TABLE") {
		t.Fatalf("compound statements = %#v", statements)
	}
}

func TestSplitMySQLStatementsDoesNotTreatDDLIfAsCompound(t *testing.T) {
	script := `CREATE TABLE IF NOT EXISTS example (id INT);
ALTER TABLE example ADD COLUMN value_text VARCHAR(8);`
	statements := splitMySQLStatements(script)
	if len(statements) != 2 || !strings.HasPrefix(statements[0], "CREATE TABLE IF NOT EXISTS") || !strings.HasPrefix(statements[1], "ALTER TABLE") {
		t.Fatalf("DDL statements = %#v", statements)
	}
}

func TestMySQLMigrationSetUsesBinaryIdentityAndRecoveryMarkers(t *testing.T) {
	files, err := orderedMySQLFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 || files[0].version != 1 || files[1].version != 2 || files[2].version != 3 || files[3].version != 4 {
		t.Fatalf("MySQL files = %#v", files)
	}
	script := files[0].statements
	joined := strings.Join(script, "\n")
	canaryScript := strings.Join(files[1].statements, "\n")
	aibotScript := strings.Join(files[2].statements, "\n")
	if !strings.Contains(canaryScript, "ADD COLUMN canary_revision") || len(files[1].statements) != 4 {
		t.Fatalf("canary migration is not checkpointed for replay: %q", canaryScript)
	}
	if !strings.Contains(aibotScript, "wecom_aibot") || len(files[2].statements) != 2 {
		t.Fatalf("AI Bot migration is not checkpointed for replay: %q", aibotScript)
	}
	for _, fragment := range []string{"utf8mb4_bin", "active_provider_account_id", "channel_binding_candidate_idx", "ENGINE=InnoDB"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("migration is missing %q", fragment)
		}
	}
	if strings.Contains(joined, "REPEATABLE READ") || strings.Contains(joined, "SECURITY DEFINER") {
		t.Fatal("MySQL migration contains an unsupported transaction/function contract")
	}
}

func TestMySQLHistoryAndArgumentHelpers(t *testing.T) {
	files, err := orderedMySQLFiles()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLHistory(nil, nil); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("empty migration set error = %v", err)
	}
	if err := validateMySQLHistory(map[int]mysqlMigrationHistory{0: {status: "applied"}}, files); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("zero history version error = %v", err)
	}
	if err := validateMySQLHistory(map[int]mysqlMigrationHistory{5: {status: "applied"}}, files); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("future history version error = %v", err)
	}
	if err := validateMySQLHistory(map[int]mysqlMigrationHistory{1: {status: "unknown"}}, files); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("unknown history status error = %v", err)
	}
	for _, status := range []string{"applied", "applying", "failed"} {
		if err := validateMySQLHistory(map[int]mysqlMigrationHistory{1: {status: status}}, files); err != nil {
			t.Fatalf("valid history status %q: %v", status, err)
		}
	}
	if got := nextMySQLVersion(map[int]mysqlMigrationHistory{4: {}, 2: {}}); got != 5 {
		t.Fatalf("next MySQL version = %d, want 5", got)
	}
	if got := nextMySQLVersion(nil); got != 1 {
		t.Fatalf("next empty MySQL version = %d, want 1", got)
	}
	tableArgs := mysqlTableArgs()
	if len(tableArgs) != len(requiredMySQLTables) || tableArgs[0] != "schema_migrations" || tableArgs[len(tableArgs)-1] != "tenant_configuration_outbox" {
		t.Fatalf("MySQL table args = %#v", tableArgs)
	}
	indexArgs := mysqlIndexArgs()
	if len(indexArgs) != len(requiredMySQLIndexes)*2 || indexArgs[0] != "tenant" || indexArgs[1] != "tenant_key_idx" {
		t.Fatalf("MySQL index args = %#v", indexArgs)
	}
}

func TestMySQLAlreadyAppliedErrors(t *testing.T) {
	if isMySQLAlreadyApplied(nil) || isMySQLAlreadyApplied(errors.New("ordinary")) {
		t.Fatal("ordinary errors were treated as already applied")
	}
	for _, number := range []uint16{1022, 1050, 1060, 1061, 1359, 1826} {
		err := fmt.Errorf("wrapped: %w", &mysqldriver.MySQLError{Number: number, Message: "duplicate"})
		if !isMySQLAlreadyApplied(err) {
			t.Fatalf("MySQL error %d was not recognized", number)
		}
	}
	if isMySQLAlreadyApplied(&mysqldriver.MySQLError{Number: 1062, Message: "duplicate key"}) {
		t.Fatal("unrelated duplicate-key error was treated as idempotent DDL")
	}
}

func TestMySQLStatementAndHistorySQLHelpers(t *testing.T) {
	db, mock, conn := newMySQLMockConn(t)
	defer closeMySQLMock(t, db, conn, mock)
	migration := mysqlMigrationFile{version: 1, name: "0001_test.sql", digest: strings.Repeat("a", 64), statements: []string{"CREATE TABLE test (id INT)"}}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO schema_migrations(version, name, sha256, status, statement_index, error_text) VALUES (?, ?, ?, ?, ?, ?)")).WithArgs(migration.version, migration.name, migration.digest, "applying", 0, "").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := insertMySQLHistory(context.Background(), conn, migration, mysqlMigrationHistory{status: "applying"}); err != nil {
		t.Fatalf("insert history = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?")).WithArgs("applying", 1, "", nil, migration.version).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := updateMySQLCheckpoint(context.Background(), conn, migration.version, 1, "applying", ""); err != nil {
		t.Fatalf("update applying checkpoint = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?")).WithArgs("applied", 1, "", sqlmock.AnyArg(), migration.version).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := updateMySQLCheckpoint(context.Background(), conn, migration.version, 1, "applied", ""); err != nil {
		t.Fatalf("update applied checkpoint = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'failed', statement_index = ?, error_text = ? WHERE version = ?")).WithArgs(0, "statement failed", migration.version).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := updateMySQLFailure(context.Background(), conn, migration.version, 0); err != nil {
		t.Fatalf("update failure checkpoint = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLHistoryReadAndEnsureErrors(t *testing.T) {
	db, mock, conn := newMySQLMockConn(t)
	defer closeMySQLMock(t, db, conn, mock)
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := ensureMySQLHistory(context.Background(), conn); err != nil {
		t.Fatalf("ensure history = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).
			AddRow(1, "0001_test.sql", strings.Repeat("a", 64), "applied", 2, ""))
	history, err := readMySQLHistory(context.Background(), conn)
	if err != nil {
		t.Fatalf("read history = %v", err)
	}
	if history[1].statementIndex != 2 || history[1].status != "applied" {
		t.Fatalf("history = %#v", history)
	}
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnError(errors.New("create failed"))
	if err := ensureMySQLHistory(context.Background(), conn); !errors.Is(err, ErrMigration) {
		t.Fatalf("ensure error = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")).WillReturnError(errors.New("read failed"))
	if _, err := readMySQLHistory(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("read query error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLHistoryRowsAndCheckpointErrors(t *testing.T) {
	db, mock, conn := newMySQLMockConn(t)
	defer closeMySQLMock(t, db, conn, mock)
	query := regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).
		AddRow(1, "0001_test.sql", strings.Repeat("a", 64), "applied", "not-an-int", ""))
	if _, err := readMySQLHistory(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("history scan error = %v", err)
	}
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).
		AddRow(1, "0001_test.sql", strings.Repeat("a", 64), "applied", 0, "").RowError(0, errors.New("row failed")))
	if _, err := readMySQLHistory(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("history rows error = %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?")).
		WithArgs("applied", 1, "", sqlmock.AnyArg(), 1).WillReturnError(errors.New("checkpoint failed"))
	if err := updateMySQLCheckpoint(context.Background(), conn, 1, 1, "applied", ""); err == nil {
		t.Fatal("checkpoint error was swallowed")
	}
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'failed', statement_index = ?, error_text = ? WHERE version = ?")).
		WithArgs(0, "statement failed", 1).WillReturnError(errors.New("failure checkpoint failed"))
	if err := updateMySQLFailure(context.Background(), conn, 1, 0); err == nil {
		t.Fatal("failure checkpoint error was swallowed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyOneMySQLMigrationLifecycleAndRecovery(t *testing.T) {
	migration := mysqlMigrationFile{version: 1, name: "0001_test.sql", digest: strings.Repeat("b", 64), statements: []string{"CREATE TABLE one (id INT)", "ALTER TABLE one ADD value_text VARCHAR(8)"}}
	t.Run("new migration", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{}
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO schema_migrations(version, name, sha256, status, statement_index, error_text) VALUES (?, ?, ?, ?, ?, ?)")).WithArgs(1, migration.name, migration.digest, "applying", 0, "").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("CREATE TABLE one").WillReturnResult(sqlmock.NewResult(0, 0))
		expectMySQLApplyingCheckpoint(mock, 1, 1)
		mock.ExpectExec("ALTER TABLE one").WillReturnResult(sqlmock.NewResult(0, 0))
		expectMySQLApplyingCheckpoint(mock, 1, 2)
		expectMySQLAppliedCheckpoint(mock, 1, 2)
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); err != nil {
			t.Fatalf("apply migration = %v", err)
		}
		entry := history[1]
		if entry.status != "applied" || entry.statementIndex != 2 || entry.digest != migration.digest {
			t.Fatalf("history entry = %#v", entry)
		}
	})
	t.Run("resume failed migration", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {name: migration.name, digest: migration.digest, status: "failed", statementIndex: 1}}
		mock.ExpectExec("ALTER TABLE one").WillReturnResult(sqlmock.NewResult(0, 0))
		expectMySQLApplyingCheckpoint(mock, 1, 2)
		expectMySQLAppliedCheckpoint(mock, 1, 2)
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); err != nil {
			t.Fatalf("resume migration = %v", err)
		}
		if history[1].status != "applied" {
			t.Fatalf("resumed history = %#v", history[1])
		}
	})
	t.Run("already applied is a no-op", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {digest: migration.digest, status: "applied", statementIndex: 2}}
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); err != nil {
			t.Fatalf("already applied migration = %v", err)
		}
	})
	t.Run("invalid history is rejected", func(t *testing.T) {
		cases := []struct {
			name    string
			history map[int]mysqlMigrationHistory
			want    error
		}{
			{"digest", map[int]mysqlMigrationHistory{1: {digest: "wrong", status: "applied"}}, ErrInvalidHistory},
			{"unknown status", map[int]mysqlMigrationHistory{1: {digest: migration.digest, status: "stale"}}, ErrInvalidHistory},
			{"checkpoint", map[int]mysqlMigrationHistory{1: {digest: migration.digest, status: "failed", statementIndex: 3}}, ErrInvalidHistory},
			{"non-next", map[int]mysqlMigrationHistory{2: {digest: migration.digest, status: "applied"}}, ErrInvalidHistory},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, conn := newMySQLMockConn(t)
				defer closeMySQLMock(t, db, conn, mock)
				if err := applyOneMySQLMigration(context.Background(), conn, tc.history, migration); !errors.Is(err, tc.want) {
					t.Fatalf("error = %v, want %v", err, tc.want)
				}
			})
		}
	})
	t.Run("insert failure", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO schema_migrations(version, name, sha256, status, statement_index, error_text) VALUES (?, ?, ?, ?, ?, ?)")).WithArgs(1, migration.name, migration.digest, "applying", 0, "").WillReturnError(errors.New("insert failed"))
		if err := applyOneMySQLMigration(context.Background(), conn, map[int]mysqlMigrationHistory{}, migration); !errors.Is(err, ErrMigration) {
			t.Fatalf("insert error = %v", err)
		}
	})
	t.Run("statement failure marks checkpoint", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {name: migration.name, digest: migration.digest, status: "applying", statementIndex: 0}}
		mock.ExpectExec("CREATE TABLE one").WillReturnError(errors.New("statement failed"))
		mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'failed', statement_index = ?, error_text = ? WHERE version = ?")).WithArgs(0, "statement failed", 1).WillReturnResult(sqlmock.NewResult(0, 1))
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); !errors.Is(err, ErrMigration) {
			t.Fatalf("statement error = %v", err)
		}
	})
	t.Run("already applied statement continues", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {name: migration.name, digest: migration.digest, status: "applying", statementIndex: 0}}
		mock.ExpectExec("CREATE TABLE one").WillReturnError(&mysqldriver.MySQLError{Number: 1050, Message: "already exists"})
		expectMySQLApplyingCheckpoint(mock, 1, 1)
		mock.ExpectExec("ALTER TABLE one").WillReturnResult(sqlmock.NewResult(0, 0))
		expectMySQLApplyingCheckpoint(mock, 1, 2)
		expectMySQLAppliedCheckpoint(mock, 1, 2)
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); err != nil {
			t.Fatalf("idempotent statement error = %v", err)
		}
	})
	t.Run("checkpoint failure", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {name: migration.name, digest: migration.digest, status: "applying", statementIndex: 0}}
		mock.ExpectExec("CREATE TABLE one").WillReturnResult(sqlmock.NewResult(0, 0))
		expectMySQLApplyingCheckpointError(mock, 1, 1)
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); !errors.Is(err, ErrMigration) {
			t.Fatalf("checkpoint error = %v", err)
		}
	})
	t.Run("final checkpoint failure", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		history := map[int]mysqlMigrationHistory{1: {name: migration.name, digest: migration.digest, status: "applying", statementIndex: 2}}
		expectMySQLAppliedCheckpointError(mock, 1, 2)
		if err := applyOneMySQLMigration(context.Background(), conn, history, migration); !errors.Is(err, ErrMigration) {
			t.Fatalf("final checkpoint error = %v", err)
		}
	})
}

func expectMySQLApplyingCheckpoint(mock sqlmock.Sqlmock, version, index int) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?")).
		WithArgs("applying", index, "", nil, version).WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectMySQLAppliedCheckpoint(mock sqlmock.Sqlmock, version, index int) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ? WHERE version = ?")).
		WithArgs("applied", index, "", sqlmock.AnyArg(), version).WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectMySQLApplyingCheckpointError(mock sqlmock.Sqlmock, version, index int) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ?")).
		WithArgs("applying", index, "", nil, version).WillReturnError(errors.New("checkpoint failed"))
}

func expectMySQLAppliedCheckpointError(mock sqlmock.Sqlmock, version, index int) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = ?, statement_index = ?, error_text = ?, applied_at = ?")).
		WithArgs("applied", index, "", sqlmock.AnyArg(), version).WillReturnError(errors.New("checkpoint failed"))
}

func TestVerifyMySQLSchemaAndIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		expectMySQLSchemaRows(mock, requiredMySQLTables)
		expectMySQLIndexRows(mock, requiredMySQLIndexes)
		expectMySQLTriggerRows(mock, requiredMySQLTriggers)
		if err := verifyMySQLSchema(context.Background(), conn); err != nil {
			t.Fatalf("verify schema = %v", err)
		}
	})
	t.Run("table query and row validation", func(t *testing.T) {
		cases := []struct {
			name string
			rows *sqlmock.Rows
		}{
			{"query error", nil},
			{"scan error", sqlmock.NewRows([]string{"table_name", "engine", "table_collation"}).AddRow(1, "InnoDB", "utf8mb4_bin")},
			{"wrong engine", sqlmock.NewRows([]string{"table_name", "engine", "table_collation"}).AddRow("schema_migrations", "MyISAM", "utf8mb4_bin")},
			{"missing table", sqlmock.NewRows([]string{"table_name", "engine", "table_collation"}).AddRow("schema_migrations", "InnoDB", "utf8mb4_bin")},
			{"row error", sqlmock.NewRows([]string{"table_name", "engine", "table_collation"}).AddRow("schema_migrations", "InnoDB", "utf8mb4_bin").RowError(0, errors.New("row failed"))},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, conn := newMySQLMockConn(t)
				defer closeMySQLMock(t, db, conn, mock)
				query := `SELECT table_name, engine, table_collation
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
				if tc.rows == nil {
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlTableArgs())...).WillReturnError(errors.New("table query failed"))
				} else {
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlTableArgs())...).WillReturnRows(tc.rows)
				}
				if err := verifyMySQLSchema(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
					t.Fatalf("schema error = %v", err)
				}
			})
		}
	})
	t.Run("index query and row validation", func(t *testing.T) {
		cases := []struct {
			name string
			rows *sqlmock.Rows
		}{
			{"query error", nil},
			{"scan error", sqlmock.NewRows([]string{"table_name", "index_name", "non_unique", "seq_in_index", "column_name", "sub_part"}).AddRow("tenant", "tenant_key_idx", "bad", 1, "tenant_key", nil)},
			{"wrong uniqueness", sqlmock.NewRows([]string{"table_name", "index_name", "non_unique", "seq_in_index", "column_name", "sub_part"}).AddRow("tenant", "tenant_key_idx", 1, 1, "tenant_key", nil)},
			{"missing index", sqlmock.NewRows([]string{"table_name", "index_name", "non_unique", "seq_in_index", "column_name", "sub_part"})},
			{"row error", sqlmock.NewRows([]string{"table_name", "index_name", "non_unique", "seq_in_index", "column_name", "sub_part"}).AddRow("tenant", "tenant_key_idx", 0, 1, "tenant_key", nil).RowError(0, errors.New("row failed"))},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, conn := newMySQLMockConn(t)
				defer closeMySQLMock(t, db, conn, mock)
				query := `SELECT table_name, index_name, non_unique, seq_in_index, column_name, sub_part
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND
		((table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?))`
				if tc.rows == nil {
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlIndexArgs())...).WillReturnError(errors.New("index query failed"))
				} else {
					mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlIndexArgs())...).WillReturnRows(tc.rows)
				}
				if err := verifyMySQLIndexes(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
					t.Fatalf("index error = %v", err)
				}
			})
		}
	})
}

func TestVerifyMySQLTriggersRejectsMetadataAndBodyDrift(t *testing.T) {
	query := `SELECT trigger_name, event_manipulation, event_object_table,
		action_timing, action_statement
		FROM information_schema.triggers
		WHERE trigger_schema = DATABASE() AND trigger_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, test := range []struct {
		name   string
		mutate func(*mysqlSchemaTrigger)
	}{
		{name: "wrong table", mutate: func(trigger *mysqlSchemaTrigger) { trigger.table = "tenant" }},
		{name: "wrong event", mutate: func(trigger *mysqlSchemaTrigger) { trigger.event = "DELETE" }},
		{name: "wrong timing", mutate: func(trigger *mysqlSchemaTrigger) { trigger.timing = "AFTER" }},
		{name: "incomplete body", mutate: func(trigger *mysqlSchemaTrigger) { trigger.actionFragments = []string{"SIGNAL SQLSTATE '45000'"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, conn := newMySQLMockConn(t)
			defer closeMySQLMock(t, db, conn, mock)
			triggers := append([]mysqlSchemaTrigger(nil), requiredMySQLTriggers...)
			test.mutate(&triggers[0])
			rows := sqlmock.NewRows([]string{"trigger_name", "event_manipulation", "event_object_table", "action_timing", "action_statement"})
			for _, trigger := range triggers {
				rows.AddRow(trigger.name, trigger.event, trigger.table, trigger.timing, strings.Join(trigger.actionFragments, "\n"))
			}
			mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlTriggerArgs())...).WillReturnRows(rows)
			if err := verifyMySQLTriggers(context.Background(), conn); !errors.Is(err, ErrInvalidHistory) {
				t.Fatalf("trigger verification error = %v", err)
			}
		})
	}
}

func expectMySQLSchemaRows(mock sqlmock.Sqlmock, tables []mysqlSchemaTable) {
	query := `SELECT table_name, engine, table_collation
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	rows := sqlmock.NewRows([]string{"table_name", "engine", "table_collation"})
	for _, table := range tables {
		rows.AddRow(table.name, "InnoDB", "utf8mb4_bin")
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlTableArgs())...).WillReturnRows(rows)
}

func expectMySQLIndexRows(mock sqlmock.Sqlmock, indexes []mysqlSchemaIndex) {
	query := `SELECT table_name, index_name, non_unique, seq_in_index, column_name, sub_part
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND
		((table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?) OR
		 (table_name = ? AND index_name = ?) OR (table_name = ? AND index_name = ?))`
	rows := sqlmock.NewRows([]string{"table_name", "index_name", "non_unique", "seq_in_index", "column_name", "sub_part"})
	for _, index := range indexes {
		nonUnique := 1
		if index.unique {
			nonUnique = 0
		}
		for sequence, column := range index.columns {
			rows.AddRow(index.table, index.name, nonUnique, sequence+1, column, nil)
		}
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlIndexArgs())...).WillReturnRows(rows)
}

func expectMySQLTriggerRows(mock sqlmock.Sqlmock, triggers []mysqlSchemaTrigger) {
	query := `SELECT trigger_name, event_manipulation, event_object_table,
		action_timing, action_statement
		FROM information_schema.triggers
		WHERE trigger_schema = DATABASE() AND trigger_name IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	rows := sqlmock.NewRows([]string{"trigger_name", "event_manipulation", "event_object_table", "action_timing", "action_statement"})
	for _, trigger := range triggers {
		rows.AddRow(trigger.name, trigger.event, trigger.table, trigger.timing, strings.Join(trigger.actionFragments, "\n"))
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(mysqlMockArgs(mysqlTriggerArgs())...).WillReturnRows(rows)
}

func TestVerifyMySQLHistoryAndSchema(t *testing.T) {
	files, err := orderedMySQLFiles()
	if err != nil {
		t.Fatal(err)
	}
	historyQuery := regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")
	t.Run("success", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		rows := sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"})
		for _, file := range files {
			rows.AddRow(file.version, file.name, file.digest, "applied", len(file.statements), "")
		}
		mock.ExpectQuery(historyQuery).WillReturnRows(rows)
		expectMySQLSchemaRows(mock, requiredMySQLTables)
		expectMySQLIndexRows(mock, requiredMySQLIndexes)
		expectMySQLTriggerRows(mock, requiredMySQLTriggers)
		if err := VerifyMySQL(context.Background(), db); err != nil {
			t.Fatalf("VerifyMySQL = %v", err)
		}
	})
	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{"missing history", sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"})},
		{"wrong state", sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).AddRow(files[0].version, files[0].name, files[0].digest, "applying", len(files[0].statements), "")},
		{"wrong digest", sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).AddRow(files[0].version, files[0].name, "wrong", "applied", len(files[0].statements), "")},
		{"wrong checkpoint", sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}).AddRow(files[0].version, files[0].name, files[0].digest, "applied", 0, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, conn := newMySQLMockConn(t)
			defer closeMySQLMock(t, db, conn, mock)
			mock.ExpectQuery(historyQuery).WillReturnRows(tc.rows)
			if err := VerifyMySQL(context.Background(), db); !errors.Is(err, ErrInvalidHistory) {
				t.Fatalf("VerifyMySQL error = %v", err)
			}
		})
	}
	t.Run("history read failure", func(t *testing.T) {
		db, mock, conn := newMySQLMockConn(t)
		defer closeMySQLMock(t, db, conn, mock)
		mock.ExpectQuery(historyQuery).WillReturnError(errors.New("history read failed"))
		if err := VerifyMySQL(context.Background(), db); !errors.Is(err, ErrInvalidHistory) {
			t.Fatalf("VerifyMySQL read error = %v", err)
		}
	})
}

func TestApplyMySQLWithSQLMock(t *testing.T) {
	files, err := orderedMySQLFiles()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("applies all statements and releases the connection lock", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).WithArgs(mysqlMigrationLock, 30).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")).
			WillReturnRows(sqlmock.NewRows([]string{"version", "name", "sha256", "status", "statement_index", "error_text"}))
		for _, migration := range files {
			mock.ExpectExec(regexp.QuoteMeta("INSERT INTO schema_migrations(version, name, sha256, status, statement_index, error_text) VALUES (?, ?, ?, ?, ?, ?)")).WithArgs(migration.version, migration.name, migration.digest, "applying", 0, "").WillReturnResult(sqlmock.NewResult(0, 1))
			for index, statement := range migration.statements {
				mock.ExpectExec("(?s).*" + regexp.QuoteMeta(strings.TrimSpace(statement[:minMySQLTestInt(len(statement), 24)]))).WillReturnResult(sqlmock.NewResult(0, 0))
				expectMySQLApplyingCheckpoint(mock, migration.version, index+1)
			}
			expectMySQLAppliedCheckpoint(mock, migration.version, len(migration.statements))
		}
		mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs(mysqlMigrationLock).WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		if err := ApplyMySQL(context.Background(), db); err != nil {
			t.Fatalf("ApplyMySQL = %v", err)
		}
	})
	t.Run("rejects lock and history failures", func(t *testing.T) {
		tests := []struct {
			name string
			set  func(sqlmock.Sqlmock)
		}{
			{"lock failure", func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).WithArgs(mysqlMigrationLock, 30).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(0))
			}},
			{"history table failure", func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).WithArgs(mysqlMigrationLock, 30).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnError(errors.New("history table failed"))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs(mysqlMigrationLock).WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
			}},
			{"history read failure", func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).WithArgs(mysqlMigrationLock, 30).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT version, name, sha256, status, statement_index, error_text FROM schema_migrations ORDER BY version")).WillReturnError(errors.New("history read failed"))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs(mysqlMigrationLock).WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
			}},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				tc.set(mock)
				if err := ApplyMySQL(context.Background(), db); !errors.Is(err, ErrMigration) && !errors.Is(err, ErrInvalidHistory) {
					t.Fatalf("ApplyMySQL error = %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func minMySQLTestInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newMySQLMockConn(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *sql.Conn) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, mock, conn
}

func mysqlMockArgs(values []any) []sqldriver.Value {
	args := make([]sqldriver.Value, len(values))
	for i, value := range values {
		args[i] = value
	}
	return args
}

func closeMySQLMock(t *testing.T, db *sql.DB, conn *sql.Conn, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
	_ = db.Close()
}
