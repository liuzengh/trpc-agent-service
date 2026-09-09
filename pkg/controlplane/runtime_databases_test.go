package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRuntimeDatabasesPartitionTotalBudgetAndCloseEachPool(t *testing.T) {
	for _, options := range []RuntimeDatabaseOptions{{25, true}, {25, false}, {10, false}, {9, true}, {300, true}} {
		var mocks []sqlmock.Sqlmock
		open := func(driver, dsn string) (*sql.DB, error) {
			if driver != "postgres" || dsn != "same-authority" {
				t.Fatal("pools changed database authority")
			}
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				return nil, err
			}
			mock.ExpectPing()
			mock.ExpectClose()
			mocks = append(mocks, mock)
			return db, nil
		}
		pools, err := openRuntimeDatabases(context.Background(), "same-authority", options, open)
		if err != nil {
			t.Fatal(err)
		}
		if pools.Control == pools.MigrationGate || (options.ExecutionFencing && (pools.Execution == pools.Control || pools.Execution == pools.MigrationGate)) {
			t.Fatal("nested locks share a pool")
		}
		total := pools.Control.Stats().MaxOpenConnections + pools.MigrationGate.Stats().MaxOpenConnections
		if options.ExecutionFencing {
			total += pools.Execution.Stats().MaxOpenConnections
		} else if pools.Execution != nil {
			t.Fatal("unused execution pool was opened")
		}
		if total != options.MaxConnections || pools.Control.Stats().MaxOpenConnections < 3 {
			t.Fatalf("budget=%d expected=%d", total, options.MaxConnections)
		}
		if err := pools.Close(); err != nil {
			t.Fatal(err)
		}
		if err := pools.Close(); err != nil {
			t.Fatal(err)
		}
		for _, db := range []*sql.DB{pools.Control, pools.Execution, pools.MigrationGate} {
			if db != nil && db.Ping() == nil {
				t.Fatal("closed runtime retained a live pool")
			}
		}
		for _, mock := range mocks {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRuntimeDatabasesCleanUpPartialStartupWithoutLeakingCredentials(t *testing.T) {
	for _, failure := range []string{"open", "ping"} {
		t.Run(failure, func(t *testing.T) {
			var mocks []sqlmock.Sqlmock
			calls := 0
			open := func(string, string) (*sql.DB, error) {
				calls++
				if calls == 3 && failure == "open" {
					return nil, errors.New("test-only-password")
				}
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					return nil, err
				}
				ping := mock.ExpectPing()
				if calls == 3 {
					ping.WillReturnError(errors.New("test-only-password"))
				}
				mock.ExpectClose()
				mocks = append(mocks, mock)
				return db, nil
			}
			if _, err := openRuntimeDatabases(context.Background(), "same-authority", RuntimeDatabaseOptions{25, true}, open); err == nil || strings.Contains(err.Error(), "test-only-password") {
				t.Fatalf("startup error=%v", err)
			}
			for _, mock := range mocks {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRuntimeDatabaseBudgetRejectsInvalidConfiguration(t *testing.T) {
	for _, value := range []string{"0", "8", "301", "-1", "invalid", "999999999999999999999"} {
		if _, err := ParseRuntimeDatabaseBudget(value, 25); err == nil {
			t.Fatalf("invalid budget %q accepted", value)
		}
	}
	if got, err := ParseRuntimeDatabaseBudget("", 25); err != nil || got != 25 {
		t.Fatalf("default budget=%d error=%v", got, err)
	}
	if got, err := ParseRuntimeDatabaseBudget("60", 25); err != nil || got != 60 {
		t.Fatalf("configured budget=%d error=%v", got, err)
	}
}
