package reliable

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresReplayInboxRequiresActiveTenant(t *testing.T) {
	testPostgresReplayRejectsInactiveTenant(t, "inbox_messages", func(store *PostgresStore) error {
		return store.ReplayInbox(context.Background(), 41, "operator", "tenant reactivated")
	})
}

func TestPostgresReplayOutboxRequiresActiveTenant(t *testing.T) {
	testPostgresReplayRejectsInactiveTenant(t, "outbox_messages", func(store *PostgresStore) error {
		return store.ReplayOutbox(context.Background(), 42, "operator", "tenant reactivated")
	})
}

func testPostgresReplayRejectsInactiveTenant(t *testing.T, table string, replay func(*PostgresStore) error) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	query := regexp.QuoteMeta("JOIN tenants t ON t.id=") + ".*" + regexp.QuoteMeta("AND t.status='active' FOR UPDATE OF")
	expect := mock.ExpectQuery(query)
	if table == "outbox_messages" {
		expect = expect.WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg())
	} else {
		expect = expect.WithArgs(sqlmock.AnyArg())
	}
	expect.WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	err = replay(store)
	if err == nil || !strings.Contains(err.Error(), "not ") {
		t.Fatalf("replay error = %v, want ineligible replay error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%s replay did not enforce active tenant query: %v", table, err)
	}
}

func TestPostgresReplayOutboxForTenantAddsTenantPredicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("WHERE o.id=$1")+".*"+regexp.QuoteMeta("AND ($2='' OR o.tenant_id=$2)")).
		WithArgs(int64(42), "tenant-a").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if err := store.ReplayOutboxForTenant(context.Background(), "tenant-a", 42, "operator", "scope check"); err == nil {
		t.Fatal("scoped replay unexpectedly succeeded for missing tenant-scoped row")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
