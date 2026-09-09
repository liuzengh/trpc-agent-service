package reliable

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const activeLeaseClockPredicate = `(?s)UPDATE (inbox_messages|outbox_messages).*lease_until > clock_timestamp\(\)`

// These are SQL-shape tests. They ensure every fenced mutation evaluates the
// lease against PostgreSQL's wall clock after lock waits, rather than the
// transaction-start timestamp returned by now(). Real lock-wait behavior
// still requires PostgreSQL integration acceptance.
func TestPostgresLeaseMutationsUseWallClockFence(t *testing.T) {
	lease := Lease{Owner: "worker-1", Fence: 3}
	nextAttempt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		queryMutation bool
		lockTable     string
		call          func(*PostgresStore) error
	}{
		{name: "renew inbox", queryMutation: true, lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			_, err := s.RenewInbox(context.Background(), 7, lease, time.Minute)
			return err
		}},
		{name: "complete inbox", queryMutation: true, lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			_, err := s.CompleteInbox(context.Background(), 7, lease, OutboxReply{Content: "reply"})
			return err
		}},
		{name: "retry inbox", lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			return s.RetryInbox(context.Background(), 7, lease, errors.New("temporary"), nextAttempt)
		}},
		{name: "retry inbox after", lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			return s.RetryInboxAfter(context.Background(), 7, lease, errors.New("temporary"), time.Second)
		}},
		{name: "block inbox", lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			return s.BlockInbox(context.Background(), 7, lease, errors.New("blocked"))
		}},
		{name: "dead-letter inbox", lockTable: "inbox_messages", call: func(s *PostgresStore) error {
			return s.DeadLetterInbox(context.Background(), 7, lease, errors.New("permanent"))
		}},
		{name: "renew outbox", queryMutation: true, lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			_, err := s.RenewOutbox(context.Background(), 8, lease, time.Minute)
			return err
		}},
		{name: "mark dispatch started", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.MarkDispatchStarted(context.Background(), 8, lease)
		}},
		{name: "mark delivered", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.MarkDelivered(context.Background(), 8, lease)
		}},
		{name: "advance outbox", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.AdvanceOutbox(context.Background(), 8, lease, 1)
		}},
		{name: "retry outbox", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.RetryOutbox(context.Background(), 8, lease, errors.New("temporary"), nextAttempt, 0)
		}},
		{name: "retry outbox after", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.RetryOutboxAfter(context.Background(), 8, lease, errors.New("temporary"), time.Second, 0)
		}},
		{name: "dead-letter outbox", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.DeadLetterOutbox(context.Background(), 8, lease, errors.New("permanent"))
		}},
		{name: "block outbox", lockTable: "outbox_messages", call: func(s *PostgresStore) error {
			return s.BlockOutbox(context.Background(), 8, lease, errors.New("blocked"))
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			mock.ExpectBegin()
			if test.name == "renew inbox" {
				mock.ExpectQuery(`SELECT tenant_id FROM inbox_messages WHERE id=\$1`).
					WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-a"))
				mock.ExpectExec(`INSERT INTO tenant_queue_schedule`).
					WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(`SELECT tenant_id FROM tenant_queue_schedule WHERE tenant_id=\$1 FOR UPDATE`).
					WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-a"))
			}
			lockID := int64(7)
			if test.lockTable == "outbox_messages" {
				lockID = 8
			}
			mock.ExpectQuery(`SELECT id FROM ` + test.lockTable + ` WHERE id=\$1 FOR UPDATE`).
				WithArgs(lockID).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(lockID))
			if test.queryMutation {
				mock.ExpectQuery(activeLeaseClockPredicate).WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectExec(activeLeaseClockPredicate).WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectRollback()

			err = test.call(NewPostgresStore(db))
			if !errors.Is(err, ErrStaleLease) {
				t.Fatalf("error=%v, want ErrStaleLease", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresOutboxSuccessMutationsRequireDispatchFence(t *testing.T) {
	tests := []struct {
		name string
		call func(*PostgresStore) error
	}{
		{name: "mark delivered", call: func(store *PostgresStore) error {
			return store.MarkDelivered(context.Background(), 8, Lease{Owner: "worker-1", Fence: 3})
		}},
		{name: "advance cursor", call: func(store *PostgresStore) error {
			return store.AdvanceOutbox(context.Background(), 8, Lease{Owner: "worker-1", Fence: 3}, 1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT id FROM outbox_messages WHERE id=\$1 FOR UPDATE`).
				WithArgs(int64(8)).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(8)))
			mock.ExpectExec(`(?s)UPDATE outbox_messages.*WHERE id=\$1 AND status='DISPATCH_STARTED' AND lease_owner=\$2.*lease_version=\$3`).
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectRollback()
			if err := test.call(NewPostgresStore(db)); !errors.Is(err, ErrStaleLease) {
				t.Fatalf("error=%v, want ErrStaleLease", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresLeaseExpirySelectionUsesWallClock(t *testing.T) {
	t.Run("claim inbox", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)FROM inbox_messages.*lease_until <= clock_timestamp\(\)`).
			WillReturnError(sql.ErrNoRows)
		mock.ExpectCommit()

		message, err := NewPostgresStore(db).ClaimInbox(context.Background(), "consumer-1", time.Minute)
		if !errors.Is(err, ErrNoWork) || message != nil {
			t.Fatalf("message=%#v err=%v, want nil/ErrNoWork", message, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("claim outbox", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)FROM outbox_messages AS candidate.*lease_until <= clock_timestamp\(\)`).
			WillReturnError(sql.ErrNoRows)
		mock.ExpectCommit()

		message, err := NewPostgresStore(db).ClaimOutbox(context.Background(), "delivery-1", time.Minute)
		if !errors.Is(err, ErrNoWork) || message != nil {
			t.Fatalf("message=%#v err=%v, want nil/ErrNoWork", message, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reap expired", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)WITH selected AS \(.*lease_until <= clock_timestamp\(\)`).
			WithArgs(1).
			WillReturnRows(sqlmock.NewRows([]string{"previous_status", "count"}))
		mock.ExpectQuery(`(?s)WITH expired AS \(.*lease_until <= clock_timestamp\(\)`).
			WithArgs(1).
			WillReturnRows(sqlmock.NewRows([]string{"previous_status", "count"}))
		mock.ExpectCommit()

		result, err := NewPostgresStore(db).ReapExpired(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if result != (ExpiredWorkReapResult{}) {
			t.Fatalf("result=%+v, want zero", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
