package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestStorageRejectsCancelledContextsBeforeQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		call func() error
	}{
		{"open", func() error { _, err := Open(ctx, "root:secret@tcp(localhost:3306)/db", Options{}); return err }},
		{"ping", func() error { return Ping(ctx, nil) }},
		{"begin", func() error { _, err := Begin(ctx, nil); return err }},
		{"begin conn", func() error { _, err := BeginConn(ctx, nil); return err }},
		{"acquire lock", func() error { return AcquireLock(ctx, nil, "name", 1) }},
		{"release lock", func() error { return ReleaseLock(ctx, nil, "name") }},
		{"current user", func() error { _, err := CurrentUser(ctx, nil); return err }},
		{"current database", func() error { _, err := CurrentDatabase(ctx, nil); return err }},
		{"privileges", func() error { return VerifyApplicationPrivileges(ctx, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) && !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestStorageNilContextsFailClosed(t *testing.T) {
	if err := Ping(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("Ping(nil) = %v", err)
	}
	if _, err := Begin(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("Begin(nil) = %v", err)
	}
	if _, err := BeginConn(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("BeginConn(nil) = %v", err)
	}
	if err := AcquireLock(nil, nil, "name", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("AcquireLock(nil) = %v", err)
	}
	if err := ReleaseLock(nil, nil, "name"); !errors.Is(err, ErrStorage) {
		t.Fatalf("ReleaseLock(nil) = %v", err)
	}
}

func TestStorageNilContextsFailClosedWithDatabase(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := Ping(nil, db); !errors.Is(err, ErrStorage) {
		t.Fatalf("Ping(nil, db) = %v", err)
	}
	if _, err := Begin(nil, db); !errors.Is(err, ErrStorage) {
		t.Fatalf("Begin(nil, db) = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageDriverFailuresAreRedacted(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectPing().WillReturnError(errors.New("driver ping details"))
	if err := Ping(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("Ping(driver failure) = %v", err)
	}
	mock.ExpectBegin().WillReturnError(errors.New("driver begin details"))
	if _, err := Begin(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("Begin(driver failure) = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMonotonicNowDoesNotMoveBeforePersistedTime(t *testing.T) {
	previous := time.Now().UTC().Add(time.Hour)
	if got := MonotonicNow(previous); !got.Equal(previous) {
		t.Fatalf("MonotonicNow(%v) = %v, want persisted timestamp", previous, got)
	}
}

func TestCommitRollsBackForNilAndCancelledContexts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(nil, tx); !errors.Is(err, ErrStorage) {
		t.Fatalf("Commit(nil) = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Commit(ctx, tx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit(cancelled) = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitRedactsDatabaseFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("driver details"))
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(context.Background(), tx); !errors.Is(err, ErrStorage) {
		t.Fatalf("Commit(driver failure) = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
