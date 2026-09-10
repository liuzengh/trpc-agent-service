package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLStoragePrimitives(t *testing.T) {
	assertPostgreSQLConnectionBoundaries(t)
	assertPostgreSQLPingPaths(t)
	assertPostgreSQLTransactionHappyPath(t)
	assertPostgreSQLPrimitiveMappingsAndHelpers(t)
}

func assertPostgreSQLConnectionBoundaries(t *testing.T) {
	t.Helper()
	if got := normalizeDSN(" PostgreSQL+PSYCOPG://db.example/test "); got != "postgresql://db.example/test" {
		t.Fatalf("normalized psycopg DSN = %q", got)
	}
	if _, err := Open(context.Background(), "", Options{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("empty DSN error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, "postgres://unused", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Open error = %v", err)
	}
	if err := Ping(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Ping error = %v", err)
	}
}

func assertPostgreSQLPingPaths(t *testing.T) {
	t.Helper()
	pingDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pingDB.Close() })
	mock.ExpectPing().WillReturnError(errors.New("ping failure"))
	if err := Ping(context.Background(), pingDB); !errors.Is(err, ErrStorage) {
		t.Fatalf("failed ping error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	pingSuccessDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pingSuccessDB.Close() })
	mock.ExpectPing()
	if err := Ping(context.Background(), pingSuccessDB); err != nil {
		t.Fatalf("successful ping error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func assertPostgreSQLTransactionHappyPath(t *testing.T) {
	t.Helper()
	transactionDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transactionDB.Close() })
	mock.ExpectBegin()
	tx, err := Begin(context.Background(), transactionDB)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectCommit()
	if err := Commit(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func assertPostgreSQLPrimitiveMappingsAndHelpers(t *testing.T) {
	t.Helper()
	notFound := errors.New("not found")
	duplicate := errors.New("duplicate")
	conflict := errors.New("conflict")
	invalid := errors.New("invalid")
	if got := MapError(context.Background(), sql.ErrNoRows, notFound, duplicate, conflict, invalid); !errors.Is(got, notFound) {
		t.Fatalf("not-found mapping = %v", got)
	}
	if got := MapError(context.Background(), &pgconn.PgError{Code: "23505"}, notFound, duplicate, conflict, invalid); !errors.Is(got, duplicate) {
		t.Fatalf("duplicate mapping = %v", got)
	}
	if got := MapError(context.Background(), errors.New("driver failure"), notFound, duplicate, conflict, invalid); !errors.Is(got, ErrStorage) {
		t.Fatalf("fallback mapping = %v", got)
	}
	if NullableInt(sql.NullInt64{}) != nil || NullableString(sql.NullString{}) != nil || NullableText("") != nil {
		t.Fatal("invalid nullable values became non-nil")
	}
	if value := NullableInt(sql.NullInt64{Int64: 4, Valid: true}); value == nil || *value != 4 {
		t.Fatalf("nullable integer = %v", value)
	}
	future := time.Now().UTC().Add(time.Hour)
	if got := MonotonicNow(future); !got.Equal(future) {
		t.Fatalf("monotonic timestamp regressed: %s", got)
	}
}

func TestPostgreSQLStorageOpenConfiguresPoolBeforeFailedReadiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	db, err := Open(ctx, "postgres://127.0.0.1:1/postgres?connect_timeout=1", Options{
		MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Second, ConnMaxIdleTime: time.Second,
	})
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrStorage) {
		t.Fatalf("unreachable database error = %v", err)
	}
}

func TestPostgreSQLStorageJSONBoundary(t *testing.T) {
	if _, err := EncodeJSON(math.NaN()); err == nil {
		t.Fatal("non-finite number encoded as JSON")
	}
	encoded, err := EncodeJSON(map[string]string{"mode": "safe"})
	if err != nil || string(encoded) != `{"mode":"safe"}` {
		t.Fatalf("JSON encode = %q, err=%v", encoded, err)
	}
	var decoded map[string]string
	if err := DecodeJSON([]byte(`{"mode":"safe"}`), &decoded); err != nil || decoded["mode"] != "safe" {
		t.Fatalf("JSON decode = %#v, err=%v", decoded, err)
	}
	for _, malformed := range [][]byte{[]byte(`{"unknown":true}`), []byte(`{} {}`), []byte(`not-json`)} {
		if err := DecodeJSON(malformed, &decoded); !errors.Is(err, ErrStorage) {
			t.Fatalf("DecodeJSON(%q) error = %v", malformed, err)
		}
	}
	var defaults struct {
		Mode string `json:"mode"`
	}
	if err := DecodeJSON(nil, &defaults); err != nil || defaults.Mode != "" {
		t.Fatalf("empty JSON decode = %#v, err=%v", defaults, err)
	}
	if err := DecodeJSON([]byte(`{"unknown":"value"}`), &defaults); !errors.Is(err, ErrStorage) {
		t.Fatalf("unknown JSON field error = %v", err)
	}
}

func TestPostgreSQLStorageErrorAndTransactionPaths(t *testing.T) {
	notFound, duplicate, conflict, invalid := storageErrorSentinels()
	assertPostgreSQLDriverErrorMappings(t, notFound, duplicate, conflict, invalid)
	assertPostgreSQLTransactionErrorPaths(t, notFound, duplicate, conflict, invalid)
	assertPostgreSQLNullableAndTimeHelpers(t)
}

func storageErrorSentinels() (error, error, error, error) {
	notFound := errors.New("not found")
	duplicate := errors.New("duplicate")
	conflict := errors.New("conflict")
	invalid := errors.New("invalid")
	return notFound, duplicate, conflict, invalid
}

func assertPostgreSQLDriverErrorMappings(t *testing.T, notFound, duplicate, conflict, invalid error) {
	t.Helper()
	for name, testCase := range map[string]struct {
		err  error
		want error
	}{
		"canceled":       {err: context.Canceled, want: context.Canceled},
		"deadline":       {err: context.DeadlineExceeded, want: context.DeadlineExceeded},
		"foreign key":    {err: &pgconn.PgError{Code: "23503"}, want: invalid},
		"check":          {err: &pgconn.PgError{Code: "23514"}, want: invalid},
		"invalid syntax": {err: &pgconn.PgError{Code: "22P02"}, want: invalid},
		"length":         {err: &pgconn.PgError{Code: "22001"}, want: invalid},
		"serialization":  {err: &pgconn.PgError{Code: "40001"}, want: conflict},
		"deadlock":       {err: &pgconn.PgError{Code: "40P01"}, want: conflict},
	} {
		t.Run(name, func(t *testing.T) {
			if got := MapError(context.Background(), testCase.err, notFound, duplicate, conflict, invalid); !errors.Is(got, testCase.want) {
				t.Fatalf("MapError(%v) = %v, want %v", testCase.err, got, testCase.want)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := MapError(canceled, errors.New("driver error"), notFound, duplicate, conflict, invalid); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled context mapping = %v", got)
	}
}

func assertPostgreSQLTransactionErrorPaths(t *testing.T, notFound, duplicate, conflict, invalid error) {
	t.Helper()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Begin(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Begin error = %v", err)
	}
	Rollback(nil)
	if err := Commit(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Commit error = %v", err)
	}
	beginDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = beginDB.Close() })
	if _, err := Begin(canceled, beginDB); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Begin error = %v", err)
	}
	mock.ExpectBegin().WillReturnError(errors.New("begin failure"))
	if _, err := Begin(context.Background(), beginDB); !errors.Is(err, ErrStorage) {
		t.Fatalf("failed Begin error = %v", err)
	}
	mock.ExpectBegin()
	tx, err := Begin(context.Background(), beginDB)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err := Commit(canceled, tx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Commit error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	commitDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commitDB.Close() })
	mock.ExpectBegin()
	tx, err = Begin(context.Background(), commitDB)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectCommit().WillReturnError(errors.New("commit failure"))
	if err := Commit(context.Background(), tx); !errors.Is(err, ErrStorage) {
		t.Fatalf("failed Commit error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

}

func assertPostgreSQLNullableAndTimeHelpers(t *testing.T) {
	t.Helper()
	if got := AsUTC(time.Date(2026, 8, 24, 1, 2, 3, 0, time.FixedZone("test", 3600))); got.Location() != time.UTC {
		t.Fatalf("UTC timestamp location = %s", got.Location())
	}
	if value := NullableString(sql.NullString{String: "value", Valid: true}); value == nil || *value != "value" {
		t.Fatalf("nullable string = %v", value)
	}
	if got := NullableText(" value "); got != " value " {
		t.Fatalf("nullable text = %#v", got)
	}
	if now := MonotonicNow(time.Time{}); now.IsZero() || now.Location() != time.UTC {
		t.Fatalf("monotonic current timestamp = %s", now)
	}
}
