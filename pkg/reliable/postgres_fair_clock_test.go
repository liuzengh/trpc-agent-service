package reliable

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresFairQueueReadinessRejectsMissingClockRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT to_regclass").
		WillReturnRows(sqlmock.NewRows([]string{"ready"}).AddRow(true))
	mock.ExpectQuery("SELECT virtual_time FROM inbox_fair_queue_clock").WillReturnError(sql.ErrNoRows)
	if err := NewPostgresStore(db).CheckFairInboxReady(context.Background()); !errors.Is(err, ErrFairQueueNotReady) {
		t.Fatalf("missing clock error=%v, want ErrFairQueueNotReady", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairClaimRechecksInflightAfterLockingSchedule(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT virtual_time.*FROM inbox_fair_queue_clock.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"virtual_time"}).AddRow(int64(0)))
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "channel_type", "channel_account_id",
		"external_message_id", "agent_app_name", "conversation_id", "reply_to_id",
		"user_id", "session_id", "is_group_chat", "session_owner_id", "routing_version",
		"session_sequence", "payload_hash", "payload", "trace_parent", "status",
		"attempt_count", "max_attempts", "next_attempt_at", "approval_deadline",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow(
		int64(7), "tenant-a", "telegram", "bot-a", "message-a", "assistant",
		"conversation-a", "reply-a", "user-a", "session-a", false, "user-a", 1,
		int64(1), strings.Repeat("a", 64), []byte(`{"content":"hello"}`), "", InboxReceived,
		0, 5, nil, nil, nil, int64(0), nil, "", now, now,
	)
	mock.ExpectQuery("WITH candidates AS").WithArgs(int64(0)).WillReturnRows(rows)
	mock.ExpectQuery("SELECT weight, max_inflight, virtual_runtime").WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"weight", "max_inflight", "virtual_runtime", "inflight"}).
			AddRow(int64(1), int64(2), int64(0), int64(2)))
	mock.ExpectRollback()
	claim, err := NewPostgresStore(db).ClaimInboxFair(context.Background(), "consumer", time.Minute)
	if claim != nil || !errors.Is(err, ErrNoWork) {
		t.Fatalf("locked quota exhausted: claim=%v error=%v, want no work without clock or lease mutation", claim, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairClaimEmptyQueueDoesNotAdvanceClock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT virtual_time.*FROM inbox_fair_queue_clock.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"virtual_time"}).AddRow(int64(20_000_000)))
	mock.ExpectQuery("WITH candidates AS").WithArgs(int64(20_000_000)).WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()
	claim, err := NewPostgresStore(db).ClaimInboxFair(context.Background(), "consumer", time.Minute)
	if claim != nil || !errors.Is(err, ErrNoWork) {
		t.Fatalf("empty queue claim=%v error=%v, want ErrNoWork", claim, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
