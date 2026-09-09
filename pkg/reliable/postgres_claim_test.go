package reliable

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresClaimInboxDoesNotRunGlobalExpiryCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, tenant_id, channel_type")).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()

	msg, err := store.ClaimInbox(context.Background(), "consumer-1", time.Minute)
	if !errors.Is(err, ErrNoWork) || msg != nil {
		t.Fatalf("ClaimInbox result=%v err=%v, want nil/ErrNoWork", msg, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresClaimsRejectInvalidLeaseOwnerBeforeTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	for _, owner := range []string{"", string([]byte{0xff}), "bad\nowner", strings.Repeat("x", 257)} {
		if _, err := store.ClaimInbox(context.Background(), owner, time.Minute); err == nil {
			t.Fatalf("ClaimInbox accepted invalid owner %q", owner)
		}
		if _, err := store.ClaimOutbox(context.Background(), owner, time.Minute); err == nil {
			t.Fatalf("ClaimOutbox accepted invalid owner %q", owner)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("invalid claims touched database: %v", err)
	}
}

func TestPostgresClaimInboxClearsApprovalDeadlineWhenResuming(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, tenant_id, channel_type")).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "channel_type", "channel_account_id", "external_message_id",
			"agent_app_name", "conversation_id", "reply_to_id", "user_id", "session_id",
			"is_group_chat", "session_owner_id", "routing_version", "session_sequence",
			"payload_hash", "payload", "trace_parent", "status",
			"attempt_count", "max_attempts", "next_attempt_at", "approval_deadline", "lease_owner",
			"lease_version", "lease_until", "last_error", "created_at", "updated_at",
		}).AddRow(
			int64(7), "tenant-a", "telegram", "account-a", "message-a", "agent-a", "conversation-a", "reply-a",
			"user-a", "session-a", false, "user-a", int64(1), int64(1), strings.Repeat("a", 64), []byte(`{"text":"hello"}`), "", "WAITING_APPROVAL",
			0, 3, nil, time.Now().UTC().Add(time.Minute), nil, int64(4), nil, "approval required", time.Now().UTC(), time.Now().UTC(),
		))
	mock.ExpectQuery(`(?s)UPDATE inbox_messages.*lease_until=clock_timestamp\(\) \+.*next_attempt_at=NULL, approval_deadline=NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"attempt_count", "lease_version", "lease_until", "updated_at"}).AddRow(
			0, int64(5), time.Now().UTC().Add(time.Minute), time.Now().UTC(),
		))
	mock.ExpectCommit()

	msg, err := store.ClaimInbox(context.Background(), "consumer-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if msg == nil || msg.Status != InboxProcessing || msg.ApprovalDeadline != nil {
		t.Fatalf("resumed approval message=%#v, want processing with no approval deadline", msg)
	}
}

func TestPostgresClaimOutboxDoesNotRunGlobalExpiryCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, inbox_id, tenant_id")).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()

	msg, err := store.ClaimOutbox(context.Background(), "delivery-1", time.Minute)
	if !errors.Is(err, ErrNoWork) || msg != nil {
		t.Fatalf("ClaimOutbox result=%v err=%v, want nil/ErrNoWork", msg, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresClaimOutboxUsesWallClockLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, inbox_id, tenant_id")).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "inbox_id", "tenant_id", "agent_app_name", "session_id", "session_sequence",
			"channel_type", "channel_account_id", "conversation_id", "reply_to_id", "content_type",
			"content", "trace_parent", "delivery_cursor", "status", "attempt_count", "max_attempts",
			"next_attempt_at", "lease_owner", "lease_version", "lease_until", "last_error", "delivered_at",
			"created_at", "updated_at",
		}).AddRow(
			int64(8), int64(7), "tenant-a", "agent-a", "session-a", int64(1),
			"telegram", "account-a", "conversation-a", "reply-a", "text", "reply", "",
			0, "REPLY_PENDING", 0, 3, nil, nil, int64(0), nil, nil, nil, now, now,
		))
	mock.ExpectQuery(`(?s)UPDATE outbox_messages.*lease_until=clock_timestamp\(\) \+.*next_attempt_at=NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"attempt_count", "lease_version", "lease_until", "updated_at"}).AddRow(
			1, int64(1), now.Add(time.Minute), now,
		))
	mock.ExpectCommit()

	message, err := NewPostgresStore(db).ClaimOutbox(context.Background(), "delivery-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.Status != OutboxDelivering || message.Lease.Fence != 1 {
		t.Fatalf("claimed message=%#v, want fenced delivery lease", message)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMarkDispatchStartedUsesLeaseFence(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM outbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(8)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(8)))
	mock.ExpectExec(`(?s)UPDATE outbox_messages.*SET status='DISPATCH_STARTED'.*status='DELIVERING'.*lease_owner=\$2.*lease_version=\$3.*lease_until > clock_timestamp\(\)`).
		WithArgs(int64(8), "delivery-1", int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.MarkDispatchStarted(context.Background(), 8, Lease{Owner: "delivery-1", Fence: 4}); err != nil {
		t.Fatalf("MarkDispatchStarted: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresReapExpiredUsesBoundedLockedCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)WITH selected AS \(.*FROM inbox_messages.*status='PROCESSING'.*OR \(status='WAITING_APPROVAL'.*ORDER BY CASE WHEN status='WAITING_APPROVAL' THEN approval_deadline ELSE lease_until END NULLS FIRST, id\s+LIMIT \$1\s+FOR UPDATE SKIP LOCKED\s*\), updated AS \(.*UPDATE inbox_messages`).
		WithArgs(25).
		WillReturnRows(sqlmock.NewRows([]string{"previous_status", "count"}).
			AddRow("PROCESSING", int64(2)).
			AddRow("WAITING_APPROVAL", int64(1)))
	mock.ExpectQuery(`(?s)WITH expired AS \(.*LIMIT \$1\s+FOR UPDATE SKIP LOCKED.*UPDATE outbox_messages`).
		WithArgs(25).
		WillReturnRows(sqlmock.NewRows([]string{"previous_status", "count"}).AddRow("DELIVERING", int64(3)))
	mock.ExpectCommit()

	result, err := store.ReapExpired(context.Background(), 25)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if result != (ExpiredWorkReapResult{InboxFinalAttemptExpired: 2, InboxApprovalExpired: 1, OutboxFinalAttemptExpired: 3}) {
		t.Fatalf("unexpected reap result: %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresReapExpiredRejectsInvalidBatchBeforeOpeningTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	if _, err := store.ReapExpired(context.Background(), MaxExpiredWorkReapBatchSize+1); !errors.Is(err, ErrInvalidExpiredWorkReapBatchSize) {
		t.Fatalf("ReapExpired error=%v, want invalid batch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRetryAfterUsesDatabaseClock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM inbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec(`(?s)UPDATE inbox_messages.*next_attempt_at=CASE.*clock_timestamp\(\) \+`).
		WithArgs(int64(7), "consumer-1", int64(3), int64(1500000), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.RetryInboxAfter(context.Background(), 7, Lease{Owner: "consumer-1", Fence: 3}, errors.New("temporary"), 1500*time.Millisecond); err != nil {
		t.Fatalf("RetryInboxAfter: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM outbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(8)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(8)))
	mock.ExpectExec(`(?s)UPDATE outbox_messages.*next_attempt_at=CASE.*clock_timestamp\(\) \+`).
		WithArgs(int64(8), "delivery-1", int64(4), int64(2500000), sqlmock.AnyArg(), 2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.RetryOutboxAfter(context.Background(), 8, Lease{Owner: "delivery-1", Fence: 4}, errors.New("temporary"), 2500*time.Millisecond, 2); err != nil {
		t.Fatalf("RetryOutboxAfter: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRetryInboxCastsExplicitNextAttemptTimestamp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nextAttempt := time.Date(2026, 8, 28, 9, 30, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM inbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec(`(?s)UPDATE inbox_messages.*next_attempt_at=CASE WHEN attempt_count >= max_attempts.*THEN NULL ELSE \$4::timestamptz END`).
		WithArgs(int64(7), "consumer-1", int64(3), nextAttempt, "temporary").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()
	if err := NewPostgresStore(db).RetryInbox(
		context.Background(), 7, Lease{Owner: "consumer-1", Fence: 3},
		errors.New("temporary"), nextAttempt,
	); err != nil {
		t.Fatalf("RetryInbox: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWaitInboxApprovalPreservesAttemptBudget(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	deadline := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT $1 > clock_timestamp()")).
		WithArgs(deadline, MaxApprovalWait.Microseconds()).
		WillReturnRows(sqlmock.NewRows([]string{"valid"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM inbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE inbox_messages")).
		WithArgs(int64(7), "consumer-1", int64(3), int64(5000000), deadline, "tool approval required", MaxApprovalWait.Microseconds()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.WaitInboxApproval(
		context.Background(), 7,
		Lease{Owner: "consumer-1", Fence: 3},
		errors.New("tool approval required"),
		5*time.Second, deadline,
	); err != nil {
		t.Fatalf("WaitInboxApproval: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWaitInboxApprovalRejectsDeadlineOutsideStoreWindow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	deadline := time.Date(2026, 8, 27, 12, 0, 1, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT $1 > clock_timestamp()")).
		WithArgs(deadline, MaxApprovalWait.Microseconds()).
		WillReturnRows(sqlmock.NewRows([]string{"valid"}).AddRow(false))

	err = store.WaitInboxApproval(
		context.Background(), 7,
		Lease{Owner: "consumer-1", Fence: 3},
		errors.New("tool approval required"),
		5*time.Second, deadline,
	)
	if !errors.Is(err, ErrApprovalDeadlineInvalid) {
		t.Fatalf("WaitInboxApproval error=%v, want invalid deadline", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWaitInboxApprovalRejectsDeadlineThatExpiresDuringTransition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	deadline := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT $1 > clock_timestamp()")).
		WithArgs(deadline, MaxApprovalWait.Microseconds()).
		WillReturnRows(sqlmock.NewRows([]string{"valid"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM inbox_messages WHERE id=\$1 FOR UPDATE`).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE inbox_messages")).
		WithArgs(int64(7), "consumer-1", int64(3), int64(5000000), deadline, "tool approval required", MaxApprovalWait.Microseconds()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT $1 > clock_timestamp()")).
		WithArgs(deadline, MaxApprovalWait.Microseconds()).
		WillReturnRows(sqlmock.NewRows([]string{"valid"}).AddRow(false))

	err = store.WaitInboxApproval(
		context.Background(), 7,
		Lease{Owner: "consumer-1", Fence: 3},
		errors.New("tool approval required"),
		5*time.Second, deadline,
	)
	if !errors.Is(err, ErrApprovalDeadlineInvalid) {
		t.Fatalf("WaitInboxApproval error=%v, want invalid deadline", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
