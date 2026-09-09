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

func TestPostgresInspectQueueReturnsDepthAndOldest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	inboxOldest := time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)
	outboxOldest := inboxOldest.Add(3 * time.Minute)
	observedAt := outboxOldest.Add(time.Minute)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\t\t(SELECT COUNT(*) FROM inbox_messages")).
		WillReturnRows(sqlmock.NewRows([]string{"inbox_depth", "inbox_oldest", "outbox_depth", "outbox_oldest", "observed_at"}).
			AddRow(int64(4), inboxOldest, int64(2), outboxOldest, observedAt))

	stats, err := store.InspectQueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboxDepth != 4 || !stats.InboxOldest.Equal(inboxOldest) ||
		stats.OutboxDepth != 2 || !stats.OutboxOldest.Equal(outboxOldest) || !stats.ObservedAt.Equal(observedAt) {
		t.Fatalf("queue stats=%+v", stats)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairClaimLocksScheduleAndMessageInOneTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	leaseUntil := now.Add(time.Minute)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT virtual_time.*FROM inbox_fair_queue_clock.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"virtual_time"}).AddRow(int64(20_000_000)))
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
	mock.ExpectQuery("(?s)WITH candidates AS.*active\\.status='PROCESSING'\\s+AND active\\.lease_until > clock_timestamp\\(\\)").
		WithArgs(int64(20_000_000)).WillReturnRows(rows)
	mock.ExpectQuery("SELECT weight, max_inflight, virtual_runtime").WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"weight", "max_inflight", "virtual_runtime", "inflight"}).AddRow(int64(4), int64(2), int64(0), int64(0)))
	mock.ExpectExec("UPDATE tenant_queue_schedule").WithArgs("tenant-a", int64(20_250_000)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE inbox_fair_queue_clock").WithArgs(int64(20_000_000)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("UPDATE inbox_messages").
		WithArgs(int64(7), "consumer-a", int64(60000), false).
		WillReturnRows(sqlmock.NewRows([]string{"attempt_count", "lease_version", "lease_until", "updated_at"}).
			AddRow(1, int64(1), leaseUntil, now))
	mock.ExpectCommit()

	msg, err := store.ClaimInboxFair(context.Background(), "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != 7 || msg.TenantID != "tenant-a" || msg.Status != InboxProcessing ||
		msg.AttemptCount != 1 || msg.Lease.Owner != "consumer-a" || msg.Lease.Fence != 1 {
		t.Fatalf("fair claim=%+v", msg)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairClaimDoesNotResurrectCompletedStaleCandidate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
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
	mock.ExpectQuery("(?s)WITH candidates AS").WithArgs(int64(0)).WillReturnRows(rows)
	mock.ExpectQuery("SELECT weight, max_inflight, virtual_runtime").WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"weight", "max_inflight", "virtual_runtime", "inflight"}).AddRow(int64(1), int64(0), int64(0), int64(0)))
	mock.ExpectExec("UPDATE tenant_queue_schedule").WithArgs("tenant-a", int64(1_000_000)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE inbox_fair_queue_clock").WithArgs(int64(0)).WillReturnResult(sqlmock.NewResult(0, 1))
	// Another transaction completed this Inbox after the candidate scan. The
	// guarded UPDATE must affect no row, roll back the schedule increment and
	// turn the lost race into ordinary ErrNoWork.
	mock.ExpectQuery("(?s)UPDATE inbox_messages.*WHERE id=\\$1.*status='RECEIVED'.*status='PROCESSING'.*lease_until <= clock_timestamp\\(\\)").
		WithArgs(int64(7), "consumer-a", int64(60000), false).
		WillReturnRows(sqlmock.NewRows([]string{"attempt_count", "lease_version", "lease_until", "updated_at"}))
	mock.ExpectRollback()

	message, err := store.ClaimInboxFair(context.Background(), "consumer-a", time.Minute)
	if message != nil || !errors.Is(err, ErrNoWork) {
		t.Fatalf("stale fair candidate result=%#v error=%v, want nil/ErrNoWork", message, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTenantQueuePolicyUsesOperatorTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	policy := TenantQueuePolicy{TenantID: "tenant-a", Weight: 3, MaxQueued: 100, MaxInflight: 4}
	mock.ExpectExec("INSERT INTO tenant_queue_schedule").
		WithArgs("tenant-a", int64(3), int64(100), int64(4)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.UpsertTenantQueuePolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("INSERT INTO tenant_queue_schedule").
		WithArgs("tenant-a").
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.DeleteTenantQueuePolicy(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairQueueReadinessRequiresMigrationObjects(t *testing.T) {
	for _, ready := range []bool{true, false} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		store := NewPostgresStore(db)
		mock.ExpectQuery("SELECT to_regclass").
			WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(ready))
		if ready {
			mock.ExpectQuery("SELECT virtual_time FROM inbox_fair_queue_clock").
				WillReturnRows(sqlmock.NewRows([]string{"virtual_time"}).AddRow(int64(0)))
		}
		err = store.CheckFairInboxReady(context.Background())
		if ready && err != nil {
			t.Fatalf("ready schema error=%v", err)
		}
		if !ready && !errors.Is(err, ErrFairQueueNotReady) {
			t.Fatalf("missing schema error=%v, want ErrFairQueueNotReady", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestPostgresQueueAdmissionRejectsBeforeSequenceAllocation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	msg := &InboxMessage{
		TenantID:          "tenant-a",
		ChannelType:       "telegram",
		ChannelAccountID:  "bot-a",
		AgentApp:          "assistant",
		ExternalMessageID: "message-a",
		ConversationID:    "conversation-a",
		ReplyToID:         "reply-a",
		UserID:            "user-a",
		SessionID:         "session-a",
		SessionOwnerID:    "user-a",
		RoutingVersion:    CurrentInboxRoutingVersion,
		PayloadHash:       strings.Repeat("a", 64),
		Payload:           []byte(`{"content":"hello","isGroupChat":false,"sessionOwnerId":"user-a"}`),
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT status").WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectExec("INSERT INTO tenant_queue_schedule").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT max_queued").WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"max_queued"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\)\\s+FROM inbox_messages").
		WithArgs("tenant-a", "telegram", "bot-a", "message-a").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("(?s)SELECT id, tenant_id.*FROM inbox_messages.*FOR KEY SHARE").
		WithArgs("tenant-a", "telegram", "bot-a", "message-a").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if inserted, err := store.EnqueueInboxWithAdmission(context.Background(), msg); !errors.Is(err, ErrTenantQueueFull) || inserted {
		t.Fatalf("queue admission inserted=%v err=%v, want ErrTenantQueueFull", inserted, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueAdmissionAcceptsDuplicateWhenQueueIsFull(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	msg := newTestInbox()
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT status").WithArgs(msg.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectExec("INSERT INTO tenant_queue_schedule").WithArgs(msg.TenantID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT max_queued").WithArgs(msg.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"max_queued"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\)\\s+FROM inbox_messages").
		WithArgs(msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("(?s)SELECT id, tenant_id.*FROM inbox_messages.*FOR KEY SHARE").
		WithArgs(msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "channel_type", "channel_account_id",
			"external_message_id", "agent_app_name", "conversation_id", "reply_to_id",
			"user_id", "session_id", "is_group_chat", "session_owner_id", "routing_version",
			"session_sequence", "payload_hash", "payload", "trace_parent", "status",
			"attempt_count", "max_attempts", "next_attempt_at", "approval_deadline",
			"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
		}).AddRow(
			int64(9), msg.TenantID, msg.ChannelType, msg.ChannelAccountID,
			msg.ExternalMessageID, msg.AgentApp, msg.ConversationID, msg.ReplyToID,
			msg.UserID, msg.SessionID, false, msg.UserID, int64(1), int64(1), msg.PayloadHash,
			msg.Payload, msg.TraceParent, InboxReceived, int64(0), int64(5), nil, nil, nil,
			int64(0), nil, "", now, now,
		))
	mock.ExpectCommit()

	inserted, err := store.EnqueueInboxWithAdmission(context.Background(), msg)
	if inserted || err != nil || msg.ID != 9 {
		t.Fatalf("duplicate queue-full enqueue = (%v, %v), id=%d; want false/nil/canonical id", inserted, err, msg.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresInspectQueueLeavesEmptyOldestZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT\n\t\t\t(SELECT COUNT(*) FROM inbox_messages")).
		WillReturnRows(sqlmock.NewRows([]string{"inbox_depth", "inbox_oldest", "outbox_depth", "outbox_oldest", "observed_at"}).
			AddRow(int64(0), nil, int64(0), nil, time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)))

	stats, err := store.InspectQueue(nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboxDepth != 0 || stats.OutboxDepth != 0 || !stats.InboxOldest.IsZero() || !stats.OutboxOldest.IsZero() || stats.ObservedAt.IsZero() {
		t.Fatalf("empty queue stats=%+v", stats)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
