package reliable

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresEnqueueRejectsInactiveTenantBeforeSequenceAllocation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	msg := newTestInbox()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT status FROM tenants WHERE id=$1 FOR SHARE")).
		WithArgs(msg.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("suspended"))
	mock.ExpectRollback()

	inserted, err := store.EnqueueInbox(context.Background(), msg)
	if inserted || !errors.Is(err, ErrTenantInactive) {
		t.Fatalf("EnqueueInbox = (%v, %v), want false/ErrTenantInactive", inserted, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresEnqueueRejectsDuplicateWithMismatchedRoutingIdentity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	msg := newTestInbox()
	now := time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT status FROM tenants WHERE id=$1 FOR SHARE")).
		WithArgs(msg.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO inbox_session_sequences")).
		WithArgs(msg.TenantID, msg.AgentApp, msg.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(int64(1)))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO inbox_messages")).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	// The durable row has the same provider key and payload, but a different
	// application route. It must not be acknowledged as a harmless duplicate.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, tenant_id, channel_type, channel_account_id")).
		WithArgs(msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "channel_type", "channel_account_id",
			"external_message_id", "agent_app_name", "conversation_id", "reply_to_id",
			"user_id", "session_id", "is_group_chat", "session_owner_id", "routing_version",
			"session_sequence", "payload_hash", "payload",
			"trace_parent", "status", "attempt_count", "max_attempts", "next_attempt_at",
			"approval_deadline", "lease_owner", "lease_version", "lease_until", "last_error",
			"created_at", "updated_at",
		}).AddRow(
			int64(9), msg.TenantID, msg.ChannelType, msg.ChannelAccountID,
			msg.ExternalMessageID, "different-agent", msg.ConversationID, msg.ReplyToID,
			msg.UserID, msg.SessionID, false, msg.UserID, int64(1), int64(1), msg.PayloadHash, msg.Payload,
			msg.TraceParent, "RECEIVED", int64(0), int64(5), nil, nil, nil,
			int64(0), nil, nil, now, now,
		))

	inserted, err := store.EnqueueInbox(context.Background(), msg)
	if inserted || !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("EnqueueInbox = (%v, %v), want false/ErrIdempotencyConflict", inserted, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
