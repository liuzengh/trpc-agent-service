package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestMySQLEventStoreAppendAndDuplicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store, err := NewMySQLEventStore(db)
	if err != nil {
		t.Fatalf("NewMySQLEventStore() error = %v", err)
	}
	event := testUserEvent()

	mock.ExpectExec("INSERT INTO session").
		WithArgs(event.SessionID, event.TenantID, event.AppID, event.Channel, event.SenderID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO message_event").
		WithArgs(event.SessionID, event.TenantID, sqlmock.AnyArg(), event.MsgID, event.Channel).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.AppendUserEvent(context.Background(), event); err != nil {
		t.Fatalf("AppendUserEvent() error = %v", err)
	}

	mock.ExpectExec("INSERT INTO session").
		WithArgs(event.SessionID, event.TenantID, event.AppID, event.Channel, event.SenderID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO message_event").
		WithArgs(event.SessionID, event.TenantID, sqlmock.AnyArg(), event.MsgID, event.Channel).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "duplicate"})
	if err := store.AppendUserEvent(context.Background(), event); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("duplicate AppendUserEvent() error = %v, want ErrDuplicateEvent", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMySQLEventStorePendingAndMarkConsumed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store, err := NewMySQLEventStore(db)
	if err != nil {
		t.Fatalf("NewMySQLEventStore() error = %v", err)
	}
	timestamp := time.Unix(1_000, 0)
	rows := sqlmock.NewRows([]string{
		"event_id", "session_id", "tenant_id", "channel", "msg_id",
		"payload_json", "consumed", "ts",
	}).AddRow(
		int64(7), "tenant-a:webui:user-1", "tenant-a", "webui", "msg-7",
		`{"sender_id":"user-1","text":"hello","trace_id":"trace-7"}`, false, timestamp,
	)
	mock.ExpectQuery("SELECT event_id, session_id, tenant_id, channel, msg_id,").
		WithArgs("tenant-a:webui:user-1", int64(0)).
		WillReturnRows(rows)

	events, err := store.PendingUserEvents(context.Background(), "tenant-a:webui:user-1", 0)
	if err != nil {
		t.Fatalf("PendingUserEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].Text != "hello" || events[0].TraceID != "trace-7" {
		t.Fatalf("PendingUserEvents() = %#v", events)
	}

	mock.ExpectExec("UPDATE message_event SET consumed = TRUE WHERE event_id IN").
		WithArgs(int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.MarkConsumed(context.Background(), []int64{7}); err != nil {
		t.Fatalf("MarkConsumed() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestMySQLEventStoreValidation(t *testing.T) {
	if _, err := NewMySQLEventStore(nil); err == nil {
		t.Fatal("NewMySQLEventStore() accepted nil database")
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	store, err := NewMySQLEventStore(db)
	if err != nil {
		t.Fatalf("NewMySQLEventStore() error = %v", err)
	}
	if err := store.AppendUserEvent(context.Background(), UserEvent{}); err == nil {
		t.Fatal("AppendUserEvent() accepted empty event")
	}
	if err := store.MarkConsumed(context.Background(), nil); err != nil {
		t.Fatalf("MarkConsumed(nil) error = %v", err)
	}
	if err := store.MarkConsumed(context.Background(), []int64{0}); err == nil {
		t.Fatal("MarkConsumed() accepted invalid event ID")
	}
}

func testUserEvent() UserEvent {
	return UserEvent{
		SessionID: "tenant-a:webui:user-1",
		TenantID:  "tenant-a",
		AppID:     "app-a",
		Channel:   "webui",
		MsgID:     "msg-1",
		SenderID:  "user-1",
		Text:      "hello",
		TraceID:   "trace-1",
	}
}
