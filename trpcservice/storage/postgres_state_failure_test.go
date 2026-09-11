package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func closedPostgresStateStore(t *testing.T) *PostgresStateStore {
	t.Helper()
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStateStore(database, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestNewPostgresStateStoreRejectsMissingDependencies(t *testing.T) {
	if _, err := NewPostgresStateStore(nil, []byte("01234567890123456789012345678901")); err == nil {
		t.Fatal("NewPostgresStateStore(nil) error = nil")
	}
	database, _ := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	defer database.Close()
	if _, err := NewPostgresStateStore(database, nil); err == nil {
		t.Fatal("NewPostgresStateStore(empty audit key) error = nil")
	}
}

func TestPostgresStateStoreRejectsIncompleteTenantKeysBeforeDatabase(t *testing.T) {
	store := closedPostgresStateStore(t)
	tests := []struct {
		name, want string
		call       func() error
	}{
		{name: "get tenant", call: func() error { _, err := store.GetSession(context.Background(), "", "session"); return err }, want: "session tenant ID"},
		{name: "get key", call: func() error { _, err := store.GetSession(context.Background(), "tenant", ""); return err }, want: "session key"},
		{name: "audit tenant", call: func() error { _, err := store.ListAudit(context.Background(), "", "trace"); return err }, want: "audit tenant ID"},
		{name: "purge tenant", call: func() error { _, err := store.PurgeAuditBefore(context.Background(), "", time.Now()); return err }, want: "audit tenant ID"},
		{name: "purge cutoff", call: func() error {
			_, err := store.PurgeAuditBefore(context.Background(), "tenant", time.Time{})
			return err
		}, want: "audit cutoff"},
		{name: "outbox tenant", call: func() error { return store.MarkOutboxDelivered(context.Background(), "", "event") }, want: "outbox tenant ID"},
		{name: "outbox event", call: func() error { return store.MarkOutboxDelivered(context.Background(), "tenant", "") }, want: "outbox event ID"},
		{name: "outbox retention tenant", call: func() error {
			_, err := store.PurgeDeliveredOutboxBefore(context.Background(), "", time.Now(), 1)
			return err
		}, want: "outbox retention tenant ID"},
		{name: "outbox retention cutoff", call: func() error {
			_, err := store.PurgeDeliveredOutboxBefore(context.Background(), "tenant", time.Time{}, 1)
			return err
		}, want: "outbox retention cutoff"},
		{name: "outbox retention limit", call: func() error {
			_, err := store.PurgeDeliveredOutboxBefore(context.Background(), "tenant", time.Now(), 0)
			return err
		}, want: "outbox retention limit"},
		{name: "archive tenant", call: func() error { return store.ArchiveSession(context.Background(), "", "session") }, want: "archive tenant ID"},
		{name: "archive key", call: func() error { return store.ArchiveSession(context.Background(), "tenant", "") }, want: "archive session key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "database is closed") {
				t.Fatalf("error = %v, want validation containing %q", err, test.want)
			}
		})
	}
}

func TestPostgresStateStoreWrapsUnavailableDatabaseErrors(t *testing.T) {
	store := closedPostgresStateStore(t)
	record := ExecutionRecord{TenantID: "tenant", AppCode: "app", SessionKey: "tenant/session", MessageID: "message", Channel: "web", BindingID: "web-console", TraceID: "trace", Action: "reply", Result: "success", OutboxType: "completed"}
	tests := []struct {
		name, want string
		call       func() error
	}{
		{name: "record", call: func() error { _, err := store.RecordExecution(context.Background(), record); return err }, want: "begin execution state transaction"},
		{name: "get", call: func() error { _, err := store.GetSession(context.Background(), "tenant", "session"); return err }, want: "get session"},
		{name: "list audit", call: func() error { _, err := store.ListAudit(context.Background(), "tenant", "trace"); return err }, want: "list audit events"},
		{name: "record audit", call: func() error {
			return store.RecordAudit(context.Background(), AuditEvent{TenantID: "tenant", TraceID: "trace", Action: "read"})
		}, want: "insert audit event"},
		{name: "purge", call: func() error { _, err := store.PurgeAuditBefore(context.Background(), "tenant", time.Now()); return err }, want: "purge audit events"},
		{name: "outbox", call: func() error { _, err := store.ListPendingOutbox(context.Background(), "tenant", 1); return err }, want: "list pending outbox"},
		{name: "sessions", call: func() error { _, err := store.ListSessions(context.Background(), "tenant", 1); return err }, want: "list sessions"},
		{name: "delivered", call: func() error { return store.MarkOutboxDelivered(context.Background(), "tenant", "event") }, want: "mark outbox delivered"},
		{name: "outbox retention", call: func() error {
			_, err := store.PurgeDeliveredOutboxBefore(context.Background(), "tenant", time.Now(), 1)
			return err
		}, want: "purge delivered outbox events"},
		{name: "active", call: func() error {
			_, err := store.ResolveSession(context.Background(), SessionRoute{
				TenantID: "tenant", AppCode: "app", Channel: "web", BindingID: "web-console",
				ConversationID: "conversation", ExternalUserID: "subject", SubjectID: "subject", Scope: "direct",
			}, "tenant/app/session/session")
			return err
		}, want: "begin session routing transaction"},
		{name: "archive", call: func() error { return store.ArchiveSession(context.Background(), "tenant", "session") }, want: "begin session archive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "database is closed") {
				t.Fatalf("error = %v, want contextual %q database error", err, test.want)
			}
		})
	}
}

func TestPostgresRecordExecutionHonorsCanceledContextBeforeDatabase(t *testing.T) {
	store := closedPostgresStateStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.RecordExecution(ctx, ExecutionRecord{}); err != context.Canceled {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
