package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/google/uuid"
)

// TestRuntimeStorePostgreSQLConformanceAndRestart is intentionally opt-in. It
// uses an already-provisioned tenant and channel binding so the runtime FK
// boundary is tested against real control-plane data without inventing it.
func TestRuntimeStorePostgreSQLConformanceAndRestart(t *testing.T) {
	dsn, tenantID, bindingID := runtimePostgresCredentials(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT 1 FROM public.tenant WHERE tenant_id=$1", tenantID).Scan(new(int)); err != nil {
		_ = db.Close()
		t.Fatalf("configured tenant is unavailable: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT 1 FROM public.channel_binding WHERE tenant_id=$1 AND binding_id=$2", tenantID, bindingID).Scan(new(int)); err != nil {
		_ = db.Close()
		t.Fatalf("configured channel binding is unavailable: %v", err)
	}

	sessionID := "runtime-restart-" + uuid.NewString()
	eventID := uuid.NewString()
	replyID := uuid.NewString()
	store := New(db)
	payload := runtimePostgresSeed(t, ctx, store, tenantID, bindingID, sessionID, eventID, replyID)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store = New(db)
	runtimePostgresAssertRecovered(t, ctx, store, tenantID, sessionID, eventID, replyID, payload)
}

func runtimePostgresCredentials(t *testing.T) (string, string, string) {
	t.Helper()
	dsn, tenantID, bindingID := os.Getenv("POSTGRES_RUNTIME_TEST_DSN"), os.Getenv("POSTGRES_RUNTIME_TEST_TENANT_ID"), os.Getenv("POSTGRES_RUNTIME_TEST_BINDING_ID")
	if dsn == "" || tenantID == "" || bindingID == "" {
		t.Skip("POSTGRES_RUNTIME_TEST_DSN, POSTGRES_RUNTIME_TEST_TENANT_ID, and POSTGRES_RUNTIME_TEST_BINDING_ID are required")
	}
	return dsn, tenantID, bindingID
}

func runtimePostgresSeed(t *testing.T, ctx context.Context, store *Store, tenantID, bindingID, sessionID, eventID, replyID string) []byte {
	t.Helper()
	created, err := store.CreateSession(ctx, tenantID, sessionID, map[string]any{"phase": "before-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 {
		t.Fatalf("created session version = %d", created.Version)
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, EventID: eventID, SessionID: sessionID, BindingID: bindingID, ExternalMessageID: "external-" + eventID})
	if err != nil || duplicate {
		t.Fatalf("first message = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	if _, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, EventID: uuid.NewString(), SessionID: sessionID, BindingID: bindingID, ExternalMessageID: "external-" + eventID}); err != nil || !duplicate {
		t.Fatalf("duplicate message = duplicate=%v err=%v", duplicate, err)
	}
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "integration-runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "integration-runner", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	payload := []byte("{\"id\":\"" + eventID + "\",\"done\":true}")
	if _, err := store.AppendEventPayload(ctx, sessionstorage.EventPayload{TenantID: tenantID, SessionID: sessionID, EventID: "runner-" + eventID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: replyID, EventID: eventID, SegmentIndex: 0, SegmentCount: 1, Payload: "durable reply"}); err != nil {
		t.Fatal(err)
	}
	return payload
}

func runtimePostgresAssertRecovered(t *testing.T, ctx context.Context, store *Store, tenantID, sessionID, eventID, replyID string, payload []byte) {
	t.Helper()
	recovered, err := store.GetSession(ctx, tenantID, sessionID)
	if err != nil || recovered.State["phase"] != "before-restart" {
		t.Fatalf("recovered session = %+v err=%v", recovered, err)
	}
	recoveredEvent, err := store.GetMessage(ctx, tenantID, eventID)
	if err != nil || recoveredEvent.Status != runtimestorage.EventCompleted {
		t.Fatalf("recovered event = %+v err=%v", recoveredEvent, err)
	}
	history, err := store.ListEventPayloads(ctx, tenantID, sessionID)
	if err != nil || len(history) != 1 || string(history[0].Payload) != string(payload) {
		t.Fatalf("recovered history = %+v err=%v", history, err)
	}
	recoveredReply, err := store.GetReply(ctx, tenantID, replyID, 0)
	if err != nil || recoveredReply.Status != runtimestorage.ReplyPending {
		t.Fatalf("recovered reply = %+v err=%v", recoveredReply, err)
	}
	if err := store.DeleteSession(ctx, tenantID, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(ctx, tenantID, sessionID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted session lookup = %v", err)
	}
}

type integrationProvider struct{ id string }

func (p integrationProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	return p.id, nil
}
func (p integrationProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
}

func TestRuntimeStorePostgreSQLOutboxWorkerRestartRecovery(t *testing.T) {
	dsn := os.Getenv("POSTGRES_RUNTIME_TEST_DSN")
	tenantID, bindingID := os.Getenv("POSTGRES_RUNTIME_TEST_TENANT_ID"), os.Getenv("POSTGRES_RUNTIME_TEST_BINDING_ID")
	if dsn == "" || tenantID == "" || bindingID == "" {
		t.Skip("POSTGRES_RUNTIME_TEST_DSN, POSTGRES_RUNTIME_TEST_TENANT_ID, and POSTGRES_RUNTIME_TEST_BINDING_ID are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	store := New(db)
	sessionID, eventID, replyID := "worker-restart-"+uuid.NewString(), uuid.NewString(), uuid.NewString()
	if _, err := store.CreateSession(ctx, tenantID, sessionID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, EventID: eventID, SessionID: sessionID, BindingID: bindingID, ExternalMessageID: "external-" + eventID}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: replyID, EventID: eventID, SegmentIndex: 0, SegmentCount: 1, Payload: "restart reply"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store = New(db)
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: integrationProvider{id: "provider-restart"}, TenantID: tenantID, Owner: "restart-worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("restart worker = %d err=%v", processed, err)
	}
	reply, err := store.GetReply(ctx, tenantID, replyID, 0)
	if err != nil || reply.Status != runtimestorage.ReplySent || reply.ProviderMessageID != "provider-restart" {
		t.Fatalf("recovered reply = %+v err=%v", reply, err)
	}
	if err := store.DeleteSession(ctx, tenantID, sessionID); err != nil {
		t.Fatal(err)
	}
}
