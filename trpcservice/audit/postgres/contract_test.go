package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
)

func TestPostgreSQLAuditRelayContract(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("TRPC_MIGRATION_TEST=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FCA"
	if _, err := db.ExecContext(ctx, `TRUNCATE audit_event,outbox CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,'audit-contract','Audit Contract') ON CONFLICT DO NOTHING`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
VALUES($1,'audit-outbox','audit','tenant-change',1,'tenant-status:1','tenant://t_01ARZ3NDEKTSV4RRFFQ69G5FCA/status/1','00-0123456789abcdef0123456789abcdef-0123456789abcdef-01')`, tenantID); err != nil {
		t.Fatal(err)
	}
	outbox := messagingpostgres.New(db)
	store := New(db)
	before, err := store.AuditBacklog(ctx, time.Now().UTC())
	if err != nil || before.Pending != 1 || before.Active() != 1 || before.DeadLetter != 0 {
		t.Fatalf("initial backlog=%#v err=%v", before, err)
	}
	claimed, err := outbox.ClaimOutbox(ctx, "audit", 1, "crashed-owner", time.Now().UTC().Add(-time.Second))
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%#v err=%v", claimed, err)
	}
	event, err := store.ResolveAuditEvent(ctx, claimed[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Emit(ctx, event); err != nil {
		t.Fatal(err)
	}
	worker := audit.Relay{Base: relay.Base{Outbox: outbox, Kind: "audit", Owner: "recovery-owner", ClaimTTL: time.Second}, Resolver: store, Sink: store}
	count, err := worker.RunOnce(ctx)
	if err != nil || count != 1 {
		t.Fatalf("recovery count=%d err=%v", count, err)
	}
	var events, attempts int
	var state string
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_event WHERE tenant_id=$1`, tenantID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT state,attempt FROM outbox WHERE tenant_id=$1 AND outbox_id='audit-outbox'`, tenantID).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if events != 1 || state != "published" || attempts != 2 || event.TraceID != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("events=%d state=%s attempts=%d event=%#v", events, state, attempts, event)
	}
	after, err := store.AuditBacklog(ctx, time.Now().UTC())
	if err != nil || after.Active() != 0 || after.DeadLetter != 0 || after.OldestAge != 0 {
		t.Fatalf("final backlog=%#v err=%v", after, err)
	}
	collision := event
	collision.Decision = "different"
	if err := store.Emit(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision=%v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE audit_event SET decision='tampered' WHERE tenant_id=$1`, tenantID); sqlState(err) != "55000" {
		t.Fatalf("immutable state=%q err=%v", sqlState(err), err)
	}
	forged := messaging.OutboxRecord{TenantID: tenantID, OutboxID: "forged", Kind: "audit", AggregateID: "request",
		PayloadRef: "confirmation://other-tenant/value", IdempotencyKey: "confirmation:value", CreatedAt: time.Now().UTC()}
	if _, err := store.ResolveAuditEvent(ctx, forged); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("forged scope=%v", err)
	}
}

// TestExecutionTerminalAuditUsesTerminalClassification pins the resolver side
// of the single-terminal-audit rule against a migrated database. The worker
// owns the CAS and idempotency key; this test ensures the relay cannot silently
// turn that durable fact back into a generic execution audit.
func TestExecutionTerminalAuditUsesTerminalClassification(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("TRPC_MIGRATION_TEST=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	record := messaging.OutboxRecord{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", OutboxID: "terminal-audit-contract",
		Kind: "audit", AggregateID: "probe-request", EventSeq: 1, IdempotencyKey: "execution-terminal:probe-request",
		PayloadRef: "execution-terminal://t_01ARZ3NDEKTSV4RRFFQ69G5FAV/probe-request/succeeded", CreatedAt: time.Now().UTC()}
	event, err := store.ResolveAuditEvent(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if event.Action != "execution.terminal" || event.Decision != "succeeded" || event.RequestID != "probe-request" {
		t.Fatalf("terminal event=%#v", event)
	}
	if err := store.Emit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := store.Emit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM audit_event WHERE tenant_id=$1 AND audit_id=$2`, event.TenantID, event.AuditID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("terminal audit count=%d", count)
	}
}

func sqlState(err error) string {
	type state interface{ SQLState() string }
	var value state
	if errors.As(err, &value) {
		return value.SQLState()
	}
	return ""
}
