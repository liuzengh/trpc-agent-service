//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/migrations"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func openDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration-tag tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := migrations.NewRunner(db).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func waitForPostgresRowLockWait(t *testing.T, db *sql.DB, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	pattern := "%FROM " + table + " WHERE id=$1 FOR UPDATE%"
	for time.Now().Before(deadline) {
		var waiting bool
		if err := db.QueryRowContext(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE wait_event_type='Lock' AND state='active' AND query LIKE $1
			)`, pattern).Scan(&waiting); err != nil {
			t.Fatalf("inspect PostgreSQL lock wait: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for PostgreSQL %s row-lock waiter", table)
}

func TestPostgresReliableLifecycle(t *testing.T) {
	db := openDatabase(t)
	tenantID := "integration-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'integration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	inbox := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
		AgentApp: "support", ExternalMessageID: "update-1", ConversationID: "chat-1",
		ReplyToID: "provider-message-1",
		UserID:    "user-1", SessionID: tenantID + ":telegram:bot-1:user-1",
		PayloadHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Payload:     []byte(`{"content":"hello"}`),
	}
	inserted, err := store.EnqueueInbox(ctx, inbox)
	if err != nil || !inserted {
		t.Fatalf("enqueue inserted=%v err=%v", inserted, err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	reply := reliable.OutboxReply{
		ContentType: "text", Content: "world",
	}
	created, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, reply)
	if err != nil {
		t.Fatal(err)
	}
	if created.TenantID != tenantID || created.ChannelAccountID != "bot-1" ||
		created.ConversationID != "chat-1" || created.ReplyToID != "provider-message-1" {
		t.Fatalf("outbox route was not derived from inbox: %#v", created)
	}
	outbox, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchStarted(ctx, outbox.ID, outbox.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceOutbox(ctx, outbox.ID, outbox.Lease, 1); err != nil {
		t.Fatal(err)
	}
	outbox, err = store.ClaimOutbox(ctx, "delivery-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if outbox.DeliveryCursor != 1 {
		t.Fatalf("delivery cursor=%d, want 1", outbox.DeliveryCursor)
	}
	if err := store.MarkDispatchStarted(ctx, outbox.ID, outbox.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, outbox.ID, outbox.Lease); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFairClaimReclaimsExpiredLeaseUnderMaxInflight(t *testing.T) {
	db := openDatabase(t)
	tenantID := "fair-expired-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'fair-expired','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenant_queue_schedule WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	if err := store.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{
		TenantID: tenantID, Weight: 1, MaxInflight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	newMessage := func(externalID, sessionID string) *reliable.InboxMessage {
		return &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
			AgentApp: "support", ExternalMessageID: externalID, ConversationID: externalID,
			ReplyToID: "reply-" + externalID, UserID: "user-1", SessionID: sessionID,
			PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
		}
	}
	first, second := newMessage("first", "session-first"), newMessage("second", "session-second")
	for _, message := range []*reliable.InboxMessage{first, second} {
		if inserted, err := store.EnqueueInbox(ctx, message); err != nil || !inserted {
			t.Fatalf("enqueue %s inserted=%t err=%v", message.ExternalMessageID, inserted, err)
		}
	}
	claim, err := store.ClaimInboxFair(ctx, "consumer-a", time.Minute)
	if err != nil || claim.ID != first.ID {
		t.Fatalf("initial fair claim=%#v err=%v", claim, err)
	}
	if _, err := db.Exec(`UPDATE inbox_messages SET lease_until=clock_timestamp() - interval '1 second' WHERE id=$1`, claim.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimInboxFair(ctx, "consumer-b", time.Minute)
	if err != nil || reclaimed.ID != first.ID {
		t.Fatalf("expired fair claim=%#v err=%v, want first message reclaimed", reclaimed, err)
	}
}

func TestPostgresReplicaTakeoverFencesCrashedWorkerAndCreatesOneOutbox(t *testing.T) {
	dbA := openDatabase(t)
	dsn := os.Getenv("TEST_DATABASE_URL")
	dbB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB.Close() })
	if err := dbB.PingContext(context.Background()); err != nil {
		t.Fatalf("connect replica B: %v", err)
	}

	tenantID := "replica-takeover-" + uuid.NewString()
	if _, err := dbA.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'replica takeover','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = dbA.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = dbA.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = dbA.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	storeA := reliable.NewPostgresStore(dbA)
	storeB := reliable.NewPostgresStore(dbB)
	ctx := context.Background()
	message := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "wecom", ChannelAccountID: "corp-1",
		AgentApp: "support", ExternalMessageID: "provider-1", ConversationID: "chat-1",
		ReplyToID: "reply-1", UserID: "user-1", SessionID: tenantID + ":wecom:corp-1:user-1",
		PayloadHash: strings.Repeat("d", 64), Payload: []byte(`{"content":"hello"}`), MaxAttempts: 3,
	}
	if inserted, err := storeA.EnqueueInbox(ctx, message); err != nil || !inserted {
		t.Fatalf("enqueue inserted=%t err=%v", inserted, err)
	}
	claimA, err := storeA.ClaimInbox(ctx, "consumer-replica-a", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Use the authoritative database clock. Replica A now disappears without
	// completing or renewing its lease.
	if _, err := dbA.ExecContext(ctx, `SELECT pg_sleep(0.08)`); err != nil {
		t.Fatal(err)
	}
	claimB, err := storeB.ClaimInbox(ctx, "consumer-replica-b", time.Minute)
	if err != nil {
		t.Fatalf("replica B reclaim: %v", err)
	}
	if claimB.ID != claimA.ID || claimB.Lease.Fence <= claimA.Lease.Fence {
		t.Fatalf("takeover claim=%#v, want id=%d and fence>%d", claimB, claimA.ID, claimA.Lease.Fence)
	}
	if _, err := storeA.CompleteInbox(ctx, claimA.ID, claimA.Lease, reliable.OutboxReply{Content: "stale reply"}); !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("crashed replica stale completion error=%v, want stale lease", err)
	}
	created, err := storeB.CompleteInbox(ctx, claimB.ID, claimB.Lease, reliable.OutboxReply{Content: "authoritative reply"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Content != "authoritative reply" || created.TenantID != tenantID {
		t.Fatalf("authoritative outbox=%#v", created)
	}
	var outboxCount int
	if err := dbA.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_messages WHERE inbox_id=$1`, claimA.ID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("outbox count=%d, want exactly one", outboxCount)
	}
}

func TestPostgresRetryInboxRejectsLeaseExpiredWhileWaitingForRowLock(t *testing.T) {
	db := openDatabase(t)
	tenantID := "retry-lock-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'retry-lock','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	message := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
		AgentApp: "support", ExternalMessageID: "lock-wait", ConversationID: "chat-1",
		ReplyToID: "reply-lock-wait", UserID: "user-1", SessionID: "session-lock-wait",
		PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
	}
	if inserted, err := store.EnqueueInbox(ctx, message); err != nil || !inserted {
		t.Fatalf("enqueue inserted=%t err=%v", inserted, err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	var lockedID int64
	if err := lockTx.QueryRowContext(ctx,
		`SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE`, claim.ID,
	).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}

	retryDone := make(chan error, 1)
	go func() {
		retryDone <- store.RetryInbox(ctx, claim.ID, claim.Lease, errors.New("temporary"), time.Now().Add(time.Minute))
	}()
	waitForPostgresRowLockWait(t, db, "inbox_messages", 2*time.Second)
	// Keep the lock held long enough for the 100ms lease to expire before the
	// waiting mutation can acquire it.
	time.Sleep(150 * time.Millisecond)
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-retryDone; !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("RetryInbox after lock wait error=%v, want stale lease", err)
	}

	var status string
	var lastError string
	if err := db.QueryRowContext(ctx,
		`SELECT status,last_error FROM inbox_messages WHERE id=$1`, lockedID,
	).Scan(&status, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != string(reliable.InboxProcessing) || lastError != "" {
		t.Fatalf("locked inbox mutated after stale retry: status=%q last_error=%q", status, lastError)
	}
}

func TestPostgresCompleteInboxRejectsLeaseExpiredWhileWaitingForRowLock(t *testing.T) {
	db := openDatabase(t)
	tenantID := "complete-lock-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'complete-lock','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	inbox := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
		AgentApp: "support", ExternalMessageID: "complete-lock", ConversationID: "chat-1",
		ReplyToID: "reply-complete-lock", UserID: "user-1", SessionID: "session-complete-lock",
		PayloadHash: strings.Repeat("c", 64), Payload: []byte(`{"content":"hello"}`),
	}
	if inserted, err := store.EnqueueInbox(ctx, inbox); err != nil || !inserted {
		t.Fatalf("enqueue inserted=%t err=%v", inserted, err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	var lockedID int64
	if err := lockTx.QueryRowContext(ctx,
		`SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE`, claim.ID,
	).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}

	completeDone := make(chan error, 1)
	go func() {
		_, completeErr := store.CompleteInbox(ctx, claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"})
		completeDone <- completeErr
	}()
	waitForPostgresRowLockWait(t, db, "inbox_messages", 2*time.Second)
	time.Sleep(150 * time.Millisecond)
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-completeDone; !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("CompleteInbox after lock wait error=%v, want stale lease", err)
	}

	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM inbox_messages WHERE id=$1`, lockedID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(reliable.InboxProcessing) {
		t.Fatalf("locked inbox completed after stale request: status=%q", status)
	}
	var outboxCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox_messages WHERE inbox_id=$1`, lockedID,
	).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 0 {
		t.Fatalf("stale CompleteInbox created %d outbox rows", outboxCount)
	}
}

func TestPostgresMarkDeliveredRejectsLeaseExpiredWhileWaitingForRowLock(t *testing.T) {
	db := openDatabase(t)
	tenantID := "outbox-lock-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'outbox-lock','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	inbox := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
		AgentApp: "support", ExternalMessageID: "outbox-lock", ConversationID: "chat-1",
		ReplyToID: "reply-outbox-lock", UserID: "user-1", SessionID: "session-outbox-lock",
		PayloadHash: strings.Repeat("b", 64), Payload: []byte(`{"content":"hello"}`),
	}
	if inserted, err := store.EnqueueInbox(ctx, inbox); err != nil || !inserted {
		t.Fatalf("enqueue inserted=%t err=%v", inserted, err)
	}
	inboxClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(ctx, inboxClaim.ID, inboxClaim.Lease, reliable.OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	// Keep enough headroom for high-latency integration transports (for
	// example, a Windows-to-K3d port-forward). Expiry is forced below with
	// PostgreSQL's own clock while the row lock remains held.
	outboxClaim, err := store.ClaimOutbox(ctx, "delivery-a", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if outboxClaim.ID != outbox.ID {
		t.Fatalf("claimed outbox id=%d, want %d", outboxClaim.ID, outbox.ID)
	}
	if err := store.MarkDispatchStarted(ctx, outboxClaim.ID, outboxClaim.Lease); err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	var lockedID int64
	if err := lockTx.QueryRowContext(ctx,
		`SELECT id FROM outbox_messages WHERE id=$1 FOR UPDATE`, outboxClaim.ID,
	).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}

	deliveredDone := make(chan error, 1)
	go func() {
		deliveredDone <- store.MarkDelivered(ctx, outboxClaim.ID, outboxClaim.Lease)
	}()
	waitForPostgresRowLockWait(t, db, "outbox_messages", 2*time.Second)
	// Hold the row lock past the delivery lease deadline using the
	// authoritative database clock. The mutation must evaluate the lease after
	// it acquires the lock, not before waiting.
	if _, err := lockTx.ExecContext(ctx, `SELECT pg_sleep(2.1)`); err != nil {
		t.Fatal(err)
	}
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-deliveredDone; !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("MarkDelivered after lock wait error=%v, want stale lease", err)
	}

	var status string
	var deliveredAt sql.NullTime
	if err := db.QueryRowContext(ctx,
		`SELECT status,delivered_at FROM outbox_messages WHERE id=$1`, lockedID,
	).Scan(&status, &deliveredAt); err != nil {
		t.Fatal(err)
	}
	if status != string(reliable.OutboxDispatchStarted) || deliveredAt.Valid {
		t.Fatalf("locked outbox mutated after stale delivery: status=%q delivered_at_valid=%t", status, deliveredAt.Valid)
	}
}

func TestPostgresDeletingQueuePolicyKeepsBacklogClaimable(t *testing.T) {
	db := openDatabase(t)
	tenantID := "fair-delete-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'fair-delete','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenant_queue_schedule WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	if err := store.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{
		TenantID: tenantID, Weight: 2, MaxQueued: 10, MaxInflight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	message := &reliable.InboxMessage{
		TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
		AgentApp: "support", ExternalMessageID: "backlog", ConversationID: "chat-1",
		ReplyToID: "reply-backlog", UserID: "user-1", SessionID: "session-backlog",
		PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
	}
	if inserted, err := store.EnqueueInbox(ctx, message); err != nil || !inserted {
		t.Fatalf("enqueue backlog inserted=%t err=%v", inserted, err)
	}
	if err := store.DeleteTenantQueuePolicy(ctx, tenantID); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInboxFair(ctx, "consumer-a", time.Minute)
	if err != nil || claim.ID != message.ID {
		t.Fatalf("claim after policy delete=%#v err=%v, want backlog message", claim, err)
	}
}

func TestPostgresReliableExpiryReaper(t *testing.T) {
	db := openDatabase(t)
	tenantID := "reaper-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'reaper','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_session_sequences WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	newInbox := func(externalID, sessionID string) *reliable.InboxMessage {
		return &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
			AgentApp: "support", ExternalMessageID: externalID, ConversationID: "chat-1",
			ReplyToID: "provider-" + externalID, UserID: "user-1", SessionID: sessionID,
			PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
		}
	}

	finalInbox := newInbox("final-attempt", "session-final")
	finalInbox.MaxAttempts = 1
	if inserted, err := store.EnqueueInbox(ctx, finalInbox); err != nil || !inserted {
		t.Fatalf("enqueue final inbox: inserted=%t err=%v", inserted, err)
	}
	finalClaim, err := store.ClaimInbox(ctx, "consumer-final", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	approvalInbox := newInbox("approval", "session-approval")
	if inserted, err := store.EnqueueInbox(ctx, approvalInbox); err != nil || !inserted {
		t.Fatalf("enqueue approval inbox: inserted=%t err=%v", inserted, err)
	}
	approvalClaim, err := store.ClaimInbox(ctx, "consumer-approval", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WaitInboxApproval(
		ctx,
		approvalClaim.ID,
		approvalClaim.Lease,
		errors.New("tool approval required"),
		time.Second,
		time.Now().UTC().Add(time.Hour),
	); err != nil {
		t.Fatal(err)
	}

	deliveryInbox := newInbox("delivery", "session-delivery")
	if inserted, err := store.EnqueueInbox(ctx, deliveryInbox); err != nil || !inserted {
		t.Fatalf("enqueue delivery inbox: inserted=%t err=%v", inserted, err)
	}
	deliveryInboxClaim, err := store.ClaimInbox(ctx, "consumer-delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deliveryMessage, err := store.CompleteInbox(ctx, deliveryInboxClaim.ID, deliveryInboxClaim.Lease, reliable.OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE outbox_messages SET max_attempts=1 WHERE id=$1`, deliveryMessage.ID); err != nil {
		t.Fatal(err)
	}
	deliveryClaim, err := store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Use database time so the test has no sleep or host-clock assumption.
	if _, err := db.Exec(`UPDATE inbox_messages SET lease_until=now() - interval '2 minutes' WHERE id=$1`, finalClaim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE inbox_messages SET approval_deadline=now() - interval '1 minute', next_attempt_at=now() - interval '1 minute' WHERE id=$1`, approvalClaim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE outbox_messages SET lease_until=now() - interval '1 minute' WHERE id=$1`, deliveryClaim.ID); err != nil {
		t.Fatal(err)
	}

	first, err := store.ReapExpired(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first != (reliable.ExpiredWorkReapResult{InboxFinalAttemptExpired: 1, OutboxFinalAttemptExpired: 1}) {
		t.Fatalf("first bounded reap=%+v", first)
	}
	second, err := store.ReapExpired(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second != (reliable.ExpiredWorkReapResult{InboxApprovalExpired: 1}) {
		t.Fatalf("second bounded reap=%+v", second)
	}

	assertInbox := func(id int64, wantError string) {
		t.Helper()
		var status, lastError string
		if err := db.QueryRow(`SELECT status,last_error FROM inbox_messages WHERE id=$1`, id).Scan(&status, &lastError); err != nil {
			t.Fatal(err)
		}
		if status != string(reliable.InboxDeadLetter) || lastError != wantError {
			t.Fatalf("inbox %d status/error=%q/%q, want %q/%q", id, status, lastError, reliable.InboxDeadLetter, wantError)
		}
	}
	assertInbox(finalClaim.ID, reliable.ErrLeaseExpiredAfterFinalAttempt.Error())
	assertInbox(approvalClaim.ID, "tool approval expired")
	var outboxStatus, outboxError string
	if err := db.QueryRow(`SELECT status,last_error FROM outbox_messages WHERE id=$1`, deliveryClaim.ID).Scan(&outboxStatus, &outboxError); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != string(reliable.OutboxDeadLetter) || outboxError != reliable.ErrLeaseExpiredAfterFinalAttempt.Error() {
		t.Fatalf("outbox status/error=%q/%q", outboxStatus, outboxError)
	}
	if _, err := store.CompleteInbox(ctx, finalClaim.ID, finalClaim.Lease, reliable.OutboxReply{Content: "late"}); !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("late final inbox completion error=%v, want stale lease", err)
	}
}

func TestPostgresReliableSessionFIFOAndReplay(t *testing.T) {
	db := openDatabase(t)
	tenantID := "fifo-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'fifo','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM message_replay_audit WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_session_sequences WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	store := reliable.NewPostgresStore(db)
	ctx := context.Background()
	newInbox := func(externalID, sessionID string) *reliable.InboxMessage {
		return &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
			AgentApp: "support", ExternalMessageID: externalID, ConversationID: "chat-1",
			ReplyToID: "provider-" + externalID, UserID: "user-1", SessionID: sessionID,
			PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
		}
	}

	first := newInbox("update-1", "session-a")
	first.MaxAttempts = 1
	second := newInbox("update-2", "session-a")
	independent := newInbox("update-3", "session-b")
	for _, message := range []*reliable.InboxMessage{first, second, independent} {
		inserted, err := store.EnqueueInbox(ctx, message)
		if err != nil || !inserted {
			t.Fatalf("enqueue %s inserted=%v err=%v", message.ExternalMessageID, inserted, err)
		}
	}
	if first.SessionSequence != 1 || second.SessionSequence != 2 || independent.SessionSequence != 1 {
		t.Fatalf("unexpected stream sequences: first=%d second=%d independent=%d",
			first.SessionSequence, second.SessionSequence, independent.SessionSequence)
	}

	// A provider redelivery keeps the immutable routing identity. A changed
	// session or conversation is an idempotency conflict, covered by the store
	// unit tests, rather than a canonical duplicate.
	duplicate := newInbox("update-1", "session-a")
	inserted, err := store.EnqueueInbox(ctx, duplicate)
	if err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	if duplicate.ID != first.ID || duplicate.SessionSequence != first.SessionSequence ||
		duplicate.SessionID != first.SessionID || duplicate.ConversationID != first.ConversationID ||
		duplicate.ReplyToID != first.ReplyToID {
		t.Fatalf("duplicate did not return canonical inbox: %#v", duplicate)
	}

	firstClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil || firstClaim.ID != first.ID {
		t.Fatalf("first claim=%#v err=%v", firstClaim, err)
	}
	independentClaim, err := store.ClaimInbox(ctx, "consumer-b", time.Minute)
	if err != nil || independentClaim.ID != independent.ID {
		t.Fatalf("independent claim=%#v err=%v", independentClaim, err)
	}
	if _, err := store.CompleteInbox(ctx, independentClaim.ID, independentClaim.Lease, reliable.OutboxReply{Content: "independent"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetryInbox(ctx, firstClaim.ID, firstClaim.Lease, errors.New("permanent"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimInbox(ctx, "consumer-c", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("successor bypassed dead-letter predecessor: %v", err)
	}
	if err := store.ReplayInbox(ctx, first.ID, "operator-a", "incident resolved"); err != nil {
		t.Fatal(err)
	}
	firstClaim, err = store.ClaimInbox(ctx, "consumer-d", time.Minute)
	if err != nil || firstClaim.ID != first.ID {
		t.Fatalf("replayed claim=%#v err=%v", firstClaim, err)
	}
	created, err := store.CompleteInbox(ctx, firstClaim.ID, firstClaim.Lease, reliable.OutboxReply{Content: "recovered"})
	if err != nil {
		t.Fatal(err)
	}
	if created.TenantID != tenantID || created.ChannelAccountID != "bot-1" ||
		created.ConversationID != "chat-1" || created.ReplyToID != "provider-update-1" {
		t.Fatalf("outbox route was not derived from inbox: %#v", created)
	}
	secondClaim, err := store.ClaimInbox(ctx, "consumer-e", time.Minute)
	if err != nil || secondClaim.ID != second.ID {
		t.Fatalf("successor after replay=%#v err=%v", secondClaim, err)
	}
	if _, err := store.CompleteInbox(ctx, secondClaim.ID, secondClaim.Lease, reliable.OutboxReply{Content: "second"}); err != nil {
		t.Fatal(err)
	}

	independentReply, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil || independentReply.SessionID != "session-b" {
		t.Fatalf("independent reply=%#v err=%v", independentReply, err)
	}
	if err := store.MarkDispatchStarted(ctx, independentReply.ID, independentReply.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, independentReply.ID, independentReply.Lease); err != nil {
		t.Fatal(err)
	}
	firstReply, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute)
	if err != nil || firstReply.SessionID != "session-a" || firstReply.SessionSequence != 1 {
		t.Fatalf("first ordered reply=%#v err=%v", firstReply, err)
	}
	if _, err := store.ClaimOutbox(ctx, "delivery-c", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("later reply bypassed predecessor: %v", err)
	}
	if err := store.MarkDispatchStarted(ctx, firstReply.ID, firstReply.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, firstReply.ID, firstReply.Lease); err != nil {
		t.Fatal(err)
	}
	secondReply, err := store.ClaimOutbox(ctx, "delivery-d", time.Minute)
	if err != nil || secondReply.SessionSequence != 2 {
		t.Fatalf("second ordered reply=%#v err=%v", secondReply, err)
	}

	var actor, reason, mode string
	if err := db.QueryRow(`SELECT requested_by,reason,replay_mode FROM message_replay_audit WHERE queue_type='inbox' AND message_id=$1`, first.ID).
		Scan(&actor, &reason, &mode); err != nil {
		t.Fatal(err)
	}
	if actor != "operator-a" || reason != "incident resolved" || mode != "restart" {
		t.Fatalf("unexpected replay audit actor=%q reason=%q mode=%q", actor, reason, mode)
	}
}

func TestPostgresReliableRollingUpgradeCompatibility(t *testing.T) {
	db := openDatabase(t)
	tenantID := "legacy-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'legacy','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_session_sequences WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	legacyPayload := []byte(`{"metadata":{"provider_message_id":"42"}}`)
	var inboxID, sequence int64
	var replyToID string
	err := db.QueryRow(`
		INSERT INTO inbox_messages (
			tenant_id, channel_type, channel_account_id, external_message_id,
			agent_app_name, conversation_id, user_id, session_id,
			payload_hash, payload, trace_parent
		) VALUES ($1,'telegram','bot-1','update-1','support','chat-1','user-1',
		          'session-a',$2,$3,'')
		RETURNING id, session_sequence, reply_to_id`,
		tenantID, strings.Repeat("a", 64), legacyPayload,
	).Scan(&inboxID, &sequence, &replyToID)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 1 || replyToID != "42" {
		t.Fatalf("legacy Inbox sequence/reply=%d/%q, want 1/42", sequence, replyToID)
	}

	var appName, sessionID string
	var outboxSequence int64
	err = db.QueryRow(`
		INSERT INTO outbox_messages (
			inbox_id, tenant_id, channel_type, channel_account_id,
			conversation_id, reply_to_id, content_type, content, trace_parent
		) VALUES ($1,$2,'telegram','bot-1','chat-1','42','text','legacy reply','')
		RETURNING agent_app_name, session_id, session_sequence`,
		inboxID, tenantID,
	).Scan(&appName, &sessionID, &outboxSequence)
	if err != nil {
		t.Fatal(err)
	}
	if appName != "support" || sessionID != "session-a" || outboxSequence != 1 {
		t.Fatalf("legacy Outbox order=%q/%q/%d", appName, sessionID, outboxSequence)
	}
}

func TestPostgresReliableConcurrentDuplicateDoesNotConsumeSequence(t *testing.T) {
	db := openDatabase(t)
	tenantID := "duplicate-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'duplicate','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM inbox_session_sequences WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	store := reliable.NewPostgresStore(db)
	newInbox := func(externalID string) *reliable.InboxMessage {
		return &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-1",
			AgentApp: "support", ExternalMessageID: externalID, ConversationID: "chat-1",
			ReplyToID: "42", UserID: "user-1", SessionID: "session-a",
			PayloadHash: strings.Repeat("a", 64), Payload: []byte(`{"content":"hello"}`),
		}
	}
	messages := []*reliable.InboxMessage{newInbox("update-1"), newInbox("update-1")}
	inserted := make([]bool, len(messages))
	errs := make([]error, len(messages))
	var wait sync.WaitGroup
	start := make(chan struct{})
	for index := range messages {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			inserted[index], errs[index] = store.EnqueueInbox(context.Background(), messages[index])
		}(index)
	}
	close(start)
	wait.Wait()
	insertCount := 0
	for index := range messages {
		if errs[index] != nil {
			t.Fatalf("concurrent enqueue %d: %v", index, errs[index])
		}
		if inserted[index] {
			insertCount++
		}
		if messages[index].SessionSequence != 1 || messages[index].ID != messages[0].ID {
			t.Fatalf("non-canonical duplicate %d: %#v", index, messages[index])
		}
	}
	if insertCount != 1 {
		t.Fatalf("insert count=%d, want 1", insertCount)
	}
	next := newInbox("update-2")
	if inserted, err := store.EnqueueInbox(context.Background(), next); err != nil || !inserted {
		t.Fatalf("next enqueue inserted=%v err=%v", inserted, err)
	}
	if next.SessionSequence != 2 {
		t.Fatalf("duplicate consumed a sequence: next=%d, want 2", next.SessionSequence)
	}
}

func TestExecutionReconcilerOnlyAbandonsStaleRunningRecords(t *testing.T) {
	db := openDatabase(t)
	tenantID := "reconcile-" + uuid.NewString()
	appID := "app-" + uuid.NewString()
	versionID := "version-" + uuid.NewString()
	deploymentID := "deployment-" + uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'reconcile','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup := []struct {
			name  string
			query string
			arg   string
		}{
			{name: "execution reconciliations", query: `DELETE FROM execution_reconciliations WHERE tenant_id=$1`, arg: tenantID},
			{name: "session execution guards", query: `DELETE FROM session_execution_guards WHERE tenant_id=$1`, arg: tenantID},
			{name: "invocation results", query: `DELETE FROM invocation_results WHERE tenant_id=$1`, arg: tenantID},
			{name: "invocation bindings", query: `DELETE FROM invocation_bindings WHERE tenant_id=$1`, arg: tenantID},
			{name: "execution records", query: `DELETE FROM execution_records WHERE tenant_id=$1`, arg: tenantID},
			{name: "deployments", query: `DELETE FROM deployments WHERE tenant_id=$1`, arg: tenantID},
			{name: "agent versions", query: `DELETE FROM agent_versions WHERE agent_app_id=$1`, arg: appID},
			{name: "agent apps", query: `DELETE FROM agent_apps WHERE id=$1`, arg: appID},
			{name: "tenant", query: `DELETE FROM tenants WHERE id=$1`, arg: tenantID},
		}
		for _, item := range cleanup {
			if _, err := db.Exec(item.query, item.arg); err != nil {
				t.Errorf("clean up %s: %v", item.name, err)
				return
			}
		}
	})
	if _, err := db.Exec(`INSERT INTO agent_apps(id,tenant_id,name,status) VALUES($1,$2,'support','active')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	snapshot := `{"agent":{"name":"support","type":"llm","defaultModel":"gpt"},"model":{"provider":"openai","modelName":"gpt"}}`
	if _, err := db.Exec(`INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by) VALUES($1,$2,1,$3,$4,'published','integration')`, versionID, appID, snapshot, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO deployments(id,tenant_id,agent_app_id,agent_version_id,kind,traffic_bps,status,created_by) VALUES($1,$2,$3,$4,'stable',10000,'active','integration')`, deploymentID, tenantID, appID, versionID); err != nil {
		t.Fatal(err)
	}

	var staleID, succeededID, recentID int64
	now := time.Now().UTC()
	insertExecution := func(status string, started time.Time) (int64, string) {
		t.Helper()
		var id int64
		sessionID := uuid.NewString()
		idempotencyKey := "integration:" + uuid.NewString()
		token := "integration-token-" + uuid.NewString()
		heartbeatAt := started.Add(-2 * time.Minute)
		leaseUntil := started.Add(-time.Minute)
		var completedAt any
		if status == "SUCCEEDED" {
			completedAt = started
		}
		err := db.QueryRow(`
			INSERT INTO execution_records(
				tenant_id,session_id,agent_app_id,agent_version_id,deployment_id,
				idempotency_key,payload_hash,attempt_number,execution_token,status,
				started_at,heartbeat_at,lease_until,completed_at
			) VALUES($1,$2,$3,$4,$5,$6,$7,1,$8,$9,$10,$11,$12,$13)
			RETURNING id`,
			tenantID, sessionID, appID, versionID, deploymentID,
			idempotencyKey, strings.Repeat("a", 64), token, status, started, heartbeatAt, leaseUntil, completedAt,
		).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		return id, sessionID
	}
	var staleSession, succeededSession, recentSession string
	staleID, staleSession = insertExecution("RUNNING", now.Add(-30*time.Minute))
	succeededID, succeededSession = insertExecution("SUCCEEDED", now.Add(-30*time.Minute))
	recentID, recentSession = insertExecution("RUNNING", now.Add(-time.Minute))
	for _, item := range []struct {
		sessionID string
		status    string
		execution *int64
	}{
		{staleSession, "RUNNING", &staleID},
		{succeededSession, "READY", nil},
		{recentSession, "RUNNING", &recentID},
	} {
		var execution any
		if item.execution != nil {
			execution = *item.execution
		}
		if _, err := db.Exec(`INSERT INTO session_execution_guards(tenant_id,agent_app_id,session_id,generation,status,current_execution_id) VALUES($1,$2,$3,1,$4,$5)`, tenantID, appID, item.sessionID, item.status, execution); err != nil {
			t.Fatal(err)
		}
	}

	updated, err := controlplane.NewExecutionRecorder(db).ReconcileExpired(context.Background(), now.Add(-15*time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 {
		t.Fatalf("reconciled %d records, want 1", updated)
	}
	assertExecution := func(id int64, wantStatus, wantError string) {
		t.Helper()
		var status, errorMessage string
		if err := db.QueryRow(`SELECT status,error_message FROM execution_records WHERE id=$1`, id).Scan(&status, &errorMessage); err != nil {
			t.Fatal(err)
		}
		if status != wantStatus || errorMessage != wantError {
			t.Fatalf("execution %d status/error = %q/%q, want %q/%q", id, status, errorMessage, wantStatus, wantError)
		}
	}
	assertExecution(staleID, "ABANDONED", "expired_execution_lease")
	assertExecution(succeededID, "SUCCEEDED", "")
	assertExecution(recentID, "RUNNING", "")
}
