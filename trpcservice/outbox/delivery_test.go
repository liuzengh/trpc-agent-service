package outbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionstore"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

type deliveryHarness struct {
	delivery *Service
	sender   *fakeSender
	scope    controlplane.Scope
	tenantID string
}

type fakeSender struct {
	outcome Outcome
	err     error
	calls   int
	texts   []string
}

func (f *fakeSender) Send(_ context.Context, reply *ClaimedReply) (Outcome, error) {
	f.calls++
	f.texts = append(f.texts, reply.Text)
	return f.outcome, f.err
}

// setupDeliveryTest runs the real pipeline end to end: accept a message,
// claim and commit an execution with a reply, and hand the resulting
// reply_outbox row to the delivery service under test. Building the row by
// hand would only prove the outbox can read its own table, not that the
// commit path produces what delivery consumes.
func setupDeliveryTest(t *testing.T, outcome Outcome, sendErr error) *deliveryHarness {
	t.Helper()
	dsn := os.Getenv("OUTBOX_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_MYSQL_TEST_DSN not set; skipping real-mysql outbox test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetDeliverySchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "acme"
	if err := cdp.CreateTenant(ctx, tenantID, "Acme"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	modelID, err := scope.CreateModelProfile(ctx, "m", "gpt-4o-mini", "", "env:MODEL_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "b", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "hi", ModelProfileID: modelID, BackendProfileID: backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}

	inboxSvc := inbox.NewService(cdp)
	if _, err := inboxSvc.Accept(ctx, tenantID, inbox.Request{
		AppID: appID, ChannelType: "webchat", BindingID: bindingID,
		ActorKey: "user-1", RevisionID: rev.ID,
		ModelProfileID: modelID, BackendProfileID: backendID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: "msg-1", Text: "hello",
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	execSvc := execution.NewService(cdp, execution.DefaultLeaseTTL)
	claim, err := execSvc.ClaimNext(ctx, tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	prepared := &sessionstore.Prepared{
		Events: []event.Event{{
			Response: &model.Response{
				Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "hi there"}}},
			},
			ID: "evt-1", Author: "assistant",
		}},
		State: map[string][]byte{"turns": []byte("1")},
	}
	if err := execSvc.Commit(ctx, claim, &execution.Result{
		Prepared: prepared,
		Parts:    []execution.ReplyPart{{Text: "hi there", IsDone: true}},
		Decision: "ok",
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	sender := &fakeSender{outcome: outcome, err: sendErr}
	return &deliveryHarness{
		delivery: NewService(cdp, sender),
		sender:   sender,
		scope:    scope,
		tenantID: tenantID,
	}
}

func resetDeliverySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"delivery_attempts", "reply_outbox", "session_events", "execution_attempts",
		"executions", "inbox_messages", "channel_reply_routes", "channel_checkpoints",
		"channel_notifications", "outbox_events", "audit_events", "sessions",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

func (h *deliveryHarness) status(t *testing.T) (string, uint32, time.Time) {
	t.Helper()
	var (
		status   string
		attempts uint32
		nextAt   time.Time
	)
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT status, attempts, next_attempt_at FROM reply_outbox WHERE tenant_id = ? ORDER BY outbox_id LIMIT 1",
		h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&status, &attempts, &nextAt); err != nil {
		t.Fatal(err)
	}
	return status, attempts, nextAt
}

func (h *deliveryHarness) sessionBlocked(t *testing.T) bool {
	t.Helper()
	var blocked sql.NullString
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT blocked_reason FROM sessions WHERE tenant_id = ? ORDER BY session_pk LIMIT 1", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	return blocked.Valid
}

func (h *deliveryHarness) claimAndDeliver(t *testing.T) (Outcome, error) {
	t.Helper()
	claim, err := h.delivery.ClaimNext(context.Background(), h.tenantID, "delivery-1")
	if err != nil {
		return Unknown, err
	}
	return h.delivery.Deliver(context.Background(), claim)
}

func TestDeliverSentEndsTheRow(t *testing.T) {
	h := setupDeliveryTest(t, Sent, nil)

	outcome, err := h.claimAndDeliver(t)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if outcome != Sent {
		t.Fatalf("outcome = %v, want Sent", outcome)
	}
	if h.sender.calls != 1 {
		t.Fatalf("sender called %d times, want 1", h.sender.calls)
	}

	status, attempts, _ := h.status(t)
	if status != "sent" {
		t.Fatalf("status = %q, want sent", status)
	}
	if attempts != 0 {
		t.Fatalf("a clean send should not count as a retry attempt, attempts = %d", attempts)
	}
	if h.sessionBlocked(t) {
		t.Fatal("a successful send must not block the session")
	}
}

func TestDeliverRejectedBacksOffForAnotherTry(t *testing.T) {
	h := setupDeliveryTest(t, Rejected, errors.New("rate limited"))

	// Rejected is "the channel said no, safely": the reason comes back to
	// the caller as an error (nothing about it is a success), while the row
	// itself is scheduled for another go rather than parked.
	_, err := h.claimAndDeliver(t)
	if err == nil || err.Error() != "rate limited" {
		t.Fatalf("rejected delivery must surface the channel's reason, got: %v", err)
	}

	status, attempts, nextAt := h.status(t)
	if status != "pending" {
		t.Fatalf("status = %q, want pending (a retryable rejection)", status)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	// The first backoff step is one second plus at most 500ms of jitter.
	delay := time.Until(nextAt)
	if delay < 500*time.Millisecond || delay > 2*time.Second {
		t.Fatalf("next attempt is %v away, want roughly the first backoff step", delay)
	}
}

func TestDeliverExhaustsIntoTheDeadLetterState(t *testing.T) {
	h := setupDeliveryTest(t, Rejected, errors.New("always failing"))

	// len(DefaultBackoff) rejected attempts exhaust the schedule; the last
	// one must settle on 'dead' rather than scheduling a sixth try.
	for i := 0; i < len(DefaultBackoff)+1; i++ {
		// Force the retry window open every time so ClaimNext picks the row
		// again immediately instead of waiting out a real backoff.
		if _, err := h.scope.Exec(context.Background(),
			"UPDATE reply_outbox SET next_attempt_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE tenant_id = ?",
			h.tenantID); err != nil {
			t.Fatal(err)
		}
		// Deliver returns the sender's own error alongside the recorded
		// outcome; it is expected on every rejected attempt, so it is not
		// asserted here — the state transitions below are what matter.
		if _, err := h.claimAndDeliver(t); err != nil && i == len(DefaultBackoff) {
			// The final pass should also not claim anything new to deliver,
			// but a claim racing the dead transition is still a real attempt.
			_ = err
		}
	}

	status, attempts, _ := h.status(t)
	if status != "dead" {
		t.Fatalf("status = %q after %d attempts, want dead", status, attempts)
	}
	if int(attempts) != len(DefaultBackoff) {
		t.Fatalf("attempts = %d, want it to stop at %d", attempts, len(DefaultBackoff))
	}
}

func TestDeliverUnknownParksWithoutAnyRetry(t *testing.T) {
	h := setupDeliveryTest(t, Unknown, errors.New("connection reset mid-write"))

	outcome, err := h.claimAndDeliver(t)
	if outcome != Unknown {
		t.Fatalf("outcome = %v, want Unknown", outcome)
	}
	if err == nil {
		t.Fatal("unknown delivery must surface the sender's error to the caller")
	}

	status, _, _ := h.status(t)
	if status != "unknown" {
		t.Fatalf("status = %q, want unknown", status)
	}
	// An unknown delivery is not eligible for ClaimNext again: no automatic
	// resend, only a human decision can move it.
	if _, err := h.delivery.ClaimNext(context.Background(), h.tenantID, "delivery-2"); !errors.Is(err, ErrNothingToSend) {
		t.Fatalf("claim after an unknown delivery: %v, want ErrNothingToSend", err)
	}
	// And it blocks the conversation, so nothing new runs past an
	// undeliverable-but-maybe-delivered reply.
	if !h.sessionBlocked(t) {
		t.Fatal("an unknown delivery must park the session it belongs to")
	}
}
