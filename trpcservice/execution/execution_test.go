package execution

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
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionstore"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

type harness struct {
	svc        *Service
	inbox      *inbox.Service
	cdp        *controlplane.DB
	scope      controlplane.Scope
	tenantID   string
	appID      int64
	bindingID  int64
	revisionID int64
	modelID    int64
	backendID  int64
}

// setupExecutionTest needs the inbox package too: a session only becomes
// claimable by having a message accepted into it, and re-implementing that
// here would mean the claim/commit tests are not actually testing the shape
// the real pipeline produces.
func setupExecutionTest(t *testing.T) *harness {
	t.Helper()
	dsn := os.Getenv("EXECUTION_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("EXECUTION_MYSQL_TEST_DSN not set; skipping real-mysql execution test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetExecutionSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "acme"
	if err := cdp.CreateTenant(ctx, tenantID, "Acme"); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	scope := cdp.MustScope(tenantID)

	modelID, err := scope.CreateModelProfile(ctx, "default-model", "gpt-4o-mini", "", "env:MODEL_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "hello", ModelProfileID: modelID, BackendProfileID: backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 120000,
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

	return &harness{
		svc:        NewService(cdp, DefaultLeaseTTL),
		inbox:      inbox.NewService(cdp),
		cdp:        cdp,
		scope:      scope,
		tenantID:   tenantID,
		appID:      appID,
		bindingID:  bindingID,
		revisionID: rev.ID,
		modelID:    modelID,
		backendID:  backendID,
	}
}

func resetExecutionSchema(t *testing.T, db *sql.DB) {
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

func (h *harness) accept(t *testing.T, msgID, text string) *inbox.Accepted {
	t.Helper()
	acc, err := h.inbox.Accept(context.Background(), h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: "webchat", BindingID: h.bindingID,
		ActorKey: "user-1", RevisionID: h.revisionID,
		ModelProfileID: h.modelID, BackendProfileID: h.backendID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: msgID, Text: text,
		Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	})
	if err != nil {
		t.Fatalf("accept %s: %v", msgID, err)
	}
	return acc
}

func samplePrepared(text string) *sessionstore.Prepared {
	return &sessionstore.Prepared{
		Events: []event.Event{{
			Response: &model.Response{
				Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: text}}},
			},
			ID:     "evt-" + text,
			Author: "assistant",
		}},
		State: map[string][]byte{"turns": []byte("1")},
	}
}

func TestClaimThenCommitAdvancesHeadOnce(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")

	claim, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.InSeq != 1 || claim.FenceToken != 1 {
		t.Fatalf("unexpected claim: in_seq=%d fence=%d", claim.InSeq, claim.FenceToken)
	}
	if claim.Traceparent == "" {
		t.Fatal("a claim must carry the trace captured at acceptance, not start a new one")
	}

	err = h.svc.Commit(ctx, claim, &Result{
		Prepared:         samplePrepared("hi"),
		Parts:            []ReplyPart{{Text: "hi", IsDone: true}},
		Decision:         "ok",
		Latency:          200 * time.Millisecond,
		PromptTokens:     5,
		CompletionTokens: 2,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if _, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1"); !errors.Is(err, ErrNothingToClaim) {
		t.Fatalf("claim after commit: %v, want ErrNothingToClaim", err)
	}

	// The committed event is now history a later claim would see.
	h.accept(t, "msg-2", "again")
	claim2, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claim2.BaseSessionVersion != 1 {
		t.Fatalf("session version after one commit = %d, want 1", claim2.BaseSessionVersion)
	}
	if len(claim2.Events) != 1 || claim2.Events[0].ID != "evt-hi" {
		t.Fatalf("committed history not visible to a later claim: %+v", claim2.Events)
	}
	if claim2.State["turns"] == nil {
		t.Fatal("committed state not visible to a later claim")
	}
	// The fence only ever moves forward.
	if claim2.FenceToken <= claim.FenceToken {
		t.Fatalf("fence went backwards: %d then %d", claim.FenceToken, claim2.FenceToken)
	}
}

func TestCommitRejectsAStaleFence(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")

	claim, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Simulate another worker taking over: bump the fence directly, as a
	// successful ClaimNext by some other worker_id would have done.
	if _, err := h.scope.Exec(ctx,
		"UPDATE sessions SET fencing_token = fencing_token + 1, lease_owner = 'worker-2' WHERE tenant_id = ? AND session_pk = ?",
		h.tenantID, claim.SessionPK); err != nil {
		t.Fatal(err)
	}

	err = h.svc.Commit(ctx, claim, &Result{
		Prepared: samplePrepared("too late"),
		Decision: "ok",
	})
	if !errors.Is(err, ErrCommitConditionFailed) {
		t.Fatalf("commit with a stolen fence: %v, want ErrCommitConditionFailed", err)
	}

	// The rejected attempt must not have left its reply anywhere: a user
	// should never get an answer from a worker that lost the lease.
	var found int
	row, err := h.scope.QueryRow(ctx,
		"SELECT COUNT(*) FROM reply_outbox WHERE tenant_id = ? AND execution_id = ?",
		h.tenantID, claim.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&found); err != nil {
		t.Fatal(err)
	}
	if found != 0 {
		t.Fatalf("a lost commit queued %d replies it should not have", found)
	}
}

func TestClaimSkipsALeaseHeldByAnotherWorker(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")

	first, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// A second worker must not be able to take the same session while the
	// first lease is alive. SKIP LOCKED means it gets "nothing to claim"
	// rather than blocking, which is what lets a cluster idle-wait cheaply.
	if _, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-2"); !errors.Is(err, ErrNothingToClaim) {
		t.Fatalf("concurrent claim while another holds the lease: %v, want ErrNothingToClaim", err)
	}
	_ = first
}

func TestExpiredLeaseIsReclaimableWithAHigherFence(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")

	first, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Force the lease into the past the way real expiry looks after a worker
	// stops renewing (a stopped process, not a clean shutdown).
	if _, err := h.scope.Exec(ctx,
		"UPDATE sessions SET lease_until = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE tenant_id = ? AND session_pk = ?",
		h.tenantID, first.SessionPK); err != nil {
		t.Fatal(err)
	}

	second, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-2")
	if err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if second.FenceToken <= first.FenceToken {
		t.Fatalf("a reclaimed session got fence %d, previous holder had %d", second.FenceToken, first.FenceToken)
	}

	// The worker that died must not be able to commit anything it had ready.
	if err := h.svc.Commit(ctx, first, &Result{
		Prepared: samplePrepared("zombie"),
		Decision: "ok",
	}); !errors.Is(err, ErrCommitConditionFailed) {
		t.Fatalf("stale worker commit after being taken over: %v, want ErrCommitConditionFailed", err)
	}
}

func TestRenewRefusesAfterFenceMoves(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")

	claim, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := h.svc.Renew(ctx, claim); err != nil {
		t.Fatalf("renew while still owner: %v", err)
	}

	if _, err := h.scope.Exec(ctx,
		"UPDATE sessions SET fencing_token = fencing_token + 1 WHERE tenant_id = ? AND session_pk = ?",
		h.tenantID, claim.SessionPK); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.Renew(ctx, claim); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("renew after fence moved: %v, want ErrStaleFence", err)
	}
}

func TestHeadMessageWithUnknownStatusBlocksTheQueue(t *testing.T) {
	h := setupExecutionTest(t)
	ctx := context.Background()
	h.accept(t, "msg-1", "hello")
	h.accept(t, "msg-2", "second")

	claim, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.InSeq != 1 {
		t.Fatalf("claimed in_seq %d, want the head (1)", claim.InSeq)
	}

	// Park the session the way a real side-effect-unknown outcome would.
	if _, err := h.scope.Exec(ctx,
		"UPDATE sessions SET blocked_reason = 'unknown side effect' WHERE tenant_id = ? AND session_pk = ?",
		h.tenantID, claim.SessionPK); err != nil {
		t.Fatal(err)
	}

	// Neither message may be claimed while the head is blocked: running the
	// second message past a stuck first one would reorder the conversation.
	if _, err := h.svc.ClaimNext(ctx, h.tenantID, "worker-2"); !errors.Is(err, ErrNothingToClaim) {
		t.Fatalf("claim while session is blocked: %v, want ErrNothingToClaim", err)
	}
}
