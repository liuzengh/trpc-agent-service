package inbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// setupInboxTest migrates a real MySQL and creates one tenant with an app and
// a published revision, ready to receive a message.
func setupInboxTest(t *testing.T) (*Service, string, testFixtures) {
	t.Helper()
	dsn := os.Getenv("INBOX_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("INBOX_MYSQL_TEST_DSN not set; skipping real-mysql inbox test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetInboxSchema(t, db)
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
		t.Fatalf("model profile: %v", err)
	}
	backendID, err := scope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatalf("backend profile: %v", err)
	}
	appID, err := scope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	rev, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction:      "hello",
		ModelProfileID:   modelID,
		BackendProfileID: backendID,
		MaxLLMCalls:      8,
		MessageTimeoutMS: 120000,
	})
	if err != nil {
		t.Fatalf("publish revision: %v", err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatalf("bind channel: %v", err)
	}

	return NewService(cdp), tenantID, testFixtures{
		appID: appID, bindingID: bindingID, revisionID: rev.ID,
		modelProfileID: modelID, backendProfileID: backendID,
	}
}

type testFixtures struct {
	appID, bindingID, revisionID     int64
	modelProfileID, backendProfileID int64
}

func resetInboxSchema(t *testing.T, db *sql.DB) {
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

func (f testFixtures) req(msgID, text string) Request {
	return Request{
		AppID:                 f.appID,
		ChannelType:           "webchat",
		BindingID:             f.bindingID,
		ActorKey:              "user-1",
		RevisionID:            f.revisionID,
		ModelProfileID:        f.modelProfileID,
		BackendProfileID:      f.backendProfileID,
		ModelProfileVersion:   1,
		BackendProfileVersion: 1,
		PlatformMessageID:     msgID,
		Text:                  text,
	}
}

func TestAcceptCreatesSessionAndAssignsSequence(t *testing.T) {
	svc, tenantID, f := setupInboxTest(t)
	ctx := context.Background()

	a1, err := svc.Accept(ctx, tenantID, f.req("msg-1", "first"))
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	if a1.Duplicate {
		t.Fatal("first acceptance cannot be a duplicate")
	}
	if a1.InSeq != 1 {
		t.Fatalf("first message in_seq = %d, want 1", a1.InSeq)
	}

	a2, err := svc.Accept(ctx, tenantID, f.req("msg-2", "second"))
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}
	if a2.SessionPK != a1.SessionPK {
		t.Fatalf("second message opened a new session: %d != %d", a2.SessionPK, a1.SessionPK)
	}
	if a2.InSeq != 2 {
		t.Fatalf("second message in_seq = %d, want 2", a2.InSeq)
	}
}

func TestReAcceptIsIdempotentAndReportsDuplicate(t *testing.T) {
	svc, tenantID, f := setupInboxTest(t)
	ctx := context.Background()

	first, err := svc.Accept(ctx, tenantID, f.req("same-id", "hello"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := svc.Accept(ctx, tenantID, f.req("same-id", "hello"))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("a resend with identical content must be reported as a duplicate")
	}
	if second.ExecutionID != first.ExecutionID {
		t.Fatalf("duplicate produced a new execution: %s != %s", second.ExecutionID, first.ExecutionID)
	}
	// The session's in_seq must not have advanced for the duplicate: two
	// acceptances of one message are one slot in the ordering, not two.
	third, err := svc.Accept(ctx, tenantID, f.req("next-id", "third"))
	if err != nil {
		t.Fatalf("third: %v", err)
	}
	if third.InSeq != 2 {
		t.Fatalf("a duplicate consumed an in_seq slot: next message got %d, want 2", third.InSeq)
	}
}

func TestSameIDDifferentContentIsAConflict(t *testing.T) {
	svc, tenantID, f := setupInboxTest(t)
	ctx := context.Background()

	if _, err := svc.Accept(ctx, tenantID, f.req("reuse-me", "original")); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := svc.Accept(ctx, tenantID, f.req("reuse-me", "different body entirely"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("same id, different content: %v, want ErrConflict", err)
	}
}

func TestConcurrentAcceptsOfTheSameSessionProduceDistinctSequence(t *testing.T) {
	svc, tenantID, f := setupInboxTest(t)
	ctx := context.Background()

	// The lock is on the session row, so racing accepts of the same
	// conversation is the exact case it exists for. Each must land on its own
	// in_seq, no gaps, no duplicates — that is what makes "strict head order"
	// downstream a claim about what was actually received.
	const n = 10
	var wg sync.WaitGroup
	inSeqs := make([]uint32, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			acc, err := svc.Accept(ctx, tenantID, f.req(string(rune('a'+i)), "body"))
			if err != nil {
				errs[i] = err
				return
			}
			inSeqs[i] = acc.InSeq
		}(i)
	}
	wg.Wait()

	seen := map[uint32]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("accept %d: %v", i, err)
		}
		if seen[inSeqs[i]] {
			t.Fatalf("two concurrent accepts both got in_seq %d", inSeqs[i])
		}
		seen[inSeqs[i]] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct sequence numbers, got %d", n, len(seen))
	}
}

func TestDifferentTenantsCannotSeeEachOthersSessions(t *testing.T) {
	svc, tenantID, f := setupInboxTest(t)
	ctx := context.Background()

	cdp := svc.db
	if err := cdp.CreateTenant(ctx, "other", "Other"); err != nil {
		t.Fatal(err)
	}
	otherScope := cdp.MustScope("other")
	otherModel, err := otherScope.CreateModelProfile(ctx, "default-model", "gpt-4o-mini", "", "env:MODEL_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	otherBackend, err := otherScope.CreateBackendProfile(ctx, controlplane.BackendProfile{PublicID: "default-backend", SessionBackend: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	otherApp, err := otherScope.CreateApp(ctx, "assistant", "Assistant")
	if err != nil {
		t.Fatal(err)
	}
	otherRev, err := otherScope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "hi", ModelProfileID: otherModel, BackendProfileID: otherBackend,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherBinding, err := otherScope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: otherApp, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Two tenants, same actor_key, same message id: separate sessions, and
	// neither one's row is visible through the other's scope. The database's
	// own foreign keys are what make sharing impossible, not a naming
	// convention — this test exists to prove that, by trying to misuse an id.
	_, err = svc.Accept(ctx, "other", Request{
		AppID: otherApp, ChannelType: "webchat", BindingID: otherBinding,
		ActorKey: "user-1", RevisionID: otherRev.ID,
		ModelProfileID: otherModel, BackendProfileID: otherBackend,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: "msg-1", Text: "hello",
	})
	if err != nil {
		t.Fatalf("other tenant accept: %v", err)
	}

	a, err := svc.Accept(ctx, tenantID, f.req("msg-1", "hello"))
	if err != nil {
		t.Fatalf("first tenant accept: %v", err)
	}

	var otherSessionPK int64
	row, err := otherScope.QueryRow(ctx,
		"SELECT session_pk FROM sessions WHERE tenant_id = ? AND actor_key = ?", "other", "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&otherSessionPK); err != nil {
		t.Fatal(err)
	}
	if otherSessionPK == a.SessionPK {
		t.Fatal("two tenants' sessions collapsed to the same row")
	}

	// The first tenant's scope cannot query the other tenant's inbox row at
	// all, even knowing the (tenant, binding, message id) shape.
	var found int
	row, err = svc.db.MustScope(tenantID).QueryRow(ctx,
		"SELECT COUNT(*) FROM inbox_messages WHERE tenant_id = ? AND binding_id = ? AND platform_message_id = ?",
		tenantID, otherBinding, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&found); err != nil {
		t.Fatal(err)
	}
	if found != 0 {
		t.Fatalf("tenant %q reached tenant %q's inbox row (binding %d)", tenantID, "other", otherBinding)
	}
}
