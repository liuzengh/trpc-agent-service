package channels

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// The durable puller is one of the few places where "it compiles" says
// nothing: the guarantees worth testing are about state that outlives the
// process (cursor, notification, inbox rows) and about two pullers not
// stepping on each other's cursor. Those are MySQL behaviours, so these tests
// run against a real one, gated the same way every other integration suite in
// this repo is.
//
//	PULLER_MYSQL_TEST_DSN='tas:taspw@tcp(127.0.0.1:3307)/tas_puller_test' \
//	    go test ./trpcservice/channels/ -run TestKfPuller -v

type kfPullerHarness struct {
	puller     *KfPuller
	db         *controlplane.DB
	inbox      *inbox.Service
	scope      controlplane.Scope
	tenantID   string
	appID      int64
	bindingID  int64
	revisionID int64
	modelID    int64
	backendID  int64
}

func setupKfPullerTest(t *testing.T) (*kfPullerHarness, *kfMock) {
	t.Helper()
	dsn := os.Getenv("PULLER_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("PULLER_MYSQL_TEST_DSN not set; skipping real-mysql KF puller test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetPullerSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	mock := newKfMock(t)
	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "t1"
	if err := cdp.CreateTenant(ctx, tenantID, "T1"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	modelID, err := scope.CreateModelProfile(ctx, "m", "fake-model", mock.srv.URL, "env:PULLER_TEST_KEY")
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
	if _, err := scope.PublishRevision(ctx, "assistant", controlplane.RevisionSpec{
		Instruction: "be terse", ModelProfileID: modelID, BackendProfileID: backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 30000,
	}); err != nil {
		t.Fatal(err)
	}
	b, err := scope.CurrentRevision(ctx, "assistant")
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	bindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "wechat_kf", PublicID: "main", CredentialRef: "env:KF",
	})
	if err != nil {
		t.Fatal(err)
	}

	inboxSvc := inbox.NewService(cdp)
	binding := testKfBinding()
	puller := NewKfPuller(func(id string) (*tenant.WeChatKfBinding, bool) {
		if id == tenantID {
			return binding, true
		}
		return nil, false
	}, cdp, inboxSvc).WithAPIBase(mock.srv.URL).WithLeaseTTL(2 * time.Second)

	return &kfPullerHarness{
		puller: puller, db: cdp, inbox: inboxSvc, scope: scope, tenantID: tenantID,
		appID: appID, bindingID: bindingID, revisionID: b.ID, modelID: modelID, backendID: backendID,
	}, mock
}

func resetPullerSchema(t *testing.T, db *sql.DB) {
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

func (h *kfPullerHarness) notificationStatus(t *testing.T) string {
	t.Helper()
	var status string
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT status FROM channel_notifications WHERE tenant_id = ? ORDER BY notification_id DESC LIMIT 1", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func (h *kfPullerHarness) cursor(t *testing.T) string {
	t.Helper()
	var value string
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT value FROM channel_checkpoints WHERE tenant_id = ? AND scope_key = 'wk1'", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (h *kfPullerHarness) inboxIDs(t *testing.T) []string {
	t.Helper()
	rows, err := h.scope.Query(context.Background(),
		"SELECT platform_message_id FROM inbox_messages WHERE tenant_id = ? ORDER BY platform_message_id", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func (h *kfPullerHarness) sessionInSeq(t *testing.T, actor string) uint32 {
	t.Helper()
	var inSeq uint32
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT in_seq FROM sessions WHERE tenant_id = ? AND actor_key = ?", h.tenantID, actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&inSeq); err != nil {
		t.Fatal(err)
	}
	return inSeq
}

func (h *kfPullerHarness) sessionPK(t *testing.T, actor string) int64 {
	t.Helper()
	var pk int64
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT session_pk FROM sessions WHERE tenant_id = ? AND actor_key = ?", h.tenantID, actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&pk); err != nil {
		t.Fatal(err)
	}
	return pk
}

func (h *kfPullerHarness) count(t *testing.T, query string) int {
	t.Helper()
	var n int
	row, err := h.scope.QueryRow(context.Background(), query, h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (h *kfPullerHarness) recordNotification(t *testing.T, token, scopeKey string) {
	t.Helper()
	if _, err := h.puller.RecordNotification(context.Background(), h.tenantID, h.bindingID, token, scopeKey); err != nil {
		t.Fatalf("record notification: %v", err)
	}
}

func TestKfPullerDrainsNotificationIntoInbox(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	now := time.Now().Unix()
	mock.syncPages = []kfSyncResponse{{
		NextCursor: "C1", HasMore: 0,
		MsgList: []kfSyncMsg{
			kfTextMsg("m1", "ext1", "hello", 3, now),
			kfTextMsg("m2", "ext1", "接待员的话", 5, now),   // origin 5：非客户消息
			{Msgid: "m3", MsgType: "event", Origin: 4}, // 事件：跳过
			kfTextMsg("m4", "ext2", "world", 3, now),
		},
	}}
	h.recordNotification(t, "EVTOK", "wk1")

	worked, err := h.puller.PullOnce(context.Background(), h.tenantID, "puller-1")
	if err != nil {
		t.Fatalf("PullOnce: %v", err)
	}
	if !worked {
		t.Fatal("PullOnce reported no work after a notification was recorded")
	}

	if got := h.inboxIDs(t); len(got) != 2 || got[0] != "m1" || got[1] != "m4" {
		t.Fatalf("inbox = %v, want [m1 m4]", got)
	}
	if n := h.count(t, "SELECT COUNT(*) FROM executions WHERE tenant_id = ?"); n != 2 {
		t.Fatalf("executions = %d, want 2", n)
	}
	if got := h.sessionInSeq(t, "ext1"); got != 1 {
		t.Fatalf("ext1 in_seq = %d, want 1", got)
	}
	if got := h.cursor(t); got != "C1" {
		t.Fatalf("cursor = %q, want C1", got)
	}
	if got := h.notificationStatus(t); got != "done" {
		t.Fatalf("notification = %q, want done", got)
	}
	if n := h.count(t, "SELECT COUNT(*) FROM channel_reply_routes WHERE tenant_id = ?"); n != 2 {
		t.Fatalf("reply routes = %d, want 2", n)
	}

	// The event token and the empty first cursor must both have been sent.
	reqs, _ := mock.requests()
	if len(reqs) != 1 {
		t.Fatalf("sync_msg calls = %d, want 1", len(reqs))
	}
	if reqs[0].Cursor != "" || reqs[0].Token != "EVTOK" || reqs[0].OpenKfID != "wk1" || reqs[0].Limit != kfSyncLimit {
		t.Fatalf("sync request = %+v", reqs[0])
	}

	// Nothing left to do.
	worked, err = h.puller.PullOnce(context.Background(), h.tenantID, "puller-1")
	if err != nil || worked {
		t.Fatalf("second PullOnce = (%v, %v), want (false, nil)", worked, err)
	}
}

func TestKfPullerRepullDedupsAndKeepsSequence(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	now := time.Now().Unix()
	mock.syncPages = []kfSyncResponse{{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "hello", 3, now)}}}
	h.recordNotification(t, "T1", "wk1")
	if _, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1"); err != nil {
		t.Fatalf("first pull: %v", err)
	}

	// Re-pull the same page plus one new message: the duplicate must not
	// create a second execution nor consume a sequence number, and the new
	// message takes the next slot.
	mock.mu.Lock()
	mock.syncPages = []kfSyncResponse{{
		NextCursor: "C2", HasMore: 0,
		MsgList: []kfSyncMsg{
			kfTextMsg("m1", "ext1", "hello", 3, now),
			kfTextMsg("m2", "ext1", "again", 3, now),
		},
	}}
	mock.mu.Unlock()
	h.recordNotification(t, "T2", "wk1")
	if _, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1"); err != nil {
		t.Fatalf("second pull: %v", err)
	}

	if n := h.count(t, "SELECT COUNT(*) FROM executions WHERE tenant_id = ?"); n != 2 {
		t.Fatalf("executions = %d, want 2 (m1 once, m2 once)", n)
	}
	if got := h.sessionInSeq(t, "ext1"); got != 2 {
		t.Fatalf("ext1 in_seq = %d, want 2 (the duplicate consumed a slot)", got)
	}
	if got := h.cursor(t); got != "C2" {
		t.Fatalf("cursor = %q, want C2", got)
	}
}

func TestKfPullerScopeLeaseBlocksASecondPuller(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	mock.syncPages = []kfSyncResponse{{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "hello", 3, time.Now().Unix())}}}
	h.recordNotification(t, "T1", "wk1")

	// Another puller already owns this binding+open_kfid.
	other, acquired, err := h.puller.acquireScope(context.Background(), h.tenantID, h.bindingID, "wk1", "other-puller")
	if err != nil || !acquired {
		t.Fatalf("acquire scope = (%v, %v), want acquired", acquired, err)
	}

	worked, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1")
	if err != nil {
		t.Fatalf("PullOnce while scope busy: %v", err)
	}
	if worked {
		t.Fatal("PullOnce reported work while another puller held the scope")
	}
	if reqs, _ := mock.requests(); len(reqs) != 0 {
		t.Fatalf("a blocked puller called sync_msg %d times, want 0", len(reqs))
	}
	if got := h.notificationStatus(t); got != "pending" {
		t.Fatalf("notification = %q, want pending (handed back, not consumed)", got)
	}

	// Once the other puller lets go, the notification is drained normally.
	other.release(context.Background())
	worked, err = h.puller.PullOnce(context.Background(), h.tenantID, "p1")
	if err != nil || !worked {
		t.Fatalf("PullOnce after release = (%v, %v), want (true, nil)", worked, err)
	}
	if got := h.cursor(t); got != "C1" {
		t.Fatalf("cursor = %q, want C1", got)
	}
}

func TestKfPullerContentConflictParksTheNotification(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	now := time.Now().Unix()

	// The same platform message id already exists with different content:
	// not a resend, and no retry will fix it.
	if _, err := h.inbox.Accept(context.Background(), h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: string(TypeWeChatKF), BindingID: h.bindingID,
		ActorKey: "ext1", RevisionID: h.revisionID,
		ModelProfileID: h.modelID, BackendProfileID: h.backendID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: "m1", Text: "original",
	}); err != nil {
		t.Fatalf("seed conflicting message: %v", err)
	}

	mock.syncPages = []kfSyncResponse{{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "different", 3, now)}}}
	h.recordNotification(t, "T1", "wk1")

	worked, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1")
	if err != nil {
		t.Fatalf("PullOnce: %v", err)
	}
	if !worked {
		t.Fatal("PullOnce reported no work for a notification that was claimed")
	}
	if got := h.notificationStatus(t); got != "failed" {
		t.Fatalf("notification = %q, want failed (parked for a human, not retried)", got)
	}
	// The cursor must not have moved: the page it covered was never
	// committed.
	var value sql.NullString
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT value FROM channel_checkpoints WHERE tenant_id = ? AND scope_key = 'wk1'", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value.String != "" {
		t.Fatalf("cursor advanced past a rejected page: %q", value.String)
	}
	if n := h.count(t, "SELECT COUNT(*) FROM executions WHERE tenant_id = ?"); n != 1 {
		t.Fatalf("executions = %d, want 1 (only the seeded one)", n)
	}
}

func TestKfPullerSendSplitsAndUsesReplyRoute(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	mock.syncPages = []kfSyncResponse{{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "hi", 3, time.Now().Unix())}}}
	h.recordNotification(t, "T1", "wk1")
	if _, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1"); err != nil {
		t.Fatalf("pull: %v", err)
	}

	long := strings.Repeat("中", 1000) // 3000 bytes -> forces a split at 2048
	outcome, err := h.puller.Send(context.Background(), &outbox.ClaimedReply{
		TenantID: h.tenantID, SessionPK: h.sessionPK(t, "ext1"), Target: "ext1", Text: long,
	})
	if err != nil || outcome != outbox.Sent {
		t.Fatalf("Send = (%v, %v), want Sent", outcome, err)
	}

	_, sent := mock.requests()
	if len(sent) != 2 {
		t.Fatalf("sent %d fragments, want 2", len(sent))
	}
	if joined := sent[0].Content + sent[1].Content; joined != long {
		t.Fatal("fragments do not reassemble to the reply text")
	}
	if sent[0].ToUser != "ext1" || sent[0].OpenKf != "wk1" {
		t.Fatalf("delivery target = %+v, want ext1/wk1 (from the reply route)", sent[0])
	}

	// A session with no recorded route has nowhere to send to: that is
	// permanent, so it must be Rejected (dead-letter), not Unknown (retry).
	outcome, err = h.puller.Send(context.Background(), &outbox.ClaimedReply{
		TenantID: h.tenantID, SessionPK: 999999, Target: "ghost", Text: "hi",
	})
	if outcome != outbox.Rejected || err == nil {
		t.Fatalf("Send without a route = (%v, %v), want Rejected", outcome, err)
	}
}

func TestWeChatKfDurableCallbackRecordsNotificationOnly(t *testing.T) {
	h, mock := setupKfPullerTest(t)
	a := NewWeChatKf(func(tenantID string) (*tenant.WeChatKfBinding, bool) {
		if tenantID == h.tenantID {
			return testKfBinding(), true
		}
		return nil, false
	}).WithDurableNotifications(func(ctx context.Context, tenantID, eventToken, scopeKey string) error {
		_, err := h.puller.RecordNotification(ctx, tenantID, h.bindingID, eventToken, scopeKey)
		return err
	})
	a.apiBase = mock.srv.URL

	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "EVTOK", OpenKfId: "wk1"}
	rw, batch, err := postKfEvent(t, a, ev)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack body = %q, want success", rw.Body.String())
	}
	if batch != nil {
		t.Fatalf("durable mode must not pull inline, got %+v", batch)
	}
	if reqs, _ := mock.requests(); len(reqs) != 0 {
		t.Fatalf("durable callback called sync_msg %d times, want 0", len(reqs))
	}

	// The notification is the durable fact, and the puller can drain it.
	if got := h.notificationStatus(t); got != "pending" {
		t.Fatalf("notification = %q, want pending", got)
	}
	mock.syncPages = []kfSyncResponse{{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m9", "ext1", "from event", 3, time.Now().Unix())}}}
	if worked, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1"); err != nil || !worked {
		t.Fatalf("PullOnce after callback = (%v, %v), want (true, nil)", worked, err)
	}
	if got := h.inboxIDs(t); len(got) != 1 || got[0] != "m9" {
		t.Fatalf("inbox = %v, want [m9]", got)
	}
}

// TestKfPullerResolveTargetNeedsAPublishedRevision pins the terminal path: a
// binding whose app has nothing published cannot answer, and retrying it
// forever would be a silent backlog rather than a visible failure.
func TestKfPullerResolveTargetNeedsAPublishedRevision(t *testing.T) {
	h, _ := setupKfPullerTest(t)
	appID, err := h.scope.CreateApp(context.Background(), "unpublished", "Unpublished")
	if err != nil {
		t.Fatal(err)
	}
	bindingID, err := h.scope.BindChannel(context.Background(), controlplane.ChannelBinding{
		AppID: appID, ChannelType: "wechat_kf", PublicID: "spare", CredentialRef: "env:KF",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.recordNotification(t, "T1", "wk1")
	if _, err := h.scope.Exec(context.Background(),
		"UPDATE channel_notifications SET binding_id = ? WHERE tenant_id = ?", bindingID, h.tenantID); err != nil {
		t.Fatal(err)
	}

	worked, err := h.puller.PullOnce(context.Background(), h.tenantID, "p1")
	if err != nil {
		t.Fatalf("PullOnce: %v", err)
	}
	if !worked {
		t.Fatal("PullOnce should still report the claim as work")
	}
	if got := h.notificationStatus(t); got != "failed" {
		t.Fatalf("notification = %q, want failed", got)
	}
}
