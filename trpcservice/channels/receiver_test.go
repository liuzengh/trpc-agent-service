package channels

// The receiver's guarantees are all about one ordering fact — persist BEFORE
// the ACK — and about state that outlives the process, so the interesting
// cases run against real MySQL, gated the same way the puller suite is:
//
//	PULLER_MYSQL_TEST_DSN='tas:taspw@tcp(127.0.0.1:3307)/tas_puller_test' \
//	    go test ./trpcservice/channels/ -run TestReceiver -v
//
// The ACK-ordering negative case needs no database: it asserts that a failed
// persistence leaves the response empty, which is what makes the platform
// retry instead of losing the event.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type receiverHarness struct {
	rc               *Receiver
	cdp              *controlplane.DB
	scope            controlplane.Scope
	tenantID         string
	appID            int64
	modelID          int64
	backendID        int64
	revisionID       int64
	webchatBindingID int64
}

func testWecomBinding() *tenant.WeComBinding {
	return &tenant.WeComBinding{
		CorpID: wecomTestCorpID, CorpSecret: "wecom-secret", AgentID: 218,
		Token: wecomTestToken, EncodingAESKey: wecomTestAESKey,
	}
}

func setupReceiverTest(t *testing.T) *receiverHarness {
	t.Helper()
	dsn := os.Getenv("PULLER_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("PULLER_MYSQL_TEST_DSN not set; skipping real-mysql receiver test")
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

	cdp := controlplane.NewDB(db)
	ctx := context.Background()
	const tenantID = "t1"
	if err := cdp.CreateTenant(ctx, tenantID, "T1"); err != nil {
		t.Fatal(err)
	}
	scope := cdp.MustScope(tenantID)

	modelID, err := scope.CreateModelProfile(ctx, "m", "fake-model", "http://127.0.0.1:1", "env:RECEIVER_TEST_KEY")
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
		Instruction: "be terse", ModelProfileID: modelID, BackendProfileID: backendID,
		MaxLLMCalls: 8, MessageTimeoutMS: 30000,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The three bindings the receiver's lookups decode. KF and WeCom carry
	// their credentials as the JSON config a real deployment writes.
	kfCfg, err := json.Marshal(testKfBinding())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "wechat_kf", PublicID: "main", Config: kfCfg,
	}); err != nil {
		t.Fatal(err)
	}
	wecomCfg, err := json.Marshal(testWecomBinding())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "wecom", PublicID: "main", Config: wecomCfg,
	}); err != nil {
		t.Fatal(err)
	}
	webBindingID, err := scope.BindChannel(ctx, controlplane.ChannelBinding{
		AppID: appID, ChannelType: "webchat", PublicID: "main", CredentialRef: "env:X",
	})
	if err != nil {
		t.Fatal(err)
	}

	return &receiverHarness{
		rc: NewReceiver(cdp), cdp: cdp, scope: scope, tenantID: tenantID,
		appID: appID, modelID: modelID, backendID: backendID, revisionID: rev.ID,
		webchatBindingID: webBindingID,
	}
}

// TestDurableNotifyFailureLeavesNoAck is the ordering contract in negative:
// a failed persistence must leave the response without "success", which is
// exactly the signal that makes WeChat retry the callback.
func TestDurableNotifyFailureLeavesNoAck(t *testing.T) {
	a := NewWeChatKf(func(string) (*tenant.WeChatKfBinding, bool) { return testKfBinding(), true }).
		WithDurableNotifications(func(context.Context, string, string, string) error {
			return errors.New("control plane down")
		})
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "EVTOK", OpenKfId: "wk1"}
	rw, _, err := postKfEvent(t, a, ev)
	if err == nil {
		t.Fatal("a failed notification must surface as an error")
	}
	if rw.Body.Len() != 0 {
		t.Fatalf("ack body = %q; the ACK must not be written before the notification lands", rw.Body.String())
	}
}

func TestReceiverKfCallbackRecordsNotificationBeforeAck(t *testing.T) {
	h := setupReceiverTest(t)
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "EVTOK", OpenKfId: "wk1"}
	rw, batch, err := postKfEvent(t, h.rc.kf, ev)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack = %q, want success", rw.Body.String())
	}
	if batch != nil {
		t.Fatalf("durable mode must not return a batch, got %+v", batch)
	}
	var n int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM channel_notifications WHERE tenant_id = ? AND status = 'pending'", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pending notifications = %d, want 1 (the row the jobs puller will drain)", n)
	}
}

func TestReceiverWebChatPersistsAndDedupes(t *testing.T) {
	h := setupReceiverTest(t)
	handler := h.rc.Routes()["/callback/webchat/"]
	body := `{"user":"u1","msg_id":"m1","text":"hello webchat"}`

	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, httptest.NewRequest(http.MethodPost, "/callback/webchat/t1", strings.NewReader(body)))
	if rw.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %q)", rw.Code, rw.Body.String())
	}

	// A resend of the same message id must be a no-op, not a second turn.
	rw2 := httptest.NewRecorder()
	handler.ServeHTTP(rw2, httptest.NewRequest(http.MethodPost, "/callback/webchat/t1", strings.NewReader(body)))
	if rw2.Code != http.StatusAccepted {
		t.Fatalf("resend status = %d, want 202", rw2.Code)
	}

	var n int
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM inbox_messages WHERE tenant_id = ?", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("inbox rows = %d, want 1 (the resend must dedupe)", n)
	}
	var actor string
	row2, err := h.scope.QueryRow(context.Background(),
		"SELECT actor_key FROM sessions WHERE tenant_id = ?", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row2.Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "u1" {
		t.Fatalf("session actor = %q, want u1", actor)
	}
}

func TestReceiverWeComPersistsMessage(t *testing.T) {
	h := setupReceiverTest(t)
	handler := h.rc.Routes()["/callback/wecom/"]
	body := fmt.Sprintf(
		"<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[218]]></AgentID></xml>",
		wecomTestCorpID, wecomTestEncrypt)
	u := fmt.Sprintf("/callback/wecom/t1?msg_signature=%s&timestamp=%s&nonce=%s",
		wecomTestSig, wecomTestTS, wecomTestNonce)

	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, httptest.NewRequest(http.MethodPost, u, strings.NewReader(body)))
	if rw.Body.String() != "success" {
		t.Fatalf("ack = %q, want success", rw.Body.String())
	}

	var actor string
	row, err := h.scope.QueryRow(context.Background(),
		"SELECT actor_key FROM sessions WHERE tenant_id = ?", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "mycreate" {
		t.Fatalf("session actor = %q, want the decrypted FromUserName", actor)
	}
}

func TestReceiverUnknownTenantFailsWithout202(t *testing.T) {
	h := setupReceiverTest(t)
	handler := h.rc.Routes()["/callback/webchat/"]
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, httptest.NewRequest(http.MethodPost,
		"/callback/webchat/ghost", strings.NewReader(`{"user":"u","text":"x"}`)))
	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: a tenant that cannot be persisted for must not be ACKed", rw.Code)
	}
}

func TestWebChatMailboxPushesSentRepliesAndMarksThem(t *testing.T) {
	h := setupReceiverTest(t)
	ctx := context.Background()
	if _, err := h.rc.inbox.Accept(ctx, h.tenantID, inbox.Request{
		AppID: h.appID, ChannelType: "webchat", BindingID: h.webchatBindingID,
		ActorKey: "u1", RevisionID: h.revisionID,
		ModelProfileID: h.modelID, BackendProfileID: h.backendID,
		ModelProfileVersion: 1, BackendProfileVersion: 1,
		PlatformMessageID: "seed-1", Text: "hi",
	}); err != nil {
		t.Fatalf("seed accept: %v", err)
	}
	var sessionPK int64
	row, err := h.scope.QueryRow(ctx,
		"SELECT session_pk FROM sessions WHERE tenant_id = ? AND actor_key = 'u1'", h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Scan(&sessionPK); err != nil {
		t.Fatal(err)
	}
	var execID string
	row2, err := h.scope.QueryRow(ctx,
		"SELECT execution_id FROM executions WHERE tenant_id = ? AND session_pk = ? LIMIT 1",
		h.tenantID, sessionPK)
	if err != nil {
		t.Fatal(err)
	}
	if err := row2.Scan(&execID); err != nil {
		t.Fatal(err)
	}

	// Two sent webchat parts (a chunk and the done part) plus one KF reply
	// that must not be pushed to this browser.
	for i, spec := range []struct {
		seq     int
		channel string
		text    string
		isDone  bool
	}{
		{1, "webchat", "part one", false},
		{2, "webchat", "final part", true},
		{3, "wechat_kf", "kf reply", true},
	} {
		if _, err := h.scope.Exec(ctx, `
			INSERT INTO reply_outbox
				(tenant_id, execution_id, session_pk, part_seq, channel_type, target, text, is_done, status, sent_at)
			VALUES (?, ?, ?, ?, ?, 'u1', ?, ?, 'sent', UTC_TIMESTAMP(6))`,
			h.tenantID, execID, sessionPK, spec.seq, spec.channel, spec.text, spec.isDone); err != nil {
			t.Fatalf("insert reply %d: %v", i, err)
		}
	}

	m := NewWebChatMailbox(h.cdp)
	rw := httptest.NewRecorder()
	if err := m.flush(ctx, rw, rw, h.tenantID, "u1"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	body := rw.Body.String()
	for _, want := range []string{`"part one"`, `"final part"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("SSE body missing %s; got %q", want, body)
		}
	}
	if strings.Contains(body, "kf reply") {
		t.Fatalf("the mailbox pushed a KF reply into a webchat stream: %q", body)
	}

	var pushed int
	row3, err := h.scope.QueryRow(ctx,
		"SELECT COUNT(*) FROM reply_outbox WHERE tenant_id = ? AND pushed_at IS NOT NULL",
		h.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := row3.Scan(&pushed); err != nil {
		t.Fatal(err)
	}
	if pushed != 2 {
		t.Fatalf("pushed rows = %d, want 2 (KF rows keep pushed_at NULL)", pushed)
	}

	// A second flush must not re-push what the browser already got.
	rw2 := httptest.NewRecorder()
	if err := m.flush(ctx, rw2, rw2, h.tenantID, "u1"); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if rw2.Body.Len() != 0 {
		t.Fatalf("second flush re-pushed %q", rw2.Body.String())
	}
}
