package wxkf

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	testToken     = "test-token"
	testAESKey    = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG" // 43 chars, per the platform spec
	testCorpID    = "ww1234567890"
	testKfAccount = "wkABCDEFGHIJ"
	testKFUser    = "wmUSER1" // external_userid, the customer side of a KF chat
)

// mapResolver resolves secret refs from a map.
type mapResolver map[string]string

func (m mapResolver) Resolve(_ context.Context, ref string) (string, error) {
	v, ok := m[ref]
	if !ok {
		return "", fmt.Errorf("unknown ref %q", ref)
	}
	return v, nil
}

// memCursorStore is an in-memory CursorStore with injectable failures; it
// records every Set so tests can assert the handle-then-save order.
type memCursorStore struct {
	mu     sync.Mutex
	m      map[string]string
	getErr error
	setErr error
	sets   []string
}

func newCursorStore() *memCursorStore {
	return &memCursorStore{m: map[string]string{}}
}

func (s *memCursorStore) Get(_ context.Context, openKfID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.m[openKfID], nil
}

func (s *memCursorStore) Set(_ context.Context, openKfID, cursor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setErr != nil {
		return s.setErr
	}
	s.m[openKfID] = cursor
	s.sets = append(s.sets, openKfID+"="+cursor)
	return nil
}

func (s *memCursorStore) saved() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sets...)
}

func testChannel(t *testing.T, apiBase string) (*Channel, *memCursorStore) {
	t.Helper()
	store := newCursorStore()
	c, err := New(Config{
		CorpID: testCorpID, KfAccount: testKfAccount,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
		Cursors: store,
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "kf-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c, store
}

// sendEnvelope is the wrapper produced by EncryptMsg; tests borrow its Encrypt
// and MsgSignature to forge the callback body and the signed query.
type sendEnvelope struct {
	XMLName      xml.Name `xml:"xml"`
	Encrypt      string   `xml:"Encrypt"`
	MsgSignature string   `xml:"MsgSignature"`
	TimeStamp    string   `xml:"TimeStamp"`
	Nonce        string   `xml:"Nonce"`
}

// forgeEvent encrypts innerXML the way the platform would and returns the
// callback body (the XML envelope EncryptMsg produces) plus the query string
// carrying a valid signature.
func forgeEvent(t *testing.T, innerXML string) (body []byte, query string) {
	t.Helper()
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg(innerXML, "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}
	query = fmt.Sprintf("msg_signature=%s&timestamp=%s&nonce=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce)
	return encrypted, query
}

// testEvent builds a kf_msg_or_event pointer for the test KF account: the
// only thing the platform pushes, and the trigger for a sync_msg pull.
func testEvent(token string) string {
	return fmt.Sprintf(`<xml><ToUserName><![CDATA[%s]]></ToUserName><CreateTime>%d</CreateTime>`+
		`<MsgType><![CDATA[event]]></MsgType><Event><![CDATA[kf_msg_or_event]]></Event>`+
		`<Token><![CDATA[%s]]></Token><OpenKfId><![CDATA[%s]]></OpenKfId></xml>`,
		testCorpID, time.Now().Unix(), token, testKfAccount)
}

// customerMsg builds one origin-3 text entry for a sync_msg page.
func customerMsg(msgID, content string) string {
	return fmt.Sprintf(`{"msgid":%q,"open_kfid":%q,"external_userid":%q,"send_time":1700000000,`+
		`"origin":3,"msgtype":"text","text":{"content":%q}}`, msgID, testKfAccount, testKFUser, content)
}

func TestVerifyURL(t *testing.T) {
	c, _ := testChannel(t, "")
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for URL verification")
		return channels.OutboundMessage{}, nil
	}))

	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg("echo-challenge", "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/wxkf/callback?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce, url.QueryEscape(env.Encrypt)), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "echo-challenge" {
		t.Fatalf("verify failed: status=%d body=%q", rec.Code, rec.Body.String())
	}
}

// TestCallbackEventPullsMessages: the callback is only a pointer — the pull it
// triggers must authenticate with the KF secret's token, target the event's
// KF account, pass the event's pull token on the first (cursor-less) page,
// normalize the pulled messages, and persist the returned cursor.
func TestCallbackEventPullsMessages(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"next-1","has_more":0,"msg_list":[%s]}`,
			customerMsg("msgid-1", "你好"))
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	var got []channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = append(got, msg)
		return channels.OutboundMessage{}, nil
	}))

	body, query := forgeEvent(t, testEvent("evt-token-1"))
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d, body %s", rec.Code, rec.Body.String())
	}
	if len(got) != 1 {
		t.Fatalf("want 1 normalized message, got %d", len(got))
	}
	m := got[0]
	if m.MsgID != "msgid-1" || m.UserID != testKFUser || m.Text != "你好" {
		t.Fatalf("bad normalization: %+v", m)
	}
	if m.SessionKey != "dm:wxkf:"+testKFUser {
		t.Fatalf("unexpected session key: %q", m.SessionKey)
	}
	if m.ChatID != "" {
		t.Fatalf("kf is direct-chat only, ChatID must be empty: %q", m.ChatID)
	}
	if m.WebhookPath != "/wxkf/callback" {
		t.Fatalf("unexpected webhook path: %q", m.WebhookPath)
	}

	syncs := fake.syncs()
	if len(syncs) != 1 {
		t.Fatalf("want 1 sync_msg call, got %d", len(syncs))
	}
	var pull map[string]any
	if err := json.Unmarshal([]byte(syncs[0]), &pull); err != nil {
		t.Fatal(err)
	}
	if pull["token"] != "evt-token-1" {
		t.Fatalf("first pull must carry the event's pull token: %v", pull)
	}
	if pull["open_kfid"] != testKfAccount {
		t.Fatalf("pull must target the event's KF account: %v", pull)
	}
	if _, has := pull["cursor"]; has {
		t.Fatalf("first pull (empty store) must not carry a cursor: %v", pull)
	}
	if saved := store.saved(); len(saved) != 1 || saved[0] != testKfAccount+"=next-1" {
		t.Fatalf("cursor must be persisted after the page, got %v", saved)
	}
}

// TestCallbackCursorPagination: has_more pages the drain — the second pull
// carries the first page's cursor, every message lands, and the final cursor
// is the one the last page returned.
func TestCallbackCursorPagination(t *testing.T) {
	pages := []string{
		fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"cur-2","has_more":1,"msg_list":[%s]}`,
			customerMsg("m1", "一")),
		fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"cur-3","has_more":0,"msg_list":[%s]}`,
			customerMsg("m2", "二")),
	}
	fake := &scriptKfAPI{onSync: func(n int) string { return pages[n-1] }}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	var got []string
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = append(got, msg.Text)
		return channels.OutboundMessage{}, nil
	}))

	body, query := forgeEvent(t, testEvent("evt-token-1"))
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d", rec.Code)
	}
	if strings.Join(got, ",") != "一,二" {
		t.Fatalf("both pages must reach the handler, got %v", got)
	}
	syncs := fake.syncs()
	if len(syncs) != 2 {
		t.Fatalf("want 2 sync_msg calls, got %d", len(syncs))
	}
	var first, second map[string]any
	if err := json.Unmarshal([]byte(syncs[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(syncs[1]), &second); err != nil {
		t.Fatal(err)
	}
	if _, has := first["cursor"]; has {
		t.Fatalf("first pull must be cursor-less: %v", first)
	}
	if second["cursor"] != "cur-2" {
		t.Fatalf("second pull must resume from the first page's cursor: %v", second)
	}
	if second["token"] != "evt-token-1" {
		t.Fatalf("pages of one drain must reuse the event's pull token: %v", second)
	}
	if saved := store.saved(); len(saved) != 2 || saved[1] != testKfAccount+"=cur-3" {
		t.Fatalf("cursor must advance per page, got %v", saved)
	}
}

// TestCallbackPullFiltersNonCustomerAndNonText: servicer replies and system
// pushes (origin 4/5) never enter the pipeline — they would feed the agent its
// own output — while a customer-sent non-text becomes a placeholder text and
// the cursor advances past every entry regardless.
func TestCallbackPullFiltersNonCustomerAndNonText(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"next-1","has_more":0,"msg_list":[`+
			`{"msgid":"s1","external_userid":%q,"origin":5,"msgtype":"text","text":{"content":"servicer"}},`+
			`{"msgid":"s2","external_userid":%q,"origin":4,"msgtype":"event","event":{"event_type":"enter_session"}},`+
			`{"msgid":"s3","external_userid":%q,"origin":3,"msgtype":"image","image":{"media_id":"M"}},`+
			`%s]}`, testKFUser, testKFUser, testKFUser, customerMsg("m1", "你好"))
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	var got []channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = append(got, msg)
		return channels.OutboundMessage{}, nil
	}))

	body, query := forgeEvent(t, testEvent("evt-token-1"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body))))

	if rec.Code != http.StatusOK || len(got) != 2 {
		t.Fatalf("only the customer's entries must be handled, status=%d handled=%d", rec.Code, len(got))
	}
	// The customer-sent image becomes a placeholder text; event entries and
	// servicer replies stay skipped.
	if got[0].MsgID != "s3" || got[0].Text != "[图片]" || got[0].Type != channels.TypeText {
		t.Fatalf("customer image must become a placeholder text message: %+v", got[0])
	}
	if got[1].MsgID != "m1" || got[1].Text != "你好" {
		t.Fatalf("customer text must pass through unchanged: %+v", got[1])
	}
	if saved := store.saved(); len(saved) != 1 {
		t.Fatalf("cursor must advance past the skipped entries, got %v", saved)
	}
}

// Event-type entries never become placeholders, whichever side they came
// from: an enter_session is a session signal, not user content.
func TestCallbackPullSkipsCustomerEvents(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"next-1","has_more":0,"msg_list":[`+
			`{"msgid":"e1","external_userid":%q,"origin":3,"msgtype":"event","event":{"event_type":"enter_session"}}]}`, testKFUser)
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("event entries must never enter the pipeline")
		return channels.OutboundMessage{}, nil
	}))

	body, query := forgeEvent(t, testEvent("evt-token-1"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body))))

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d", rec.Code)
	}
	if saved := store.saved(); len(saved) != 1 {
		t.Fatalf("cursor must still advance past the event entry, got %v", saved)
	}
}

// TestCallbackSkipsUnrelatedEvents: callbacks that are not new-content
// pointers are acked without touching the sync API.
func TestCallbackSkipsUnrelatedEvents(t *testing.T) {
	fake := &scriptKfAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, _ := testChannel(t, srv.URL)
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unrelated event")
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><CreateTime>1700000000</CreateTime>` +
		`<MsgType><![CDATA[event]]></MsgType><Event><![CDATA[change_contact]]></Event></xml>`
	body, query := forgeEvent(t, inner)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body))))

	if rec.Code != http.StatusOK {
		t.Fatalf("unrelated event must be acked, got %d", rec.Code)
	}
	if n := fake.syncCalls(); n != 0 {
		t.Fatalf("an unrelated event must not trigger a pull, got %d", n)
	}
}

// TestCallbackSkipsBadSignatures: tampered signatures and non-XML envelopes
// are acked (no redelivery) and never handled.
func TestCallbackSkipsBadSignatures(t *testing.T) {
	fake := &scriptKfAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, _ := testChannel(t, srv.URL)
	called := false
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		called = true
		return channels.OutboundMessage{}, nil
	}))

	body, _ := forgeEvent(t, testEvent("evt-token-1"))
	// Tampered signature: acked, never handled.
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("bad signature must be acked and skipped, status=%d called=%v", rec.Code, called)
	}

	// A body that is not an XML envelope: acked the same way.
	req = httptest.NewRequest(http.MethodPost, "/wxkf/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(`{"foo":"bar"}`))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("a non-XML envelope must be acked and skipped, status=%d called=%v", rec.Code, called)
	}
	if n := fake.syncCalls(); n != 0 {
		t.Fatalf("unverified callbacks must not trigger pulls, got %d", n)
	}
}

// TestCallbackDuplicateVersusFailure: ErrDuplicate is a success outcome — 200
// and the cursor advances — while a real failure stays 5xx with the cursor
// untouched, so the redelivery re-pulls the page and dedup absorbs the rest.
func TestCallbackDuplicateVersusFailure(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"next-1","has_more":0,"msg_list":[%s]}`,
			customerMsg("msgid-1", "你好"))
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	post := func(t *testing.T, h channels.HandlerFunc) (*memCursorStore, int) {
		t.Helper()
		c, store := testChannel(t, srv.URL)
		mux := http.NewServeMux()
		c.RegisterRoutes(mux, h)
		body, query := forgeEvent(t, testEvent("evt-token-1"))
		req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return store, rec.Code
	}

	store, code := post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, channels.ErrDuplicate
	})
	if code != http.StatusOK {
		t.Fatalf("duplicate must be answered 200, got %d", code)
	}
	if saved := store.saved(); len(saved) != 1 {
		t.Fatalf("a duplicate page must still advance the cursor, got %v", saved)
	}

	store, code = post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: boom")
	})
	if code != http.StatusInternalServerError {
		t.Fatalf("real failure must be answered 5xx so the IM retries, got %d", code)
	}
	if saved := store.saved(); len(saved) != 0 {
		t.Fatalf("a failed page must not advance the cursor, got %v", saved)
	}
}

// TestCallbackPullFailureIsNotAcked: a sync_msg rejection (bad credentials,
// platform errors) must answer 5xx so the platform redelivers the event —
// acking would drop every message the pointer stood for.
func TestCallbackPullFailureIsNotAcked(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return `{"errcode":60020,"errmsg":"not allowed"}`
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	rec := postCallback(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for a failed pull")
		return channels.OutboundMessage{}, nil
	}))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("a failed pull must answer 5xx, got %d", rec.Code)
	}
	if saved := store.saved(); len(saved) != 0 {
		t.Fatalf("a failed pull must not advance the cursor, got %v", saved)
	}
}

// TestCallbackCursorSaveFailureIsNotAcked: enqueueing succeeded but persisting
// the cursor failed — the next pull would re-deliver the page, and dedup
// absorbs it; acking would make that safety net load-bearing against a cursor
// that never advanced.
func TestCallbackCursorSaveFailureIsNotAcked(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"next-1","has_more":0,"msg_list":[%s]}`,
			customerMsg("msgid-1", "你好"))
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	store.setErr = fmt.Errorf("redis down")
	rec := postCallback(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, nil
	}))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("a cursor save failure must answer 5xx, got %d", rec.Code)
	}
}

// TestCallbackEmptyNextCursorKeepsSaved: an empty next_cursor must not
// overwrite the persisted position — saving "" would make the next pull resume
// from scratch and re-pull three days of history.
func TestCallbackEmptyNextCursorKeepsSaved(t *testing.T) {
	fake := &scriptKfAPI{onSync: func(int) string {
		return fmt.Sprintf(`{"errcode":0,"errmsg":"ok","next_cursor":"","has_more":0,"msg_list":[%s]}`,
			customerMsg("msgid-1", "你好"))
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c, store := testChannel(t, srv.URL)
	if err := store.Set(t.Context(), testKfAccount, "cur-old"); err != nil {
		t.Fatal(err)
	}
	var got []channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = append(got, msg)
		return channels.OutboundMessage{}, nil
	}))

	body, query := forgeEvent(t, testEvent("evt-token-1"))
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || len(got) != 1 {
		t.Fatalf("the page must still be handled, status=%d handled=%d", rec.Code, len(got))
	}
	saved, err := store.Get(t.Context(), testKfAccount)
	if err != nil {
		t.Fatal(err)
	}
	if saved != "cur-old" {
		t.Fatalf("an empty next_cursor must keep the saved cursor, got %q", saved)
	}
	for _, s := range store.saved() {
		if s == testKfAccount+"=" {
			t.Fatalf("an empty cursor must never be persisted, sets=%v", store.saved())
		}
	}
}

// postCallback serves one forged event on a binding-scoped handler.
func postCallback(t *testing.T, c *Channel, h channels.Handler) *httptest.ResponseRecorder {
	t.Helper()
	body, query := forgeEvent(t, testEvent("evt-token-1"))
	return postRaw(t, c, h, body, query)
}
