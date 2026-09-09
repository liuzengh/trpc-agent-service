package channels

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// WeChat KF shares the official callback crypto scheme with WeCom, so the
// golden vectors from wecom_test.go (corpId/token/encodingAesKey) are reused
// to pin signature and AES handling; the pull/push flows below run against a
// mock qyapi server and follow the official sync_msg / send_msg wire format.

func testKfBinding() *tenant.WeChatKfBinding {
	return &tenant.WeChatKfBinding{
		CorpID:         wecomTestCorpID,
		Secret:         "kf-secret",
		Token:          wecomTestToken,
		EncodingAESKey: wecomTestAESKey,
	}
}

func testKf(t *testing.T) *WeChatKf {
	t.Helper()
	b := testKfBinding()
	return NewWeChatKf(func(tenantID string) (*tenant.WeChatKfBinding, bool) {
		if tenantID == "t1" {
			return b, true
		}
		return nil, false
	})
}

// kfSentMsg is one recorded kf/send_msg call.
type kfSentMsg struct {
	ToUser  string `json:"touser"`
	OpenKf  string `json:"open_kfid"`
	Content string
}

// kfMock is a stateful mock of the WeCom kf API endpoints: gettoken counts
// calls, sync_msg serves queued pages while recording requests, send_msg
// records deliveries.
type kfMock struct {
	srv        *httptest.Server
	tokenCalls atomic.Int32

	mu        sync.Mutex
	syncReqs  []kfSyncRequest
	syncPages []kfSyncResponse
	syncErr   string // when set, sync_msg fails with this errmsg
	sent      []kfSentMsg
	t         *testing.T
}

func newKfMock(t *testing.T, pages ...kfSyncResponse) *kfMock {
	t.Helper()
	m := &kfMock{syncPages: pages, t: t}
	m.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			if got := r.URL.Query().Get("corpsecret"); got != "kf-secret" {
				t.Errorf("gettoken corpsecret = %q, want kf-secret", got)
			}
			m.tokenCalls.Add(1)
			_ = json.NewEncoder(rw).Encode(map[string]any{
				"errcode": 0, "errmsg": "ok", "access_token": "TOK", "expires_in": 7200})
		case "/cgi-bin/kf/sync_msg":
			if got := r.URL.Query().Get("access_token"); got != "TOK" {
				t.Errorf("sync_msg access_token = %q", got)
			}
			var req kfSyncRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode sync_msg: %v", err)
			}
			m.mu.Lock()
			m.syncReqs = append(m.syncReqs, req)
			if m.syncErr != "" {
				m.mu.Unlock()
				_ = json.NewEncoder(rw).Encode(map[string]any{"errcode": 40001, "errmsg": m.syncErr})
				return
			}
			var page kfSyncResponse
			if len(m.syncPages) > 0 {
				page, m.syncPages = m.syncPages[0], m.syncPages[1:]
			}
			m.mu.Unlock()
			_ = json.NewEncoder(rw).Encode(page)
		case "/cgi-bin/kf/send_msg":
			if got := r.URL.Query().Get("access_token"); got != "TOK" {
				t.Errorf("send_msg access_token = %q", got)
			}
			var req struct {
				ToUser   string `json:"touser"`
				OpenKfID string `json:"open_kfid"`
				MsgType  string `json:"msgtype"`
				Text     struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode send_msg: %v", err)
			}
			if req.MsgType != "text" {
				t.Errorf("send_msg msgtype = %q, want text", req.MsgType)
			}
			m.mu.Lock()
			m.sent = append(m.sent, kfSentMsg{ToUser: req.ToUser, OpenKf: req.OpenKfID, Content: req.Text.Content})
			m.mu.Unlock()
			_ = json.NewEncoder(rw).Encode(map[string]any{"errcode": 0, "errmsg": "ok", "msgid": "MSGID"})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *kfMock) requests() ([]kfSyncRequest, []kfSentMsg) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]kfSyncRequest(nil), m.syncReqs...), append([]kfSentMsg(nil), m.sent...)
}

// xmlMarshalEvent renders one callback event into the plaintext XML the
// official crypto envelope wraps.
func xmlMarshalEvent(ev kfEvent) (string, error) {
	b, err := xml.Marshal(ev)
	return string(b), err
}

// postKfEvent encrypts one callback event with the official vector key and
// posts it to the adapter, returning the recorder and the parsed batch.
func postKfEvent(t *testing.T, a *WeChatKf, ev kfEvent) (*httptest.ResponseRecorder, []*InboundMessage, error) {
	t.Helper()
	plain, err := xmlMarshalEvent(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	aesKey, err := weComAESKey(wecomTestAESKey)
	if err != nil {
		t.Fatalf("aes key: %v", err)
	}
	enc, err := weComEncrypt(aesKey, plain, wecomTestCorpID)
	if err != nil {
		t.Fatalf("encrypt event: %v", err)
	}
	sig := weComSign(wecomTestToken, wecomTestTS, wecomTestNonce, enc)
	body := fmt.Sprintf(
		"<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt></xml>",
		wecomTestCorpID, enc)
	u := fmt.Sprintf("/callback/wechat_kf/t1?msg_signature=%s&timestamp=%s&nonce=%s",
		sig, wecomTestTS, wecomTestNonce)
	rw := httptest.NewRecorder()
	batch, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, u, strings.NewReader(body)))
	return rw, batch, err
}

func kfTextMsg(msgid, user, content string, origin uint32, sendTime int64) kfSyncMsg {
	m := kfSyncMsg{Msgid: msgid, ExternalUserID: user, Origin: origin, MsgType: "text", SendTime: sendTime}
	m.Text.Content = content
	return m
}

func TestWeChatKfCallbackSyncFlow(t *testing.T) {
	now := time.Now().Unix()
	page1 := kfSyncResponse{
		NextCursor: "C1", HasMore: 1,
		MsgList: []kfSyncMsg{
			kfTextMsg("m1", "ext1", "hello", 3, now),
			kfTextMsg("m2", "ext1", "接待员的话", 5, now),   // origin 5: 非客户消息，跳过
			{Msgid: "m3", MsgType: "event", Origin: 4}, // 事件消息，跳过
			kfTextMsg("m4", "ext2", "world", 3, now),
			kfTextMsg("m5", "ext3", "三年前的老消息", 3, now-3*86400), // 首次拉取跳过启动前历史
		},
	}
	page2 := kfSyncResponse{
		NextCursor: "C2", HasMore: 0,
		MsgList: []kfSyncMsg{kfTextMsg("m6", "ext1", "again", 3, now)},
	}
	mock := newKfMock(t, page1, page2)

	a := testKf(t)
	a.apiBase = mock.srv.URL
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "EVTOK", OpenKfId: "wk1"}
	rw, batch, err := postKfEvent(t, a, ev)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack body = %q, want success", rw.Body.String())
	}

	want := []struct{ msgID, user, text string }{
		{"m1", "ext1", "hello"}, {"m4", "ext2", "world"}, {"m6", "ext1", "again"},
	}
	if len(batch) != len(want) {
		t.Fatalf("batch = %+v, want %d messages", batch, len(want))
	}
	for i, w := range want {
		in := batch[i]
		if in.MsgID != w.msgID || in.UserID != w.user || in.Text != w.text ||
			in.TenantID != "t1" || in.Channel != TypeWeChatKF {
			t.Fatalf("batch[%d] = %+v, want %+v", i, in, w)
		}
	}

	// sync_msg pagination: two calls, cursors ""→"C1", event token forwarded.
	reqs, _ := mock.requests()
	if len(reqs) != 2 {
		t.Fatalf("sync_msg calls = %d, want 2", len(reqs))
	}
	if reqs[0].Cursor != "" || reqs[1].Cursor != "C1" {
		t.Fatalf("cursors = %q, %q; want \"\", C1", reqs[0].Cursor, reqs[1].Cursor)
	}
	for _, r := range reqs {
		if r.Token != "EVTOK" || r.OpenKfID != "wk1" || r.Limit != kfSyncLimit {
			t.Fatalf("sync request = %+v", r)
		}
	}
	if got := a.cursorOf("t1", "wk1"); got != "C2" {
		t.Fatalf("persisted cursor = %q, want C2", got)
	}
	if got := a.kfIDOf("t1:wechat_kf:ext1"); got != "wk1" {
		t.Fatalf("session open_kfid = %q, want wk1", got)
	}
	if mock.tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls = %d, want 1", mock.tokenCalls.Load())
	}
}

func TestWeChatKfCallbackIncrementalCursor(t *testing.T) {
	now := time.Now().Unix()
	mock := newKfMock(t,
		kfSyncResponse{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "第一条", 3, now)}},
		kfSyncResponse{NextCursor: "C2", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m2", "ext1", "第二条", 3, now)}},
	)
	a := testKf(t)
	a.apiBase = mock.srv.URL
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "T1", OpenKfId: "wk1"}

	if _, batch, err := postKfEvent(t, a, ev); err != nil || len(batch) != 1 {
		t.Fatalf("first callback: batch=%v err=%v", batch, err)
	}
	ev.Token = "T2"
	if _, batch, err := postKfEvent(t, a, ev); err != nil || len(batch) != 1 {
		t.Fatalf("second callback: batch=%v err=%v", batch, err)
	}
	reqs, _ := mock.requests()
	if len(reqs) != 2 || reqs[1].Cursor != "C1" {
		t.Fatalf("second call must resume from C1: %+v", reqs)
	}
	if got := a.cursorOf("t1", "wk1"); got != "C2" {
		t.Fatalf("cursor = %q, want C2", got)
	}
}

func TestWeChatKfCallbackNonKfEvent(t *testing.T) {
	mock := newKfMock(t) // no pages: any sync call would fail the test
	a := testKf(t)
	a.apiBase = mock.srv.URL
	ev := kfEvent{MsgType: "event", Event: "subscribe"}
	rw, batch, err := postKfEvent(t, a, ev)
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if batch != nil {
		t.Fatalf("non-kf event must not produce inbound, got %+v", batch)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack body = %q, want success", rw.Body.String())
	}
	if reqs, _ := mock.requests(); len(reqs) != 0 {
		t.Fatalf("non-kf event must not call sync_msg, got %+v", reqs)
	}
}

func TestWeChatKfCallbackBadSignature(t *testing.T) {
	a := testKf(t)
	body := "<xml><Encrypt><![CDATA[deadbeef]]></Encrypt></xml>"
	u := fmt.Sprintf("/callback/wechat_kf/t1?msg_signature=deadbeef&timestamp=%s&nonce=%s",
		wecomTestTS, wecomTestNonce)
	rw := httptest.NewRecorder()
	if _, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, u, strings.NewReader(body))); err == nil {
		t.Fatal("bad signature must be rejected")
	}
}

func TestWeChatKfCallbackUnboundTenant(t *testing.T) {
	a := testKf(t)
	rw := httptest.NewRecorder()
	_, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, "/callback/wechat_kf/nope", strings.NewReader("<xml/>")))
	if err == nil {
		t.Fatal("unbound tenant must be rejected")
	}
}

func TestWeChatKfProbe(t *testing.T) {
	a := testKf(t)
	aesKey, err := weComAESKey(wecomTestAESKey)
	if err != nil {
		t.Fatalf("aes key: %v", err)
	}
	echo := "1616140317555161061"
	enc, err := weComEncrypt(aesKey, echo, wecomTestCorpID)
	if err != nil {
		t.Fatalf("encrypt echostr: %v", err)
	}
	sig := weComSign(wecomTestToken, wecomTestTS, wecomTestNonce, enc)
	u := fmt.Sprintf("/callback/wechat_kf/t1?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
		sig, wecomTestTS, wecomTestNonce, url.QueryEscape(enc))
	rw := httptest.NewRecorder()
	msgs, err := a.Callback(rw, httptest.NewRequest(http.MethodGet, u, nil))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if msgs != nil {
		t.Fatalf("probe must not produce inbound, got %+v", msgs)
	}
	if rw.Body.String() != echo {
		t.Fatalf("probe echo = %q, want %q", rw.Body.String(), echo)
	}
}

func TestWeChatKfSyncFirstRoundError(t *testing.T) {
	mock := newKfMock(t)
	mock.syncErr = "invalid credential"
	a := testKf(t)
	a.apiBase = mock.srv.URL
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "T", OpenKfId: "wk1"}
	rw, batch, err := postKfEvent(t, a, ev)
	if err == nil {
		t.Fatalf("first-round sync failure must surface, batch=%+v", batch)
	}
	if !strings.Contains(err.Error(), "sync_msg") {
		t.Fatalf("error = %v, want sync_msg context", err)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack must stay written, body = %q", rw.Body.String())
	}
	if got := a.cursorOf("t1", "wk1"); got != "" {
		t.Fatalf("cursor must not advance on failure, got %q", got)
	}
}

// TestWeChatKfSendAggregatesSplits runs the outbound path: a callback first
// records the session's open_kfid, then chunks aggregate until Done and long
// replies split at 2048 bytes on rune boundaries via kf/send_msg.
func TestWeChatKfSendAggregatesSplits(t *testing.T) {
	now := time.Now().Unix()
	mock := newKfMock(t,
		kfSyncResponse{NextCursor: "C1", HasMore: 0, MsgList: []kfSyncMsg{kfTextMsg("m1", "ext1", "hi", 3, now)}},
	)
	a := testKf(t)
	a.apiBase = mock.srv.URL
	ev := kfEvent{MsgType: "event", Event: "kf_msg_or_event", Token: "T", OpenKfId: "wk1"}
	if _, _, err := postKfEvent(t, a, ev); err != nil {
		t.Fatalf("callback: %v", err)
	}

	ctx := context.Background()
	target := InboundMessage{TenantID: "t1", Channel: TypeWeChatKF, UserID: "ext1", MsgID: "m1"}
	long := strings.Repeat("中", 1000) // 3000 bytes -> forces a split
	for _, chunk := range []string{"你好", long} {
		if err := a.Send(ctx, &OutboundMessage{Target: target, Text: chunk, Chunk: true}); err != nil {
			t.Fatalf("send chunk: %v", err)
		}
	}
	if err := a.Send(ctx, &OutboundMessage{Target: target, Done: true}); err != nil {
		t.Fatalf("send done: %v", err)
	}

	_, sent := mock.requests()
	if len(sent) != 2 {
		t.Fatalf("sent %d messages, want 2: %+v", len(sent), sent)
	}
	if joined := sent[0].Content + sent[1].Content; joined != "你好"+long {
		t.Fatal("aggregated reply mismatch")
	}
	if len(sent[0].Content) > kfMaxTextBytes {
		t.Fatalf("first fragment = %d bytes, over limit", len(sent[0].Content))
	}
	if sent[0].ToUser != "ext1" || sent[0].OpenKf != "wk1" {
		t.Fatalf("delivery target = %+v", sent[0])
	}
	if mock.tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls = %d, want 1 (cached across sync and send)", mock.tokenCalls.Load())
	}
}

func TestWeChatKfSendWithoutCallback(t *testing.T) {
	mock := newKfMock(t)
	a := testKf(t)
	a.apiBase = mock.srv.URL
	ctx := context.Background()
	target := InboundMessage{TenantID: "t1", Channel: TypeWeChatKF, UserID: "ext1"}
	err := a.Send(ctx, &OutboundMessage{Target: target, Text: "hi", Done: true})
	if err == nil || !strings.Contains(err.Error(), "open_kfid") {
		t.Fatalf("send without a recorded open_kfid must fail clearly, got %v", err)
	}
}
