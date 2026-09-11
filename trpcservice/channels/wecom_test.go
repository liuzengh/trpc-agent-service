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

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Golden vectors from the official WeCom crypto doc (企业微信文档 90968):
// corpId/token/encodingAesKey plus one real callback with its msg_signature,
// ciphertext and decrypted plaintext. They pin signature, AES and XML
// parsing to the official protocol without needing a real WeCom tenant.
const (
	wecomTestCorpID  = "wx5823bf96d3bd56c7"
	wecomTestToken   = "QDG6eK"
	wecomTestAESKey  = "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"
	wecomTestSig     = "477715d11cdb4164915debcba66cb864d751f3e6"
	wecomTestTS      = "1409659813"
	wecomTestNonce   = "1372623149"
	wecomTestEncrypt = "RypEvHKD8QQKFhvQ6QleEB4J58tiPdvo+rtK1I9qca6aM/wvqnLSV5zEPeusUiX5L5X/0lWfrf0QADHHhGd3QczcdCUpj911L3vg3W/sYYvuJTs3TUUkSUXxaccAS0qhxchrRYt66wiSpGLYL42aM6A8dTT+6k4aSknmPj48kzJs8qLjvd4Xgpue06DOdnLxAUHzM6+kDZ+HMZfJYuR+LtwGc2hgf5gsijff0ekUNXZiqATP7PF5mZxZ3Izoun1s4zG4LUMnvw2r+KqCKIw+3IQH03v+BCA9nMELNqbSf6tiWSrXJB3LAVGUcallcrw8V2t9EL4EhzJWrQUax5wLVMNS0+rUPA3k22Ncx4XXZS9o0MBH27Bo6BpNelZpS+/uh9KsNlY6bHCmJU9p8g7m3fVKn28H3KDYA5Pl/T8Z1ptDAVe0lXdQ2YoyyH2uyPIGHBZZIs2pDBS8R07+qN+E7Q=="
)

func testBinding() *tenant.WeComBinding {
	return &tenant.WeComBinding{
		CorpID:         wecomTestCorpID,
		CorpSecret:     "test-secret",
		AgentID:        218,
		Token:          wecomTestToken,
		EncodingAESKey: wecomTestAESKey,
	}
}

func testWecom(t *testing.T) *WeCom {
	t.Helper()
	b := testBinding()
	a := NewWeCom(func(tenantID string) (*tenant.WeComBinding, bool) {
		if tenantID == "t1" {
			return b, true
		}
		return nil, false
	})
	return a
}

func TestWecomSignOfficialVector(t *testing.T) {
	got := weComSign(wecomTestToken, wecomTestTS, wecomTestNonce, wecomTestEncrypt)
	if got != wecomTestSig {
		t.Fatalf("sign = %q, want official %q", got, wecomTestSig)
	}
}

func TestWecomDecryptOfficialVector(t *testing.T) {
	aesKey, err := weComAESKey(wecomTestAESKey)
	if err != nil {
		t.Fatalf("aes key: %v", err)
	}
	plain, err := weComDecrypt(aesKey, wecomTestEncrypt, wecomTestCorpID)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	var msg wecomMessage
	if err := xml.Unmarshal([]byte(plain), &msg); err != nil {
		t.Fatalf("parse plaintext: %v (plain=%q)", err, plain)
	}
	if msg.FromUserName != "mycreate" || msg.MsgType != "text" ||
		msg.Content != "hello" || msg.MsgID != 4561255354251345929 {
		t.Fatalf("decrypted msg = %+v", msg)
	}

	if _, err := weComDecrypt(aesKey, wecomTestEncrypt, "wrong-corp"); err == nil {
		t.Fatal("decrypt with wrong receiveid must fail")
	}
}

func TestWecomEncryptRoundTrip(t *testing.T) {
	aesKey, err := weComAESKey(wecomTestAESKey)
	if err != nil {
		t.Fatalf("aes key: %v", err)
	}
	original := "<xml><Content><![CDATA[你好，世界]]></Content></xml>"
	enc, err := weComEncrypt(aesKey, original, wecomTestCorpID)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := weComDecrypt(aesKey, enc, wecomTestCorpID)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if got != original {
		t.Fatalf("round trip = %q, want %q", got, original)
	}
}

func TestWecomCallbackMessage(t *testing.T) {
	a := testWecom(t)
	body := fmt.Sprintf(
		"<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[218]]></AgentID></xml>",
		wecomTestCorpID, wecomTestEncrypt)
	u := fmt.Sprintf("/callback/wecom/t1?msg_signature=%s&timestamp=%s&nonce=%s",
		wecomTestSig, wecomTestTS, wecomTestNonce)
	rw := httptest.NewRecorder()
	batch, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, u, strings.NewReader(body)))
	if err != nil {
		t.Fatalf("callback: %v", err)
	}
	if rw.Body.String() != "success" {
		t.Fatalf("ack body = %q, want success", rw.Body.String())
	}
	if len(batch) != 1 {
		t.Fatalf("batch size = %d, want 1", len(batch))
	}
	in := batch[0]
	if in.TenantID != "t1" || in.Channel != TypeWeCom || in.UserID != "mycreate" ||
		in.Text != "hello" || in.MsgID != "4561255354251345929" {
		t.Fatalf("inbound = %+v", in)
	}
	if in.SessionID() != "t1:wecom:mycreate" {
		t.Fatalf("session id = %q", in.SessionID())
	}
}

func TestWecomCallbackBadSignature(t *testing.T) {
	a := testWecom(t)
	body := fmt.Sprintf("<xml><Encrypt><![CDATA[%s]]></Encrypt></xml>", wecomTestEncrypt)
	u := fmt.Sprintf("/callback/wecom/t1?msg_signature=deadbeef&timestamp=%s&nonce=%s",
		wecomTestTS, wecomTestNonce)
	rw := httptest.NewRecorder()
	if _, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, u, strings.NewReader(body))); err == nil {
		t.Fatal("bad signature must be rejected")
	}
}

func TestWecomCallbackUnboundTenant(t *testing.T) {
	a := testWecom(t)
	rw := httptest.NewRecorder()
	_, err := a.Callback(rw, httptest.NewRequest(http.MethodPost, "/callback/wecom/nope", strings.NewReader("<xml/>")))
	if err == nil {
		t.Fatal("unbound tenant must be rejected")
	}
}

func TestWecomProbe(t *testing.T) {
	a := testWecom(t)
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
	u := fmt.Sprintf("/callback/wecom/t1?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
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

// TestWecomSendAggregateSplit runs the outbound path against a mock WeCom
// API: chunks aggregate until Done, long replies split at 2048 bytes on
// rune boundaries, and the access_token is cached across sends.
func TestWecomSendAggregateSplit(t *testing.T) {
	var tokenCalls atomic.Int32
	var mu sync.Mutex
	var contents []string
	var toUsers []string
	var agentIDs []int

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokenCalls.Add(1)
			_ = json.NewEncoder(rw).Encode(map[string]any{
				"errcode": 0, "errmsg": "ok", "access_token": "TOK", "expires_in": 7200})
		case "/cgi-bin/message/send":
			if got := r.URL.Query().Get("access_token"); got != "TOK" {
				t.Errorf("access_token = %q", got)
			}
			var req struct {
				ToUser  string `json:"touser"`
				AgentID int    `json:"agentid"`
				Text    struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode send: %v", err)
			}
			mu.Lock()
			contents = append(contents, req.Text.Content)
			toUsers = append(toUsers, req.ToUser)
			agentIDs = append(agentIDs, req.AgentID)
			mu.Unlock()
			_ = json.NewEncoder(rw).Encode(map[string]any{"errcode": 0, "errmsg": "ok"})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	a := testWecom(t)
	a.apiBase = srv.URL
	ctx := context.Background()
	target := InboundMessage{TenantID: "t1", Channel: TypeWeCom, UserID: "mycreate", MsgID: "1"}

	// 3000 bytes of CJK (3 bytes/rune) plus a short prefix: aggregated into
	// one reply of 3006 bytes -> split into 2 messages, rune-safe.
	long := strings.Repeat("中", 1000)
	for _, chunk := range []string{"你好", long} {
		if err := a.Send(ctx, &OutboundMessage{Target: target, Text: chunk, Chunk: true}); err != nil {
			t.Fatalf("send chunk: %v", err)
		}
	}
	if err := a.Send(ctx, &OutboundMessage{Target: target, Done: true}); err != nil {
		t.Fatalf("send done: %v", err)
	}

	mu.Lock()
	firstRound := append([]string(nil), contents...)
	toUser, agentID := toUsers[0], agentIDs[0]
	mu.Unlock()

	if tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls = %d, want 1", tokenCalls.Load())
	}
	if len(firstRound) != 2 {
		t.Fatalf("sent %d messages, want 2: %q", len(firstRound), firstRound)
	}
	if joined := firstRound[0] + firstRound[1]; joined != "你好"+long {
		t.Fatal("aggregated reply mismatch")
	}
	if len(firstRound[0]) > wecomMaxTextBytes {
		t.Fatalf("first fragment = %d bytes, over limit", len(firstRound[0]))
	}
	if toUser != "mycreate" || agentID != 218 {
		t.Fatalf("touser=%q agentid=%d", toUser, agentID)
	}

	// Second reply reuses the cached token.
	if err := a.Send(ctx, &OutboundMessage{Target: target, Text: "again", Done: true}); err != nil {
		t.Fatalf("send second: %v", err)
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("gettoken calls = %d after second send, want 1 (cached)", tokenCalls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(contents) != 3 || contents[2] != "again" {
		t.Fatalf("second round messages = %q", contents)
	}
}

func TestSplitUTF8RuneSafe(t *testing.T) {
	s := strings.Repeat("中文", 600) // 3600 bytes
	parts := splitUTF8(s, wecomMaxTextBytes)
	if len(parts) != 2 {
		t.Fatalf("parts = %d, want 2", len(parts))
	}
	if strings.Join(parts, "") != s {
		t.Fatal("split lost content")
	}
	for _, p := range parts {
		if len(p) > wecomMaxTextBytes {
			t.Fatalf("part of %d bytes over limit", len(p))
		}
	}
	if parts := splitUTF8("short", wecomMaxTextBytes); len(parts) != 1 {
		t.Fatalf("short split = %v", parts)
	}
}
