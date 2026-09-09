package wecom

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	testToken  = "test-token"
	testAESKey = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG" // 43 chars, per the platform spec
	testCorpID = "ww1234567890"
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

func testChannel(t *testing.T, apiBase string) *Channel {
	t.Helper()
	c, err := New(Config{
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// sendEnvelope is the wrapper produced by EncryptMsg; the test borrows its
// Encrypt and MsgSignature to forge a callback body the callback can decrypt.
type sendEnvelope struct {
	XMLName      xml.Name `xml:"xml"`
	Encrypt      string   `xml:"Encrypt"`
	MsgSignature string   `xml:"MsgSignature"`
	TimeStamp    string   `xml:"TimeStamp"`
	Nonce        string   `xml:"Nonce"`
}

// freshCreateTime rewrites a forged callback's CreateTime to now: the
// adapter rejects timestamps outside a five-minute freshness window, so a
// hardcoded fixture date would be dropped before reaching the handler.
func freshCreateTime(innerXML string) string {
	return regexp.MustCompile(`<CreateTime>\d+</CreateTime>`).
		ReplaceAllString(innerXML, fmt.Sprintf("<CreateTime>%d</CreateTime>", time.Now().Unix()))
}

// forgeCallback encrypts innerXML the way the platform would and returns the
// callback body plus the query string carrying a valid signature.
func forgeCallback(t *testing.T, innerXML string) (body []byte, query string) {
	t.Helper()
	innerXML = freshCreateTime(innerXML)
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg(innerXML, "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}
	body = []byte(fmt.Sprintf(`<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[1000002]]></AgentID></xml>`,
		testCorpID, env.Encrypt))
	query = fmt.Sprintf("msg_signature=%s&timestamp=%s&nonce=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce)
	return body, query
}

func TestCallbackRoundTrip(t *testing.T) {
	c := testChannel(t, "")
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[你好]]></Content><MsgId>9876543210</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)

	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d, body %s", rec.Code, rec.Body.String())
	}
	if got.MsgID != "9876543210" || got.UserID != "zhangsan" || got.Text != "你好" {
		t.Fatalf("bad normalization: %+v", got)
	}
	if got.SessionKey != "dm:wecom:zhangsan" {
		t.Fatalf("unexpected session key: %q", got.SessionKey)
	}
	if got.WebhookPath != "/wecom/callback" {
		t.Fatalf("unexpected webhook path: %q", got.WebhookPath)
	}
}

func TestCallbackGroupChat(t *testing.T) {
	c := testChannel(t, "")
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[群里好]]></Content><MsgId>9876543211</MsgId><AgentID>1000002</AgentID><ChatId><![CDATA[roomA]]></ChatId></xml>`
	body, query := forgeCallback(t, inner)

	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d", rec.Code)
	}
	if got.ChatID != "roomA" || got.SessionKey != "group:wecom:roomA" {
		t.Fatalf("group message misrouted: %+v", got)
	}
}

func TestCallbackSkipsEventsAndBadSignatures(t *testing.T) {
	c := testChannel(t, "")
	called := false
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		called = true
		return channels.OutboundMessage{}, nil
	}))

	// Event callbacks (no MsgId) are acked and skipped.
	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[enter_agent]]></Event><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)
	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("event must be acked and skipped, status=%d called=%v", rec.Code, called)
	}

	// Tampered signature: acked (no redelivery), never handled.
	req = httptest.NewRequest(http.MethodPost, "/wecom/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(string(body)))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("bad signature must be acked and skipped, status=%d called=%v", rec.Code, called)
	}
}

// ErrDuplicate is a success outcome: the callback must answer 200 so the
// platform stops redelivering. A real failure stays 5xx — that is what makes
// the platform retry (and the gateway rolls the dedup key back).
func TestCallbackDuplicateVersusFailure(t *testing.T) {
	const inner = `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[你好]]></Content><MsgId>9876543212</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)

	post := func(t *testing.T, h channels.HandlerFunc) int {
		t.Helper()
		c := testChannel(t, "")
		mux := http.NewServeMux()
		c.RegisterRoutes(mux, h)
		req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, channels.ErrDuplicate
	}); code != http.StatusOK {
		t.Fatalf("duplicate must be answered 200, got %d", code)
	}

	if code := post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: boom")
	}); code != http.StatusInternalServerError {
		t.Fatalf("real failure must be answered 5xx so the IM retries, got %d", code)
	}
}

// Non-text callbacks without retrievable content (video/location/link) enter
// the pipeline as a placeholder text so the agent can tell the user the type
// is unsupported.
func TestCallbackNonTextPlaceholder(t *testing.T) {
	c := testChannel(t, "")
	var got []channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = append(got, msg)
		return channels.OutboundMessage{}, nil
	}))

	post := func(inner string) int {
		body, query := forgeCallback(t, inner)
		req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}

	video := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[video]]></MsgType><MediaId><![CDATA[MEDIA456]]></MediaId><MsgId>9876543220</MsgId><AgentID>1000002</AgentID></xml>`
	if code := post(video); code != http.StatusOK {
		t.Fatalf("video callback status = %d", code)
	}
	if len(got) != 1 || got[0].Text != "[视频]" || got[0].Type != channels.TypeText || got[0].MsgID != "9876543220" {
		t.Fatalf("video must become a placeholder text message: %+v", got)
	}

	// Unknown types (and placeholder types without a MsgId) stay acked and
	// skipped.
	unknown := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[emotion]]></MsgType><MsgId>9876543221</MsgId><AgentID>1000002</AgentID></xml>`
	if code := post(unknown); code != http.StatusOK || len(got) != 1 {
		t.Fatalf("unknown types must be acked and skipped, status=%d handled=%d", code, len(got))
	}
}

func TestVerifyURL(t *testing.T) {
	c := testChannel(t, "")
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

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/wecom/callback?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce, url.QueryEscape(env.Encrypt)), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "echo-challenge" {
		t.Fatalf("verify failed: status=%d body=%q", rec.Code, rec.Body.String())
	}
}

// fakeMediaStore captures SaveMedia calls.
type fakeMediaStore struct{ refs []string }

func (f *fakeMediaStore) SaveMedia(_ context.Context, channel, msgID, filename, mime string, data []byte) (string, error) {
	f.refs = append(f.refs, filename)
	return "s3://bucket/artifact/" + filename, nil
}

func TestCallbackRecall(t *testing.T) {
	c := testChannel(t, "")
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[revoke]]></Event><MsgId>555000111</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)
	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("recall callback status = %d", rec.Code)
	}
	if got.Type != channels.TypeRecall {
		t.Fatalf("want recall type, got %+v", got)
	}
	// The recall gets its own dedup namespace so it is never dropped as a
	// duplicate of the recalled message.
	if got.MsgID != "recall:555000111" {
		t.Fatalf("recall msg id must be namespaced, got %q", got.MsgID)
	}
}

func TestCallbackMedia(t *testing.T) {
	// Fake WeCom API: token + media download.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/cgi-bin/gettoken"):
			_, _ = w.Write([]byte(`{"access_token":"t-1","expires_in":7200}`))
		case strings.HasPrefix(r.URL.Path, "/cgi-bin/media/get"):
			w.Header().Set("Content-Type", "image/jpeg")
			_, _ = w.Write([]byte("jpeg-bytes"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()

	c, err := New(Config{
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: api.URL,
		Media: &fakeMediaStore{},
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
	if err != nil {
		t.Fatal(err)
	}
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[image]]></MsgType><PicUrl><![CDATA[https://wework.qpic.cn/x]]></PicUrl><MediaId><![CDATA[MEDIA123]]></MediaId><MsgId>777000222</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)
	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("media callback status = %d", rec.Code)
	}
	if got.Type != channels.TypeMedia {
		t.Fatalf("want media type, got %+v", got)
	}
	if !strings.HasPrefix(got.MediaRef, "s3://bucket/artifact/") || !strings.HasSuffix(got.MediaRef, ".jpg") {
		t.Fatalf("media must land in the artifact store with a reference: %q", got.MediaRef)
	}
	if !strings.Contains(got.Text, "图片") {
		t.Fatalf("media text must carry a placeholder: %q", got.Text)
	}
}

// Per-binding callbacks: each binding verifies under
// its own token/AES key, so binding2's handler must reject — with a 200 ack,
// never a 5xx retry storm — a callback encrypted for binding1, and the same
// inner message encrypted for binding2 must come through on its own handler.
func TestCallbackHandlerPerBinding(t *testing.T) {
	resolver := mapResolver{
		"tok": testToken, "aes": testAESKey, "secret": "corp-secret", // env default
		"tok-b2": testToken + "-b2", "aes-b2": testAESKey, // binding 2 creds
	}
	c, err := New(Config{
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret",
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}

	var got channels.InboundMessage
	recording := channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	})

	h1, err := c.CallbackHandler(recording, channels.BindingCredentials{
		BindingID: "b1", TokenRef: "tok", AESKeyRef: "aes",
	})
	if err != nil {
		t.Fatal(err)
	}

	// A binding-scoped callback with empty refs must be refused: falling back
	// to the env-global keys would let whoever holds them forge this
	// tenant's callbacks.
	if _, err := c.CallbackHandler(recording, channels.BindingCredentials{BindingID: "b0"}); err == nil {
		t.Fatal("binding-scoped handler must refuse empty credential refs")
	}
	h2, err := c.CallbackHandler(recording, channels.BindingCredentials{
		BindingID: "b2", TokenRef: "tok-b2", AESKeyRef: "aes-b2",
	})
	if err != nil {
		t.Fatal(err)
	}

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[绑定一]]></Content><MsgId>9876543222</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)

	// Binding1's handler decrypts its own callback.
	req := httptest.NewRequest(http.MethodPost, "/callback/wecom/b1?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	h1(rec, req)
	if rec.Code != http.StatusOK || got.Text != "绑定一" {
		t.Fatalf("binding1 callback must decrypt: status=%d got=%+v", rec.Code, got)
	}

	// Binding2's handler must not decrypt binding1's callback (wrong keys),
	// and must still ack 200 so the platform does not redeliver.
	got = channels.InboundMessage{}
	req = httptest.NewRequest(http.MethodPost, "/callback/wecom/b2?"+query, strings.NewReader(string(body)))
	rec = httptest.NewRecorder()
	h2(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cross-binding decrypt failure must ack 200, got %d", rec.Code)
	}
	if got.MsgID != "" {
		t.Fatalf("cross-binding callback must not enter the pipeline, got %+v", got)
	}

	// A callback encrypted under binding2's own keys reaches the pipeline.
	crypt2 := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken+"-b2", testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt2.EncryptMsg(freshCreateTime(inner), "1700000000", "nonce-2")
	if cerr != nil {
		t.Fatalf("encrypt b2: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}
	body2 := []byte(fmt.Sprintf(`<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[1000002]]></AgentID></xml>`, env.Encrypt))
	query2 := fmt.Sprintf("msg_signature=%s&timestamp=%s&nonce=%s", env.MsgSignature, env.TimeStamp, env.Nonce)
	req = httptest.NewRequest(http.MethodPost, "/callback/wecom/b2?"+query2, strings.NewReader(string(body2)))
	rec = httptest.NewRecorder()
	h2(rec, req)
	if rec.Code != http.StatusOK || got.Text != "绑定一" {
		t.Fatalf("binding2 callback must decrypt under its own keys: status=%d got=%+v", rec.Code, got)
	}
}
