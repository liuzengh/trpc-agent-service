package channels

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

// wecomFixture builds signed+encrypted WeCom callbacks the same way the
// provider does, so the adapter is verified against real wire format.
type wecomFixture struct {
	aesKey      string
	token       string
	corpid      string
	agentID     string
	now         time.Time
	timestamp   string
	nonce       string
	cipher      *wecomCipher
	tokenHits   int
	mu          sync.Mutex
	accessToken string
	sendHits    int
	sendPayload []map[string]any
	server      *httptest.Server
}

func newWeComFixture(t *testing.T) *wecomFixture {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	aesKey := base64.StdEncoding.EncodeToString(raw)[:43]
	cipherInstance, err := newWeComCipher(aesKey)
	if err != nil {
		t.Fatal(err)
	}
	f := &wecomFixture{
		aesKey: aesKey, token: "callback-token", corpid: "wwCorpId123",
		agentID: "1000002", now: time.Unix(1_700_000_000, 0),
		nonce: "n0nce", cipher: cipherInstance, accessToken: "cached-access-token",
	}
	f.timestamp = fmt.Sprintf("%d", f.now.Unix())
	return f
}

func (f *wecomFixture) binding() config.ChannelConfig {
	return config.ChannelConfig{
		Type: "wecom", BindingID: "acme-wecom",
		TokenEnv: "WECOM_CORP_SECRET", SigningSecretEnv: "WECOM_CALLBACK_TOKEN",
		EncryptionKeyEnv: "WECOM_AES_KEY",
		WorkspaceID:      f.corpid, ApplicationID: f.agentID,
		MaxMessageLength: 2048,
	}
}

func (f *wecomFixture) env(t *testing.T) {
	t.Helper()
	t.Setenv("WECOM_CORP_SECRET", "corp-secret-value")
	t.Setenv("WECOM_CALLBACK_TOKEN", f.token)
	t.Setenv("WECOM_AES_KEY", f.aesKey)
}

func (f *wecomFixture) encrypt(t *testing.T, plaintext string) string {
	t.Helper()
	ciphertext, err := f.cipher.encrypt([]byte(plaintext), []byte(f.corpid))
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(ciphertext)
}

func (f *wecomFixture) sign(encrypted string) string {
	return wecomSignature(f.token, f.timestamp, f.nonce, encrypted)
}

// callback builds a POST body and signature for a plaintext XML message.
func (f *wecomFixture) callback(t *testing.T, plaintextXML string) ([]byte, string) {
	t.Helper()
	encrypted := f.encrypt(t, plaintextXML)
	body, err := xml.Marshal(struct {
		XMLName struct{} `xml:"xml"`
		Encrypt string   `xml:"Encrypt"`
	}{Encrypt: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	return body, f.sign(encrypted)
}

// protocolCiphertext is intentionally independent of wecomCipher.encrypt.
// It models the provider's documented 32-byte PKCS#7 padding rule so a local
// round trip cannot make the implementation and fixture wrong in the same way.
func protocolCiphertext(t *testing.T, key, message, receiveID []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	plain := make([]byte, 16, 20+len(message)+len(receiveID)+wecomPKCS7BlockSize)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(message)))
	plain = append(plain, length...)
	plain = append(plain, message...)
	plain = append(plain, receiveID...)
	padding := wecomPKCS7BlockSize - len(plain)%wecomPKCS7BlockSize
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	return encrypted
}

func TestWeComCipherUsesProtocol32BytePadding(t *testing.T) {
	f := newWeComFixture(t)
	// 20-byte prefix + 1-byte message + 11-byte receive ID is already 32,
	// therefore the protocol adds a full 32-byte padding block. The previous
	// AES-block-sized implementation rejected that padding value.
	ciphertext := protocolCiphertext(t, f.cipher.key, []byte("x"), []byte(f.corpid))
	if len(ciphertext) != 64 {
		t.Fatalf("protocol ciphertext length = %d, want 64", len(ciphertext))
	}
	message, receiveID, err := f.cipher.decrypt(ciphertext)
	if err != nil {
		t.Fatalf("decrypt provider-compatible ciphertext: %v", err)
	}
	if string(message) != "x" || string(receiveID) != f.corpid {
		t.Fatalf("decrypted message=%q receiveID=%q", message, receiveID)
	}
	generated, err := f.cipher.encrypt([]byte("x"), []byte(f.corpid))
	if err != nil {
		t.Fatal(err)
	}
	if len(generated)%wecomPKCS7BlockSize != 0 {
		t.Fatalf("generated ciphertext length %d is not a 32-byte multiple", len(generated))
	}
}

func (f *wecomFixture) postRequest(t *testing.T, body []byte, signature string) *http.Request {
	t.Helper()
	req := httptest.NewRequest("POST", "/webhooks/wecom/acme-wecom?msg_signature="+signature+"&timestamp="+f.timestamp+"&nonce="+f.nonce, bytes.NewReader(body))
	return req
}

func TestWeComVerifyParseDirectMessage(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	adapter := NewWeCom(nil)
	adapter.now = func() time.Time { return f.now }

	plain := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>zhangsan</FromUserName><CreateTime>1700000000</CreateTime><MsgType>text</MsgType><Content>你好</Content><MsgId>7758</MsgId><AgentID>1000002</AgentID></xml>`
	body, signature := f.callback(t, plain)
	req := f.postRequest(t, body, signature)
	if err := adapter.Verify(req, body, f.binding()); err != nil {
		t.Fatal(err)
	}
	parsed, err := adapter.Parse(body, f.binding())
	if err != nil {
		t.Fatal(err)
	}
	msg := parsed.Messages[0]
	if msg.Scope != domain.ScopeDirect || msg.ExternalUserID != "zhangsan" ||
		msg.ExternalMessageID != "7758" || msg.ReplyTarget != "zhangsan" || msg.Text != "你好" {
		t.Fatalf("unexpected normalized wecom message: %+v", msg)
	}

	// Tampered signature must be rejected before any parsing.
	if err := adapter.Verify(f.postRequest(t, body, "deadbeef"+signature), body, f.binding()); err != ErrInvalidSignature {
		t.Fatalf("expected signature rejection, got %v", err)
	}
	// Stale timestamp outside the replay window must be rejected.
	stale := NewWeCom(nil)
	stale.now = func() time.Time { return f.now.Add(6 * time.Minute) }
	if err := stale.Verify(req, body, f.binding()); err != ErrInvalidSignature {
		t.Fatalf("expected stale request rejection, got %v", err)
	}
}

func TestWeComParseGroupChatAndBindingChecks(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	adapter := NewWeCom(nil)
	adapter.now = func() time.Time { return f.now }

	groupPlain := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>lisi</FromUserName><CreateTime>1700000001</CreateTime><MsgType>text</MsgType><Content>群消息</Content><MsgId>7759</MsgId><AgentID>1000002</AgentID><ChatId>wrGroupChat1</ChatId></xml>`
	body, _ := f.callback(t, groupPlain)
	parsed, err := adapter.Parse(body, f.binding())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Messages[0].Scope != domain.ScopeGroup || parsed.Messages[0].ReplyTarget != "wrGroupChat1" ||
		parsed.Messages[0].ConversationID != "wrGroupChat1" {
		t.Fatalf("group chat was not normalized: %+v", parsed.Messages[0])
	}

	// Same corpid but different agentid must not cross route into this binding.
	otherApp := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>wangwu</FromUserName><CreateTime>1700000002</CreateTime><MsgType>text</MsgType><Content>hi</Content><MsgId>7760</MsgId><AgentID>1000099</AgentID></xml>`
	otherBody, _ := f.callback(t, otherApp)
	if _, err := adapter.Parse(otherBody, f.binding()); err != ErrBindingMismatch {
		t.Fatalf("expected agentid binding mismatch, got %v", err)
	}
	// Missing AgentID must not bypass application-level binding when one is
	// configured for this callback.
	missingApp := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>wangwu</FromUserName><CreateTime>1700000002</CreateTime><MsgType>text</MsgType><Content>hi</Content><MsgId>7761</MsgId></xml>`
	missingAppBody, _ := f.callback(t, missingApp)
	if _, err := adapter.Parse(missingAppBody, f.binding()); err != ErrBindingMismatch {
		t.Fatalf("expected missing agentid binding mismatch, got %v", err)
	}

	// Event callbacks (e.g. subscribe) are ignored without an error.
	event := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>zhangsan</FromUserName><CreateTime>1700000003</CreateTime><MsgType>event</MsgType><Event>subscribe</Event><AgentID>1000002</AgentID></xml>`
	eventBody, _ := f.callback(t, event)
	if _, err := adapter.Parse(eventBody, f.binding()); err != ErrUnsupportedEvent {
		t.Fatalf("expected unsupported event, got %v", err)
	}
}

func TestWeComParsesTopLevelMediaFields(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	adapter := NewWeCom(nil)
	image := `<xml><ToUserName>` + f.corpid + `</ToUserName><FromUserName>zhangsan</FromUserName><CreateTime>1700000004</CreateTime><MsgType>image</MsgType><PicUrl>https://example.invalid/p.jpg</PicUrl><MediaId>media-image-1</MediaId><MsgId>7762</MsgId><AgentID>1000002</AgentID></xml>`
	body, _ := f.callback(t, image)
	parsed, err := adapter.Parse(body, f.binding())
	if err != nil {
		t.Fatal(err)
	}
	attachments := parsed.Messages[0].Attachments
	if len(attachments) != 1 || attachments[0].Type != "image" || attachments[0].FileID != "media-image-1" {
		t.Fatalf("top-level media fields not normalized: %+v", attachments)
	}
}

func TestWeComURLVerification(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	adapter := NewWeCom(nil)
	adapter.now = func() time.Time { return f.now }

	echoPlain := f.encrypt(t, "random-echo-plaintext")
	signature := f.sign(echoPlain)
	encodedEcho := url.QueryEscape(echoPlain)
	req := httptest.NewRequest("GET", "/webhooks/wecom/acme-wecom?msg_signature="+signature+"&timestamp="+f.timestamp+"&nonce="+f.nonce+"&echostr="+encodedEcho, nil)
	echo, err := adapter.VerifyURL(req, f.binding())
	if err != nil {
		t.Fatal(err)
	}
	if echo != "random-echo-plaintext" {
		t.Fatalf("unexpected echo plaintext: %q", echo)
	}

	badSig := httptest.NewRequest("GET", "/webhooks/wecom/acme-wecom?msg_signature=tampered&timestamp="+f.timestamp+"&nonce="+f.nonce+"&echostr="+encodedEcho, nil)
	if _, err := adapter.VerifyURL(badSig, f.binding()); err != ErrInvalidSignature {
		t.Fatalf("expected URL verification rejection, got %v", err)
	}

	// A decrypted payload for another corpid must not be accepted.
	foreign := &wecomFixture{aesKey: f.aesKey, token: f.token, corpid: "wwOtherCorp", cipher: f.cipher, now: f.now, nonce: f.nonce}
	foreign.timestamp = f.timestamp
	foreignEcho := foreign.encrypt(t, "foreign-echo")
	foreignSig := wecomSignature(f.token, f.timestamp, f.nonce, foreignEcho)
	foreignReq := httptest.NewRequest("GET", "/webhooks/wecom/acme-wecom?msg_signature="+foreignSig+"&timestamp="+f.timestamp+"&nonce="+f.nonce+"&echostr="+url.QueryEscape(foreignEcho), nil)
	if _, err := adapter.VerifyURL(foreignReq, f.binding()); err != ErrBindingMismatch {
		t.Fatalf("expected corpid binding mismatch, got %v", err)
	}
}

type wecomDoer struct {
	fixture *wecomFixture
}

type wecomOutcomeDoer struct {
	sendStatus int
	sendBody   string
	sendHeader http.Header
	sendErr    error
	sendCalls  int
}

func (d *wecomOutcomeDoer) Do(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/cgi-bin/gettoken" {
		return wecomJSONResponse(`{"errcode":0,"access_token":"fresh-token","expires_in":7200}`), nil
	}
	d.sendCalls++
	if d.sendErr != nil {
		return nil, d.sendErr
	}
	status := d.sendStatus
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(d.sendBody)),
		Header:     d.sendHeader.Clone(),
	}, nil
}

func wecomJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func (d *wecomDoer) Do(req *http.Request) (*http.Response, error) {
	path := req.URL.Path
	if path == "/cgi-bin/gettoken" {
		d.fixture.mu.Lock()
		d.fixture.tokenHits++
		d.fixture.mu.Unlock()
		return wecomJSONResponse(`{"errcode":0,"errmsg":"ok","access_token":"` + d.fixture.accessToken + `","expires_in":7200}`), nil
	}
	d.fixture.mu.Lock()
	defer d.fixture.mu.Unlock()
	d.fixture.sendHits++
	payload := map[string]any{}
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(req.Body)
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		return nil, err
	}
	d.fixture.sendPayload = append(d.fixture.sendPayload, payload)
	if req.URL.Query().Get("access_token") != d.fixture.accessToken {
		return wecomJSONResponse(`{"errcode":42001,"errmsg":"access token expired"}`), nil
	}
	return wecomJSONResponse(`{"errcode":0,"errmsg":"ok"}`), nil
}

func TestWeComPlanAndDeliverCachesTokenSplitsBytesAndRoutesByScope(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	doer := &wecomDoer{fixture: f}
	adapter := NewWeCom(doer)
	adapter.now = func() time.Time { return f.now }

	// Chinese characters are 3 bytes each; 700 chars = 2100 bytes > 2048.
	// The 2048-byte cut lands mid-rune, so part one backs off to 2046 bytes.
	long := strings.Repeat("好", 700)
	parts, err := adapter.Plan(f.binding(), domain.OutboundMessage{
		Target: "zhangsan", Scope: domain.ScopeDirect, Text: long,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || len(parts[0].Message.Text) != 2046 {
		t.Fatalf("unexpected WeCom plan: %+v", parts)
	}
	for _, part := range parts {
		result := adapter.Deliver(context.Background(), f.binding(), delivery.Request{
			OperationKey: fmt.Sprintf("direct-part-%d", part.Index), AttemptNo: 1, Message: part.Message,
		})
		if result.Outcome != delivery.Confirmed {
			t.Fatalf("direct part %d: %+v", part.Index, result)
		}
	}

	// Group replies go through appchat/send with chatid only.
	result := adapter.Deliver(context.Background(), f.binding(), delivery.Request{
		OperationKey: "group-part-0", AttemptNo: 1,
		Message: domain.OutboundMessage{Target: "wrGroupChat1", Scope: domain.ScopeGroup, Text: "群回复"},
	})
	if result.Outcome != delivery.Confirmed {
		t.Fatalf("group delivery: %+v", result)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sendPayload) != 3 {
		t.Fatalf("expected 2 byte-limited parts plus 1 group reply, got %d sends", len(f.sendPayload))
	}
	if got := len(f.sendPayload[0]["text"].(map[string]any)["content"].(string)); got != 2046 {
		t.Fatalf("first part should stop at rune boundary 2046 bytes, got %d", got)
	}
	if f.sendPayload[0]["touser"] != "zhangsan" || f.sendPayload[0]["agentid"].(float64) != 1000002 {
		t.Fatalf("direct payload routing wrong: %#v", f.sendPayload[0])
	}
	if _, hasChat := f.sendPayload[0]["chatid"]; hasChat {
		t.Fatalf("direct message must not use chatid: %#v", f.sendPayload[0])
	}
	last := f.sendPayload[len(f.sendPayload)-1]
	if last["chatid"] != "wrGroupChat1" || last["agentid"] != nil {
		t.Fatalf("group payload routing wrong: %#v", last)
	}
	if f.tokenHits != 1 {
		t.Fatalf("access token should be fetched once and reused, got %d fetches", f.tokenHits)
	}
}

func TestWeComDeliverRetriesOnceOnExpiredTokenWithoutLeakingSecret(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	f.accessToken = "rotated-token-value"
	doer := &wecomDoer{fixture: f}
	// Simulate a stale cached token by pre-seeding the adapter cache with the
	// old value, then expiring it on first send.
	adapter := NewWeCom(doer)
	adapter.now = func() time.Time { return f.now }
	adapter.tokens[f.corpid+"\x1fWECOM_CORP_SECRET"] = wecomTokenEntry{
		accessToken: "stale-token", expiresAt: f.now.Add(time.Hour),
	}
	// The doer rejects the stale token once; after invalidation the fresh one
	// is accepted.
	result := adapter.Deliver(context.Background(), f.binding(), delivery.Request{
		OperationKey: "rotation-part-0", AttemptNo: 1,
		Message: domain.OutboundMessage{Target: "zhangsan", Scope: domain.ScopeDirect, Text: "after rotation"},
	})
	if result.Outcome != delivery.Confirmed {
		t.Fatalf("delivery result: %+v", result)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendHits != 2 {
		t.Fatalf("expected stale-token retry, send hits = %d", f.sendHits)
	}
	if f.sendPayload[0]["touser"] != "zhangsan" || f.sendPayload[1]["touser"] != "zhangsan" {
		t.Fatalf("retry must resend the same part: %#v", f.sendPayload)
	}
}

func TestWeComDeliveryErrorNeverLeaksSecrets(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	result := NewWeCom(leakingErrorDoer{}).Deliver(context.Background(), f.binding(), delivery.Request{
		OperationKey: "secret-part-0", AttemptNo: 1,
		Message: domain.OutboundMessage{Target: "zhangsan", Scope: domain.ScopeDirect, Text: "hello"},
	})
	if result.Outcome != delivery.RetryableNotSent || result.Err == nil {
		t.Fatalf("expected safe token preflight retry, got %+v", result)
	}
	for _, secret := range []string{os.Getenv("WECOM_CORP_SECRET"), f.aesKey, f.token} {
		if secret != "" && strings.Contains(result.Err.Error(), secret) {
			t.Fatalf("secret leaked in error: %v", result.Err)
		}
	}
}

func TestChunksUTF8BytesSplitsOnRuneBoundary(t *testing.T) {
	got, err := chunksUTF8Bytes("你好世界", 6) // 2 runes per chunk
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "你好" || got[1] != "世界" {
		t.Fatalf("unexpected byte chunks: %#v", got)
	}
	if got, err := chunksUTF8Bytes("abc", 6); err != nil || len(got) != 1 || got[0] != "abc" {
		t.Fatalf("short text must stay whole: %#v", got)
	}
	if got, err := chunksUTF8Bytes("好", 2); err == nil || got != nil {
		t.Fatalf("impossible byte limit must fail rather than split UTF-8: got=%#v err=%v", got, err)
	}
}

func TestWeComStructuredDeliveryOutcomes(t *testing.T) {
	f := newWeComFixture(t)
	f.env(t)
	request := delivery.Request{
		OperationKey: "wecom-op", AttemptNo: 1,
		Message: domain.OutboundMessage{Target: "zhangsan", Scope: domain.ScopeDirect, Text: "hello"},
	}
	tests := []struct {
		name       string
		doer       *wecomOutcomeDoer
		outcome    delivery.Outcome
		code       string
		retryAfter time.Duration
		messageID  string
		requestID  string
	}{
		{
			name: "confirmed", doer: &wecomOutcomeDoer{
				sendBody:   `{"errcode":0,"errmsg":"ok","msgid":"wecom-message-1"}`,
				sendHeader: http.Header{"X-Request-Id": []string{"wecom-request-1"}},
			}, outcome: delivery.Confirmed, messageID: "wecom-message-1", requestID: "wecom-request-1",
		},
		{
			name: "http rate limited", doer: &wecomOutcomeDoer{
				sendStatus: http.StatusTooManyRequests,
				sendBody:   `{"errcode":45009,"errmsg":"rate limited"}`,
				sendHeader: http.Header{"Retry-After": []string{"11"}},
			}, outcome: delivery.RetryableNotSent, code: "45009", retryAfter: 11 * time.Second,
		},
		{
			name: "provider transient", doer: &wecomOutcomeDoer{sendBody: `{"errcode":-1,"errmsg":"busy"}`},
			outcome: delivery.RetryableNotSent, code: "-1",
		},
		{
			name: "permanent rejection", doer: &wecomOutcomeDoer{sendBody: `{"errcode":40003,"errmsg":"invalid user"}`},
			outcome: delivery.PermanentRejected, code: "40003",
		},
		{
			name: "malformed success", doer: &wecomOutcomeDoer{sendBody: `{bad-json`}, outcome: delivery.Unknown,
		},
		{
			name: "send transport unknown", doer: &wecomOutcomeDoer{sendErr: errors.New("connection reset")}, outcome: delivery.Unknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewWeCom(test.doer)
			adapter.now = func() time.Time { return f.now }
			result := adapter.Deliver(context.Background(), f.binding(), request)
			if result.Outcome != test.outcome || result.ProviderCode != test.code || result.RetryAfter != test.retryAfter ||
				result.ProviderMessageID != test.messageID || result.ProviderRequestID != test.requestID {
				t.Fatalf("result = %+v", result)
			}
			if test.doer.sendErr == nil && len(result.ResponseHash) != 64 {
				t.Fatalf("response hash = %q", result.ResponseHash)
			}
			if test.doer.sendCalls != 1 {
				t.Fatalf("send calls = %d, want exactly one", test.doer.sendCalls)
			}
		})
	}
}
