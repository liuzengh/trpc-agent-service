package wecom_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- test vectors implement the WeCom protocol.
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway/openclaw"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
)

const (
	testToken     = "callback-token"
	testCorpID    = "ww-test-corp"
	testAgentID   = "1000002"
	testTimestamp = "1720000000"
	testNonce     = "nonce-1"
)

var testAESKey = []byte("0123456789abcdef0123456789abcdef")

type captureSubmitter struct {
	mu       sync.Mutex
	requests []gateway.RunRequest
}

type mediaDownloaderFunc func(context.Context, gateway.MediaReference) (channels.MediaDownload, error)

func (download mediaDownloaderFunc) Download(ctx context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
	return download(ctx, ref)
}

func TestControlledImageDownloadEntersRunnerWithoutProviderKey(t *testing.T) {
	crypt, _ := wecom.NewCrypt(testToken, strings.TrimSuffix(base64.StdEncoding.EncodeToString(testAESKey), "="), testCorpID)
	binding := wecom.Binding{TenantID: "tenant-a", AppID: "assistant", BindingID: "corp-a", CorpID: testCorpID, AgentID: testAgentID, ConfigVersion: 7, Crypt: crypt}
	submitter := &captureSubmitter{}
	core := &openclaw.Handler{Inbox: idempotency.NewMemoryStore(), Submitter: submitter, ClaimOwner: "gateway", ClaimTTL: time.Minute}
	adapter, err := wecom.NewDynamicHandlerWithMedia(core, func(string) []wecom.Binding { return []wecom.Binding{binding} }, func(wecom.Binding) channels.MediaDownloader {
		return mediaDownloaderFunc(func(_ context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
			if ref.Key != "private-media-key" {
				t.Fatalf("unexpected media reference")
			}
			png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0}
			return channels.MediaDownload{Body: io.NopCloser(bytes.NewReader(png)), ContentType: "image/png", Size: int64(len(png))}, nil
		})
	}, channels.MediaPolicy{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	plain := `<xml><ToUserName>` + testCorpID + `</ToUserName><FromUserName>alice</FromUserName><CreateTime>1720000000</CreateTime><MsgType>image</MsgType><MsgId>media-message</MsgId><AgentID>` + testAgentID + `</AgentID><MediaId>private-media-key</MediaId></xml>`
	encrypted := encryptTestMessage(t, plain, testCorpID)
	body := `<xml><Encrypt><![CDATA[` + encrypted + `]]></Encrypt></xml>`
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, httptest.NewRequest(http.MethodPost, callbackURL("corp-a", signature(testToken, testTimestamp, testNonce, encrypted), encrypted), strings.NewReader(body)))
	if response.Code != http.StatusOK || len(submitter.requests) != 1 {
		t.Fatalf("status=%d requests=%d", response.Code, len(submitter.requests))
	}
	request := submitter.requests[0]
	serialized, _ := json.Marshal(request)
	if len(request.Attachments) != 1 || request.Attachments[0].Kind != "image" || strings.Contains(string(serialized), "private-media-key") {
		t.Fatalf("unsafe durable request: %s", serialized)
	}
}

func (submitter *captureSubmitter) Submit(request gateway.RunRequest) error {
	submitter.mu.Lock()
	submitter.requests = append(submitter.requests, request)
	submitter.mu.Unlock()
	return nil
}

func TestEncryptedCallbackEntersDurableGatewayOnce(t *testing.T) {
	crypt, err := wecom.NewCrypt(testToken, strings.TrimSuffix(base64.StdEncoding.EncodeToString(testAESKey), "="), testCorpID)
	if err != nil {
		t.Fatal(err)
	}
	submitter := &captureSubmitter{}
	core := &openclaw.Handler{Inbox: idempotency.NewMemoryStore(), Submitter: submitter, ClaimOwner: "wecom-gateway", ClaimTTL: time.Minute}
	adapter, err := wecom.NewHandler(core, wecom.Binding{TenantID: "tenant-a", AppID: "assistant", BindingID: "corp-a", CorpID: testCorpID, AgentID: testAgentID, ConfigVersion: 7, Crypt: crypt})
	if err != nil {
		t.Fatal(err)
	}
	plain := `<xml><ToUserName><![CDATA[ww-test-corp]]></ToUserName><FromUserName><![CDATA[alice]]></FromUserName><CreateTime>1720000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hello agent]]></Content><MsgId>9001</MsgId><AgentID>1000002</AgentID></xml>`
	encrypted := encryptTestMessage(t, plain, testCorpID)
	body := `<xml><ToUserName><![CDATA[ww-test-corp]]></ToUserName><AgentID>1000002</AgentID><Encrypt><![CDATA[` + encrypted + `]]></Encrypt></xml>`
	endpoint := callbackURL("corp-a", signature(testToken, testTimestamp, testNonce, encrypted), encrypted)

	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "success" {
			t.Fatalf("attempt %d status=%d body=%q", i, response.Code, response.Body.String())
		}
	}

	submitter.mu.Lock()
	defer submitter.mu.Unlock()
	if len(submitter.requests) != 1 {
		t.Fatalf("submitted requests=%d, want 1", len(submitter.requests))
	}
	request := submitter.requests[0]
	if request.TenantID != "tenant-a" || request.AppID != "assistant" || request.BindingID != "corp-a" || request.ConfigVersion != 7 {
		t.Fatalf("scope=%+v", request)
	}
	if request.ExternalMessageID != "9001" || request.ExternalUserID != "alice" || request.UserID != "wecom/corp-a/alice" || request.SessionID != "dm/corp-a/alice" || request.Text != "hello agent" {
		t.Fatalf("normalized request=%+v", request)
	}
}

func TestOfficialWeComDecryptVector(t *testing.T) {
	const (
		token     = "QDG6eK"
		corpID    = "wx5823bf96d3bd56c7"
		aesKey    = "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"
		signature = "477715d11cdb4164915debcba66cb864d751f3e6"
		timestamp = "1409659813"
		nonce     = "1372623149"
		encrypted = "RypEvHKD8QQKFhvQ6QleEB4J58tiPdvo+rtK1I9qca6aM/wvqnLSV5zEPeusUiX5L5X/0lWfrf0QADHHhGd3QczcdCUpj911L3vg3W/sYYvuJTs3TUUkSUXxaccAS0qhxchrRYt66wiSpGLYL42aM6A8dTT+6k4aSknmPj48kzJs8qLjvd4Xgpue06DOdnLxAUHzM6+kDZ+HMZfJYuR+LtwGc2hgf5gsijff0ekUNXZiqATP7PF5mZxZ3Izoun1s4zG4LUMnvw2r+KqCKIw+3IQH03v+BCA9nMELNqbSf6tiWSrXJB3LAVGUcallcrw8V2t9EL4EhzJWrQUax5wLVMNS0+rUPA3k22Ncx4XXZS9o0MBH27Bo6BpNelZpS+/uh9KsNlY6bHCmJU9p8g7m3fVKn28H3KDYA5Pl/T8Z1ptDAVe0lXdQ2YoyyH2uyPIGHBZZIs2pDBS8R07+qN+E7Q=="
	)
	crypt, err := wecom.NewCrypt(token, aesKey, corpID)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := crypt.VerifyAndDecrypt(signature, timestamp, nonce, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"<FromUserName><![CDATA[mycreate]]>", "<Content><![CDATA[hello]]>", "<MsgId>4561255354251345929</MsgId>"} {
		if !strings.Contains(string(plain), want) {
			t.Fatalf("decrypted official vector missing %q: %s", want, plain)
		}
	}
}

func TestVerifyURLReturnsDecryptedEcho(t *testing.T) {
	crypt, err := wecom.NewCrypt(testToken, strings.TrimSuffix(base64.StdEncoding.EncodeToString(testAESKey), "="), testCorpID)
	if err != nil {
		t.Fatal(err)
	}
	core := &acceptorStub{}
	adapter, err := wecom.NewHandler(core, wecom.Binding{TenantID: "tenant-a", AppID: "app", BindingID: "corp-a", CorpID: testCorpID, ConfigVersion: 1, Crypt: crypt})
	if err != nil {
		t.Fatal(err)
	}
	encrypted := encryptTestMessage(t, "verified-echo", testCorpID)
	query := url.Values{"msg_signature": {signature(testToken, testTimestamp, testNonce, encrypted)}, "timestamp": {testTimestamp}, "nonce": {testNonce}, "echostr": {encrypted}}
	request := httptest.NewRequest(http.MethodGet, "/channels/wecom/corp-a?"+query.Encode(), nil)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "verified-echo" {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestRejectsBadSignatureAndBindingMismatch(t *testing.T) {
	crypt, _ := wecom.NewCrypt(testToken, strings.TrimSuffix(base64.StdEncoding.EncodeToString(testAESKey), "="), testCorpID)
	adapter, _ := wecom.NewHandler(&acceptorStub{}, wecom.Binding{TenantID: "tenant-a", AppID: "app", BindingID: "corp-a", CorpID: testCorpID, AgentID: testAgentID, ConfigVersion: 1, Crypt: crypt})
	plain := `<xml><ToUserName>other-corp</ToUserName><FromUserName>alice</FromUserName><CreateTime>1720000000</CreateTime><MsgType>text</MsgType><Content>hello</Content><MsgId>1</MsgId><AgentID>1000002</AgentID></xml>`
	encrypted := encryptTestMessage(t, plain, testCorpID)
	body := `<xml><Encrypt><![CDATA[` + encrypted + `]]></Encrypt></xml>`

	bad := httptest.NewRecorder()
	adapter.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, callbackURL("corp-a", "bad", encrypted), strings.NewReader(body)))
	if bad.Code != http.StatusUnauthorized {
		t.Fatalf("bad signature status=%d", bad.Code)
	}
	mismatch := httptest.NewRecorder()
	adapter.ServeHTTP(mismatch, httptest.NewRequest(http.MethodPost, callbackURL("corp-a", signature(testToken, testTimestamp, testNonce, encrypted), encrypted), strings.NewReader(body)))
	if mismatch.Code != http.StatusUnauthorized {
		t.Fatalf("binding mismatch status=%d", mismatch.Code)
	}
}

func TestDynamicBindingDisambiguatesTenantAndFailsClosed(t *testing.T) {
	aesKey := strings.TrimSuffix(base64.StdEncoding.EncodeToString(testAESKey), "=")
	cryptA, _ := wecom.NewCrypt("tenant-a-token", aesKey, testCorpID)
	cryptB, _ := wecom.NewCrypt("tenant-b-token", aesKey, testCorpID)
	bindings := []wecom.Binding{
		{TenantID: "tenant-a", AppID: "app", BindingID: "shared", CorpID: testCorpID, AgentID: testAgentID, ConfigVersion: 1, Crypt: cryptA},
		{TenantID: "tenant-b", AppID: "app", BindingID: "shared", CorpID: testCorpID, AgentID: testAgentID, ConfigVersion: 1, Crypt: cryptB},
	}
	capture := &capturingAcceptor{}
	adapter, err := wecom.NewDynamicHandler(capture, func(string) []wecom.Binding { return bindings })
	if err != nil {
		t.Fatal(err)
	}
	plain := `<xml><ToUserName><![CDATA[ww-test-corp]]></ToUserName><FromUserName><![CDATA[same-user]]></FromUserName><CreateTime>1720000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[synthetic]]></Content><MsgId>same-message</MsgId><AgentID>1000002</AgentID></xml>`
	encrypted := encryptTestMessage(t, plain, testCorpID)
	body := `<xml><Encrypt><![CDATA[` + encrypted + `]]></Encrypt></xml>`
	request := httptest.NewRequest(http.MethodPost, callbackURL("shared", signature("tenant-b-token", testTimestamp, testNonce, encrypted), encrypted), strings.NewReader(body))
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(capture.messages) != 1 || capture.messages[0].TenantID != "tenant-b" {
		t.Fatalf("status=%d accepted=%d", response.Code, len(capture.messages))
	}

	// Identical server-owned credentials cannot prove which tenant owns the
	// callback, so the adapter must reject instead of using database row order.
	capture.messages = nil
	bindings[0].Crypt = cryptB
	ambiguous := httptest.NewRecorder()
	adapter.ServeHTTP(ambiguous, httptest.NewRequest(http.MethodPost, callbackURL("shared", signature("tenant-b-token", testTimestamp, testNonce, encrypted), encrypted), strings.NewReader(body)))
	if ambiguous.Code != http.StatusUnauthorized || len(capture.messages) != 0 {
		t.Fatalf("ambiguous status=%d accepted=%d", ambiguous.Code, len(capture.messages))
	}
}

type acceptorStub struct{}

func (*acceptorStub) AcceptInbound(_ context.Context, message gateway.InboundMessage) (gateway.AcceptedMessage, error) {
	return gateway.AcceptedMessage{RequestID: message.ExternalMessageID, SessionID: message.SessionID, TraceID: message.TraceID}, nil
}

type capturingAcceptor struct{ messages []gateway.InboundMessage }

func (acceptor *capturingAcceptor) AcceptInbound(_ context.Context, message gateway.InboundMessage) (gateway.AcceptedMessage, error) {
	acceptor.messages = append(acceptor.messages, message)
	return gateway.AcceptedMessage{RequestID: message.ExternalMessageID}, nil
}

func callbackURL(bindingID, messageSignature, _ string) string {
	query := url.Values{"msg_signature": {messageSignature}, "timestamp": {testTimestamp}, "nonce": {testNonce}}
	return "/channels/wecom/" + bindingID + "?" + query.Encode()
}

func signature(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	sum := sha1.Sum([]byte(strings.Join(values, ""))) // #nosec G401 -- protocol requirement.
	return hex.EncodeToString(sum[:])
}

func encryptTestMessage(t *testing.T, message, receiveID string) string {
	t.Helper()
	plain := make([]byte, 20+len(message)+len(receiveID))
	copy(plain[:16], []byte("0123456789abcdef"))
	binary.BigEndian.PutUint32(plain[16:20], uint32(len(message)))
	copy(plain[20:], message)
	copy(plain[20+len(message):], receiveID)
	padding := 32 - len(plain)%32
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(testAESKey)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, testAESKey[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	return base64.StdEncoding.EncodeToString(encrypted)
}
