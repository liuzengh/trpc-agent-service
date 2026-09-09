package wecom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// postForged forges one encrypted callback for innerXML and serves it on a
// binding-scoped handler; the recorder carries the answer.
func postForged(t *testing.T, c *Channel, h channels.Handler, innerXML string) *httptest.ResponseRecorder {
	t.Helper()
	body, query := forgeCallback(t, innerXML)
	return postRaw(t, c, h, body, query)
}

// postRaw serves one callback body under the test binding's credentials.
func postRaw(t *testing.T, c *Channel, h channels.Handler, body []byte, query string) *httptest.ResponseRecorder {
	t.Helper()
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{
		CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/callback/wecom/b1?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// craftCallback encrypts an arbitrary buffer — not the random+length+message+
// corpid layout EncryptMsg builds — and signs it correctly. That combination is
// what production produces when our AES key is wrong or a ciphertext is
// truncated: the signature verifies, so the callback is provably the platform's,
// and the plaintext behind it is unreadable.
func craftCallback(t *testing.T, plaintext string) (body []byte, query string) {
	t.Helper()
	aeskey, err := base64.StdEncoding.DecodeString(testAESKey + "=")
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(aeskey)
	if err != nil {
		t.Fatal(err)
	}
	// The library's own PKCS7: pad to its 32-byte block, always adding at
	// least one byte.
	const blockSize = 32
	pad := blockSize - len(plaintext)%blockSize
	padded := append([]byte(plaintext), make([]byte, pad)...)
	for i := len(padded) - pad; i < len(padded); i++ {
		padded[i] = byte(pad)
	}
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, aeskey[:aes.BlockSize]).CryptBlocks(ciphertext, padded)
	encrypt := base64.StdEncoding.EncodeToString(ciphertext)

	const timestamp, nonce = "1700000000", "nonce-1"
	signature := sha1Hex(testToken, timestamp, nonce, encrypt)
	body = []byte(fmt.Sprintf(`<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[1000002]]></AgentID></xml>`,
		testCorpID, encrypt))
	query = fmt.Sprintf("msg_signature=%s&timestamp=%s&nonce=%s",
		url.QueryEscape(signature), timestamp, nonce)
	return body, query
}

// sha1Hex is the platform's signature rule: sort the four parts, concatenate,
// sha1, lowercase hex.
func sha1Hex(parts ...string) string {
	sorted := append([]string(nil), parts...)
	sort.Strings(sorted)
	sha := sha1.New()
	for _, p := range sorted {
		sha.Write([]byte(p))
	}
	return fmt.Sprintf("%x", sha.Sum(nil))
}

// overflowPlaintext is a decrypted buffer whose declared message length is
// 0xFFFFFFFF: 20+msg_len wraps to 19 in uint32, so a guard written as
// "text_len < 20+msg_len" passes and the slice expression panics on inverted
// bounds.
func overflowPlaintext() string {
	buf := binary.BigEndian.AppendUint32([]byte("0123456789abcdef"), 0xFFFFFFFF)
	return string(append(buf, []byte("hello"+testCorpID)...))
}

const mediaInner = `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[image]]></MsgType><PicUrl><![CDATA[https://wework.qpic.cn/x]]></PicUrl><MediaId><![CDATA[MEDIA123]]></MediaId><MsgId>777000333</MsgId><AgentID>1000002</AgentID></xml>`

func TestNewValidation(t *testing.T) {
	resolver := mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing corpid", Config{AgentID: 1, TokenRef: "tok", AESKeyRef: "aes"}, "CorpID and AgentID are required"},
		{"missing agentid", Config{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"}, "CorpID and AgentID are required"},
		{"unresolvable token", Config{CorpID: testCorpID, AgentID: 1, TokenRef: "nope", AESKeyRef: "aes"}, "resolve token"},
		{"unresolvable aes key", Config{CorpID: testCorpID, AgentID: 1, TokenRef: "tok", AESKeyRef: "nope"}, "resolve aes key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg, resolver); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCryptFor(t *testing.T) {
	c := testChannel(t, "")

	// Empty refs fall back to the env single-binding default, and the same
	// resolved credential set must be served from the cache on the second call.
	first, err := c.cryptFor("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.cryptFor("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("the same resolved credentials must share one cached crypt")
	}

	if _, err := c.cryptFor("corp-x", "nope", "aes"); err == nil || !strings.Contains(err.Error(), "resolve token") {
		t.Fatalf("unresolvable token ref must error, got %v", err)
	}
	if _, err := c.cryptFor("corp-x", "tok", "nope"); err == nil || !strings.Contains(err.Error(), "resolve aes key") {
		t.Fatalf("unresolvable aes ref must error, got %v", err)
	}

	// The value-keyed cache resets once full, so key rotation cannot grow it.
	for i := 0; i < maxCryptCacheEntries+3; i++ {
		if _, err := c.cryptFor(fmt.Sprintf("corp-%d", i), "tok", "aes"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(c.crypts); got > maxCryptCacheEntries {
		t.Fatalf("crypt cache must stay bounded, got %d entries", got)
	}
}

// RegisterRoutes is defensive against a misconfigured channel: a broken
// credential set must mount nothing so the path 404s instead of half-serving.
func TestRegisterRoutesMountFailure(t *testing.T) {
	c := &Channel{crypts: map[string]*wxbizmsgcrypt.WXBizMsgCrypt{}}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run on an unmounted path")
		return channels.OutboundMessage{}, nil
	}))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/wecom/callback", strings.NewReader("<xml/>")))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("broken channel must mount nothing (404), got %d", rec.Code)
	}
}

func TestCallbackHandlerRejectsBadCredentials(t *testing.T) {
	c := testChannel(t, "")
	h := channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, nil
	})
	if _, err := c.CallbackHandler(h, channels.BindingCredentials{BindingID: "b0"}); err == nil {
		t.Fatal("binding-scoped handler must refuse empty credential refs")
	}
	if _, err := c.CallbackHandler(h, channels.BindingCredentials{BindingID: "b0", TokenRef: "nope", AESKeyRef: "aes"}); err == nil {
		t.Fatal("binding-scoped handler must refuse unresolvable refs")
	}
}

func TestCallbackMethodNotAllowed(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for other methods")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPut, "/callback/wecom/b1", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT must be rejected 405, got %d", rec.Code)
	}
}

func TestVerifyURLRejectsBadSignature(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for a failed verification")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/callback/wecom/b1?msg_signature=tampered&timestamp=1700000000&nonce=n&echostr=x", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bad signature must be rejected 403, got %d", rec.Code)
	}
}

// A callback whose body cannot be read (over the 1 MiB cap) is a bad request,
// not an ack: the platform must redeliver a truncated callback.
func TestReceiveOversizedBody(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unreadable body")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", (1<<20)+64)
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/callback/wecom/b1", strings.NewReader(big)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized body must answer 400, got %d", rec.Code)
	}
}

// A decryptable callback carrying garbage XML is acked (no redelivery) and
// never enters the pipeline.
func TestReceiveUnparsableXML(t *testing.T) {
	c := testChannel(t, "")
	rec := postForged(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for unparsable xml")
		return channels.OutboundMessage{}, nil
	}), `not-xml<<<`)
	if rec.Code != http.StatusOK || rec.Body.String() != "success" {
		t.Fatalf("unparsable xml must be acked, got %d %q", rec.Code, rec.Body.String())
	}
}

// A callback that never proved it came from the platform is junk — a bad
// signature or an envelope that is not XML. It stays acked: redelivery cannot
// make it readable, and a 5xx would let any scanner turn this public endpoint
// into an error-rate firehose.
func TestReceiveUnverifiedCallbackIsAcked(t *testing.T) {
	c := testChannel(t, "")
	rec := postRaw(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unverified callback")
		return channels.OutboundMessage{}, nil
	}), []byte(`<xml><Encrypt><![CDATA[abcd]]></Encrypt></xml>`),
		"msg_signature=tampered&timestamp=1700000000&nonce=n")
	if rec.Code != http.StatusOK || rec.Body.String() != "success" {
		t.Fatalf("an unverified callback must be acked, got %d %q", rec.Code, rec.Body.String())
	}
}

// A callback whose signature verifies is provably the platform's, so failing to
// read it is our fault: it must answer 5xx so the platform redelivers and a
// fixed credential recovers the backlog.
func TestReceiveAuthenticatedButUnreadableIsNotAcked(t *testing.T) {
	c := testChannel(t, "")
	body, query := craftCallback(t, "short")
	rec := postRaw(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unreadable callback")
		return channels.OutboundMessage{}, nil
	}), body, query)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("an authenticated but unreadable callback must answer 5xx, got %d", rec.Code)
	}
}

// The declared message length is read out of the decrypted buffer, so a
// corrupted or wrongly-keyed callback can carry 0xFFFFFFFF: 20+msg_len wraps to
// 19 in uint32, which slips past a "text_len < 20+msg_len" guard and inverts
// the slice bounds. It must be a bounds error like any other unreadable body.
func TestReceiveOverflowingMsgLenDoesNotPanic(t *testing.T) {
	c := testChannel(t, "")
	body, query := craftCallback(t, overflowPlaintext())
	rec := postRaw(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unreadable callback")
		return channels.OutboundMessage{}, nil
	}), body, query)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("an overflowing msg_len must be refused as unreadable, got %d", rec.Code)
	}
}

// Without a media store the media message degrades to a placeholder text that
// names the failure, so the agent still acknowledges the user.
func TestMediaDegradesWithoutStore(t *testing.T) {
	c := testChannel(t, "") // Media: nil
	var got channels.InboundMessage
	rec := postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}), mediaInner)
	if rec.Code != http.StatusOK {
		t.Fatalf("media callback status = %d", rec.Code)
	}
	if got.Type != channels.TypeMedia {
		t.Fatalf("want media type, got %+v", got)
	}
	if !strings.Contains(got.Text, "素材拉取失败") || got.MediaRef != "" {
		t.Fatalf("missing store must degrade to a failure placeholder: %+v", got)
	}
}

// Every media-fetch failure must degrade to the placeholder path instead of
// failing the callback (the platform would redeliver a media event forever).
func TestMediaFetchFailures(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // nothing listens anymore: every request fails at transport

	cases := []struct {
		name string
		cfg  Config
	}{
		{"unresolvable corpsecret", Config{CorpID: testCorpID, AgentID: 1000002, TokenRef: "tok", AESKeyRef: "aes", SecretRef: "nope", Media: &fakeMediaStore{}}},
		{"unbuildable request", Config{CorpID: testCorpID, AgentID: 1000002, TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: "://bad", Media: &fakeMediaStore{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(tc.cfg, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
			if err != nil {
				t.Fatal(err)
			}
			var got channels.InboundMessage
			postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
				got = msg
				return channels.OutboundMessage{}, nil
			}), mediaInner)
			if !strings.Contains(got.Text, "素材拉取失败") {
				t.Fatalf("media fetch failure must degrade to the placeholder: %+v", got)
			}
		})
	}

	t.Run("media request unbuildable after token cached", func(t *testing.T) {
		fake := &scriptWecomAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannelMedia(t, srv.URL)
		// Warm the token cache so the failure lands in the media-get request
		// build rather than in gettoken.
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "0", UserID: "u", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		c.cfg.APIBase = "://bad"
		var got channels.InboundMessage
		postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if !strings.Contains(got.Text, "素材拉取失败") {
			t.Fatalf("unbuildable media request must degrade to the placeholder: %+v", got)
		}
	})

	t.Run("media get unreachable", func(t *testing.T) {
		fake := &scriptWecomAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannelMedia(t, srv.URL)
		// Warm the token cache so the failure lands in the media-get request
		// rather than in gettoken.
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "0", UserID: "u", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		c.cfg.APIBase = deadURL
		var got channels.InboundMessage
		postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if !strings.Contains(got.Text, "素材拉取失败") {
			t.Fatalf("media get transport failure must degrade to the placeholder: %+v", got)
		}
	})

	t.Run("media get status 500", func(t *testing.T) {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/gettoken"):
				_, _ = w.Write([]byte(`{"access_token":"t-1","expires_in":7200}`))
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/media/get"):
				http.Error(w, "boom", http.StatusInternalServerError)
			default:
				http.NotFound(w, r)
			}
		}))
		defer api.Close()
		c := testChannelMedia(t, api.URL)
		var got channels.InboundMessage
		postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if !strings.Contains(got.Text, "素材拉取失败") {
			t.Fatalf("non-200 media get must degrade to the placeholder: %+v", got)
		}
	})

	t.Run("media download exceeds cap", func(t *testing.T) {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/gettoken"):
				_, _ = w.Write([]byte(`{"access_token":"t-1","expires_in":7200}`))
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/media/get"):
				_, _ = w.Write(make([]byte, (20<<20)+64)) // over the 20 MiB fetch cap
			default:
				http.NotFound(w, r)
			}
		}))
		defer api.Close()
		c := testChannelMedia(t, api.URL)
		var got channels.InboundMessage
		postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if !strings.Contains(got.Text, "素材拉取失败") {
			t.Fatalf("oversized media must degrade to the placeholder: %+v", got)
		}
	})
}

// media/get reports failures as HTTP 200 with a JSON error body; the JSON must
// never be stored as media bytes (only the placeholder goes through), and a
// 40014 invalidates the cached token and retries the download once.
func TestMediaGetJSONErrorBody(t *testing.T) {
	t.Run("json error body is not stored", func(t *testing.T) {
		store := &fakeMediaStore{}
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/gettoken"):
				_, _ = w.Write([]byte(`{"access_token":"t-1","expires_in":7200}`))
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/media/get"):
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"errcode":40005,"errmsg":"invalid media_id hint"}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer api.Close()
		c, err := New(Config{
			CorpID: testCorpID, AgentID: 1000002,
			TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: api.URL,
			Media: store,
		}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
		if err != nil {
			t.Fatal(err)
		}
		var got channels.InboundMessage
		rec := postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if rec.Code != http.StatusOK {
			t.Fatalf("media callback status = %d", rec.Code)
		}
		if !strings.Contains(got.Text, "素材拉取失败") || got.MediaRef != "" {
			t.Fatalf("a json error body must degrade to the placeholder: %+v", got)
		}
		if len(store.refs) != 0 {
			t.Fatalf("the json error body must never reach the artifact store, got %v", store.refs)
		}
	})

	t.Run("40014 refreshes the token and retries once", func(t *testing.T) {
		store := &fakeMediaStore{}
		var tokenCalls, mediaCalls int32
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/gettoken"):
				n := atomic.AddInt32(&tokenCalls, 1)
				_, _ = fmt.Fprintf(w, `{"access_token":"t-%d","expires_in":7200}`, n)
			case strings.HasPrefix(r.URL.Path, "/cgi-bin/media/get"):
				if atomic.AddInt32(&mediaCalls, 1) == 1 {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"errcode":40014,"errmsg":"invalid access_token"}`))
					return
				}
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
			Media: store,
		}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
		if err != nil {
			t.Fatal(err)
		}
		var got channels.InboundMessage
		rec := postForged(t, c, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
			got = msg
			return channels.OutboundMessage{}, nil
		}), mediaInner)
		if rec.Code != http.StatusOK {
			t.Fatalf("media callback status = %d", rec.Code)
		}
		if got.MediaRef == "" {
			t.Fatalf("the retried download must land in the artifact store: %+v", got)
		}
		if n := atomic.LoadInt32(&tokenCalls); n != 2 {
			t.Fatalf("40014 must trigger exactly one token refresh, got %d fetches", n)
		}
		if n := atomic.LoadInt32(&mediaCalls); n != 2 {
			t.Fatalf("40014 must trigger exactly one media retry, got %d downloads", n)
		}
	})
}

// testChannelMedia is testChannel with an artifact store attached.
func testChannelMedia(t *testing.T, apiBase string) *Channel {
	t.Helper()
	c, err := New(Config{
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
		Media: &fakeMediaStore{},
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMediaFileExt(t *testing.T) {
	cases := []struct {
		msgType, mime, want string
	}{
		{"voice", "image/jpeg", "amr"},
		{"file", "", "bin"},
		{"image", "image/png", "png"},
		{"image", "image/gif", "gif"},
		{"image", "image/jpeg", "jpg"},
		{"image", "application/octet-stream", "jpg"},
	}
	for _, tc := range cases {
		if got := mediaFileExt(tc.msgType, tc.mime); got != tc.want {
			t.Fatalf("mediaFileExt(%q, %q) = %q, want %q", tc.msgType, tc.mime, got, tc.want)
		}
	}
}

// scriptWecomAPI is a configurable fake of the WeCom HTTP API: each call can
// be answered by an injected body (empty script entries answer success).
type scriptWecomAPI struct {
	mu       sync.Mutex
	tokenN   int
	sendN    int
	onToken  func(n int) string // 1-based call index → raw response body
	onSend   func(n int) string
	sendSeen []string
}

func (f *scriptWecomAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.tokenN++
		n := f.tokenN
		f.mu.Unlock()
		if f.onToken != nil {
			_, _ = fmt.Fprint(w, f.onToken(n))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"access_token":"tok","expires_in":7200}`))
	})
	mux.HandleFunc("/cgi-bin/message/send", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sendN++
		n := f.sendN
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.sendSeen = append(f.sendSeen, string(body))
		f.mu.Unlock()
		if f.onSend != nil {
			_, _ = fmt.Fprint(w, f.onSend(n))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func (f *scriptWecomAPI) sends() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sendSeen...)
}

func (f *scriptWecomAPI) tokenCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokenN
}

func TestSendMarkdown(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "**加粗**", TextType: channels.TextTypeMarkdown,
	}); err != nil {
		t.Fatal(err)
	}
	sends := fake.sends()
	if len(sends) != 1 {
		t.Fatalf("want 1 send, got %d", len(sends))
	}
	if !strings.Contains(sends[0], `"msgtype":"markdown"`) || !strings.Contains(sends[0], `"markdown":{"content":"**加粗**"}`) {
		t.Fatalf("markdown must ride the markdown msgtype: %s", sends[0])
	}
}

// A platform rejection after some segments already landed surfaces as a
// partial-delivery error naming the failed segment, so the sender can retry.
func TestSendPartialDelivery(t *testing.T) {
	fake := &scriptWecomAPI{onSend: func(n int) string {
		if n == 2 {
			return `{"errcode":45001,"errmsg":"api forbidden"}`
		}
		return `{"errcode":0,"errmsg":"ok"}`
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes → 3 segments
	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: long,
	})
	if err == nil || !strings.Contains(err.Error(), "2/3") || !strings.Contains(err.Error(), "partial delivery") {
		t.Fatalf("second-segment failure must surface as partial delivery, got %v", err)
	}
	if n := len(fake.sends()); n != 2 {
		t.Fatalf("send must stop at the failing segment, got %d sends", n)
	}
}

func TestSendPlatformRejects(t *testing.T) {
	fake := &scriptWecomAPI{onSend: func(int) string {
		return `{"errcode":45001,"errmsg":"api forbidden"}`
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	})
	if err == nil || !strings.Contains(err.Error(), "errcode 45001") {
		t.Fatalf("platform rejection must surface as an error, got %v", err)
	}
}

func TestSendTokenFailures(t *testing.T) {
	t.Run("unresolvable corpsecret", func(t *testing.T) {
		c, err := New(Config{CorpID: testCorpID, AgentID: 1000002, TokenRef: "tok", AESKeyRef: "aes", SecretRef: "nope"},
			mapResolver{"tok": testToken, "aes": testAESKey})
		if err != nil {
			t.Fatal(err)
		}
		err = c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "resolve corpsecret") {
			t.Fatalf("unresolvable corpsecret must surface, got %v", err)
		}
	})

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	t.Run("gettoken unreachable", func(t *testing.T) {
		c := testChannel(t, deadURL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "gettoken") {
			t.Fatalf("gettoken transport failure must surface, got %v", err)
		}
	})
	t.Run("gettoken bad json", func(t *testing.T) {
		fake := &scriptWecomAPI{onToken: func(int) string { return `not-json` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "gettoken decode") {
			t.Fatalf("gettoken decode failure must surface, got %v", err)
		}
	})
	t.Run("gettoken errcode", func(t *testing.T) {
		fake := &scriptWecomAPI{onToken: func(int) string { return `{"errcode":40013,"errmsg":"invalid corpid"}` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "errcode 40013") {
			t.Fatalf("gettoken rejection must surface, got %v", err)
		}
	})
	t.Run("short ttl is extended to an hour", func(t *testing.T) {
		// expires_in below the refresh margin would compute a non-positive TTL;
		// the token must still be cached (one fetch for two sends).
		fake := &scriptWecomAPI{onToken: func(int) string {
			return `{"errcode":0,"access_token":"tok","expires_in":60}`
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		for i := 0; i < 2; i++ {
			if err := c.Send(t.Context(), channels.OutboundMessage{
				Channel: "wecom", MsgID: fmt.Sprint(i), UserID: "u", Text: "hi",
			}); err != nil {
				t.Fatal(err)
			}
		}
		if n := fake.tokenCalls(); n != 1 {
			t.Fatalf("token must be cached despite the short TTL, got %d fetches", n)
		}
	})
	t.Run("unbuildable gettoken request", func(t *testing.T) {
		c := testChannel(t, "://bad")
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil {
			t.Fatalf("unparseable API base must surface, got %v", err)
		}
	})
}

func TestSendPostMessageFailures(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	t.Run("send unreachable", func(t *testing.T) {
		fake := &scriptWecomAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "0", UserID: "u", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		// Token is now cached; point the API at a dead endpoint.
		c.cfg.APIBase = deadURL
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send request") {
			t.Fatalf("send transport failure must surface, got %v", err)
		}
	})
	t.Run("unbuildable send request", func(t *testing.T) {
		fake := &scriptWecomAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "0", UserID: "u", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		c.cfg.APIBase = "://bad"
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil {
			t.Fatalf("unparseable API base must surface, got %v", err)
		}
	})
	t.Run("send bad json", func(t *testing.T) {
		fake := &scriptWecomAPI{onSend: func(int) string { return `not-json` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send decode") {
			t.Fatalf("send decode failure must surface, got %v", err)
		}
	})
	t.Run("token refresh then gettoken fails", func(t *testing.T) {
		fake := &scriptWecomAPI{
			onToken: func(n int) string {
				if n == 1 {
					return `{"errcode":0,"access_token":"tok","expires_in":7200}`
				}
				return `{"errcode":40001,"errmsg":"invalid credential"}`
			},
			onSend: func(int) string { return `{"errcode":40014,"errmsg":"invalid access_token"}` },
		}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "errcode 40001") {
			t.Fatalf("failure of the refreshed token fetch must surface, got %v", err)
		}
	})
	t.Run("token refresh then send fails", func(t *testing.T) {
		fake := &scriptWecomAPI{onSend: func(n int) string {
			if n == 1 {
				return `{"errcode":40014,"errmsg":"invalid access_token"}`
			}
			return `not-json` // the retried send cannot be decoded either
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wecom", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send decode") {
			t.Fatalf("failure of the retried send must surface, got %v", err)
		}
	})
}
