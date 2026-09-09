package channels

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// wecomMaxTextBytes is the byte limit of one WeCom text app message; longer
// replies are split on rune boundaries (proposal doc 3.3).
const wecomMaxTextBytes = 2048

// wecomDefaultAPIBase is the official WeCom API endpoint, overridable in
// tests with an httptest server.
const wecomDefaultAPIBase = "https://qyapi.weixin.qq.com"

// WeCom is the channel adapter for a WeCom (企业微信) self-built app.
//
// Inbound: callbacks arrive at /callback/wecom/{tenant_id}. The adapter
// verifies msg_signature = SHA1(sort(token, timestamp, nonce, encrypt)),
// AES-256-CBC decrypts the body, writes the "success" ACK, and returns the
// normalized message; GET requests with echostr are the URL-verification
// probe and echo the decrypted plaintext.
//
// Outbound: streaming chunks are aggregated per conversation and flushed on
// Done through the message/send app-message API (token cached until 5
// minutes before expiry), split at wecomMaxTextBytes.
type WeCom struct {
	lookup  func(tenantID string) (*tenant.WeComBinding, bool)
	apiBase string
	client  *http.Client

	mu     sync.Mutex
	tokens map[string]*wecomToken      // key: corp_id
	bufs   map[string]*strings.Builder // key: SessionID(), pending aggregated reply
}

// NewWeCom builds the WeCom adapter. lookup resolves the per-tenant app
// binding; tenants without one reject callbacks with a clear error.
func NewWeCom(lookup func(tenantID string) (*tenant.WeComBinding, bool)) *WeCom {
	return &WeCom{
		lookup:  lookup,
		apiBase: wecomDefaultAPIBase,
		client:  &http.Client{Timeout: 10 * time.Second},
		tokens:  make(map[string]*wecomToken),
		bufs:    make(map[string]*strings.Builder),
	}
}

// Type implements Adapter.
func (w *WeCom) Type() Type { return TypeWeCom }

// wecomEnvelope is the outer encrypted callback XML.
type wecomEnvelope struct {
	XMLName xml.Name `xml:"xml"`
	Encrypt string   `xml:"Encrypt"`
}

// wecomMessage is the decrypted message XML (text messages only in v1).
type wecomMessage struct {
	XMLName      xml.Name `xml:"xml"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        int64    `xml:"MsgId"`
}

// Callback implements Adapter. It answers the URL-verification probe (GET)
// and message callbacks (POST) directly on the ResponseWriter, returning
// nil for probes and non-text messages after the ACK.
func (w *WeCom) Callback(rw http.ResponseWriter, r *http.Request) ([]*InboundMessage, error) {
	tenantID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/callback/wecom/"), "/")
	b, ok := w.lookup(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %q has no wecom binding", tenantID)
	}
	q := r.URL.Query()
	sig, ts, nonce := q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce")

	if r.Method == http.MethodGet { // URL 验证：解密 echostr 并原样回显明文
		echo := q.Get("echostr")
		if weComSign(b.Token, ts, nonce, echo) != sig {
			return nil, errors.New("wecom probe signature mismatch")
		}
		plain, err := w.decrypt(b, echo)
		if err != nil {
			return nil, err
		}
		_, _ = io.WriteString(rw, plain)
		return nil, nil
	}
	if r.Method != http.MethodPost {
		return nil, errors.New("wecom callback requires GET or POST")
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var env wecomEnvelope
	if err := xml.Unmarshal(body, &env); err != nil || env.Encrypt == "" {
		return nil, fmt.Errorf("parse callback xml: %v", err)
	}
	if weComSign(b.Token, ts, nonce, env.Encrypt) != sig {
		return nil, errors.New("wecom signature mismatch")
	}
	plain, err := w.decrypt(b, env.Encrypt)
	if err != nil {
		return nil, err
	}
	var msg wecomMessage
	if err := xml.Unmarshal([]byte(plain), &msg); err != nil {
		return nil, fmt.Errorf("parse decrypted msg: %w", err)
	}
	_, _ = io.WriteString(rw, "success") // ACK 先落，处理走异步
	if msg.MsgType != "text" {
		return nil, nil // 非文本消息（事件、图片等）v1 只 ACK 不处理
	}

	in := &InboundMessage{
		TenantID: tenantID,
		Channel:  TypeWeCom,
		UserID:   msg.FromUserName,
		Text:     msg.Content,
		MsgID:    strconv.FormatInt(msg.MsgID, 10),
	}
	if msg.MsgID == 0 {
		in.MsgID = fmt.Sprintf("%s-%d", msg.FromUserName, msg.CreateTime)
	}
	return []*InboundMessage{in}, nil
}

// Send implements Adapter: aggregates streaming chunks per conversation and
// flushes the reply as (possibly split) app messages when Done arrives.
func (w *WeCom) Send(ctx context.Context, msg *OutboundMessage) error {
	b, ok := w.lookup(msg.Target.TenantID)
	if !ok {
		return fmt.Errorf("tenant %q has no wecom binding", msg.Target.TenantID)
	}
	if msg.Target.GroupID != "" {
		return nil // v1：群聊只 ACK 不推送
	}

	key := msg.Target.SessionID()
	w.mu.Lock()
	if msg.Text != "" {
		buf, ok := w.bufs[key]
		if !ok {
			buf = &strings.Builder{}
			w.bufs[key] = buf
		}
		buf.WriteString(msg.Text)
	}
	var full string
	if msg.Done {
		if buf, ok := w.bufs[key]; ok {
			full = buf.String()
			delete(w.bufs, key)
		}
	}
	w.mu.Unlock()
	if full == "" {
		return nil
	}

	token, err := w.accessToken(ctx, b)
	if err != nil {
		return err
	}
	for _, part := range splitUTF8(full, wecomMaxTextBytes) {
		if err := w.sendText(ctx, b, token, msg.Target.UserID, part); err != nil {
			return err
		}
	}
	return nil
}

func (w *WeCom) decrypt(b *tenant.WeComBinding, b64 string) (string, error) {
	aesKey, err := weComAESKey(b.EncodingAESKey)
	if err != nil {
		return "", err
	}
	return weComDecrypt(aesKey, b64, b.CorpID)
}

type wecomToken struct {
	token     string
	expiresAt time.Time
}

// accessToken returns the cached corp access_token, refreshing it 5 minutes
// before expiry to avoid races at the boundary.
func (w *WeCom) accessToken(ctx context.Context, b *tenant.WeComBinding) (string, error) {
	w.mu.Lock()
	if t, ok := w.tokens[b.CorpID]; ok && time.Now().Before(t.expiresAt) {
		w.mu.Unlock()
		return t.token, nil
	}
	w.mu.Unlock()

	token, ttl, err := fetchAccessToken(ctx, w.client, w.apiBase, b.CorpID, b.CorpSecret)
	if err != nil {
		return "", err
	}
	w.mu.Lock()
	w.tokens[b.CorpID] = &wecomToken{token: token, expiresAt: time.Now().Add(ttl)}
	w.mu.Unlock()
	return token, nil
}

// sendText posts one text app message, retrying once with a fresh token
// when the cached one is rejected (40014/42001).
func (w *WeCom) sendText(ctx context.Context, b *tenant.WeComBinding, token, toUser, content string) error {
	err := w.postMessage(ctx, b, token, toUser, content)
	var apiErr *wecomAPIError
	if errors.As(err, &apiErr) && apiErr.tokenInvalid() {
		w.mu.Lock()
		delete(w.tokens, b.CorpID)
		w.mu.Unlock()
		fresh, ferr := w.accessToken(ctx, b)
		if ferr != nil {
			return ferr
		}
		return w.postMessage(ctx, b, fresh, toUser, content)
	}
	return err
}

func (w *WeCom) postMessage(ctx context.Context, b *tenant.WeComBinding, token, toUser, content string) error {
	payload := struct {
		ToUser  string `json:"touser"`
		MsgType string `json:"msgtype"`
		AgentID int    `json:"agentid"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
		Safe int `json:"safe"`
	}{ToUser: toUser, MsgType: "text", AgentID: b.AgentID, Safe: 0}
	payload.Text.Content = content
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	u := w.apiBase + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
	var out struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := apiDoJSON(ctx, w.client, http.MethodPost, u, body, &out); err != nil {
		return err
	}
	if out.ErrCode != 0 {
		return &wecomAPIError{ErrCode: out.ErrCode, ErrMsg: out.ErrMsg}
	}
	return nil
}

// fetchAccessToken calls the gettoken endpoint shared by WeCom apps and
// WeChat KF (each adapter passes its own secret and keeps its own cache).
// The returned TTL is already clamped to expire 5 minutes early.
func fetchAccessToken(ctx context.Context, client *http.Client, apiBase, corpID, secret string) (string, time.Duration, error) {
	u := fmt.Sprintf("%s/cgi-bin/gettoken?corpid=%s&corpsecret=%s",
		apiBase, url.QueryEscape(corpID), url.QueryEscape(secret))
	var out struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := apiDoJSON(ctx, client, http.MethodGet, u, nil, &out); err != nil {
		return "", 0, err
	}
	if out.ErrCode != 0 || out.AccessToken == "" {
		return "", 0, &wecomAPIError{ErrCode: out.ErrCode, ErrMsg: out.ErrMsg}
	}
	ttl := time.Duration(out.ExpiresIn-300) * time.Second
	if ttl < time.Minute {
		ttl = time.Minute
	}
	return out.AccessToken, ttl, nil
}

// apiDoJSON performs one JSON API call against the WeCom-family endpoints
// (qyapi.weixin.qq.com), decoding the response into out.
func apiDoJSON(ctx context.Context, client *http.Client, method, u string, body []byte, out any) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wecom api %s: http %d", u, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

type wecomAPIError struct {
	ErrCode int
	ErrMsg  string
}

func (e *wecomAPIError) Error() string {
	return fmt.Sprintf("wecom api: [%d] %s", e.ErrCode, e.ErrMsg)
}

// tokenInvalid reports the errcodes meaning the access_token is unusable.
func (e *wecomAPIError) tokenInvalid() bool { return e.ErrCode == 40014 || e.ErrCode == 42001 }

// weComSign computes msg_signature = SHA1(sort(token, timestamp, nonce, encrypt)),
// lowercase hex, per the official callback crypto scheme.
func weComSign(token, ts, nonce, encrypt string) string {
	parts := []string{token, ts, nonce, encrypt}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}

// weComAESKey decodes the 43-char EncodingAESKey into the 32-byte AES key.
func weComAESKey(encodingAESKey string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil || len(key) != 32 {
		return nil, errors.New("invalid wecom encoding_aes_key")
	}
	return key, nil
}

// weComDecrypt AES-256-CBC decrypts one callback ciphertext and strips the
// random(16) + msg_len(4, network order) header and the receiveid suffix;
// PKCS#7 padding uses the 32-byte block size.
func weComDecrypt(aesKey []byte, b64, receiveID string) (string, error) {
	cipherText, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("decode ciphertext: %w", err)
	}
	if len(cipherText)%aes.BlockSize != 0 {
		return "", errors.New("invalid ciphertext length")
	}
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", err
	}
	plain := make([]byte, len(cipherText))
	cipher.NewCBCDecrypter(block, aesKey[:aes.BlockSize]).CryptBlocks(plain, cipherText)
	plain = pkcs7Unpad(plain, 32)
	if len(plain) < 20 {
		return "", errors.New("plaintext too short")
	}
	msgLen := binary.BigEndian.Uint32(plain[16:20])
	if uint32(len(plain)) < 20+msgLen {
		return "", errors.New("plaintext length mismatch")
	}
	msg := string(plain[20 : 20+msgLen])
	if id := string(plain[20+msgLen:]); id != receiveID {
		return "", fmt.Errorf("receiveid mismatch: %q", id)
	}
	return msg, nil
}

// weComEncrypt is the inverse of weComDecrypt, used to build test vectors
// and mock callbacks.
func weComEncrypt(aesKey []byte, msg, receiveID string) (string, error) {
	buf := make([]byte, 0, 20+len(msg)+len(receiveID)+32)
	rnd := make([]byte, 16)
	if _, err := rand.Read(rnd); err != nil {
		return "", err
	}
	buf = append(buf, rnd...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, msg...)
	buf = append(buf, receiveID...)
	buf = pkcs7Pad(buf, 32)

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", err
	}
	out := make([]byte, len(buf))
	cipher.NewCBCEncrypter(block, aesKey[:aes.BlockSize]).CryptBlocks(out, buf)
	return base64.StdEncoding.EncodeToString(out), nil
}

func pkcs7Pad(b []byte, blockSize int) []byte {
	n := blockSize - len(b)%blockSize
	return append(b, bytes.Repeat([]byte{byte(n)}, n)...)
}

func pkcs7Unpad(b []byte, blockSize int) []byte {
	if len(b) == 0 || len(b)%blockSize != 0 {
		return nil
	}
	n := int(b[len(b)-1])
	if n == 0 || n > blockSize || n > len(b) {
		return nil
	}
	return b[:len(b)-n]
}

// splitUTF8 cuts s into fragments of at most maxBytes without splitting
// multi-byte runes.
func splitUTF8(s string, maxBytes int) []string {
	var out []string
	for len(s) > maxBytes {
		cut := maxBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 {
			cut = maxBytes // defensive: unreachable for valid UTF-8
		}
		out = append(out, s[:cut])
		s = s[cut:]
	}
	if s != "" {
		out = append(out, s)
	}
	return out
}
