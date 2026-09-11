package channels

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

// WeCom implements the adapter contract for 企业微信 (WeCom) self-built
// applications. Unlike Telegram/Slack, WeCom encrypts callback payloads with
// AES-256-CBC and authenticates them with an SHA1 signature over
// (token, timestamp, nonce, ciphertext). Replies are sent proactively through
// the message/send API, so the callback only needs to be ACKed quickly.
type WeCom struct {
	client HTTPDoer
	now    func() time.Time

	mu     sync.Mutex
	tokens map[string]wecomTokenEntry
}

type wecomTokenEntry struct {
	accessToken string
	expiresAt   time.Time
}

const (
	wecomDefaultAPIBase = "https://qyapi.weixin.qq.com"
	wecomMaxClockSkew   = 5 * time.Minute
	// The WeCom wire protocol uses PKCS#7 with a 32-byte padding block even
	// though AES-CBC itself has a 16-byte cipher block.
	wecomPKCS7BlockSize = 32
	// WeCom rejects text messages beyond 2048 UTF-8 bytes.
	wecomDefaultMessageBytes = 2048
)

func NewWeCom(client HTTPDoer) *WeCom {
	if client == nil {
		client = http.DefaultClient
	}
	return &WeCom{client: client, now: time.Now, tokens: make(map[string]wecomTokenEntry)}
}

func (*WeCom) Name() string { return "wecom" }

// wecomCipher implements the WeCom callback encryption scheme: AES-256-CBC
// with IV = key[:16] and PKCS#7 padding over random(16) || len(4) || msg || receiveID.
type wecomCipher struct{ key []byte }

func newWeComCipher(encodingAESKey string) (*wecomCipher, error) {
	if len(encodingAESKey) != 43 {
		return nil, errors.New("invalid wecom encoding aes key length")
	}
	key, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil || len(key) != 32 {
		return nil, errors.New("invalid wecom encoding aes key")
	}
	return &wecomCipher{key: key}, nil
}

func wecomSignature(token, timestamp, nonce, encrypted string) string {
	parts := []string{token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}

func wecomConstantTimeEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}

func (c *wecomCipher) decrypt(ciphertext []byte) (msg, receiveID []byte, err error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, nil, errors.New("invalid wecom cipher key")
	}
	if len(ciphertext) < aes.BlockSize || len(ciphertext)%aes.BlockSize != 0 {
		return nil, nil, errors.New("invalid wecom ciphertext size")
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, c.key[:aes.BlockSize]).CryptBlocks(plaintext, ciphertext)
	plaintext, err = wecomPKCS7Unpad(plaintext)
	if err != nil {
		return nil, nil, err
	}
	if len(plaintext) < 20 {
		return nil, nil, errors.New("truncated wecom plaintext")
	}
	msgLen := int(binary.BigEndian.Uint32(plaintext[16:20]))
	if msgLen < 0 || msgLen > len(plaintext)-20 {
		return nil, nil, errors.New("invalid wecom message length")
	}
	return plaintext[20 : 20+msgLen], plaintext[20+msgLen:], nil
}

func (c *wecomCipher) encrypt(msg, receiveID []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, errors.New("invalid wecom cipher key")
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, errors.New("generate wecom plaintext prefix failed")
	}
	plaintext := make([]byte, 0, 20+len(msg)+len(receiveID)+aes.BlockSize)
	plaintext = append(plaintext, random...)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(msg)))
	plaintext = append(plaintext, length...)
	plaintext = append(plaintext, msg...)
	plaintext = append(plaintext, receiveID...)
	padded := wecomPKCS7Pad(plaintext)
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, c.key[:aes.BlockSize]).CryptBlocks(ciphertext, padded)
	return ciphertext, nil
}

func wecomPKCS7Pad(data []byte) []byte {
	padding := wecomPKCS7BlockSize - len(data)%wecomPKCS7BlockSize
	return append(data, bytes.Repeat([]byte{byte(padding)}, padding)...)
}

func wecomPKCS7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty wecom plaintext")
	}
	padding := int(data[len(data)-1])
	if padding == 0 || padding > len(data) || padding > wecomPKCS7BlockSize {
		return nil, errors.New("invalid wecom padding")
	}
	for _, b := range data[len(data)-padding:] {
		if int(b) != padding {
			return nil, errors.New("invalid wecom padding")
		}
	}
	return data[:len(data)-padding], nil
}

func (wc *WeCom) verifyTimestamp(timestamp string) error {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrInvalidSignature
	}
	skew := wc.now().Sub(time.Unix(seconds, 0))
	if skew > wecomMaxClockSkew || -skew > wecomMaxClockSkew {
		return ErrInvalidSignature
	}
	return nil
}

// VerifyURL handles the one-time GET callback verification WeCom performs
// when an operator registers the webhook URL. It re-computes the signature
// over the echostr, decrypts it, checks the embedded receiveID equals the
// configured corpid, and returns the plaintext that must be echoed back.
func (wc *WeCom) VerifyURL(r *http.Request, binding config.ChannelConfig) (string, error) {
	token, err := config.Secret(binding.SigningSecretEnv)
	if err != nil {
		return "", err
	}
	query := r.URL.Query()
	echostr := query.Get("echostr")
	if echostr == "" {
		return "", ErrInvalidSignature
	}
	if err := wc.verifyTimestamp(query.Get("timestamp")); err != nil {
		return "", err
	}
	signature := wecomSignature(token, query.Get("timestamp"), query.Get("nonce"), echostr)
	if !wecomConstantTimeEqual(signature, query.Get("msg_signature")) {
		return "", ErrInvalidSignature
	}
	cipherInstance, err := newWeComCipherFromBinding(binding)
	if err != nil {
		return "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(echostr)
	if err != nil {
		return "", ErrInvalidSignature
	}
	msg, receiveID, err := cipherInstance.decrypt(ciphertext)
	if err != nil {
		return "", ErrInvalidSignature
	}
	if binding.WorkspaceID != "" && string(receiveID) != binding.WorkspaceID {
		return "", ErrBindingMismatch
	}
	return string(msg), nil
}

// Verify authenticates a POST callback: signature over (token, timestamp,
// nonce, Encrypt) plus a bounded clock skew window, mirroring the Slack
// replay protection.
func (wc *WeCom) Verify(r *http.Request, body []byte, binding config.ChannelConfig) error {
	token, err := config.Secret(binding.SigningSecretEnv)
	if err != nil {
		return err
	}
	query := r.URL.Query()
	if err := wc.verifyTimestamp(query.Get("timestamp")); err != nil {
		return err
	}
	encrypted, err := wecomEncryptField(body)
	if err != nil {
		return ErrInvalidSignature
	}
	signature := wecomSignature(token, query.Get("timestamp"), query.Get("nonce"), encrypted)
	if !wecomConstantTimeEqual(signature, query.Get("msg_signature")) {
		return ErrInvalidSignature
	}
	return nil
}

func wecomEncryptField(body []byte) (string, error) {
	var envelope struct {
		Encrypt string `xml:"Encrypt"`
	}
	if err := xml.Unmarshal(body, &envelope); err != nil || envelope.Encrypt == "" {
		return "", errors.New("missing wecom Encrypt field")
	}
	return envelope.Encrypt, nil
}

type wecomMessage struct {
	ToUserName   string `xml:"ToUserName"`
	FromUserName string `xml:"FromUserName"`
	CreateTime   int64  `xml:"CreateTime"`
	MsgType      string `xml:"MsgType"`
	Content      string `xml:"Content"`
	MsgID        int64  `xml:"MsgId"`
	AgentID      int64  `xml:"AgentID"`
	ChatID       string `xml:"ChatId"`
	MediaID      string `xml:"MediaId"`
	FileName     string `xml:"FileName"`
	PicURL       string `xml:"PicUrl"`
	// Retain compatibility with older nested fixtures while preferring the
	// top-level MediaId fields emitted by the actual callback protocol.
	Image *struct {
		MediaID string `xml:"MediaId"`
	} `xml:"Image"`
	File *struct {
		MediaID  string `xml:"MediaId"`
		FileName string `xml:"FileName"`
	} `xml:"File"`
	Event *struct {
		EventType string `xml:"Event"`
	} `xml:"Event"`
}

func (wc *WeCom) Parse(body []byte, binding config.ChannelConfig) (ParsedWebhook, error) {
	encrypted, err := wecomEncryptField(body)
	if err != nil {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}
	cipherInstance, err := newWeComCipherFromBinding(binding)
	if err != nil {
		return ParsedWebhook{}, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return ParsedWebhook{}, ErrInvalidSignature
	}
	plaintext, receiveID, err := cipherInstance.decrypt(ciphertext)
	if err != nil {
		return ParsedWebhook{}, ErrInvalidSignature
	}
	// receiveID is the corpid; ToUserName repeats it inside the plaintext.
	if binding.WorkspaceID != "" && string(receiveID) != binding.WorkspaceID {
		return ParsedWebhook{}, ErrBindingMismatch
	}
	var m wecomMessage
	if err := xml.Unmarshal(plaintext, &m); err != nil {
		return ParsedWebhook{}, errors.New("decode wecom message: malformed plaintext")
	}
	if m.ToUserName != "" && binding.WorkspaceID != "" && m.ToUserName != binding.WorkspaceID {
		return ParsedWebhook{}, ErrBindingMismatch
	}
	// AgentID binding prevents cross-application routing when multiple apps
	// share one corpid callback domain, mirroring the Slack app binding.
	if binding.ApplicationID != "" {
		if m.AgentID == 0 || strconv.FormatInt(m.AgentID, 10) != binding.ApplicationID {
			return ParsedWebhook{}, ErrBindingMismatch
		}
	}
	if m.MsgType == "event" || m.FromUserName == "" {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}

	attachments := make([]domain.Attachment, 0, 2)
	mediaID := m.MediaID
	if mediaID == "" && m.Image != nil {
		mediaID = m.Image.MediaID
	}
	fileName := m.FileName
	if mediaID == "" && m.File != nil {
		mediaID = m.File.MediaID
		fileName = m.File.FileName
	}
	switch {
	case m.MsgType == "image" && mediaID != "":
		attachments = append(attachments, domain.Attachment{Type: "image", FileID: mediaID, URL: m.PicURL})
	case m.MsgType == "file" && mediaID != "":
		attachments = append(attachments, domain.Attachment{
			Type: "file", FileID: mediaID, Name: fileName,
		})
	}
	if strings.TrimSpace(m.Content) == "" && len(attachments) == 0 {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}

	scope := domain.ScopeDirect
	replyTarget := m.FromUserName
	if m.ChatID != "" {
		scope = domain.ScopeGroup
		replyTarget = m.ChatID
	}
	messageID := strconv.FormatInt(m.MsgID, 10)
	if m.MsgID == 0 {
		messageID = fmt.Sprintf("%s:%d", replyTarget, m.CreateTime)
	}
	received := time.Now().UTC()
	if m.CreateTime != 0 {
		received = time.Unix(m.CreateTime, 0).UTC()
	}
	return ParsedWebhook{Messages: []domain.InboundMessage{{
		BindingID: binding.BindingID, Channel: "wecom", ExternalMessageID: messageID,
		ExternalUserID: m.FromUserName, ConversationID: replyTarget,
		Scope: scope, Text: m.Content, Attachments: attachments, ReceivedAt: received,
		ReplyTarget: replyTarget,
	}}}, nil
}

func newWeComCipherFromBinding(binding config.ChannelConfig) (*wecomCipher, error) {
	key, err := config.Secret(binding.EncryptionKeyEnv)
	if err != nil {
		return nil, err
	}
	return newWeComCipher(key)
}

type wecomTokenResponse struct {
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

type wecomSendResponse struct {
	ErrCode int             `json:"errcode"`
	ErrMsg  string          `json:"errmsg"`
	MsgID   json.RawMessage `json:"msgid"`
}

// accessToken fetches and caches the app access token per (corpid, secret).
// The token appears only in request URLs, so every error path returns a
// category instead of the underlying URL-bearing failure.
func (wc *WeCom) accessToken(ctx context.Context, binding config.ChannelConfig) (string, delivery.Result) {
	secret, err := config.Secret(binding.TokenEnv)
	if err != nil {
		return "", delivery.Result{
			Outcome: delivery.PermanentRejected, ErrorType: "provider_auth",
			Err: errors.New("load wecom token secret failed"),
		}
	}
	cacheKey := binding.WorkspaceID + "\x1f" + binding.TokenEnv
	wc.mu.Lock()
	cached, ok := wc.tokens[cacheKey]
	if ok && wc.now().Before(cached.expiresAt) {
		token := cached.accessToken
		wc.mu.Unlock()
		return token, delivery.Result{Outcome: delivery.Confirmed}
	}
	wc.mu.Unlock()

	base := strings.TrimRight(binding.APIBaseURL, "/")
	if base == "" {
		base = wecomDefaultAPIBase
	}
	endpoint := base + "/cgi-bin/gettoken?" + url.Values{
		"corpid":     {binding.WorkspaceID},
		"corpsecret": {secret},
	}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", delivery.Result{
			Outcome: delivery.PermanentRejected, ErrorType: "request_invalid",
			Err: errors.New("create wecom token request failed"),
		}
	}
	resp, err := wc.client.Do(req)
	if err != nil {
		return "", delivery.Result{
			Outcome: delivery.RetryableNotSent, ErrorType: "token_transport",
			Err: errors.New("wecom token request failed"),
		}
	}
	exchange := readHTTPResponse(resp)
	classified, terminal := classifyHTTP(exchange, wc.now(), "X-Request-Id")
	if terminal {
		// No message request has been made yet. Even a truncated token response
		// is safe to retry rather than operation-unknown.
		if classified.Outcome == delivery.Unknown {
			classified.Outcome = delivery.RetryableNotSent
			classified.ErrorType = "token_transport"
		}
		return "", classified
	}
	var tokenResp wecomTokenResponse
	if err := decodeJSONResponse(exchange, &tokenResp); err != nil {
		result := responseMetadata(exchange, "X-Request-Id")
		result.Outcome = delivery.RetryableNotSent
		result.ErrorType = "token_response_invalid"
		result.Err = errors.New("decode wecom token response failed")
		return "", result
	}
	if tokenResp.ErrCode != 0 || tokenResp.AccessToken == "" {
		result := responseMetadata(exchange, "X-Request-Id")
		result.ProviderCode = strconv.Itoa(tokenResp.ErrCode)
		if wecomTransientCode(tokenResp.ErrCode) || (tokenResp.ErrCode == 0 && tokenResp.AccessToken == "") {
			result.Outcome = delivery.RetryableNotSent
			result.ErrorType = "token_transient"
			result.RetryAfter = parseRetryAfter(exchange.header.Get("Retry-After"), wc.now())
		} else {
			result.Outcome = delivery.PermanentRejected
			result.ErrorType = "provider_auth"
		}
		return "", result
	}
	expiresIn := time.Duration(tokenResp.ExpiresIn) * time.Second
	if expiresIn <= 0 {
		expiresIn = 7200 * time.Second
	}
	// Refresh five minutes before the real expiry to avoid borderline 42001.
	usable := expiresIn - 5*time.Minute
	if usable <= 0 {
		usable = expiresIn / 2
	}
	wc.mu.Lock()
	wc.tokens[cacheKey] = wecomTokenEntry{
		accessToken: tokenResp.AccessToken,
		expiresAt:   wc.now().Add(usable),
	}
	wc.mu.Unlock()
	return tokenResp.AccessToken, delivery.Result{Outcome: delivery.Confirmed}
}

func (wc *WeCom) invalidateToken(binding config.ChannelConfig) {
	cacheKey := binding.WorkspaceID + "\x1f" + binding.TokenEnv
	wc.mu.Lock()
	delete(wc.tokens, cacheKey)
	wc.mu.Unlock()
}

func (wc *WeCom) sendOnce(ctx context.Context, binding config.ChannelConfig, token, path string, payload map[string]any) delivery.Result {
	base := strings.TrimRight(binding.APIBaseURL, "/")
	if base == "" {
		base = wecomDefaultAPIBase
	}
	endpoint := base + path + "?access_token=" + url.QueryEscape(token)
	var response wecomSendResponse
	exchange := postJSON(ctx, wc.client, endpoint, nil, payload)
	classified, terminal := classifyHTTP(exchange, wc.now(), "X-Request-Id")
	decodeErr := decodeJSONResponse(exchange, &response)
	if terminal {
		if decodeErr == nil && response.ErrCode != 0 {
			classified.ProviderCode = strconv.Itoa(response.ErrCode)
			if response.ErrCode == 40014 || response.ErrCode == 42001 {
				classified.Outcome = delivery.RetryableNotSent
				classified.ErrorType = "provider_auth_expired"
			} else if wecomTransientCode(response.ErrCode) {
				classified.Outcome = delivery.RetryableNotSent
				classified.ErrorType = "provider_transient"
			}
		}
		return classified
	}
	if decodeErr != nil {
		return malformedSuccess(exchange, "X-Request-Id")
	}
	result := responseMetadata(exchange, "X-Request-Id")
	result.ProviderCode = strconv.Itoa(response.ErrCode)
	if response.ErrCode == 40014 || response.ErrCode == 42001 {
		result.Outcome = delivery.RetryableNotSent
		result.ErrorType = "provider_auth_expired"
		return result
	}
	if response.ErrCode != 0 {
		if wecomTransientCode(response.ErrCode) {
			result.Outcome = delivery.RetryableNotSent
			result.ErrorType = "provider_transient"
			result.RetryAfter = parseRetryAfter(exchange.header.Get("Retry-After"), wc.now())
		} else {
			result.Outcome = delivery.PermanentRejected
			result.ErrorType = "provider_rejected"
		}
		return result
	}
	result.Outcome = delivery.Confirmed
	result.ProviderCode = ""
	result.ProviderMessageID = jsonIdentifier(response.MsgID)
	return result
}

func (*WeCom) Plan(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	pieces, err := chunksUTF8Bytes(msg.Text, binding.MaxMessageLength)
	if err != nil {
		return nil, err
	}
	parts := make([]delivery.Part, 0, len(pieces))
	for index, text := range pieces {
		partMessage := cloneOutbound(msg)
		partMessage.Text = text
		parts = append(parts, delivery.Part{Message: partMessage, Index: index, Total: len(pieces)})
	}
	return parts, nil
}

func (wc *WeCom) Deliver(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result {
	msg := request.Message
	agentID := int64(0)
	if binding.ApplicationID != "" {
		parsed, err := strconv.ParseInt(binding.ApplicationID, 10, 64)
		if err != nil {
			return delivery.Result{
				Outcome: delivery.PermanentRejected, ErrorType: "request_invalid",
				Err: errors.New("invalid wecom agentid configuration"),
			}
		}
		agentID = parsed
	}
	// Group chats use appchat/send keyed by chatid; direct messages use
	// message/send keyed by touser plus agentid.
	path := "/cgi-bin/message/send"
	if msg.Scope == domain.ScopeGroup {
		path = "/cgi-bin/appchat/send"
	}
	payload := map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": msg.Text},
	}
	if msg.Scope == domain.ScopeGroup {
		payload["chatid"] = msg.Target
	} else {
		payload["touser"] = msg.Target
		payload["agentid"] = agentID
	}
	token, tokenResult := wc.accessToken(ctx, binding)
	if token == "" {
		return tokenResult
	}
	result := wc.sendOnce(ctx, binding, token, path, payload)
	if result.ErrorType != "provider_auth_expired" {
		return result
	}
	// The provider explicitly rejected the first request before sending because
	// its token was stale. Refreshing and retrying this same part once is safe.
	wc.invalidateToken(binding)
	token, tokenResult = wc.accessToken(ctx, binding)
	if token == "" {
		return tokenResult
	}
	return wc.sendOnce(ctx, binding, token, path, payload)
}

// chunksUTF8Bytes splits text into parts of at most limit bytes without
// cutting a multi-byte rune; WeCom's content limit is expressed in bytes.
func chunksUTF8Bytes(text string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = wecomDefaultMessageBytes
	}
	if !utf8.ValidString(text) {
		return nil, errors.New("wecom text is not valid UTF-8")
	}
	if len(text) <= limit {
		if len(text) == 0 {
			return []string{""}, nil
		}
		return []string{text}, nil
	}
	result := make([]string, 0, (len(text)+limit-1)/limit)
	for len(text) > 0 {
		n := 0
		for _, r := range text {
			width := utf8.RuneLen(r)
			if n+width > limit {
				break
			}
			n += width
		}
		if n == 0 {
			return nil, fmt.Errorf("wecom message byte limit %d is smaller than one UTF-8 code point", limit)
		}
		result = append(result, text[:n])
		text = text[n:]
	}
	return result, nil
}

func wecomTransientCode(code int) bool {
	switch code {
	case -1, 45009:
		return true
	default:
		return false
	}
}

func jsonIdentifier(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return ""
		}
		return sanitizeIdentifier(value, 160)
	}
	return sanitizeIdentifier(string(raw), 160)
}
