// Package wecom implements enterprise WeChat callback and send protocols.
package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"golang.org/x/sync/singleflight"
)

const defaultAPIBase = "https://qyapi.weixin.qq.com"

type bindingConfig struct {
	CorpID              string `json:"corp_id"`
	AgentID             int64  `json:"agent_id"`
	CallbackTokenRef    string `json:"callback_token_ref"`
	EncodingAESKeyRef   string `json:"encoding_aes_key_ref"`
	AppSecretRef        string `json:"app_secret_ref"`
	APIBaseURL          string `json:"api_base_url,omitempty"`
	MaxClockSkewSeconds int64  `json:"max_clock_skew_seconds,omitempty"`
}

type tokenEntry struct {
	value     string
	expiresAt time.Time
}

// Adapter verifies encrypted callbacks and sends application text messages.
type Adapter struct {
	secrets secret.Store
	client  *http.Client
	mu      sync.Mutex
	tokens  map[string]tokenEntry
	group   singleflight.Group
}

func New(secrets secret.Store, client *http.Client) (*Adapter, error) {
	if secrets == nil {
		return nil, fmt.Errorf("WeCom secret store is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Adapter{secrets: secrets, client: client, tokens: make(map[string]tokenEntry)}, nil
}

func (a *Adapter) Type() string { return "wecom" }

func (a *Adapter) Capabilities() channels.Capabilities {
	return channels.Capabilities{MaxTextRunes: 1900, SupportsCard: true, SupportsFile: true}
}

func (a *Adapter) Callback(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	request *http.Request,
) (channels.CallbackResult, error) {
	cfg, err := parseBinding(binding)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	token, err := a.secrets.Resolve(ctx, cfg.CallbackTokenRef)
	if err != nil {
		return channels.CallbackResult{}, fmt.Errorf("resolve WeCom callback token: %w", err)
	}
	aesKeyText, err := a.secrets.Resolve(ctx, cfg.EncodingAESKeyRef)
	if err != nil {
		return channels.CallbackResult{}, fmt.Errorf("resolve WeCom AES key: %w", err)
	}
	query := request.URL.Query()
	timestamp := query.Get("timestamp")
	nonce := query.Get("nonce")
	signature := query.Get("msg_signature")
	if err := validateTimestamp(timestamp, cfg.MaxClockSkewSeconds); err != nil {
		return channels.CallbackResult{}, err
	}
	if request.Method == http.MethodGet {
		encrypted := query.Get("echostr")
		if !validSignature(token, timestamp, nonce, encrypted, signature) {
			return channels.CallbackResult{}, fmt.Errorf("invalid WeCom callback signature")
		}
		plaintext, err := decryptMessage(aesKeyText, cfg.CorpID, encrypted)
		if err != nil {
			return channels.CallbackResult{}, err
		}
		return channels.CallbackResult{
			StatusCode:  http.StatusOK,
			ContentType: "text/plain; charset=utf-8",
			Body:        plaintext,
		}, nil
	}
	if request.Method != http.MethodPost {
		return channels.CallbackResult{}, fmt.Errorf("unsupported WeCom callback method")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil {
		return channels.CallbackResult{}, fmt.Errorf("read WeCom callback: %w", err)
	}
	var envelope struct {
		Encrypt string `xml:"Encrypt"`
	}
	if err := xml.Unmarshal(body, &envelope); err != nil || envelope.Encrypt == "" {
		return channels.CallbackResult{}, fmt.Errorf("decode WeCom encrypted envelope")
	}
	if !validSignature(token, timestamp, nonce, envelope.Encrypt, signature) {
		return channels.CallbackResult{}, fmt.Errorf("invalid WeCom callback signature")
	}
	plaintext, err := decryptMessage(aesKeyText, cfg.CorpID, envelope.Encrypt)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	message, err := decodeMessage(plaintext)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	result := channels.CallbackResult{
		StatusCode:  http.StatusOK,
		ContentType: "text/plain; charset=utf-8",
		Body:        []byte("success"),
	}
	if message.MsgType != "text" || strings.TrimSpace(message.Content) == "" {
		return result, nil
	}
	chatType := "direct"
	chatID := message.FromUserName
	if message.ChatID != "" {
		chatType = "group"
		chatID = message.ChatID
	}
	externalID := message.MsgID
	if externalID == "" {
		externalID = fallbackMessageID(
			message.FromUserName, chatID, message.CreateTime, message.Content,
		)
	}
	result.Messages = []channels.InboundEnvelope{{
		ExternalMessageID: externalID,
		ExternalUserID:    message.FromUserName,
		ExternalChatID:    chatID,
		ChatType:          chatType,
		MessageType:       message.MsgType,
		Text:              strings.TrimSpace(message.Content),
		OccurredAt:        time.Unix(message.CreateTime, 0).UTC(),
	}}
	return result, nil
}

type callbackMessage struct {
	ToUserName   string `xml:"ToUserName"`
	FromUserName string `xml:"FromUserName"`
	CreateTime   int64  `xml:"CreateTime"`
	MsgType      string `xml:"MsgType"`
	Content      string `xml:"Content"`
	MsgID        string `xml:"MsgId"`
	ChatID       string `xml:"ChatId"`
}

func decodeMessage(plaintext []byte) (callbackMessage, error) {
	var message callbackMessage
	if err := xml.Unmarshal(plaintext, &message); err != nil {
		return callbackMessage{}, fmt.Errorf("decode WeCom message XML: %w", err)
	}
	if message.FromUserName == "" || message.MsgType == "" {
		return callbackMessage{}, fmt.Errorf("WeCom message identity and type are required")
	}
	return message, nil
}

func (a *Adapter) Send(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	message channels.OutboundMessage,
) (channels.DeliveryReceipt, error) {
	cfg, err := parseBinding(binding)
	if err != nil {
		return channels.DeliveryReceipt{}, err
	}
	if message.ReplyTarget == "" {
		return channels.DeliveryReceipt{}, fmt.Errorf("WeCom reply target is required")
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, tokenErr := a.accessToken(ctx, binding, cfg)
		if tokenErr != nil {
			return channels.DeliveryReceipt{}, tokenErr
		}
		providerID, invalidToken, sendErr := a.sendText(ctx, cfg, token, message)
		if invalidToken && attempt == 0 {
			a.invalidateToken(binding)
			continue
		}
		if sendErr != nil {
			return channels.DeliveryReceipt{}, sendErr
		}
		return channels.DeliveryReceipt{
			ProviderMessageID: providerID,
			SentAt:            time.Now().UTC(),
		}, nil
	}
	return channels.DeliveryReceipt{}, fmt.Errorf("WeCom token refresh did not recover delivery")
}

func (a *Adapter) accessToken(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	cfg bindingConfig,
) (string, error) {
	cacheKey := fmt.Sprintf("%s:%d", binding.ID, binding.Version)
	a.mu.Lock()
	entry := a.tokens[cacheKey]
	a.mu.Unlock()
	if entry.value != "" && time.Now().Before(entry.expiresAt) {
		return entry.value, nil
	}
	value, err, _ := a.group.Do(cacheKey, func() (any, error) {
		a.mu.Lock()
		cached := a.tokens[cacheKey]
		a.mu.Unlock()
		if cached.value != "" && time.Now().Before(cached.expiresAt) {
			return cached.value, nil
		}
		appSecret, err := a.secrets.Resolve(ctx, cfg.AppSecretRef)
		if err != nil {
			return "", fmt.Errorf("resolve WeCom app secret: %w", err)
		}
		base := apiBase(cfg)
		endpoint := base + "/cgi-bin/gettoken?" + url.Values{
			"corpid":     {cfg.CorpID},
			"corpsecret": {appSecret},
		}.Encode()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		resp, err := a.client.Do(req)
		if err != nil {
			return "", &channels.DeliveryError{Cause: err, Retryable: true}
		}
		defer resp.Body.Close()
		var payload struct {
			ErrCode     int    `json:"errcode"`
			ErrMsg      string `json:"errmsg"`
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
			return "", &channels.DeliveryError{Cause: err, Retryable: resp.StatusCode >= 500}
		}
		if payload.ErrCode != 0 || payload.AccessToken == "" {
			return "", providerError("get token", payload.ErrCode, payload.ErrMsg)
		}
		expires := time.Duration(payload.ExpiresIn) * time.Second
		if expires <= time.Minute {
			expires = 2 * time.Minute
		}
		entry := tokenEntry{value: payload.AccessToken, expiresAt: time.Now().Add(expires - time.Minute)}
		a.mu.Lock()
		a.tokens[cacheKey] = entry
		a.mu.Unlock()
		return entry.value, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (a *Adapter) sendText(
	ctx context.Context,
	cfg bindingConfig,
	token string,
	message channels.OutboundMessage,
) (string, bool, error) {
	body, _ := json.Marshal(map[string]any{
		"touser":  message.ReplyTarget,
		"msgtype": "text",
		"agentid": cfg.AgentID,
		"text":    map[string]string{"content": message.Text},
	})
	endpoint := apiBase(cfg) + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", false, &channels.DeliveryError{Cause: err, Retryable: true}
	}
	defer resp.Body.Close()
	var payload struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MsgID   string `json:"msgid"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return "", false, &channels.DeliveryError{Cause: err, Retryable: resp.StatusCode >= 500}
	}
	if payload.ErrCode == 40014 || payload.ErrCode == 42001 {
		return "", true, providerError("send", payload.ErrCode, payload.ErrMsg)
	}
	if payload.ErrCode != 0 {
		return "", false, providerError("send", payload.ErrCode, payload.ErrMsg)
	}
	if payload.MsgID == "" {
		payload.MsgID = message.OutboundID
	}
	return payload.MsgID, false, nil
}

func (a *Adapter) invalidateToken(binding controlplane.ChannelBinding) {
	a.mu.Lock()
	delete(a.tokens, fmt.Sprintf("%s:%d", binding.ID, binding.Version))
	a.mu.Unlock()
}

func providerError(operation string, code int, message string) error {
	retryable := code == -1 || code == 45009
	retryAfter := time.Duration(0)
	if code == 45009 {
		retryAfter = time.Minute
	}
	return &channels.DeliveryError{
		Cause:      fmt.Errorf("WeCom %s failed: code=%d message=%s", operation, code, message),
		Retryable:  retryable,
		RetryAfter: retryAfter,
	}
}

func parseBinding(binding controlplane.ChannelBinding) (bindingConfig, error) {
	if binding.ChannelType != "wecom" || binding.Status != controlplane.StatusActive {
		return bindingConfig{}, fmt.Errorf("WeCom binding is unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(binding.Config))
	decoder.DisallowUnknownFields()
	var cfg bindingConfig
	if err := decoder.Decode(&cfg); err != nil {
		return bindingConfig{}, fmt.Errorf("decode WeCom binding config: %w", err)
	}
	if cfg.CorpID == "" || cfg.AgentID <= 0 || cfg.CallbackTokenRef == "" ||
		cfg.EncodingAESKeyRef == "" || cfg.AppSecretRef == "" {
		return bindingConfig{}, fmt.Errorf("WeCom binding config is incomplete")
	}
	return cfg, nil
}

func apiBase(cfg bindingConfig) string {
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return defaultAPIBase
	}
	return strings.TrimRight(cfg.APIBaseURL, "/")
}

func validSignature(token, timestamp, nonce, encrypted, signature string) bool {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	digest := sha1.Sum([]byte(strings.Join(values, "")))
	expected := hex.EncodeToString(digest[:])
	return subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(signature))) == 1
}

func validateTimestamp(raw string, maxSkewSeconds int64) error {
	timestamp, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid WeCom callback timestamp")
	}
	if maxSkewSeconds <= 0 {
		maxSkewSeconds = 300
	}
	delta := time.Now().Unix() - timestamp
	if delta < 0 {
		delta = -delta
	}
	if delta > maxSkewSeconds {
		return fmt.Errorf("WeCom callback timestamp is outside the allowed window")
	}
	return nil
}

func decryptMessage(keyText string, expectedCorpID string, encrypted string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(keyText + "=")
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid WeCom encoding AES key")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("invalid WeCom encrypted payload")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create WeCom AES cipher: %w", err)
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plaintext, ciphertext)
	plaintext, err = unpadPKCS7(plaintext)
	if err != nil {
		return nil, err
	}
	if len(plaintext) < 20 {
		return nil, fmt.Errorf("WeCom plaintext is too short")
	}
	messageLength := int(binary.BigEndian.Uint32(plaintext[16:20]))
	if messageLength < 0 || 20+messageLength > len(plaintext) {
		return nil, fmt.Errorf("WeCom plaintext message length is invalid")
	}
	message := plaintext[20 : 20+messageLength]
	receiverID := string(plaintext[20+messageLength:])
	if receiverID != expectedCorpID {
		return nil, fmt.Errorf("WeCom receiver ID mismatch")
	}
	return message, nil
}

func unpadPKCS7(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("WeCom plaintext padding is empty")
	}
	padding := int(value[len(value)-1])
	if padding <= 0 || padding > 32 || padding > len(value) {
		return nil, fmt.Errorf("WeCom plaintext padding is invalid")
	}
	for _, item := range value[len(value)-padding:] {
		if int(item) != padding {
			return nil, fmt.Errorf("WeCom plaintext padding is invalid")
		}
	}
	return value[:len(value)-padding], nil
}

func fallbackMessageID(user string, chat string, created int64, content string) string {
	digest := sha1.Sum([]byte(user + "\x00" + chat + "\x00" + strconv.FormatInt(created, 10) + "\x00" + content))
	return "wecom-" + hex.EncodeToString(digest[:])
}

var _ channels.CallbackAdapter = (*Adapter)(nil)
