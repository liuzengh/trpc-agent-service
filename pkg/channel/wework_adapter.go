//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"crypto/sha256"
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
	"unicode/utf8"

	"golang.org/x/sync/singleflight"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const (
	weWorkPKCS7BlockSize             = 32
	maxWeWorkAccessTokenCacheEntries = 1024
	maxWeWorkResponseBytes           = 64 << 10
	maxWeWorkXMLDepth                = 32
	defaultWeWorkRateLimitRetryAfter = 5 * time.Second
	maxWeWorkRetryAfterSeconds       = int64((1<<63 - 1) / int64(time.Second))
)

// WeWorkAdapter implements the Adapter interface for WeChat Work.
type WeWorkAdapter struct {
	client       *http.Client
	accessTokens map[string]*accessTokenCache
	tokenMu      sync.RWMutex
	tokenGroup   singleflight.Group
	tokenUse     uint64
}

type accessTokenCache struct {
	token     string
	expiresAt time.Time
	lastUsed  uint64
}

type weWorkMessageXML struct {
	XMLName      xml.Name `xml:"xml"`
	ToUserName   string   `xml:"ToUserName"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgId        string   `xml:"MsgId"`
	AgentID      string   `xml:"AgentID"`
	Encrypt      string   `xml:"Encrypt"`
}

// WeCom returns a small JSON envelope for both token and message requests.
// Keep errcode as a pointer so a malformed 200 response that omits the field
// cannot be mistaken for the documented success value zero.
type weWorkAPIResponse struct {
	ErrCode *int   `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

type weWorkTokenResponse struct {
	weWorkAPIResponse
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// decodeWeWorkXML validates the document with an iterative token walk before
// using encoding/xml's struct decoder. VerifySignature calls this on
// unauthenticated bytes, so a size limit alone is not sufficient: an attacker
// could otherwise force the recursive unmarshal path through extreme nesting
// before the callback signature is checked.
func decodeWeWorkXML(body []byte, destination *weWorkMessageXML) error {
	if destination == nil {
		return fmt.Errorf("WeChat Work XML destination is required")
	}
	if len(body) == 0 || !utf8.Valid(body) {
		return fmt.Errorf("WeChat Work XML must be non-empty valid UTF-8")
	}
	decoder := xml.NewDecoder(bytes.NewReader(body))
	depth, roots := 0, 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid WeChat Work XML: %w", err)
		}
		switch token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
				if roots != 1 {
					return fmt.Errorf("WeChat Work XML must contain exactly one root element")
				}
			}
			depth++
			if depth > maxWeWorkXMLDepth {
				return fmt.Errorf("WeChat Work XML exceeds %d nesting levels", maxWeWorkXMLDepth)
			}
		case xml.EndElement:
			if depth <= 0 {
				return fmt.Errorf("WeChat Work XML has an invalid closing element")
			}
			depth--
		}
	}
	if roots != 1 || depth != 0 {
		return fmt.Errorf("WeChat Work XML must contain one complete document")
	}
	if err := xml.Unmarshal(body, destination); err != nil {
		return fmt.Errorf("decode WeChat Work XML: %w", err)
	}
	if destination.XMLName.Local != "xml" || destination.XMLName.Space != "" {
		return fmt.Errorf("WeChat Work XML root element is invalid")
	}
	return nil
}

// decodeWeWorkJSON reads a bounded, single JSON document. json.Unmarshal
// rejects trailing non-whitespace and multiple top-level values, while the
// explicit byte cap prevents a provider or intermediary from consuming
// unbounded memory on an otherwise successful HTTP response.
func decodeWeWorkJSON(r io.Reader, dst any) error {
	if r == nil {
		return fmt.Errorf("provider response body is missing")
	}
	body, err := io.ReadAll(io.LimitReader(r, maxWeWorkResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxWeWorkResponseBytes {
		return fmt.Errorf("provider response exceeds %d bytes", maxWeWorkResponseBytes)
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return err
	}
	return nil
}

// VerifyURL validates and decrypts the WeCom initial echostr challenge.
func (a *WeWorkAdapter) VerifyURL(req *http.Request, binding *tenant.ChannelBinding) (string, error) {
	if a == nil || req == nil || binding == nil {
		return "", permanentCredentialError("WeWork")
	}
	encryptedEcho, err := singleWeWorkQueryValue(req, "echostr")
	if err != nil {
		return "", err
	}
	timestamp, err := singleWeWorkQueryValue(req, "timestamp")
	if err != nil {
		return "", err
	}
	nonce, err := singleWeWorkQueryValue(req, "nonce")
	if err != nil {
		return "", err
	}
	signature, err := singleWeWorkQueryValue(req, "msg_signature")
	if err != nil {
		return "", err
	}
	if err := verifyWeWorkSignature(
		binding.Token,
		timestamp,
		nonce,
		encryptedEcho,
		signature,
	); err != nil {
		return "", err
	}
	plaintext, err := decryptWeWorkMessage(binding, encryptedEcho)
	if err != nil {
		return "", fmt.Errorf("decrypt echostr: %w", err)
	}
	return string(plaintext), nil
}

// NewWeWorkAdapter creates a new WeChat Work adapter.
func NewWeWorkAdapter() *WeWorkAdapter {
	return &WeWorkAdapter{
		client: &http.Client{
			Timeout:       10 * time.Second,
			CheckRedirect: providerRedirectPolicy("qyapi.weixin.qq.com"),
		},
		accessTokens: make(map[string]*accessTokenCache),
	}
}

// VerifySignature validates the incoming webhook signature.
func (a *WeWorkAdapter) VerifySignature(req *http.Request, binding *tenant.ChannelBinding) error {
	if req == nil || binding == nil {
		return fmt.Errorf("WeChat Work request and binding are required")
	}
	msgSignature, err := singleWeWorkQueryValue(req, "msg_signature")
	if err != nil {
		return err
	}
	timestamp, err := singleWeWorkQueryValue(req, "timestamp")
	if err != nil {
		return err
	}
	nonce, err := singleWeWorkQueryValue(req, "nonce")
	if err != nil {
		return err
	}

	// Read and restore the bounded body because ParseInbound consumes the exact
	// bytes after signature verification.
	body, err := readAdapterInboundBody(req, true)
	if err != nil {
		return err
	}

	// Extract Encrypt field from XML
	var payload weWorkMessageXML
	if err := decodeWeWorkXML(body, &payload); err != nil {
		return err
	}

	return verifyWeWorkSignature(binding.Token, timestamp, nonce, payload.Encrypt, msgSignature)
}

// ParseInbound converts WeChat Work webhook to InboundMessage.
func (a *WeWorkAdapter) ParseInbound(req *http.Request, binding *tenant.ChannelBinding) (*InboundMessage, error) {
	body, err := readAdapterInboundBody(req, false)
	if err != nil {
		return nil, err
	}

	var payload weWorkMessageXML
	if err := decodeWeWorkXML(body, &payload); err != nil {
		return nil, err
	}
	if payload.Encrypt != "" {
		if binding == nil {
			return nil, fmt.Errorf("WeChat Work channel binding is required for encrypted callback")
		}
		plaintext, err := decryptWeWorkMessage(binding, payload.Encrypt)
		if err != nil {
			return nil, fmt.Errorf("decrypt callback: %w", err)
		}
		// Only authenticated plaintext fields may establish callback identity.
		payload = weWorkMessageXML{}
		if err := decodeWeWorkXML(plaintext, &payload); err != nil {
			return nil, fmt.Errorf("decode decrypted callback XML: %w", err)
		}
	}
	if binding != nil && binding.AppID != "" {
		configuredID, configuredErr := strconv.ParseUint(binding.AppID, 10, 32)
		callbackID, callbackErr := strconv.ParseUint(payload.AgentID, 10, 32)
		if configuredErr != nil || callbackErr != nil || configuredID == 0 || callbackID != configuredID {
			return nil, fmt.Errorf("callback application does not match configured agent ID")
		}
	}
	// The platform also posts image, voice, location, menu and lifecycle
	// events to the same callback. They are valid provider updates but are not
	// user text for this adapter; acknowledge them without invoking the Agent.
	if payload.MsgType != "text" || strings.TrimSpace(payload.Content) == "" {
		return nil, fmt.Errorf("%w: WeChat Work update is not a non-empty text message", ErrIgnoredInbound)
	}
	if strings.TrimSpace(payload.FromUserName) == "" || payload.MsgId == "" || payload.CreateTime <= 0 {
		return nil, fmt.Errorf("WeChat Work text message is missing required routing fields")
	}

	msg := &InboundMessage{
		ChannelType:    string(ChannelTypeWeWork),
		ExternalUserID: payload.FromUserName,
		ConversationID: payload.FromUserName, // For 1:1 chat
		MessageID:      payload.MsgId,
		ReplyToID:      payload.MsgId,
		Content:        payload.Content,
		Timestamp:      time.Unix(payload.CreateTime, 0),
		IsGroupChat:    false,
		Metadata:       map[string]string{"agentID": payload.AgentID},
	}

	return msg, nil
}

func verifyWeWorkSignature(token, timestamp, nonce, encrypted, signature string) error {
	if token == "" {
		return fmt.Errorf("channel binding has no webhook token configured")
	}
	if timestamp == "" || nonce == "" || signature == "" || encrypted == "" {
		return fmt.Errorf("missing signature parameters")
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp format")
	}
	now := time.Now().Unix()
	const maxClockSkewSeconds int64 = 300
	// Compare against bounded lower/upper limits instead of subtracting the
	// attacker-controlled timestamp. Subtraction can overflow for MinInt64 and
	// accidentally turn an ancient timestamp into an in-window value.
	if ts < now-maxClockSkewSeconds || ts > now+maxClockSkewSeconds {
		return fmt.Errorf("timestamp out of acceptable window")
	}
	parts := []string{token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	hash := sha1.Sum([]byte(strings.Join(parts, "")))
	expected := hex.EncodeToString(hash[:])
	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

func singleWeWorkQueryValue(req *http.Request, name string) (string, error) {
	if req == nil || req.URL == nil {
		return "", fmt.Errorf("missing signature parameters")
	}
	values, ok := req.URL.Query()[name]
	if !ok || len(values) != 1 || values[0] == "" || strings.ContainsAny(values[0], "\x00\r\n") {
		return "", fmt.Errorf("invalid query parameter %q", name)
	}
	return values[0], nil
}

func decryptWeWorkMessage(binding *tenant.ChannelBinding, encrypted string) ([]byte, error) {
	if binding == nil {
		return nil, fmt.Errorf("channel binding is required")
	}
	encodedKey := binding.EncodingAESKey
	if encodedKey == "" {
		encodedKey = binding.Config["encoding_aes_key"]
	}
	if encodedKey == "" {
		return nil, fmt.Errorf("encoding_aes_key is not configured")
	}
	key, err := base64.StdEncoding.DecodeString(encodedKey + "=")
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid encoding_aes_key")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("ciphertext has invalid length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plaintext, ciphertext)
	plaintext, err = unpadWeWork(plaintext)
	if err != nil {
		return nil, err
	}
	if len(plaintext) < 20 {
		return nil, fmt.Errorf("plaintext is too short")
	}
	messageLength := int(binary.BigEndian.Uint32(plaintext[16:20]))
	if messageLength < 0 || 20+messageLength > len(plaintext) {
		return nil, fmt.Errorf("plaintext message length is invalid")
	}
	message := plaintext[20 : 20+messageLength]
	receiverID := string(plaintext[20+messageLength:])
	expectedReceiver := binding.Config["corp_id"]
	if expectedReceiver == "" {
		return nil, fmt.Errorf("corp_id is not configured")
	}
	if subtle.ConstantTimeCompare([]byte(receiverID), []byte(expectedReceiver)) != 1 {
		return nil, fmt.Errorf("callback receiver does not match configured corp_id")
	}
	return message, nil
}

func unpadWeWork(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty padded plaintext")
	}
	padding := int(data[len(data)-1])
	if padding < 1 || padding > weWorkPKCS7BlockSize || padding > len(data) {
		return nil, fmt.Errorf("invalid PKCS7 padding")
	}
	for _, value := range data[len(data)-padding:] {
		if int(value) != padding {
			return nil, fmt.Errorf("invalid PKCS7 padding bytes")
		}
	}
	return data[:len(data)-padding], nil
}

// SendReply sends a reply to WeChat Work.
func (a *WeWorkAdapter) SendReply(ctx context.Context, binding *tenant.ChannelBinding, msg *OutboundMessage) error {
	if a == nil || a.client == nil {
		return retryableTransportError("WeWork")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if msg == nil || msg.ConversationID == "" || !utf8.ValidString(msg.Content) {
		return invalidOutboundMessageError("WeWork")
	}
	if err := validateWeWorkDeliveryBinding(binding); err != nil {
		return permanentCredentialError("WeWork")
	}
	agentID, err := strconv.ParseUint(binding.AppID, 10, 32)
	if err != nil || agentID == 0 {
		return permanentCredentialError("WeWork")
	}
	cacheKey := weWorkCredentialCacheKey(binding)
	token, err := a.getAccessToken(ctx, binding)
	if err != nil {
		return err
	}
	cursor, err := OutboundDeliveryCursor(msg)
	if err != nil {
		return PermanentDeliveryError(err)
	}
	chunks, err := splitUTF8ByBytes(msg.Content, 2048)
	if err != nil {
		return PermanentDeliveryError(fmt.Errorf("split WeChat Work text: %w", err))
	}
	if cursor >= len(chunks) {
		return PermanentDeliveryError(fmt.Errorf("WeChat Work delivery cursor %d exceeds %d chunks", cursor, len(chunks)))
	}
	payload := map[string]interface{}{
		"touser":  msg.ConversationID,
		"msgtype": "text",
		"agentid": agentID,
		"text": map[string]string{
			"content": chunks[cursor],
		},
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return PermanentDeliveryError(fmt.Errorf("failed to marshal payload: %w", err))
	}
	endpoint := weWorkEndpoint("/cgi-bin/message/send", url.Values{"access_token": []string{token}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return permanentRequestBuildError("WeWork")
	}
	req.Header.Set("Content-Type", "application/json")
	if err := setDeliveryIdempotencyKey(req, msg); err != nil {
		return PermanentDeliveryError(err)
	}
	telemetry.InjectHTTP(ctx, req)
	resp, requestWritten, err := doProviderRequest(a.client, req)
	if err != nil {
		// WeChat Work's message/send endpoint has no provider-side idempotency.
		// A pre-write DNS/connect failure is safe to retry, while an error after
		// net/http entered the write boundary requires reconciliation.
		return providerTransportFailure("WeWork", requestWritten)
	}
	if resp == nil {
		return providerTransportFailure("WeWork", requestWritten)
	}
	if resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		providerErr := fmt.Errorf("WeChat Work API error status=%d", resp.StatusCode)
		if resp.StatusCode == http.StatusTooManyRequests {
			return RateLimitedDeliveryError(providerErr, weWorkRetryAfter(resp.Header, time.Now()))
		}
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
			// A server error is not proof that the provider rejected the send.
			return UnknownDeliveryError(retryableTransportError("WeWork"))
		}
		return PermanentDeliveryError(providerErr)
	}
	var result weWorkAPIResponse
	if err := decodeWeWorkJSON(resp.Body, &result); err != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return UnknownDeliveryError(retryableTransportError("WeWork"))
	}
	_ = resp.Body.Close()
	if result.ErrCode == nil {
		return UnknownDeliveryError(retryableTransportError("WeWork"))
	}
	errcode := *result.ErrCode
	if errcode != 0 {
		providerErr := fmt.Errorf("WeChat Work API error code=%d", errcode)
		if errcode == 45009 || errcode == 45011 {
			return RateLimitedDeliveryError(providerErr, weWorkRetryAfter(resp.Header, time.Now()))
		}
		if errcode == 42001 {
			a.invalidateAccessToken(cacheKey, token)
			return RetryableDeliveryError(providerErr)
		}
		return PermanentDeliveryError(providerErr)
	}
	SetOutboundDeliveryProgress(msg, cursor+1, cursor+1 >= len(chunks))
	return nil
}

// invalidateAccessToken removes only the token used by the failed request.
// A concurrent refresh for the same credential may already have installed a
// newer token; comparing the value prevents an older provider response from
// deleting that replacement.
func (a *WeWorkAdapter) invalidateAccessToken(cacheKey, token string) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	cached, ok := a.accessTokens[cacheKey]
	if ok && cached != nil && cached.token == token {
		delete(a.accessTokens, cacheKey)
	}
}

// SendStreamChunk sends a streaming chunk (not supported by WeChat Work).
func (a *WeWorkAdapter) SendStreamChunk(ctx context.Context, binding *tenant.ChannelBinding, chunk *StreamChunk) error {
	return PermanentDeliveryError(fmt.Errorf("%w: WeChat Work", ErrStreamingUnsupported))
}

// SupportsStreaming returns false for WeChat Work.
func (a *WeWorkAdapter) SupportsStreaming() bool {
	return false
}

// HandleRateLimit implements rate limit backoff.
func (a *WeWorkAdapter) HandleRateLimit(err error) time.Duration {
	_, delay := DeliveryFailure(err)
	return delay
}

// getAccessToken retrieves or refreshes the access token.
func (a *WeWorkAdapter) getAccessToken(ctx context.Context, binding *tenant.ChannelBinding) (string, error) {
	if a == nil || a.client == nil {
		return "", retryableTransportError("WeWork")
	}
	if err := validateWeWorkDeliveryBinding(binding); err != nil {
		return "", permanentCredentialError("WeWork")
	}
	cacheKey := weWorkCredentialCacheKey(binding)

	if token, ok := a.cachedAccessToken(cacheKey, time.Now()); ok {
		return token, nil
	}

	value, err, _ := a.tokenGroup.Do(cacheKey, func() (interface{}, error) {
		// Double-check after joining the per-credential singleflight.
		if token, ok := a.cachedAccessToken(cacheKey, time.Now()); ok {
			return token, nil
		}

		endpoint := weWorkEndpoint("/cgi-bin/gettoken", url.Values{
			"corpid":     []string{binding.Config["corp_id"]},
			"corpsecret": []string{binding.Secret},
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, permanentRequestBuildError("WeWork")
		}
		telemetry.InjectHTTP(ctx, req)
		resp, err := a.client.Do(req)
		if err != nil {
			return nil, retryableTransportError("WeWork")
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, RateLimitedDeliveryError(
				fmt.Errorf("WeChat Work token endpoint rate limited"),
				weWorkRetryAfter(resp.Header, time.Now()),
			)
		}
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
			return nil, RetryableDeliveryError(fmt.Errorf("WeChat Work token endpoint returned %d", resp.StatusCode))
		}
		if resp.StatusCode >= 400 {
			return nil, PermanentDeliveryError(fmt.Errorf("WeChat Work token endpoint returned %d", resp.StatusCode))
		}

		var result weWorkTokenResponse
		if err := decodeWeWorkJSON(resp.Body, &result); err != nil {
			return nil, RetryableDeliveryError(fmt.Errorf("failed to decode response: %w", err))
		}
		if result.ErrCode == nil {
			return nil, RetryableDeliveryError(fmt.Errorf("WeChat Work token response omitted errcode"))
		}
		errcode := *result.ErrCode
		if errcode != 0 || !validWeWorkAccessToken(result.AccessToken) {
			providerErr := fmt.Errorf("WeChat Work API error code=%d", errcode)
			if errcode == 45009 || errcode == 45011 {
				return nil, RateLimitedDeliveryError(providerErr, weWorkRetryAfter(resp.Header, time.Now()))
			}
			if errcode == 42001 {
				return nil, RetryableDeliveryError(providerErr)
			}
			return nil, PermanentDeliveryError(providerErr)
		}
		ttl := time.Duration(result.ExpiresIn) * time.Second
		if ttl <= 0 {
			ttl = time.Hour
		}
		a.storeAccessToken(cacheKey, result.AccessToken, time.Now().Add(ttl*3/4))
		return result.AccessToken, nil
	})
	if err != nil {
		return "", err
	}
	token, ok := value.(string)
	if !ok || !validWeWorkAccessToken(token) {
		return "", retryableTransportError("WeWork")
	}
	return token, nil
}

func (a *WeWorkAdapter) cachedAccessToken(cacheKey string, now time.Time) (string, bool) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	cached, ok := a.accessTokens[cacheKey]
	if !ok {
		return "", false
	}
	if cached == nil || !now.Before(cached.expiresAt) {
		delete(a.accessTokens, cacheKey)
		return "", false
	}
	a.tokenUse++
	cached.lastUsed = a.tokenUse
	return cached.token, true
}

func (a *WeWorkAdapter) storeAccessToken(cacheKey, token string, expiresAt time.Time) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	if a.accessTokens == nil {
		a.accessTokens = make(map[string]*accessTokenCache)
	}
	now := time.Now()
	for key, cached := range a.accessTokens {
		if cached == nil || !now.Before(cached.expiresAt) {
			delete(a.accessTokens, key)
		}
	}
	if _, exists := a.accessTokens[cacheKey]; !exists && len(a.accessTokens) >= maxWeWorkAccessTokenCacheEntries {
		var leastRecentlyUsedKey string
		var leastRecentlyUsed uint64
		for key, cached := range a.accessTokens {
			if leastRecentlyUsedKey == "" || cached.lastUsed < leastRecentlyUsed ||
				(cached.lastUsed == leastRecentlyUsed && key < leastRecentlyUsedKey) {
				leastRecentlyUsedKey = key
				leastRecentlyUsed = cached.lastUsed
			}
		}
		delete(a.accessTokens, leastRecentlyUsedKey)
	}
	a.tokenUse++
	a.accessTokens[cacheKey] = &accessTokenCache{
		token:     token,
		expiresAt: expiresAt,
		lastUsed:  a.tokenUse,
	}
}

func weWorkCredentialCacheKey(binding *tenant.ChannelBinding) string {
	digest := sha256.Sum256([]byte(binding.Config["corp_id"] + "\x00" + binding.Secret))
	return hex.EncodeToString(digest[:])
}

func weWorkEndpoint(path string, query url.Values) string {
	return (&url.URL{
		Scheme:   "https",
		Host:     "qyapi.weixin.qq.com",
		Path:     path,
		RawQuery: query.Encode(),
	}).String()
}

// weWorkRetryAfter accepts the single Retry-After value from a provider
// response. Ambiguous, expired, or malformed values retain the established
// conservative backoff; an oversized numeric value saturates so the delivery
// pipeline can apply its own configured retry maximum.
func weWorkRetryAfter(header http.Header, now time.Time) time.Duration {
	values := header.Values("Retry-After")
	if len(values) != 1 {
		return defaultWeWorkRateLimitRetryAfter
	}
	value := strings.TrimSpace(values[0])
	if retryAfterDeltaSeconds(value) {
		seconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil || seconds > maxWeWorkRetryAfterSeconds {
			return time.Duration(maxWeWorkRetryAfterSeconds) * time.Second
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return defaultWeWorkRateLimitRetryAfter
}

func retryAfterDeltaSeconds(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// splitMessageByRunes splits providers whose documented limit is measured in
// Unicode characters rather than encoded bytes.
func splitMessageByRunes(content string, maxLength int) []string {
	if maxLength <= 0 {
		return nil
	}
	if len(content) <= maxLength {
		return []string{content}
	}

	var chunks []string
	runes := []rune(content)

	for len(runes) > 0 {
		end := maxLength
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[:end]))
		runes = runes[end:]
	}

	return chunks
}

// splitUTF8ByBytes enforces a provider byte limit without cutting a UTF-8
// sequence. It rejects malformed text so delivery is deterministic rather
// than silently replacing bytes during JSON encoding.
func splitUTF8ByBytes(content string, maxBytes int) ([]string, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maximum chunk size must be positive")
	}
	if !utf8.ValidString(content) {
		return nil, fmt.Errorf("content is not valid UTF-8")
	}
	if len(content) <= maxBytes {
		return []string{content}, nil
	}

	chunks := make([]string, 0, (len(content)+maxBytes-1)/maxBytes)
	for start := 0; start < len(content); {
		end := start
		for end < len(content) {
			_, size := utf8.DecodeRuneInString(content[end:])
			if end+size-start > maxBytes {
				break
			}
			end += size
		}
		if end == start {
			return nil, fmt.Errorf("maximum chunk size cannot hold one Unicode character")
		}
		chunks = append(chunks, content[start:end])
		start = end
	}
	return chunks, nil
}
