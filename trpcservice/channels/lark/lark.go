package lark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	Channel = "lark"

	DefaultBaseURL = "https://open.feishu.cn"
	LarkBaseURL    = "https://open.larksuite.com"

	tenantAccessTokenPath = "/open-apis/auth/v3/tenant_access_token/internal"
	messageCreatePath     = "/open-apis/im/v1/messages"

	ReceiverIDTypeOpenID  = "open_id"
	ReceiverIDTypeUserID  = "user_id"
	ReceiverIDTypeUnionID = "union_id"
	ReceiverIDTypeEmail   = "email"
	ReceiverIDTypeChatID  = "chat_id"

	DefaultRequestTimeout = 10 * time.Second
	MaxRequestTimeout     = 5 * time.Minute
)

var (
	ErrInvalidConfig        = errors.New("lark: invalid sender configuration")
	ErrCredentialResolution = errors.New("lark: credential resolution failed")
	ErrTokenUnavailable     = errors.New("lark: access token service unavailable")
	ErrTokenRateLimited     = errors.New("lark: access token service rate limited")
	ErrTokenAuth            = errors.New("lark: access token authentication failed")
	ErrTokenMalformed       = errors.New("lark: malformed access token response")
	ErrProviderAuth         = errors.New("lark: provider authentication failed")
	ErrProviderForbidden    = errors.New("lark: provider permission denied")
	ErrProviderRejected     = errors.New("lark: provider rejected message")
	ErrProviderMalformed    = errors.New("lark: malformed provider response")
)

// Binding is the server-owned identity and routing configuration for one Lark
// tenant binding. It is never decoded from an Outbox payload.
type Binding struct {
	TenantID       string
	BindingID      string
	Channel        string
	AppID          string
	SecretRef      string
	ReceiverIDType string
	Enabled        bool
}

func (b Binding) Validate() error {
	if !validIdentity(b.TenantID) || !validIdentity(b.BindingID) || b.Channel != Channel ||
		!validOpaque(b.AppID, 256) || !validOpaque(b.SecretRef, 256) || !validReceiverIDType(b.ReceiverIDType) {
		return ErrInvalidConfig
	}
	return nil
}

// SecretResolver is the only credential lookup boundary. Implementations must
// return the secret to memory only; callers must not persist or log it.
type SecretResolver interface {
	Resolve(context.Context, string) (string, error)
}

type SecretResolverFunc func(context.Context, string) (string, error)

func (f SecretResolverFunc) Resolve(ctx context.Context, ref string) (string, error) {
	if f == nil {
		return "", ErrCredentialResolution
	}
	return f(ctx, ref)
}

// TokenResolver returns a short-lived tenant access token for a validated,
// server-owned binding. It does not receive an Outbox payload or repository.
type TokenResolver interface {
	Resolve(context.Context, Binding) (string, error)
}

type TokenResolverFunc func(context.Context, Binding) (string, error)

func (f TokenResolverFunc) Resolve(ctx context.Context, binding Binding) (string, error) {
	if f == nil {
		return "", ErrCredentialResolution
	}
	return f(ctx, binding)
}

// AccessTokenResolver is a descriptive compatibility alias.
type AccessTokenResolver = TokenResolver

// HTTPAccessTokenResolver implements the official tenant_access_token flow.
type HTTPAccessTokenResolver struct {
	secrets          SecretResolver
	client           channels.HTTPDoer
	baseURL          string
	maxResponseBytes int
}

type TokenResolverConfig struct {
	Secrets          SecretResolver
	Client           channels.HTTPDoer
	BaseURL          string
	MaxResponseBytes int
}

func NewHTTPAccessTokenResolver(config TokenResolverConfig) (*HTTPAccessTokenResolver, error) {
	if config.Secrets == nil || config.Client == nil {
		return nil, ErrInvalidConfig
	}
	baseURL, err := validateBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = channels.DefaultResponseBodyLimit
	}
	if config.MaxResponseBytes < 1 || config.MaxResponseBytes > channels.MaxReplyResponseBytes {
		return nil, ErrInvalidConfig
	}
	return &HTTPAccessTokenResolver{secrets: config.Secrets, client: config.Client, baseURL: baseURL, maxResponseBytes: config.MaxResponseBytes}, nil
}

func (r *HTTPAccessTokenResolver) Resolve(ctx context.Context, binding Binding) (string, error) {
	if r == nil || r.client == nil || r.secrets == nil || ctx == nil {
		return "", ErrInvalidConfig
	}
	if err := binding.Validate(); err != nil {
		return "", ErrInvalidConfig
	}
	secret, err := r.secrets.Resolve(ctx, binding.SecretRef)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "", ErrTokenTimeout
		}
		return "", ErrCredentialResolution
	}
	if secret == "" || len(secret) > 4096 {
		return "", ErrCredentialResolution
	}
	body, err := json.Marshal(struct {
		AppID     string `json:"app_id"`
		AppSecret string `json:"app_secret"`
	}{AppID: binding.AppID, AppSecret: secret})
	if err != nil {
		return "", ErrCredentialResolution
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+tenantAccessTokenPath, bytes.NewReader(body))
	if err != nil {
		return "", ErrTokenMalformed
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return "", classifyTokenTransport(ctx, err)
	}
	if response == nil || response.Body == nil {
		return "", ErrTokenMalformed
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", classifyTokenStatus(response.StatusCode)
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, int64(r.maxResponseBytes)+1))
	if err != nil || len(encoded) > r.maxResponseBytes {
		return "", ErrTokenMalformed
	}
	var result struct {
		Code   int64  `json:"code"`
		Token  string `json:"tenant_access_token"`
		Expire int64  `json:"expire"`
	}
	if len(encoded) == 0 || json.Unmarshal(encoded, &result) != nil || result.Code != 0 || !validOpaque(result.Token, 4096) || result.Expire < 1 {
		return "", ErrTokenMalformed
	}
	return result.Token, nil
}

var ErrTokenTimeout = errors.New("lark: access token request timed out")

// TransportError lets a transport prove that a request was not written. A
// plain network error remains unknown because the provider may have accepted it.
type TransportError struct {
	Err  error
	Sent bool
}

func (e TransportError) Error() string     { return "lark: transport request failed" }
func (e TransportError) Unwrap() error     { return e.Err }
func (e TransportError) RequestSent() bool { return e.Sent }

// Sender is a real Lark message sender with no durable-state dependency.
type Sender struct {
	bindings         map[string]Binding
	tokens           TokenResolver
	client           channels.HTTPDoer
	baseURL          string
	timeout          time.Duration
	maxResponseBytes int
}

type SenderConfig struct {
	Bindings         []Binding
	Tokens           TokenResolver
	Client           channels.HTTPDoer
	BaseURL          string
	Timeout          time.Duration
	MaxResponseBytes int
}

func NewSender(config SenderConfig) (*Sender, error) {
	if config.Tokens == nil || config.Client == nil {
		return nil, ErrInvalidConfig
	}
	baseURL, err := validateBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultRequestTimeout
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = channels.DefaultResponseBodyLimit
	}
	if config.Timeout <= 0 || config.Timeout > MaxRequestTimeout || config.MaxResponseBytes < 1 || config.MaxResponseBytes > channels.MaxReplyResponseBytes {
		return nil, ErrInvalidConfig
	}
	bindings := make(map[string]Binding, len(config.Bindings))
	for _, binding := range config.Bindings {
		if err := binding.Validate(); err != nil {
			return nil, err
		}
		if _, exists := bindings[binding.BindingID]; exists {
			return nil, ErrInvalidConfig
		}
		bindings[binding.BindingID] = binding
	}
	return &Sender{bindings: bindings, tokens: config.Tokens, client: config.Client, baseURL: baseURL, timeout: config.Timeout, maxResponseBytes: config.MaxResponseBytes}, nil
}

var _ channels.Sender = (*Sender)(nil)

func (s *Sender) Send(ctx context.Context, message storage.OutboxMessage) channels.SenderOutcome {
	if s == nil || s.client == nil || s.tokens == nil || ctx == nil {
		return permanent(channels.LarkSenderRejectedCode)
	}
	outbound, err := channels.DecodeReplyOutboxMessage(message)
	if err != nil {
		return permanent(classifyPayload(err))
	}
	payload := outbound.Payload
	if payload.Channel != Channel {
		return permanent(channels.LarkSenderRejectedCode)
	}
	binding, ok := s.bindings[payload.BindingID]
	if !ok || !binding.Enabled {
		return permanent(channels.LarkSenderNotConfiguredCode)
	}
	if binding.TenantID != payload.TenantID || binding.Channel != payload.Channel || !destinationMatchesBinding(payload.DestinationType, binding.ReceiverIDType) {
		return permanent(channels.LarkSenderInvalidDestinationCode)
	}

	sendCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	token, err := s.tokens.Resolve(sendCtx, binding)
	if err != nil {
		return classifyTokenError(sendCtx, err)
	}
	if !validOpaque(token, 4096) {
		return permanent(channels.LarkSenderAuthFailedCode)
	}
	body, err := messageBody(payload, outbound.IdempotencyKey)
	if err != nil {
		return permanent(channels.LarkSenderRejectedCode)
	}
	target := s.baseURL + messageCreatePath + "?" + url.Values{"receive_id_type": {binding.ReceiverIDType}}.Encode()
	request, err := http.NewRequestWithContext(sendCtx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return permanent(channels.LarkSenderRejectedCode)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := s.client.Do(request)
	if err != nil {
		return classifyMessageTransport(sendCtx, err)
	}
	if response == nil || response.Body == nil {
		return unknown()
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Retry-After is intentionally ignored. The existing bounded Dispatcher
		// RetryPolicy owns timing and no provider header enters durable state.
		return classifyMessageStatus(response.StatusCode)
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, int64(s.maxResponseBytes)+1))
	if err != nil {
		return unknown()
	}
	if len(encoded) == 0 {
		return unknown()
	}
	if len(encoded) > s.maxResponseBytes {
		return permanent(channels.LarkSenderMalformedResponseCode)
	}
	if err := validateMessageSuccess(encoded); err != nil {
		switch {
		case errors.Is(err, ErrProviderAuth):
			return permanent(channels.LarkSenderAuthFailedCode)
		case errors.Is(err, ErrProviderForbidden):
			return permanent(channels.LarkSenderForbiddenCode)
		case errors.Is(err, ErrProviderRejected):
			return permanent(channels.LarkSenderRejectedCode)
		default:
			return permanent(channels.LarkSenderMalformedResponseCode)
		}
	}
	return channels.SenderOutcome{Class: channels.OutcomeDelivered}
}

func messageBody(payload channels.ReplyOutboxPayload, idempotencyKey string) ([]byte, error) {
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: payload.ReplyText})
	if err != nil {
		return nil, ErrProviderMalformed
	}
	return json.Marshal(struct {
		ReceiveID string `json:"receive_id"`
		MsgType   string `json:"msg_type"`
		Content   string `json:"content"`
		UUID      string `json:"uuid"`
	}{ReceiveID: payload.DestinationID, MsgType: "text", Content: string(content), UUID: providerUUID(idempotencyKey)})
}

func providerUUID(idempotencyKey string) string {
	digest := sha256.Sum256([]byte(idempotencyKey))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func validateMessageSuccess(body []byte) error {
	var result struct {
		Code int64 `json:"code"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &result) != nil {
		return ErrProviderMalformed
	}
	if result.Code != 0 {
		return ErrProviderRejected
	}
	if !validOpaque(result.Data.MessageID, channels.MaxRoutingIdentityBytes) {
		return ErrProviderMalformed
	}
	return nil
}

func classifyPayload(err error) string {
	if errors.Is(err, channels.ErrInvalidDestination) || errors.Is(err, channels.ErrUnsupportedDestination) {
		return channels.LarkSenderInvalidDestinationCode
	}
	return channels.LarkSenderRejectedCode
}

func classifyTokenError(ctx context.Context, err error) channels.SenderOutcome {
	switch {
	case errors.Is(err, ErrTokenTimeout), errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return retryable(channels.LarkSenderTimeoutCode)
	case errors.Is(err, ErrTokenRateLimited):
		return retryable(channels.LarkSenderRateLimitedCode)
	case errors.Is(err, ErrTokenUnavailable):
		return retryable(channels.LarkSenderUnavailableCode)
	case errors.Is(err, ErrTokenAuth), errors.Is(err, ErrCredentialResolution):
		return permanent(channels.LarkSenderAuthFailedCode)
	case errors.Is(err, ErrTokenMalformed):
		return permanent(channels.LarkSenderMalformedResponseCode)
	default:
		return permanent(channels.LarkSenderAuthFailedCode)
	}
}

func classifyTokenTransport(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrTokenTimeout
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return ErrTokenTimeout
	}
	return ErrTokenUnavailable
}

func classifyTokenStatus(status int) error {
	switch {
	case status == http.StatusTooManyRequests:
		return ErrTokenRateLimited
	case status >= 500:
		return ErrTokenUnavailable
	case status >= 400:
		return ErrTokenAuth
	default:
		return ErrTokenMalformed
	}
}

func classifyMessageTransport(ctx context.Context, err error) channels.SenderOutcome {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return retryable(channels.LarkSenderTimeoutCode)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return retryable(channels.LarkSenderTimeoutCode)
	}
	var requestState interface{ RequestSent() bool }
	if errors.As(err, &requestState) && !requestState.RequestSent() {
		return retryable(channels.LarkSenderUnavailableCode)
	}
	return unknown()
}

func classifyMessageStatus(status int) channels.SenderOutcome {
	switch {
	case status == http.StatusTooManyRequests:
		return retryable(channels.LarkSenderRateLimitedCode)
	case status >= 500:
		return retryable(channels.LarkSenderUnavailableCode)
	case status == http.StatusUnauthorized:
		return permanent(channels.LarkSenderAuthFailedCode)
	case status == http.StatusForbidden:
		return permanent(channels.LarkSenderForbiddenCode)
	case status >= 400:
		return permanent(channels.LarkSenderRejectedCode)
	default:
		return permanent(channels.LarkSenderRejectedCode)
	}
}

func permanent(code string) channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: code}
}

func retryable(code string) channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomeRetryableFailure, Code: code}
}

func unknown() channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomeUnknown, Code: channels.LarkDeliveryOutcomeUnknownCode}
}

func destinationMatchesBinding(destinationType, receiverIDType string) bool {
	if destinationType == channels.DestinationTypeChat {
		return receiverIDType == ReceiverIDTypeChatID
	}
	return destinationType == channels.DestinationTypeUser && receiverIDType != ReceiverIDTypeChatID
}

func validReceiverIDType(value string) bool {
	switch value {
	case ReceiverIDTypeOpenID, ReceiverIDTypeUserID, ReceiverIDTypeUnionID, ReceiverIDTypeEmail, ReceiverIDTypeChatID:
		return true
	default:
		return false
	}
}

func validateBaseURL(value string) (string, error) {
	if value == "" {
		value = DefaultBaseURL
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", ErrInvalidConfig
	}
	if parsed.Host != "open.feishu.cn" && parsed.Host != "open.larksuite.com" {
		return "", ErrInvalidConfig
	}
	return strings.TrimRight(value, "/"), nil
}

func validIdentity(value string) bool { return validOpaque(value, 128) }

func validOpaque(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
