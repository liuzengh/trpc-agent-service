package telegram

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	Channel               = "telegram"
	DefaultBaseURL        = "https://api.telegram.org"
	apiMethodPrefix       = "/bot"
	getMeMethod           = "getMe"
	sendMessageMethod     = "sendMessage"
	MaxMessageTextRunes   = 4096
	MaxWebhookBodyBytes   = 1 << 20
	DefaultRequestTimeout = 10 * time.Second
	MaxRequestTimeout     = 5 * time.Minute
	HeaderSecretToken     = "X-Telegram-Bot-Api-Secret-Token"
)

const (
	TelegramSenderTimeoutCode            = channels.TelegramSenderTimeoutCode
	TelegramSenderRateLimitedCode        = channels.TelegramSenderRateLimitedCode
	TelegramSenderUnavailableCode        = channels.TelegramSenderUnavailableCode
	TelegramSenderInvalidDestinationCode = channels.TelegramSenderInvalidDestinationCode
	TelegramSenderAuthFailedCode         = channels.TelegramSenderAuthFailedCode
	TelegramSenderForbiddenCode          = channels.TelegramSenderForbiddenCode
	TelegramSenderRejectedCode           = channels.TelegramSenderRejectedCode
	TelegramSenderMessageTooLongCode     = channels.TelegramSenderMessageTooLongCode
	TelegramSenderNotConfiguredCode      = channels.TelegramSenderNotConfiguredCode
	TelegramSenderMalformedResponseCode  = channels.TelegramSenderMalformedResponseCode
	TelegramDeliveryOutcomeUnknownCode   = channels.TelegramDeliveryOutcomeUnknownCode
)

var (
	ErrInvalidConfig        = errors.New("telegram: invalid sender configuration")
	ErrNotConfigured        = errors.New("telegram: sender is not configured")
	ErrCredentialResolution = errors.New("telegram: credential resolution failed")
	ErrAuthFailed           = errors.New("telegram: bot authentication failed")
	ErrForbidden            = errors.New("telegram: bot is forbidden")
	ErrRejected             = errors.New("telegram: provider rejected request")
	ErrRateLimited          = errors.New("telegram: provider rate limited request")
	ErrUnavailable          = errors.New("telegram: provider unavailable")
	ErrTimeout              = errors.New("telegram: request timed out")
	ErrMalformedResponse    = errors.New("telegram: malformed provider response")
	ErrOutcomeUnknown       = errors.New("telegram: delivery outcome is unknown")
	ErrInvalidDestination   = errors.New("telegram: invalid destination")
	ErrMessageTooLong       = errors.New("telegram: message is too long")
	ErrInvalidWebhookSecret = errors.New("telegram: invalid webhook secret")
	ErrUnsupportedUpdate    = errors.New("telegram: unsupported webhook update")
	ErrInvalidUpdate        = errors.New("telegram: invalid webhook update")
	ErrUnsupportedChat      = errors.New("telegram: unsupported chat type")
	ErrUnsupportedThread    = errors.New("telegram: unsupported message thread")
)

type Binding struct {
	TenantID          string
	BindingID         string
	Channel           string
	BotTokenSecretRef string
	WebhookSecretRef  string
	Enabled           bool
}

func (b Binding) Validate() error {
	if !validIdentity(b.TenantID) || !validIdentity(b.BindingID) || b.Channel != Channel ||
		!validReference(b.BotTokenSecretRef) || (b.WebhookSecretRef != "" && !validReference(b.WebhookSecretRef)) {
		return ErrInvalidConfig
	}
	return nil
}

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

// EnvironmentSecretResolver is opt-in and only resolves env:// references.
// Values remain in the process memory and are never included in errors.
type EnvironmentSecretResolver struct{}

func (EnvironmentSecretResolver) Resolve(ctx context.Context, ref string) (string, error) {
	if ctx == nil {
		return "", ErrCredentialResolution
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !strings.HasPrefix(ref, "env://") {
		return "", ErrCredentialResolution
	}
	name := strings.TrimPrefix(ref, "env://")
	if !validEnvironmentName(name) {
		return "", ErrCredentialResolution
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", ErrCredentialResolution
	}
	return value, nil
}

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

type TransportError struct {
	Err  error
	Sent bool
}

func (e TransportError) Error() string {
	return "telegram: transport request failed"
}

func (e TransportError) Unwrap() error {
	return e.Err
}

func (e TransportError) RequestSent() bool {
	return e.Sent
}

type SenderConfig struct {
	Bindings         []Binding
	Tokens           TokenResolver
	Client           channels.HTTPDoer
	Timeout          time.Duration
	MaxResponseBytes int
}

type Sender struct {
	bindings         map[string]Binding
	tokens           TokenResolver
	client           channels.HTTPDoer
	timeout          time.Duration
	maxResponseBytes int
}

func NewSender(config SenderConfig) (*Sender, error) {
	if config.Tokens == nil || config.Client == nil || len(config.Bindings) == 0 {
		return nil, ErrInvalidConfig
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
	return &Sender{bindings: bindings, tokens: config.Tokens, client: config.Client, timeout: config.Timeout, maxResponseBytes: config.MaxResponseBytes}, nil
}

var _ channels.Sender = (*Sender)(nil)

func (s *Sender) GetMe(ctx context.Context, bindingID string) error {
	binding, err := s.binding(bindingID)
	if err != nil {
		return err
	}
	if ctx == nil {
		return ErrInvalidConfig
	}
	sendCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	token, code := s.resolveToken(sendCtx, binding)
	if code != "" {
		return safeError(code)
	}
	response, body, outcome := s.execute(sendCtx, token, getMeMethod, nil)
	if outcome != nil {
		return safeError(outcome.Code)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return safeError(classifyHTTPStatus(response.StatusCode).Code)
	}
	return safeError(classifyGetMeSuccess(body))
}

func (s *Sender) Send(ctx context.Context, message storage.OutboxMessage) channels.SenderOutcome {
	if s == nil || s.client == nil || s.tokens == nil || ctx == nil {
		return permanent(channels.TelegramSenderNotConfiguredCode)
	}
	outbound, err := channels.DecodeReplyOutboxMessage(message)
	if err != nil {
		return permanent(classifyPayloadError(err))
	}
	payload := outbound.Payload
	if payload.Channel != Channel {
		return permanent(channels.TelegramSenderRejectedCode)
	}
	binding, err := s.binding(payload.BindingID)
	if err != nil {
		return permanent(channels.TelegramSenderNotConfiguredCode)
	}
	if !binding.Enabled {
		return permanent(channels.TelegramSenderNotConfiguredCode)
	}
	if binding.TenantID != payload.TenantID || binding.Channel != payload.Channel || payload.DestinationType != channels.DestinationTypeChat {
		return permanent(channels.TelegramSenderInvalidDestinationCode)
	}
	if err := channels.ValidateDestination(payload.Channel, payload.DestinationType, payload.DestinationID); err != nil {
		return permanent(channels.TelegramSenderInvalidDestinationCode)
	}
	if err := channels.ValidateMessageThread(payload.Channel, payload.MessageThreadID); err != nil {
		return permanent(channels.TelegramSenderInvalidDestinationCode)
	}
	if !utf8.ValidString(payload.ReplyText) || payload.ReplyText == "" {
		return permanent(channels.TelegramSenderRejectedCode)
	}
	if utf8.RuneCountInString(payload.ReplyText) > MaxMessageTextRunes {
		return permanent(channels.TelegramSenderMessageTooLongCode)
	}
	body, err := messageBody(payload)
	if err != nil {
		return permanent(channels.TelegramSenderRejectedCode)
	}
	sendCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	token, code := s.resolveToken(sendCtx, binding)
	if code != "" {
		return outcomeForCode(code)
	}
	response, encoded, outcome := s.execute(sendCtx, token, sendMessageMethod, body)
	if outcome != nil {
		return *outcome
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return classifyHTTPStatus(response.StatusCode)
	}
	return classifySendMessageSuccess(encoded)
}

func (s *Sender) binding(bindingID string) (Binding, error) {
	if s == nil || bindingID == "" {
		return Binding{}, ErrNotConfigured
	}
	binding, ok := s.bindings[bindingID]
	if !ok {
		return Binding{}, ErrNotConfigured
	}
	return binding, nil
}

func (s *Sender) resolveToken(ctx context.Context, binding Binding) (string, string) {
	token, err := s.tokens.Resolve(ctx, binding)
	if err != nil {
		return "", classifyTokenError(ctx, err)
	}
	if !validBotToken(token) {
		return "", channels.TelegramSenderAuthFailedCode
	}
	return token, ""
}

func (s *Sender) execute(ctx context.Context, token, method string, body []byte) (*http.Response, []byte, *channels.SenderOutcome) {
	if err := ctx.Err(); err != nil {
		outcome := classifyTransportError(ctx, TransportError{Err: err, Sent: false})
		return nil, nil, &outcome
	}
	target := DefaultBaseURL + apiMethodPrefix + token + "/" + method
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		outcome := permanent(channels.TelegramSenderRejectedCode)
		return nil, nil, &outcome
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		outcome := classifyTransportError(ctx, err)
		return nil, nil, &outcome
	}
	if response == nil || response.Body == nil {
		outcome := unknown()
		return nil, nil, &outcome
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		return response, nil, nil
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, int64(s.maxResponseBytes)+1))
	if err != nil {
		outcome := unknown()
		return nil, nil, &outcome
	}
	if len(encoded) > s.maxResponseBytes {
		outcome := permanent(channels.TelegramSenderMalformedResponseCode)
		return nil, nil, &outcome
	}
	return response, encoded, nil
}

func messageBody(payload channels.ReplyOutboxPayload) ([]byte, error) {
	request := struct {
		ChatID          string `json:"chat_id"`
		Text            string `json:"text"`
		MessageThreadID *int64 `json:"message_thread_id,omitempty"`
	}{ChatID: payload.DestinationID, Text: payload.ReplyText, MessageThreadID: payload.MessageThreadID}
	return json.Marshal(request)
}

type responseEnvelope struct {
	OK         *bool           `json:"ok"`
	Result     json.RawMessage `json:"result"`
	ErrorCode  *int            `json:"error_code"`
	Parameters struct {
		RetryAfter *int `json:"retry_after"`
	} `json:"parameters"`
}

func classifyGetMeSuccess(body []byte) string {
	var envelope responseEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.OK == nil || !*envelope.OK || len(envelope.Result) == 0 {
		if envelope.OK != nil && !*envelope.OK && envelope.ErrorCode != nil {
			return classifyProviderErrorCode(*envelope.ErrorCode).Code
		}
		return channels.TelegramSenderMalformedResponseCode
	}
	var user struct {
		ID    *int64 `json:"id"`
		IsBot *bool  `json:"is_bot"`
	}
	if json.Unmarshal(envelope.Result, &user) != nil || user.ID == nil || *user.ID <= 0 || user.IsBot == nil || !*user.IsBot {
		return channels.TelegramSenderMalformedResponseCode
	}
	return ""
}

func classifySendMessageSuccess(body []byte) channels.SenderOutcome {
	var envelope responseEnvelope
	if json.Unmarshal(body, &envelope) != nil || envelope.OK == nil {
		return permanent(channels.TelegramSenderMalformedResponseCode)
	}
	if !*envelope.OK {
		if envelope.ErrorCode == nil {
			return permanent(channels.TelegramSenderMalformedResponseCode)
		}
		return classifyProviderErrorCode(*envelope.ErrorCode)
	}
	if len(envelope.Result) == 0 {
		return permanent(channels.TelegramSenderMalformedResponseCode)
	}
	var message struct {
		MessageID *int64 `json:"message_id"`
	}
	if json.Unmarshal(envelope.Result, &message) != nil || message.MessageID == nil || *message.MessageID <= 0 {
		return permanent(channels.TelegramSenderMalformedResponseCode)
	}
	return channels.SenderOutcome{Class: channels.OutcomeDelivered}
}

func classifyHTTPStatus(status int) channels.SenderOutcome {
	switch {
	case status == http.StatusTooManyRequests:
		return retryable(channels.TelegramSenderRateLimitedCode)
	case status >= 500:
		return retryable(channels.TelegramSenderUnavailableCode)
	case status == http.StatusUnauthorized:
		return permanent(channels.TelegramSenderAuthFailedCode)
	case status == http.StatusForbidden:
		return permanent(channels.TelegramSenderForbiddenCode)
	case status >= 400:
		return permanent(channels.TelegramSenderRejectedCode)
	default:
		return permanent(channels.TelegramSenderMalformedResponseCode)
	}
}

func classifyProviderErrorCode(code int) channels.SenderOutcome {
	switch {
	case code == http.StatusTooManyRequests:
		return retryable(channels.TelegramSenderRateLimitedCode)
	case code >= 500:
		return retryable(channels.TelegramSenderUnavailableCode)
	case code == http.StatusUnauthorized:
		return permanent(channels.TelegramSenderAuthFailedCode)
	case code == http.StatusForbidden:
		return permanent(channels.TelegramSenderForbiddenCode)
	default:
		return permanent(channels.TelegramSenderRejectedCode)
	}
}

func classifyTokenError(ctx context.Context, err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return channels.TelegramSenderTimeoutCode
	case errors.Is(err, ErrRateLimited):
		return channels.TelegramSenderRateLimitedCode
	case errors.Is(err, ErrUnavailable):
		return channels.TelegramSenderUnavailableCode
	case errors.Is(err, ErrAuthFailed), errors.Is(err, ErrCredentialResolution):
		return channels.TelegramSenderAuthFailedCode
	default:
		return channels.TelegramSenderAuthFailedCode
	}
}

func classifyTransportError(ctx context.Context, err error) channels.SenderOutcome {
	var sentState interface{ RequestSent() bool }
	if errors.As(err, &sentState) {
		if sentState.RequestSent() {
			return unknown()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return retryable(channels.TelegramSenderTimeoutCode)
		}
		var networkErr net.Error
		if errors.As(err, &networkErr) && networkErr.Timeout() {
			return retryable(channels.TelegramSenderTimeoutCode)
		}
		return retryable(channels.TelegramSenderUnavailableCode)
	}
	// A standard http.Client error does not prove whether Telegram accepted the
	// request, so timeout and cancellation remain OutcomeUnknown here.
	_ = ctx
	return unknown()
}

func classifyPayloadError(err error) string {
	if errors.Is(err, channels.ErrInvalidDestination) || errors.Is(err, channels.ErrUnsupportedDestination) || errors.Is(err, storage.ErrTenantMismatch) {
		return channels.TelegramSenderInvalidDestinationCode
	}
	return channels.TelegramSenderRejectedCode
}

func outcomeForCode(code string) channels.SenderOutcome {
	switch code {
	case channels.TelegramSenderTimeoutCode:
		return retryable(code)
	case channels.TelegramSenderRateLimitedCode:
		return retryable(code)
	case channels.TelegramSenderUnavailableCode:
		return retryable(code)
	case channels.TelegramSenderAuthFailedCode:
		return permanent(code)
	default:
		return permanent(channels.TelegramSenderRejectedCode)
	}
}

func permanent(code string) channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: code}
}

func retryable(code string) channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomeRetryableFailure, Code: code}
}

func unknown() channels.SenderOutcome {
	return channels.SenderOutcome{Class: channels.OutcomeUnknown, Code: channels.TelegramDeliveryOutcomeUnknownCode}
}

type safeCodeError string

func (e safeCodeError) Error() string { return string(e) }

func safeError(code string) error {
	if code == "" {
		return nil
	}
	return safeCodeError(code)
}

type WebhookConfig struct {
	Binding Binding
	Secret  string
}

type WebhookAdapter struct {
	binding Binding
	secret  string
}

// Adapter is the stable name for the Telegram webhook boundary.
type Adapter = WebhookAdapter

func NewAdapter(config WebhookConfig) (*Adapter, error) {
	return NewWebhookAdapter(config)
}

func NewWebhookAdapter(config WebhookConfig) (*WebhookAdapter, error) {
	if err := config.Binding.Validate(); err != nil || config.Binding.WebhookSecretRef == "" || !validWebhookSecret(config.Secret) {
		return nil, ErrInvalidConfig
	}
	return &WebhookAdapter{binding: config.Binding, secret: config.Secret}, nil
}

func (a *WebhookAdapter) Binding() Binding {
	if a == nil {
		return Binding{}
	}
	return a.binding
}

func (a *WebhookAdapter) Verify(request *http.Request, _ []byte) error {
	if a == nil || !a.binding.Enabled || request == nil {
		return ErrInvalidWebhookSecret
	}
	values := request.Header.Values(HeaderSecretToken)
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(a.secret)) != 1 {
		return ErrInvalidWebhookSecret
	}
	return nil
}

type User struct {
	ID int64 `json:"id"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type Message struct {
	MessageID       int64   `json:"message_id"`
	MessageThreadID *int64  `json:"message_thread_id"`
	From            *User   `json:"from"`
	Chat            Chat    `json:"chat"`
	Text            *string `json:"text"`
}

type Update struct {
	UpdateID *int64   `json:"update_id"`
	Message  *Message `json:"message"`
}

type Incoming struct {
	UpdateID        int64
	MessageID       int64
	UserID          int64
	ChatID          int64
	ChatType        string
	MessageThreadID *int64
	Text            string
}

func (a *WebhookAdapter) Parse(body []byte) (Incoming, error) {
	if a == nil || len(body) == 0 || len(body) > MaxWebhookBodyBytes || !json.Valid(body) {
		return Incoming{}, ErrInvalidUpdate
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var update Update
	if err := decoder.Decode(&update); err != nil {
		return Incoming{}, ErrInvalidUpdate
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Incoming{}, ErrInvalidUpdate
	}
	if update.UpdateID == nil || *update.UpdateID <= 0 || update.Message == nil {
		return Incoming{}, ErrUnsupportedUpdate
	}
	message := update.Message
	if message.MessageID <= 0 || message.From == nil || message.From.ID <= 0 || message.Chat.ID == 0 || message.Text == nil {
		return Incoming{}, ErrUnsupportedUpdate
	}
	switch message.Chat.Type {
	case "private", "group", "supergroup":
	default:
		return Incoming{}, ErrUnsupportedChat
	}
	if message.MessageThreadID != nil {
		if *message.MessageThreadID <= 0 {
			return Incoming{}, ErrInvalidUpdate
		}
		if message.Chat.Type == "group" {
			return Incoming{}, ErrUnsupportedThread
		}
	}
	return Incoming{UpdateID: *update.UpdateID, MessageID: message.MessageID, UserID: message.From.ID, ChatID: message.Chat.ID, ChatType: message.Chat.Type, MessageThreadID: message.MessageThreadID, Text: *message.Text}, nil
}

func (i Incoming) DedupKey(tenantID, bindingID string) (string, error) {
	if !validIdentity(tenantID) || !validIdentity(bindingID) || i.UpdateID <= 0 {
		return "", ErrInvalidUpdate
	}
	return tenantID + "|" + Channel + "|" + bindingID + "|" + strconv.FormatInt(i.UpdateID, 10), nil
}

func validIdentity(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validReference(value string) bool {
	return validIdentity(value)
}

func validEnvironmentName(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for index, r := range value {
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || index > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func validBotToken(value string) bool {
	if len(value) < 3 || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	colon := strings.IndexByte(value, ':')
	if colon < 1 || colon == len(value)-1 {
		return false
	}
	for index, r := range value {
		if index < colon {
			if r < '0' || r > '9' {
				return false
			}
			continue
		}
		if index == colon {
			continue
		}
		if !(r == '_' || r == '-' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func validWebhookSecret(value string) bool {
	if len(value) < 1 || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if !(r == '_' || r == '-' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
