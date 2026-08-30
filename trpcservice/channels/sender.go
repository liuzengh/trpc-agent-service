package channels

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	// ReplyOutboxSchemaVersion adds the explicit Telegram topic projection.
	// Decode still accepts the previous schema for old Web/Lark payloads.
	ReplyOutboxSchemaVersion         = 4
	PreviousReplyOutboxSchemaVersion = 3
	ReplyOutboxKind                  = "agent.reply"
	SenderRoutingVersion             = 1

	DestinationTypeUser = "user"
	DestinationTypeChat = "chat"

	MaxReplyPayloadBytes         = 64 << 10
	MaxReplyTextBytes            = 60 << 10
	MaxReplyResponseBytes        = 64 << 10
	MaxDestinationIDBytes        = 256
	MaxRoutingIdentityBytes      = 256
	MaxFinishTypeBytes           = 64
	DefaultResponseBodyLimit     = MaxReplyResponseBytes
	SenderDeliveredCode          = ""
	SenderUnavailableCode        = "sender_unavailable"
	SenderTimeoutCode            = "sender_timeout"
	SenderRateLimitedCode        = "sender_rate_limited"
	SenderInvalidPayloadCode     = "sender_invalid_payload"
	SenderInvalidDestinationCode = "sender_invalid_destination"
	SenderUnknownChannelCode     = "sender_unknown_channel"
	SenderMalformedResponseCode  = "sender_malformed_response"
	SenderRejectedCode           = "sender_rejected"
	SenderNotConfiguredCode      = "sender_not_configured"
	DeliveryOutcomeUnknownCode   = "delivery_outcome_unknown"

	LarkSenderTimeoutCode            = "lark_sender_timeout"
	LarkSenderRateLimitedCode        = "lark_sender_rate_limited"
	LarkSenderUnavailableCode        = "lark_sender_unavailable"
	LarkSenderInvalidDestinationCode = "lark_sender_invalid_destination"
	LarkSenderAuthFailedCode         = "lark_sender_auth_failed"
	LarkSenderForbiddenCode          = "lark_sender_forbidden"
	LarkSenderRejectedCode           = "lark_sender_rejected"
	LarkSenderNotConfiguredCode      = "lark_sender_not_configured"
	LarkSenderMalformedResponseCode  = "lark_sender_malformed_response"
	LarkDeliveryOutcomeUnknownCode   = "lark_delivery_outcome_unknown"

	TelegramSenderTimeoutCode            = "telegram_sender_timeout"
	TelegramSenderRateLimitedCode        = "telegram_sender_rate_limited"
	TelegramSenderUnavailableCode        = "telegram_sender_unavailable"
	TelegramSenderInvalidDestinationCode = "telegram_sender_invalid_destination"
	TelegramSenderAuthFailedCode         = "telegram_sender_auth_failed"
	TelegramSenderForbiddenCode          = "telegram_sender_forbidden"
	TelegramSenderRejectedCode           = "telegram_sender_rejected"
	TelegramSenderMessageTooLongCode     = "telegram_sender_message_too_long"
	TelegramSenderNotConfiguredCode      = "telegram_sender_not_configured"
	TelegramSenderMalformedResponseCode  = "telegram_sender_malformed_response"
	TelegramDeliveryOutcomeUnknownCode   = "telegram_delivery_outcome_unknown"
)

var (
	ErrInvalidReplyPayload    = errors.New("channels: invalid reply payload")
	ErrInvalidDestination     = errors.New("channels: invalid sender destination")
	ErrUnsupportedDestination = errors.New("channels: unsupported sender destination type")
	ErrUnknownChannel         = errors.New("channels: unknown channel")
	ErrDuplicateChannel       = errors.New("channels: duplicate channel adapter")
	ErrChannelPolicy          = errors.New("channels: tenant channel policy denied")
	ErrInvalidRegistry        = errors.New("channels: invalid registry")
	ErrInvalidEndpoint        = errors.New("channels: invalid sender endpoint")
	ErrMalformedResponse      = errors.New("channels: malformed sender response")
)

// OutcomeClass is deliberately independent from outbox. The outbox package
// converts this result to its existing durable retry policy.
type OutcomeClass string

const (
	OutcomeDelivered        OutcomeClass = "delivered"
	OutcomeRetryableFailure OutcomeClass = "retryable_failure"
	OutcomePermanentFailure OutcomeClass = "permanent_failure"
	OutcomeUnknown          OutcomeClass = "outcome_unknown"
)

// SenderOutcome contains only a finite class and safe code. It never carries
// provider error text, response bodies, or credentials.
type SenderOutcome struct {
	Class OutcomeClass
	Code  string
}

// ReplyOutboxPayload is the only durable input understood by a channel
// sender. It contains delivery facts, not prompts, history, or provider data.
type ReplyOutboxPayload struct {
	SchemaVersion        int    `json:"schema_version"`
	Kind                 string `json:"kind"`
	TenantID             string `json:"tenant_id"`
	SessionID            string `json:"session_id"`
	JobID                string `json:"job_id"`
	ExecutionID          string `json:"execution_id"`
	RequestID            string `json:"request_id"`
	MessageID            string `json:"message_id"`
	TraceID              string `json:"trace_id"`
	BindingID            string `json:"binding_id"`
	Channel              string `json:"channel"`
	DestinationType      string `json:"destination_type"`
	DestinationID        string `json:"destination_id"`
	MessageThreadID      *int64 `json:"message_thread_id,omitempty"`
	ReplyText            string `json:"reply_text"`
	FinishType           string `json:"finish_type,omitempty"`
	SenderRoutingVersion int    `json:"sender_routing_version"`
}

// ReplyRouting is the explicit channel and destination projection used by the
// execution boundary. ExternalChat and ExternalUser are already resolved by
// the tenant binding; a sender never invents a fallback destination.
type ReplyRouting struct {
	BindingID       string
	Channel         string
	DestinationType string
	DestinationID   string
	MessageThreadID *int64
}

// OutboundMessage is the in-memory sender input after a committed Outbox
// payload has been decoded and validated.
type OutboundMessage struct {
	Payload        ReplyOutboxPayload
	OutboxID       string
	IdempotencyKey string
}

// OutboundRequest is a provider-neutral HTTP request shape. The HTTP sender
// adds the stable idempotency header and never adds credentials.
type OutboundRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

// SenderAdapter builds a bounded provider request and validates its minimum
// success response. It does not perform I/O or mutate durable state.
type SenderAdapter interface {
	Name() string
	BuildOutbound(OutboundMessage) (OutboundRequest, error)
	ValidateSuccess([]byte) error
}

// Sender consumes only a storage Outbox message. The caller is responsible
// for claiming it; this type never marks an Outbox message complete or retry.
type Sender interface {
	Send(context.Context, storage.OutboxMessage) SenderOutcome
}

type SenderFunc func(context.Context, storage.OutboxMessage) SenderOutcome

func (f SenderFunc) Send(ctx context.Context, message storage.OutboxMessage) SenderOutcome {
	if f == nil {
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
	return f(ctx, message)
}

// RoutingFromTenantContext turns resolved tenant binding fields into an
// explicit destination. Telegram requires a chat; web and WeCom may target a
// resolved chat or user. Both fields are validated before they reach a body.
func RoutingFromTenantContext(tc tenant.TenantContext) (ReplyRouting, error) {
	if err := validateRoutingIdentity(tc.TenantID); err != nil {
		return ReplyRouting{}, err
	}
	if err := validateRoutingIdentity(tc.BindingID); err != nil {
		return ReplyRouting{}, err
	}
	if err := validateChannelName(tc.Channel); err != nil {
		return ReplyRouting{}, err
	}
	routing := ReplyRouting{BindingID: tc.BindingID, Channel: tc.Channel}
	switch tc.Channel {
	case "telegram":
		if tc.ExternalChat == "" {
			return ReplyRouting{}, ErrInvalidDestination
		}
		routing.DestinationType = DestinationTypeChat
		routing.DestinationID = tc.ExternalChat
		if tc.ExternalThreadID != "" {
			threadID, err := parseCanonicalPositiveInt64(tc.ExternalThreadID)
			if err != nil {
				return ReplyRouting{}, ErrInvalidDestination
			}
			routing.MessageThreadID = &threadID
		}
	case "web", "wecom", "lark":
		if tc.ExternalThreadID != "" {
			return ReplyRouting{}, ErrInvalidDestination
		}
		switch {
		case tc.ExternalChat != "":
			routing.DestinationType = DestinationTypeChat
			routing.DestinationID = tc.ExternalChat
		case tc.ExternalUser != "":
			routing.DestinationType = DestinationTypeUser
			routing.DestinationID = tc.ExternalUser
		default:
			return ReplyRouting{}, ErrInvalidDestination
		}
	}
	if err := ValidateDestination(routing.Channel, routing.DestinationType, routing.DestinationID); err != nil {
		return ReplyRouting{}, err
	}
	if err := ValidateMessageThread(routing.Channel, routing.MessageThreadID); err != nil {
		return ReplyRouting{}, err
	}
	return routing, nil
}

// ValidateDestination enforces the finite destination contract independently
// of tenant context construction so decoded durable payloads get the same
// checks as newly built payloads.
func ValidateDestination(channel, destinationType, destinationID string) error {
	if err := validateChannelName(channel); err != nil {
		return err
	}
	if destinationType != DestinationTypeUser && destinationType != DestinationTypeChat {
		return ErrUnsupportedDestination
	}
	if err := validateBoundedText(destinationID, MaxDestinationIDBytes, false); err != nil {
		return ErrInvalidDestination
	}
	if channel == "telegram" {
		if destinationType != DestinationTypeChat {
			return ErrUnsupportedDestination
		}
		if _, err := parseCanonicalNonZeroInt64(destinationID); err != nil {
			return ErrInvalidDestination
		}
	}
	return nil
}

func ValidateMessageThread(channel string, threadID *int64) error {
	if threadID == nil {
		return nil
	}
	if channel != "telegram" || *threadID <= 0 {
		return ErrInvalidDestination
	}
	return nil
}

func parseCanonicalPositiveInt64(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, ErrInvalidDestination
	}
	return parsed, nil
}

func parseCanonicalNonZeroInt64(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed == 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, ErrInvalidDestination
	}
	return parsed, nil
}

func (p ReplyOutboxPayload) Validate() error {
	if (p.SchemaVersion != ReplyOutboxSchemaVersion && p.SchemaVersion != PreviousReplyOutboxSchemaVersion) || p.Kind != ReplyOutboxKind || p.SenderRoutingVersion != SenderRoutingVersion {
		return ErrInvalidReplyPayload
	}
	for _, value := range []struct {
		value string
		limit int
	}{
		{p.TenantID, MaxRoutingIdentityBytes}, {p.SessionID, MaxRoutingIdentityBytes},
		{p.JobID, MaxRoutingIdentityBytes}, {p.ExecutionID, MaxRoutingIdentityBytes},
		{p.RequestID, MaxRoutingIdentityBytes}, {p.MessageID, MaxRoutingIdentityBytes},
		{p.TraceID, MaxRoutingIdentityBytes}, {p.BindingID, MaxRoutingIdentityBytes}, {p.Channel, MaxRoutingIdentityBytes},
	} {
		if err := validateBoundedText(value.value, value.limit, false); err != nil {
			return ErrInvalidReplyPayload
		}
	}
	if err := ValidateDestination(p.Channel, p.DestinationType, p.DestinationID); err != nil {
		return err
	}
	if p.SchemaVersion == PreviousReplyOutboxSchemaVersion && p.MessageThreadID != nil {
		return ErrInvalidReplyPayload
	}
	if err := ValidateMessageThread(p.Channel, p.MessageThreadID); err != nil {
		return err
	}
	if err := validateBoundedText(p.ReplyText, MaxReplyTextBytes, true); err != nil {
		return ErrInvalidReplyPayload
	}
	if p.FinishType != "" {
		if err := validateBoundedText(p.FinishType, MaxFinishTypeBytes, false); err != nil {
			return ErrInvalidReplyPayload
		}
	}
	return nil
}

func EncodeReplyOutboxPayload(payload ReplyOutboxPayload) ([]byte, error) {
	if err := payload.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: encode failed", ErrInvalidReplyPayload)
	}
	if len(encoded) > MaxReplyPayloadBytes {
		return nil, ErrInvalidReplyPayload
	}
	return append([]byte(nil), encoded...), nil
}

func DecodeReplyOutboxPayload(encoded []byte) (ReplyOutboxPayload, error) {
	if len(encoded) == 0 || len(encoded) > MaxReplyPayloadBytes || !json.Valid(encoded) {
		return ReplyOutboxPayload{}, ErrInvalidReplyPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var payload ReplyOutboxPayload
	if err := decoder.Decode(&payload); err != nil {
		return ReplyOutboxPayload{}, ErrInvalidReplyPayload
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ReplyOutboxPayload{}, ErrInvalidReplyPayload
	}
	if err := payload.Validate(); err != nil {
		return ReplyOutboxPayload{}, err
	}
	return payload, nil
}

// DecodeReplyOutboxMessage checks the durable Outbox identity in addition to
// the payload. This prevents a sender from routing a payload under another
// tenant, aggregate, or outbox ID.
func DecodeReplyOutboxMessage(message storage.OutboxMessage) (OutboundMessage, error) {
	if message.TenantID == "" || message.Kind != ReplyOutboxKind || message.AggregateID == "" || message.ID == "" {
		return OutboundMessage{}, ErrInvalidReplyPayload
	}
	payload, err := DecodeReplyOutboxPayload(message.Payload)
	if err != nil {
		return OutboundMessage{}, err
	}
	if payload.TenantID != message.TenantID {
		return OutboundMessage{}, storage.ErrTenantMismatch
	}
	if payload.ExecutionID != message.AggregateID || message.ID != "reply-"+payload.ExecutionID || message.DedupKey != replyDedupKey(message.TenantID, payload.ExecutionID) {
		return OutboundMessage{}, ErrInvalidReplyPayload
	}
	return OutboundMessage{Payload: payload, OutboxID: message.ID, IdempotencyKey: externalIdempotencyKey(payload)}, nil
}

// ExternalIdempotencyKey is a deterministic provider-facing key derived from
// the canonical tenant/execution/kind identity. It contains no secret, time,
// or random UUID and remains unchanged across Outbox retries.
func ExternalIdempotencyKey(payload ReplyOutboxPayload) string {
	return externalIdempotencyKey(payload)
}

func replyDedupKey(tenantID, executionID string) string {
	canonical := tenantID + "|" + executionID + "|" + ReplyOutboxKind
	if len(canonical) <= 256 {
		return canonical
	}
	digest := sha256.Sum256([]byte(canonical))
	return "reply-sha256-" + hex.EncodeToString(digest[:])
}

func externalIdempotencyKey(payload ReplyOutboxPayload) string {
	canonical := payload.TenantID + "|" + payload.ExecutionID + "|" + payload.Kind
	digest := sha256.Sum256([]byte(canonical))
	return "reply-v1-" + hex.EncodeToString(digest[:])
}

// Registry is immutable after construction. Lookup and policy checks are safe
// for concurrent dispatcher reads because no map is exposed or mutated.
type Registry struct {
	adapters       map[string]SenderAdapter
	tenantChannels map[string]map[string]struct{}
}

func NewRegistry(adapters ...SenderAdapter) (*Registry, error) {
	return newRegistry(adapters, nil)
}

func NewRegistryWithPolicy(adapters []SenderAdapter, policies map[string][]string) (*Registry, error) {
	return newRegistry(adapters, policies)
}

func newRegistry(adapters []SenderAdapter, policies map[string][]string) (*Registry, error) {
	if len(adapters) == 0 {
		return nil, ErrInvalidRegistry
	}
	registered := make(map[string]SenderAdapter, len(adapters))
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, ErrInvalidRegistry
		}
		name := adapter.Name()
		if err := validateChannelName(name); err != nil {
			return nil, ErrInvalidRegistry
		}
		if _, exists := registered[name]; exists {
			return nil, ErrDuplicateChannel
		}
		registered[name] = adapter
	}
	copiedPolicies := make(map[string]map[string]struct{}, len(policies))
	for tenantID, channels := range policies {
		if err := validateRoutingIdentity(tenantID); err != nil || len(channels) == 0 {
			return nil, ErrInvalidRegistry
		}
		allowed := make(map[string]struct{}, len(channels))
		for _, channel := range channels {
			if _, exists := registered[channel]; !exists {
				return nil, ErrInvalidRegistry
			}
			if _, exists := allowed[channel]; exists {
				return nil, ErrInvalidRegistry
			}
			allowed[channel] = struct{}{}
		}
		copiedPolicies[tenantID] = allowed
	}
	return &Registry{adapters: registered, tenantChannels: copiedPolicies}, nil
}

func DefaultRegistry() *Registry {
	return &Registry{adapters: map[string]SenderAdapter{
		"web":      WebAdapter{},
		"telegram": TelegramAdapter{},
		"wecom":    WeComAdapter{},
		"lark":     larkPlaceholderAdapter{},
	}}
}

func (r *Registry) Lookup(channel string) (SenderAdapter, error) {
	if r == nil {
		return nil, ErrUnknownChannel
	}
	adapter, ok := r.adapters[channel]
	if !ok {
		return nil, ErrUnknownChannel
	}
	return adapter, nil
}

func (r *Registry) ValidateTenantChannel(tenantID, channel string) error {
	if r == nil {
		return ErrUnknownChannel
	}
	if err := validateRoutingIdentity(tenantID); err != nil {
		return ErrChannelPolicy
	}
	if err := validateChannelName(channel); err != nil {
		return err
	}
	if _, err := r.Lookup(channel); err != nil {
		return err
	}
	if len(r.tenantChannels) == 0 {
		return nil
	}
	allowed, ok := r.tenantChannels[tenantID]
	if !ok {
		return ErrChannelPolicy
	}
	if _, ok := allowed[channel]; !ok {
		return ErrChannelPolicy
	}
	return nil
}

// HTTPDoer is the only network seam. Production callers can supply an
// explicitly configured client; tests use a fake without contacting a provider.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type HTTPSenderConfig struct {
	Registry         *Registry
	Client           HTTPDoer
	Endpoints        map[string]string
	MaxResponseBytes int
}

type HTTPSender struct {
	registry         *Registry
	client           HTTPDoer
	endpoints        map[string]string
	maxResponseBytes int
}

func NewHTTPSender(config HTTPSenderConfig) (*HTTPSender, error) {
	if config.Client == nil {
		return nil, ErrInvalidRegistry
	}
	if config.Registry == nil {
		config.Registry = DefaultRegistry()
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = DefaultResponseBodyLimit
	}
	if config.MaxResponseBytes < 1 || config.MaxResponseBytes > MaxReplyResponseBytes {
		return nil, ErrInvalidRegistry
	}
	endpoints := make(map[string]string, len(config.Endpoints))
	for channel, endpoint := range config.Endpoints {
		if _, err := config.Registry.Lookup(channel); err != nil {
			return nil, ErrInvalidEndpoint
		}
		if err := validateEndpoint(endpoint); err != nil {
			return nil, err
		}
		endpoints[channel] = endpoint
	}
	return &HTTPSender{registry: config.Registry, client: config.Client, endpoints: endpoints, maxResponseBytes: config.MaxResponseBytes}, nil
}

func (s *HTTPSender) Send(ctx context.Context, message storage.OutboxMessage) SenderOutcome {
	if s == nil || s.client == nil || ctx == nil {
		return permanentOutcome(SenderInvalidPayloadCode)
	}
	outbound, err := DecodeReplyOutboxMessage(message)
	if err != nil {
		return permanentOutcome(classifyPayloadError(err))
	}
	if err := s.registry.ValidateTenantChannel(outbound.Payload.TenantID, outbound.Payload.Channel); err != nil {
		switch {
		case errors.Is(err, ErrUnknownChannel):
			return permanentOutcome(SenderUnknownChannelCode)
		case errors.Is(err, ErrChannelPolicy):
			return permanentOutcome(SenderRejectedCode)
		default:
			return permanentOutcome(SenderInvalidPayloadCode)
		}
	}
	adapter, err := s.registry.Lookup(outbound.Payload.Channel)
	if err != nil {
		return permanentOutcome(SenderUnknownChannelCode)
	}
	endpoint, ok := s.endpoints[outbound.Payload.Channel]
	if !ok {
		return permanentOutcome(SenderNotConfiguredCode)
	}
	outboundRequest, err := adapter.BuildOutbound(outbound)
	if err != nil {
		return permanentOutcome(classifyPayloadError(err))
	}
	target, err := joinEndpoint(endpoint, outboundRequest.Path)
	if err != nil {
		return permanentOutcome(SenderInvalidPayloadCode)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(outboundRequest.Body))
	if err != nil {
		return permanentOutcome(SenderInvalidPayloadCode)
	}
	for key, values := range outboundRequest.Headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	request.Header.Set("Idempotency-Key", outbound.IdempotencyKey)
	response, err := s.client.Do(request)
	if err != nil {
		return classifyTransportError(ctx, err)
	}
	if response == nil {
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
	if response.Body == nil {
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return permanentOutcome(SenderMalformedResponseCode)
		}
		return classifyHTTPStatus(response.StatusCode)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return classifyHTTPStatus(response.StatusCode)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(s.maxResponseBytes)+1))
	if readErr != nil {
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
	if len(body) > s.maxResponseBytes {
		return permanentOutcome(SenderMalformedResponseCode)
	}
	if err := adapter.ValidateSuccess(body); err != nil {
		return permanentOutcome(SenderMalformedResponseCode)
	}
	return SenderOutcome{Class: OutcomeDelivered, Code: SenderDeliveredCode}
}

func classifyPayloadError(err error) string {
	switch {
	case errors.Is(err, ErrAdapterNotConfigured):
		return SenderNotConfiguredCode
	case errors.Is(err, ErrInvalidDestination):
		return SenderInvalidDestinationCode
	case errors.Is(err, ErrUnsupportedDestination):
		return SenderInvalidDestinationCode
	case errors.Is(err, ErrUnknownChannel):
		return SenderUnknownChannelCode
	default:
		return SenderInvalidPayloadCode
	}
}

func permanentOutcome(code string) SenderOutcome {
	return SenderOutcome{Class: OutcomePermanentFailure, Code: code}
}

func classifyTransportError(ctx context.Context, err error) SenderOutcome {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: SenderTimeoutCode}
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: SenderTimeoutCode}
	}
	// A connection error can happen after the remote accepted the request.
	// Unknown is the conservative at-least-once result.
	return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
}

func classifyHTTPStatus(status int) SenderOutcome {
	switch {
	case status == http.StatusTooManyRequests:
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: SenderRateLimitedCode}
	case status == http.StatusRequestTimeout || status == http.StatusTooEarly:
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: SenderTimeoutCode}
	case status >= 500:
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: SenderUnavailableCode}
	case status >= 400 && status < 500:
		return permanentOutcome(SenderRejectedCode)
	default:
		return permanentOutcome(SenderRejectedCode)
	}
}

func validateEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrInvalidEndpoint
	}
	return nil
}

func joinEndpoint(endpoint, requestPath string) (string, error) {
	if requestPath == "" || !strings.HasPrefix(requestPath, "/") || strings.ContainsAny(requestPath, "?#") {
		return "", ErrInvalidEndpoint
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		return "", ErrInvalidEndpoint
	}
	base.Path = strings.TrimRight(base.Path, "/") + requestPath
	return base.String(), nil
}

func validateChannelName(channel string) error {
	switch channel {
	case "web", "telegram", "wecom", "lark":
		return nil
	default:
		return ErrUnknownChannel
	}
}

func validateRoutingIdentity(value string) error {
	return validateBoundedText(value, MaxRoutingIdentityBytes, false)
}

func validateBoundedText(value string, maxBytes int, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return ErrInvalidReplyPayload
	}
	if len(value) > maxBytes || strings.TrimSpace(value) != value {
		return ErrInvalidReplyPayload
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return ErrInvalidReplyPayload
		}
	}
	return nil
}

func responseObject(body []byte) (map[string]json.RawMessage, error) {
	if len(body) == 0 {
		return nil, ErrMalformedResponse
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		return nil, ErrMalformedResponse
	}
	return object, nil
}

func boolField(object map[string]json.RawMessage, name string) (bool, bool) {
	value, ok := object[name]
	if !ok {
		return false, false
	}
	var result bool
	if json.Unmarshal(value, &result) != nil {
		return false, false
	}
	return result, true
}

func intField(object map[string]json.RawMessage, name string) (int64, bool) {
	value, ok := object[name]
	if !ok {
		return 0, false
	}
	var result int64
	if json.Unmarshal(value, &result) != nil {
		return 0, false
	}
	return result, true
}

func (WebAdapter) BuildOutbound(message OutboundMessage) (OutboundRequest, error) {
	if err := message.Payload.Validate(); err != nil {
		return OutboundRequest{}, err
	}
	if message.Payload.Channel != "web" {
		return OutboundRequest{}, ErrUnknownChannel
	}
	if err := ValidateDestination(message.Payload.Channel, message.Payload.DestinationType, message.Payload.DestinationID); err != nil {
		return OutboundRequest{}, err
	}
	body, err := json.Marshal(struct {
		DestinationType string `json:"destination_type"`
		DestinationID   string `json:"destination_id"`
		Text            string `json:"text"`
	}{message.Payload.DestinationType, message.Payload.DestinationID, message.Payload.ReplyText})
	if err != nil {
		return OutboundRequest{}, ErrInvalidReplyPayload
	}
	return outboundRequest("/replies", body), nil
}

func (WebAdapter) ValidateSuccess(body []byte) error {
	object, err := responseObject(body)
	if err != nil {
		return err
	}
	if accepted, ok := boolField(object, "accepted"); ok && accepted {
		return nil
	}
	if accepted, ok := boolField(object, "ok"); ok && accepted {
		return nil
	}
	return ErrMalformedResponse
}

func (TelegramAdapter) BuildOutbound(message OutboundMessage) (OutboundRequest, error) {
	if err := message.Payload.Validate(); err != nil {
		return OutboundRequest{}, err
	}
	if message.Payload.Channel != "telegram" {
		return OutboundRequest{}, ErrUnknownChannel
	}
	if err := ValidateDestination(message.Payload.Channel, message.Payload.DestinationType, message.Payload.DestinationID); err != nil {
		return OutboundRequest{}, err
	}
	body, err := json.Marshal(struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}{message.Payload.DestinationID, message.Payload.ReplyText})
	if err != nil {
		return OutboundRequest{}, ErrInvalidReplyPayload
	}
	return outboundRequest("/sendMessage", body), nil
}

func (TelegramAdapter) ValidateSuccess(body []byte) error {
	object, err := responseObject(body)
	if err != nil {
		return err
	}
	if ok, present := boolField(object, "ok"); present && ok {
		return nil
	}
	return ErrMalformedResponse
}

func (WeComAdapter) BuildOutbound(message OutboundMessage) (OutboundRequest, error) {
	if err := message.Payload.Validate(); err != nil {
		return OutboundRequest{}, err
	}
	if message.Payload.Channel != "wecom" {
		return OutboundRequest{}, ErrUnknownChannel
	}
	if err := ValidateDestination(message.Payload.Channel, message.Payload.DestinationType, message.Payload.DestinationID); err != nil {
		return OutboundRequest{}, err
	}
	body := struct {
		MsgType string `json:"msgtype"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
		ToUser string `json:"touser,omitempty"`
		ChatID string `json:"chatid,omitempty"`
	}{MsgType: "text"}
	body.Text.Content = message.Payload.ReplyText
	if message.Payload.DestinationType == DestinationTypeChat {
		body.ChatID = message.Payload.DestinationID
	} else {
		body.ToUser = message.Payload.DestinationID
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return OutboundRequest{}, ErrInvalidReplyPayload
	}
	return outboundRequest("/message/send", encoded), nil
}

func (WeComAdapter) ValidateSuccess(body []byte) error {
	object, err := responseObject(body)
	if err != nil {
		return err
	}
	if code, present := intField(object, "errcode"); present && code == 0 {
		return nil
	}
	return ErrMalformedResponse
}

var ErrAdapterNotConfigured = errors.New("channels: adapter is not configured")

type larkPlaceholderAdapter struct{}

func (larkPlaceholderAdapter) Name() string { return "lark" }
func (larkPlaceholderAdapter) BuildOutbound(OutboundMessage) (OutboundRequest, error) {
	return OutboundRequest{}, ErrAdapterNotConfigured
}
func (larkPlaceholderAdapter) ValidateSuccess([]byte) error { return ErrAdapterNotConfigured }

func outboundRequest(path string, body []byte) OutboundRequest {
	return OutboundRequest{
		Method: http.MethodPost,
		Path:   path,
		Headers: http.Header{
			"Accept":       []string{"application/json"},
			"Content-Type": []string{"application/json"},
		},
		Body: append([]byte(nil), body...),
	}
}
