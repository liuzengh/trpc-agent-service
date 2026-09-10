// Package channels defines tenant-scoped Channel Bindings and the trusted
// inbound boundary used before an IM message can enter a Runner execution.
package channels

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	// SchemaVersionV1 is the first supported Channel Binding configuration
	// schema.
	SchemaVersionV1          = 1
	maxBindingKeyLength      = 64
	maxProviderAccountLength = 256
	maxSecretRefLength       = 256
	maxRouteDigestLength     = 64
	maxProtocolValueLength   = 256
)

var (
	// ErrInvalid reports malformed Channel Binding state or input.
	ErrInvalid = errors.New("invalid channel binding")
	// ErrNotFound reports that a Binding does not exist in the requested tenant.
	ErrNotFound = errors.New("channel binding not found")
	// ErrConflict reports an optimistic-lock conflict.
	ErrConflict = errors.New("channel binding version conflict")
	// ErrDuplicateKey reports a tenant-local Binding key or active account collision.
	ErrDuplicateKey = errors.New("channel binding key already exists")
	// ErrDisabled reports an operation rejected by the terminal state.
	ErrDisabled = errors.New("channel binding is disabled")
	// ErrInvalidTransition reports an unsupported lifecycle transition.
	ErrInvalidTransition = errors.New("invalid channel binding status transition")
	// ErrCandidateUnavailable is intentionally shared by missing, inactive,
	// expired, stale, and unauthorized candidate lookups.
	ErrCandidateUnavailable = errors.New("candidate binding unavailable")
	// ErrVerificationFailed is the provider-safe error for every failed
	// candidate verification. It never contains a tenant or Secret detail.
	ErrVerificationFailed = errors.New("candidate verification failed")
)

// Channel identifies the external IM protocol owned by a Binding.
type Channel string

const (
	// ChannelWeCom identifies an enterprise WeCom callback.
	ChannelWeCom Channel = "wecom"
	// ChannelWeComAIBot identifies a WeCom AI Bot WebSocket connection.
	ChannelWeComAIBot Channel = "wecom_aibot"
	// ChannelTelegram identifies a Telegram Bot callback.
	ChannelTelegram Channel = "telegram"
)

// Validate checks whether the Channel is explicitly supported.
func (c Channel) Validate() error {
	switch c {
	case ChannelWeCom, ChannelWeComAIBot, ChannelTelegram:
		return nil
	default:
		return fmt.Errorf("%w: unknown channel", ErrInvalid)
	}
}

// Status is the lifecycle state of a Channel Binding.
type Status string

const (
	// StatusDraft is editable and cannot be discovered for new inbound traffic.
	StatusDraft Status = "draft"
	// StatusActive admits new candidate discovery and trusted routing.
	StatusActive Status = "active"
	// StatusSuspended rejects new traffic but can be resumed after validation.
	StatusSuspended Status = "suspended"
	// StatusDisabled is terminal and cannot be resumed.
	StatusDisabled Status = "disabled"
)

// WeComProtocolConfiguration contains only non-secret WeCom schema fields.
// Tokens, AES keys, and other credentials are resolved from SecretRef later.
type WeComProtocolConfiguration struct {
	CorpID    string `json:"corp_id,omitempty"`
	AgentID   string `json:"agent_id,omitempty"`
	ReceiveID string `json:"receive_id,omitempty"`
}

// WeComAIBotProtocolConfiguration contains non-secret AI Bot settings.
// The Bot Secret is resolved from Binding.SecretRef at runtime.
type WeComAIBotProtocolConfiguration struct {
	BotID string `json:"bot_id,omitempty"`
	WSURL string `json:"ws_url,omitempty"`
}

// TelegramProtocolConfiguration contains only non-secret Telegram schema
// fields. Bot tokens are resolved from SecretRef and are never stored here.
type TelegramProtocolConfiguration struct {
	APIBaseURL  string `json:"api_base_url,omitempty"`
	WebhookPath string `json:"webhook_path,omitempty"`
}

// ProtocolConfiguration is the explicit, channel-specific non-secret schema.
// Only the field matching Channel may be populated.
type ProtocolConfiguration struct {
	WeCom      *WeComProtocolConfiguration      `json:"wecom,omitempty"`
	WeComAIBot *WeComAIBotProtocolConfiguration `json:"wecom_aibot,omitempty"`
	Telegram   *TelegramProtocolConfiguration   `json:"telegram,omitempty"`
}

// UnmarshalJSON rejects fields outside the explicit protocol schema. This is
// important when configuration arrives from an admin payload rather than a Go
// struct literal: silently dropping a typo or credential-shaped field would
// make the stored digest differ from what the caller thought it submitted.
func (c *ProtocolConfiguration) UnmarshalJSON(data []byte) error {
	if c == nil {
		return fmt.Errorf("%w: protocol configuration receiver is nil", ErrInvalid)
	}
	type protocolConfiguration ProtocolConfiguration
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded protocolConfiguration
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("%w: protocol configuration is invalid: %v", ErrInvalid, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: protocol configuration has trailing JSON", ErrInvalid)
		}
		return fmt.Errorf("%w: protocol configuration has invalid trailing JSON: %v", ErrInvalid, err)
	}
	*c = ProtocolConfiguration(decoded)
	return nil
}

// Clone returns a defensive copy of protocol configuration.
func (c ProtocolConfiguration) Clone() ProtocolConfiguration {
	clone := c
	if c.WeCom != nil {
		value := *c.WeCom
		clone.WeCom = &value
	}
	if c.Telegram != nil {
		value := *c.Telegram
		clone.Telegram = &value
	}
	if c.WeComAIBot != nil {
		value := *c.WeComAIBot
		clone.WeComAIBot = &value
	}
	return clone
}

func normalizeProtocolConfiguration(channel Channel, configuration ProtocolConfiguration) (ProtocolConfiguration, error) {
	if err := channel.Validate(); err != nil {
		return ProtocolConfiguration{}, err
	}
	if err := validateProtocolConfiguration(channel, configuration); err != nil {
		return ProtocolConfiguration{}, err
	}
	normalized := configuration.Clone()
	if normalized.WeCom != nil {
		value, err := normalizeWeComConfiguration(*normalized.WeCom)
		if err != nil {
			return ProtocolConfiguration{}, err
		}
		normalized.WeCom = &value
	}
	if normalized.Telegram != nil {
		value, err := normalizeTelegramConfiguration(*normalized.Telegram)
		if err != nil {
			return ProtocolConfiguration{}, err
		}
		normalized.Telegram = &value
	}
	if normalized.WeComAIBot != nil {
		value, err := normalizeWeComAIBotConfiguration(*normalized.WeComAIBot)
		if err != nil {
			return ProtocolConfiguration{}, err
		}
		normalized.WeComAIBot = &value
	}
	return normalized, nil
}

func validateProtocolConfiguration(channel Channel, configuration ProtocolConfiguration) error {
	switch channel {
	case ChannelWeCom:
		if configuration.Telegram != nil || configuration.WeComAIBot != nil {
			return fmt.Errorf("%w: telegram configuration does not match channel", ErrInvalid)
		}
	case ChannelTelegram:
		if configuration.WeCom != nil || configuration.WeComAIBot != nil {
			return fmt.Errorf("%w: wecom configuration does not match channel", ErrInvalid)
		}
	case ChannelWeComAIBot:
		if configuration.WeCom != nil || configuration.Telegram != nil {
			return fmt.Errorf("%w: protocol configuration does not match channel", ErrInvalid)
		}
	}
	return nil
}

func normalizeWeComConfiguration(configuration WeComProtocolConfiguration) (WeComProtocolConfiguration, error) {
	var err error
	configuration.CorpID, err = normalizeProtocolValue(configuration.CorpID, "corp id")
	if err != nil {
		return WeComProtocolConfiguration{}, err
	}
	configuration.AgentID, err = normalizeProtocolValue(configuration.AgentID, "agent id")
	if err != nil {
		return WeComProtocolConfiguration{}, err
	}
	configuration.ReceiveID, err = normalizeProtocolValue(configuration.ReceiveID, "receive id")
	if err != nil {
		return WeComProtocolConfiguration{}, err
	}
	return configuration, nil
}

func normalizeTelegramConfiguration(configuration TelegramProtocolConfiguration) (TelegramProtocolConfiguration, error) {
	var err error
	configuration.APIBaseURL, err = normalizeAPIBaseURL(configuration.APIBaseURL)
	if err != nil {
		return TelegramProtocolConfiguration{}, err
	}
	configuration.WebhookPath, err = normalizeWebhookPath(configuration.WebhookPath)
	if err != nil {
		return TelegramProtocolConfiguration{}, err
	}
	return configuration, nil
}

func normalizeWeComAIBotConfiguration(configuration WeComAIBotProtocolConfiguration) (WeComAIBotProtocolConfiguration, error) {
	var err error
	configuration.BotID, err = normalizeProtocolValue(configuration.BotID, "bot id")
	if err != nil {
		return WeComAIBotProtocolConfiguration{}, err
	}
	if configuration.BotID == "" {
		return WeComAIBotProtocolConfiguration{}, fmt.Errorf("%w: wecom ai bot bot id is required", ErrInvalid)
	}
	configuration.WSURL, err = normalizeWSURL(configuration.WSURL)
	if err != nil {
		return WeComAIBotProtocolConfiguration{}, err
	}
	if configuration.WSURL == "" {
		configuration.WSURL = "wss://openws.work.weixin.qq.com"
	}
	return configuration, nil
}

func normalizeProtocolValue(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if len([]rune(value)) > maxProtocolValueLength || hasControl(value) {
		return "", fmt.Errorf("%w: %s is invalid", ErrInvalid, label)
	}
	return value, nil
}

func normalizeAPIBaseURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: telegram API base URL must be an https origin", ErrInvalid)
	}
	if len([]rune(value)) > maxProtocolValueLength || hasControl(value) {
		return "", fmt.Errorf("%w: telegram API base URL is invalid", ErrInvalid)
	}
	return value, nil
}

func normalizeWSURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "wss" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: wecom ai bot websocket URL must be a wss origin", ErrInvalid)
	}
	if len([]rune(value)) > maxProtocolValueLength || hasControl(value) {
		return "", fmt.Errorf("%w: websocket URL is invalid", ErrInvalid)
	}
	return value, nil
}

func normalizeWebhookPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "?#") || len([]rune(value)) > maxProtocolValueLength || hasControl(value) {
		return "", fmt.Errorf("%w: telegram webhook path is invalid", ErrInvalid)
	}
	return value, nil
}

// CreateInput contains caller-selected fields for a new Binding. BindingID is
// generated by the service so callers cannot choose the stable identity.
type CreateInput struct {
	TenantID             string
	BindingKey           string
	Channel              Channel
	ProviderAccountID    string
	PublicRouteKeyDigest string
	AppID                string
	SecretRef            string
	Protocol             ProtocolConfiguration
	Status               Status
	Metadata             ChangeMetadata
}

// Binding is the stable tenant-scoped control-plane root. It stores only a
// Secret Manager reference and validated non-secret protocol configuration.
type Binding struct {
	TenantID             string
	BindingID            string
	BindingKey           string
	Channel              Channel
	ProviderAccountID    string
	PublicRouteKeyDigest string
	AppID                string
	SecretRef            string
	Protocol             ProtocolConfiguration
	Status               Status
	Version              int64
	ConfigDigest         string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// NewBinding validates, normalizes, and constructs a new Binding using the
// wall clock.
func NewBinding(input CreateInput) (*Binding, error) {
	return NewBindingAt(input, time.Now().UTC())
}

// NewBindingAt validates, normalizes, and constructs a new Binding at the
// supplied UTC time. Repositories with an injectable clock should use this
// constructor so creation and later lifecycle mutations share one time source.
func NewBindingAt(input CreateInput, now time.Time) (*Binding, error) {
	if err := validateTenantID(input.TenantID); err != nil {
		return nil, err
	}
	key, err := normalizeBindingKey(input.BindingKey)
	if err != nil {
		return nil, err
	}
	if err := input.Channel.Validate(); err != nil {
		return nil, err
	}
	providerAccountID, err := normalizeRequiredValue(input.ProviderAccountID, maxProviderAccountLength, "provider account id")
	if err != nil {
		return nil, err
	}
	routeDigest, err := normalizeRouteKeyDigest(input.PublicRouteKeyDigest)
	if err != nil {
		return nil, err
	}
	if err := validateAppID(input.AppID); err != nil {
		return nil, err
	}
	secretRef, err := normalizeRequiredValue(input.SecretRef, maxSecretRefLength, "secret reference")
	if err != nil {
		return nil, err
	}
	protocol, err := normalizeProtocolConfiguration(input.Channel, input.Protocol)
	if err != nil {
		return nil, err
	}
	status := input.Status
	if status == "" {
		status = StatusDraft
	}
	if status != StatusDraft && status != StatusActive && status != StatusSuspended {
		if status == StatusDisabled {
			return nil, fmt.Errorf("%w: new binding cannot be disabled", ErrInvalid)
		}
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalid, status)
	}
	id, err := newBindingID()
	if err != nil {
		return nil, fmt.Errorf("generate channel binding id: %w", err)
	}
	now = now.UTC()
	if now.IsZero() {
		return nil, fmt.Errorf("%w: binding creation time must be initialized", ErrInvalid)
	}
	binding := &Binding{
		TenantID: input.TenantID, BindingID: id, BindingKey: key, Channel: input.Channel,
		ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest,
		AppID: input.AppID, SecretRef: secretRef, Protocol: protocol, Status: status,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	binding.ConfigDigest, err = binding.computeConfigDigest()
	if err != nil {
		return nil, err
	}
	return binding, nil
}

// Clone returns a defensive copy suitable for repository and snapshot
// boundaries.
func (b Binding) Clone() Binding {
	b.Protocol = b.Protocol.Clone()
	return b
}

// Validate checks all Binding invariants, including the digest and lifecycle.
func (b Binding) Validate() error {
	if err := validateBindingIdentity(b); err != nil {
		return err
	}
	if err := validateBindingFields(b); err != nil {
		return err
	}
	if err := validateBindingProtocol(b); err != nil {
		return err
	}
	if err := validateBindingLifecycle(b); err != nil {
		return err
	}
	digest, err := b.computeConfigDigest()
	if err != nil {
		return err
	}
	if b.ConfigDigest != digest {
		return fmt.Errorf("%w: config digest does not match configuration", ErrInvalid)
	}
	return nil
}

func validateBindingIdentity(b Binding) error {
	if err := validateTenantID(b.TenantID); err != nil {
		return err
	}
	if err := validateBindingID(b.BindingID); err != nil {
		return err
	}
	key, err := normalizeBindingKey(b.BindingKey)
	if err != nil {
		return err
	}
	if key != b.BindingKey {
		return fmt.Errorf("%w: binding key must be normalized", ErrInvalid)
	}
	if err := b.Channel.Validate(); err != nil {
		return err
	}
	return nil
}

func validateBindingFields(b Binding) error {
	providerAccountID, err := normalizeRequiredValue(b.ProviderAccountID, maxProviderAccountLength, "provider account id")
	if err != nil || providerAccountID != b.ProviderAccountID {
		return fmt.Errorf("%w: provider account id must be normalized", ErrInvalid)
	}
	routeDigest, err := normalizeRouteKeyDigest(b.PublicRouteKeyDigest)
	if err != nil || routeDigest != b.PublicRouteKeyDigest {
		return fmt.Errorf("%w: route key digest must be normalized", ErrInvalid)
	}
	if err := validateAppID(b.AppID); err != nil {
		return err
	}
	secretRef, err := normalizeRequiredValue(b.SecretRef, maxSecretRefLength, "secret reference")
	if err != nil || secretRef != b.SecretRef {
		return fmt.Errorf("%w: secret reference must be normalized", ErrInvalid)
	}
	return nil
}

func validateBindingProtocol(b Binding) error {
	protocol, err := normalizeProtocolConfiguration(b.Channel, b.Protocol)
	if err != nil {
		return err
	}
	if !protocolEqual(protocol, b.Protocol) {
		return fmt.Errorf("%w: protocol configuration must be normalized", ErrInvalid)
	}
	return nil
}

func validateBindingLifecycle(b Binding) error {
	if b.Status != StatusDraft && b.Status != StatusActive && b.Status != StatusSuspended && b.Status != StatusDisabled {
		return fmt.Errorf("%w: unknown status %q", ErrInvalid, b.Status)
	}
	if b.Version < 1 || b.CreatedAt.IsZero() || b.UpdatedAt.IsZero() || b.UpdatedAt.Before(b.CreatedAt) || b.CreatedAt.Location() != time.UTC || b.UpdatedAt.Location() != time.UTC {
		return fmt.Errorf("%w: version and UTC timestamps must be initialized and ordered", ErrInvalid)
	}
	return nil
}

// CanAcceptInbound reports whether the Binding may admit a new inbound
// candidate. Tenant and App gates are checked separately at RoutingTarget.
func (b Binding) CanAcceptInbound() bool { return b.Status == StatusActive }

// CanTransitionTo reports whether a lifecycle transition is allowed.
func (b Binding) CanTransitionTo(next Status) bool {
	switch b.Status {
	case StatusDraft:
		return next == StatusActive || next == StatusDisabled
	case StatusActive:
		return next == StatusSuspended || next == StatusDisabled
	case StatusSuspended:
		return next == StatusActive || next == StatusDisabled
	default:
		return false
	}
}

func (b Binding) computeConfigDigest() (string, error) {
	payload := struct {
		SchemaVersion        int                   `json:"schema_version"`
		Channel              Channel               `json:"channel"`
		ProviderAccountID    string                `json:"provider_account_id"`
		PublicRouteKeyDigest string                `json:"public_route_key_digest"`
		AppID                string                `json:"app_id"`
		SecretRef            string                `json:"secret_ref"`
		Protocol             ProtocolConfiguration `json:"protocol"`
	}{SchemaVersionV1, b.Channel, b.ProviderAccountID, b.PublicRouteKeyDigest, b.AppID, b.SecretRef, b.Protocol.Clone()}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: encode configuration digest: %v", ErrInvalid, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// DigestPublicRouteKey returns the irreversible lookup digest for one public
// route key. The raw key is never returned and must not be persisted or logged.
func DigestPublicRouteKey(channel Channel, routeKey string) (string, error) {
	if err := channel.Validate(); err != nil {
		return "", err
	}
	routeKey = strings.TrimSpace(routeKey)
	if routeKey == "" || len([]rune(routeKey)) > 1024 || hasControl(routeKey) {
		return "", fmt.Errorf("%w: public route key is invalid", ErrInvalid)
	}
	hash := sha256.New()
	writeLengthPrefixed(hash, string(channel))
	writeLengthPrefixed(hash, routeKey)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ValidatePublicRouteKeyDigest validates the normalized digest accepted by
// candidate lookup and Binding storage.
func ValidatePublicRouteKeyDigest(digest string) error {
	_, err := normalizeRouteKeyDigest(digest)
	return err
}

func writeLengthPrefixed(builder interface{ Write([]byte) (int, error) }, value string) {
	_, _ = builder.Write([]byte(strconv.Itoa(len([]byte(value)))))
	_, _ = builder.Write([]byte{':'})
	_, _ = builder.Write([]byte(value))
}

func normalizeBindingKey(key string) (string, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if len(key) < 2 || len(key) > maxBindingKeyLength || key[0] < 'a' || key[0] > 'z' {
		return "", fmt.Errorf("%w: binding key must match [a-z][a-z0-9-]{1,63}", ErrInvalid)
	}
	for i := 1; i < len(key); i++ {
		c := key[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return "", fmt.Errorf("%w: binding key must match [a-z][a-z0-9-]{1,63}", ErrInvalid)
		}
	}
	return key, nil
}

func normalizeRequiredValue(value string, maxLength int, label string) (string, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" || len([]rune(normalized)) > maxLength || hasControl(normalized) {
		return "", fmt.Errorf("%w: %s is invalid", ErrInvalid, label)
	}
	return normalized, nil
}

func normalizeRouteKeyDigest(digest string) (string, error) {
	if len(digest) != maxRouteDigestLength || strings.ToLower(digest) != digest {
		return "", fmt.Errorf("%w: route key digest must be lowercase sha-256", ErrInvalid)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: route key digest must be lowercase sha-256", ErrInvalid)
	}
	return digest, nil
}

func protocolEqual(left, right ProtocolConfiguration) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func hasControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

// ValidateTenantID validates the stable tenant identifier used by channel
// adapter composition. It is intentionally the same validator used by the
// channel domain and does not authorize access to a tenant.
func ValidateTenantID(id string) error { return validateTenantID(id) }

func validateTenantID(id string) error  { return validateCrockfordID(id, "t_", "tenant") }
func validateAppID(id string) error     { return validateCrockfordID(id, "app_", "agent app") }
func validateBindingID(id string) error { return validateCrockfordID(id, "cb_", "channel binding") }

func validateCrockfordID(id, prefix, label string) error {
	payload := strings.TrimPrefix(id, prefix)
	if payload == id || len(payload) != 26 {
		return fmt.Errorf("%w: %s id must be %s followed by 26 Crockford characters", ErrInvalid, label, prefix)
	}
	if payload[0] > '7' {
		return fmt.Errorf("%w: %s id has invalid ULID padding bits", ErrInvalid, label)
	}
	for _, c := range payload {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", c) {
			return fmt.Errorf("%w: invalid %s id", ErrInvalid, label)
		}
	}
	return nil
}

const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func newBindingID() (string, error) {
	var data [16]byte
	milliseconds := time.Now().UnixMilli()
	for i := 5; i >= 0; i-- {
		data[i] = byte(milliseconds)
		milliseconds >>= 8
	}
	if _, err := rand.Read(data[6:]); err != nil {
		return "", err
	}
	value := new(big.Int).SetBytes(data[:])
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		encoded[i] = crockfordAlphabet[value.Uint64()&31]
		value.Rsh(value, 5)
	}
	return "cb_" + string(encoded[:]), nil
}
