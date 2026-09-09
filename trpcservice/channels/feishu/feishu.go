// Package feishu speaks the Feishu / Lark open platform for one thing: direct (p2p)
// text into one self-built bot, one text reply back out.
package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrConfig = errors.New("feishu: invalid adapter configuration")

	ErrPlatform = errors.New("feishu: the open platform did not confirm this app")

	ErrConnection = errors.New("feishu: the event connection stopped")

	ErrServing = errors.New("feishu: this adapter has already served")

	ErrTargetInvalid = errors.New("feishu: delivery target is not usable by this binding")

	errNotAccepted = errors.New("feishu: event not accepted")
)

const (
	maxExternalIDBytes = 256

	maxInboundTextBytes = channels.MaxMessageTextBytes

	maxReplyTextBytes = 4096
)

// externalIDPattern is what may become a URL path segment or a digest field.
var externalIDPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]{1,256}$`)

func checkExternalID(value string) bool {
	return len(value) <= maxExternalIDBytes && externalIDPattern.MatchString(value)
}

// Binding is the static, server-side trust anchor for one Feishu bot.
type Binding struct {
	TenantID   string
	AgentAppID string
	BindingID  string
	// AppID is the external Feishu app id, matched exactly against every event
	// header.
	AppID string
	// SecretRef names the app secret in the platform's "env:VAR" syntax.
	SecretRef string
}

// Validate reports whether this binding is well formed, before a Client is built
// and before the secret is resolved.
func (b Binding) Validate() error {
	if err := tenant.ValidateResourceID("tenant_id", b.TenantID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := tenant.ValidateResourceID("agent_app_id", b.AgentAppID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := tenant.ValidateResourceID("channel_binding_id", b.BindingID); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}

	if !checkExternalID(b.AppID) {
		return fmt.Errorf("%w: app_id is empty, oversized or not a plain identifier", ErrConfig)
	}

	if _, err := secretref.EnvName(b.SecretRef); err != nil {
		return fmt.Errorf("%w: secret_ref: %w", ErrConfig, err)
	}
	return nil
}

// Config builds one Client. There is no endpoint field and no logger: the origin
// is the official one, and everything here is too sensitive for a log.
type Config struct {
	// Binding is the static trust anchor. Required.
	Binding Binding

	// Authorizer entitles Binding.SecretRef to Binding.TenantID.
	Authorizer security.SecretRefAuthorizer

	// Getenv resolves the entitled reference. Defaults to os.Getenv.
	Getenv func(name string) string

	baseURL string
}

// Client is one bot's credential and its confirmed identity on the platform.
type Client struct {
	binding Binding

	secret string
	api    *openAPI

	tenantKey string
}

// New validates the configuration, entitles the credential reference by exact string
// before anything looks it up, resolves it, and confirms with the platform that the
// three describe one working app.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.Binding.Validate(); err != nil {
		return nil, err
	}
	if cfg.Authorizer == nil {
		return nil, fmt.Errorf("%w: an authorizer is required", ErrConfig)
	}
	if err := cfg.Authorizer.AuthorizeSecretRef(cfg.Binding.TenantID, cfg.Binding.SecretRef); err != nil {
		return nil, err
	}
	getenv := cfg.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}

	name, err := secretref.EnvName(cfg.Binding.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("%w: secret_ref: %w", ErrConfig, err)
	}
	secret := getenv(name)
	if secret == "" {

		return nil, fmt.Errorf("%w: secret_ref environment variable %q is unset or empty",
			ErrConfig, name)
	}
	base := cfg.baseURL
	if base == "" {
		base = officialBaseURL
	}
	client := &Client{binding: cfg.Binding, secret: secret, api: newOpenAPI(base)}
	if err := client.preflight(ctx); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) preflight(ctx context.Context) error {
	token, err := c.api.tenantAccessToken(ctx, c.binding.AppID, c.secret)
	if err != nil {
		return err
	}
	if err := c.api.confirmBot(ctx, token); err != nil {
		return err
	}
	tenantKey, err := c.api.confirmTenant(ctx, token)
	if err != nil {
		return err
	}
	c.tenantKey = tenantKey
	return nil
}

// Identity domains. Each digest names its own scheme so two hashes of the same
// fields for different purposes cannot collide.
const (
	principalDomain = "im-principal-v1"
	sessionDomain   = "im-session-v1"
	replyDomain     = "im-reply-uuid-v1"

	directEpoch = "0"
)

func digest(fields ...string) string {
	encoded, err := json.Marshal(fields)
	if err != nil {
		panic("feishu: marshalling a string array cannot fail: " + err.Error())
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func principalID(b Binding, tenantKey, openID string) string {
	return "p-" + digest(principalDomain,
		b.TenantID, b.AgentAppID, b.BindingID, b.AppID, tenantKey, "user", openID)
}

func directSessionID(b Binding, tenantKey, principal, chatID string) string {
	return "d-" + digest(sessionDomain,
		b.TenantID, b.AgentAppID, b.BindingID, b.AppID, tenantKey,
		"p2p", principal, chatID, directEpoch)
}

// replyUUID is the platform's request-deduplication key for one reply, derived from the
// message being answered rather than random so that the same answer offered twice is
// one message to the user for as long as the platform's window lasts.
func replyUUID(b Binding, tenantKey, messageID string) string {

	return digest(replyDomain,
		b.TenantID, b.AgentAppID, b.BindingID, b.AppID, tenantKey, messageID)[:32]
}

// targetVersion is stored beside every target so an unknown version is refused
// rather than parsed hopefully.
const targetVersion = 1

// targetPayload is the version-1 Feishu delivery target: the origin message and the
// whole scope it was accepted under.
type targetPayload struct {
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id"`
	BindingID  string `json:"binding_id"`
	AppID      string `json:"app_id"`
	TenantKey  string `json:"tenant_key"`
	MessageID  string `json:"message_id"`
}

func encodeTarget(b Binding, tenantKey, messageID string) (channels.DeliveryTarget, error) {
	payload, err := json.Marshal(targetPayload{
		TenantID:   b.TenantID,
		AgentAppID: b.AgentAppID,
		BindingID:  b.BindingID,
		AppID:      b.AppID,
		TenantKey:  tenantKey,
		MessageID:  messageID,
	})
	if err != nil {
		return channels.DeliveryTarget{}, ErrTargetInvalid
	}
	target := channels.DeliveryTarget{
		Channel: channels.ChannelFeishu,
		Version: targetVersion,
		Payload: payload,
	}
	if err := target.Validate(); err != nil {
		return channels.DeliveryTarget{}, ErrTargetInvalid
	}
	return target, nil
}

func decodeTarget(b Binding, tenantKey string, target channels.DeliveryTarget) (string, error) {
	if target.Channel != channels.ChannelFeishu || target.Version != targetVersion {
		return "", ErrTargetInvalid
	}
	var payload targetPayload
	if err := json.Unmarshal(target.Payload, &payload); err != nil {
		return "", ErrTargetInvalid
	}
	if payload.TenantID != b.TenantID || payload.AgentAppID != b.AgentAppID ||
		payload.BindingID != b.BindingID || payload.AppID != b.AppID ||
		payload.TenantKey != tenantKey || tenantKey == "" {
		return "", ErrTargetInvalid
	}
	if !checkExternalID(payload.MessageID) {
		return "", ErrTargetInvalid
	}
	return payload.MessageID, nil
}
