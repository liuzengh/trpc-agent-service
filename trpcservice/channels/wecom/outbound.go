package wecom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const maxWeComReplyBytes = 20480

var errWeComReplyTooLarge = errors.New("wecom reply is too large")

var errWeComReplyCapability = errors.New("wecom reply capability is unavailable")

var errWeComStreamStateLost = errors.New("wecom stream provider state is unavailable")

var errWeComProviderMessageID = errors.New("wecom provider message id is missing")

// ProviderSendError is the stable, body-redacted error returned by one
// WeCom protocol send. Reply Outbox owns retry and attempt state.
type ProviderSendError struct {
	Code            int
	Retryable       bool
	Uncertain       bool
	RetryAfterDelay time.Duration
	cause           error
}

func (e *ProviderSendError) Error() string {
	if e == nil {
		return "wecom provider send failed"
	}
	if e.Code != 0 {
		return fmt.Sprintf("wecom provider returned code %d", e.Code)
	}
	return "wecom provider send failed"
}

func (e *ProviderSendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *ProviderSendError) IsRetryable() bool {
	return e != nil && e.Retryable
}

func (e *ProviderSendError) IsSideEffectUncertain() bool {
	return e != nil && e.Uncertain
}

func (e *ProviderSendError) RetryAfter() time.Duration {
	if e == nil || e.RetryAfterDelay < 0 {
		return 0
	}
	return e.RetryAfterDelay
}

// MessageSender is the provider operation used by Reply Outbox.
type MessageSender interface {
	SendMessage(context.Context, string, string) (string, error)
}

type streamMessageSender interface {
	SendStream(context.Context, string, string, string, string, bool, bool) (string, error)
}

type cardMessageSender interface {
	SendCard(context.Context, string, map[string]any) (string, error)
}

// OutboundOption configures one binding-scoped sender.
type OutboundOption func(*outboundConfig) error

type outboundConfig struct {
	sender        MessageSender
	clientOptions []ClientOption
}

// WithMessageSender injects a protocol sender for focused provider tests.
func WithMessageSender(sender MessageSender) OutboundOption {
	return func(config *outboundConfig) error {
		if sender == nil {
			return errors.New("wecom message sender is required")
		}
		config.sender = sender
		return nil
	}
}

// WithClientOptions passes protocol options to the binding-scoped client.
func WithClientOptions(options ...ClientOption) OutboundOption {
	return func(config *outboundConfig) error {
		config.clientOptions = append(config.clientOptions, options...)
		return nil
	}
}

// OutboundClient performs exactly one official aibot_send_msg operation. It
// does not claim, persist, retry, or otherwise manage Reply Outbox state.
type OutboundClient struct {
	sender MessageSender
}

// NewOutboundClient resolves the binding's WeCom Bot Secret and creates a
// binding-scoped long-connection sender. It must be constructed by the single
// Channel owner; the sender reconnects independently and is closed by the
// outbound resolver lifecycle.
func NewOutboundClient(
	ctx context.Context,
	secrets platformsecret.SecretProvider,
	binding channels.BindingSnapshot,
	opts ...OutboundOption,
) (*OutboundClient, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("wecom binding: %w", err)
	}
	if binding.Channel != channels.ChannelWeCom {
		return nil, errors.New("wecom outbound client received another channel")
	}
	if binding.Secret.Name == "" {
		return nil, errors.New("wecom bot secret reference is required")
	}
	botSecret, err := secrets.ResolveSecret(ctx, binding.Scope(), binding.Secret)
	if err != nil {
		return nil, fmt.Errorf("resolve wecom bot secret: %w", err)
	}
	if botSecret == "" {
		return nil, errors.New("wecom bot secret is required")
	}
	config := outboundConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, err
		}
	}
	if config.sender == nil {
		config.sender, err = NewClient(binding.ExternalAccount, botSecret, config.clientOptions...)
		if err != nil {
			return nil, err
		}
	}
	return &OutboundClient{sender: config.sender}, nil
}

// NewOutboundClientWithSender wraps the already connected binding-scoped
// sender owned by Adapter. It never creates or starts another WeCom client;
// one BotID must have exactly one live long connection.
func NewOutboundClientWithSender(sender MessageSender) (*OutboundClient, error) {
	if sender == nil {
		return nil, errors.New("wecom message sender is required")
	}
	return &OutboundClient{sender: sender}, nil
}

// SendOnce sends one Reply to the resolved WeCom user ID or group chat ID.
func (c *OutboundClient) SendOnce(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.sender == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom outbound client is not initialized")}
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("wecom reply: %w", err)
	}
	if reply.Channel != channels.ChannelWeCom {
		return channels.ProviderReceipt{}, errors.New("wecom outbound client received another channel")
	}
	target, err := decodeProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom provider target is invalid")}
	}
	if !utf8.ValidString(reply.Text) || len([]byte(reply.Text)) > maxWeComReplyBytes {
		return channels.ProviderReceipt{}, errWeComReplyTooLarge
	}
	providerMessageID, err := c.sender.SendMessage(ctx, target.Destination, reply.Text)
	if err != nil {
		return channels.ProviderReceipt{}, providerError(err)
	}
	if strings.TrimSpace(providerMessageID) == "" {
		return channels.ProviderReceipt{}, &ProviderSendError{Uncertain: true, cause: errWeComProviderMessageID}
	}
	return channels.ProviderReceipt{ProviderMessageID: providerMessageID}, nil
}

// SendStream sends one durable full-content stream snapshot. WeCom requires
// the callback request ID for its response command; the sealed message target
// carries it only for the originating inbound callback.
func (c *OutboundClient) SendStream(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
	previousProviderMessageID string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.sender == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom outbound client is not initialized")}
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("wecom stream reply: %w", err)
	}
	if reply.Channel != channels.ChannelWeCom || reply.ReplyKind() != channels.ReplyKindStream {
		return channels.ProviderReceipt{}, errWeComReplyCapability
	}
	if !utf8.ValidString(reply.Text) || len([]byte(reply.Text)) > maxWeComReplyBytes {
		return channels.ProviderReceipt{}, errWeComReplyTooLarge
	}
	target, err := decodeProviderTarget(providerTarget)
	if err != nil || target.RequestID == "" {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom stream callback target is missing")}
	}
	sender, ok := c.sender.(streamMessageSender)
	if !ok {
		return channels.ProviderReceipt{}, errWeComReplyCapability
	}
	if previousProviderMessageID == "" &&
		reply.StreamPhase != channels.StreamPhaseStart &&
		reply.StreamPhase != channels.StreamPhaseEnd &&
		reply.StreamPhase != channels.StreamPhaseAbort {
		return channels.ProviderReceipt{}, errWeComStreamStateLost
	}
	finish := reply.StreamPhase == channels.StreamPhaseEnd || reply.StreamPhase == channels.StreamPhaseAbort
	update := reply.StreamPhase != channels.StreamPhaseStart && previousProviderMessageID != ""
	providerMessageID, err := sender.SendStream(ctx, target.Destination, target.RequestID,
		reply.StreamID, reply.Text, finish, update)
	if err != nil {
		return channels.ProviderReceipt{}, providerError(err)
	}
	if strings.TrimSpace(providerMessageID) == "" {
		return channels.ProviderReceipt{}, &ProviderSendError{Uncertain: true, cause: errWeComProviderMessageID}
	}
	return channels.ProviderReceipt{ProviderMessageID: providerMessageID}, nil
}

// SendCard sends one WeCom template card. Card actions are rendered as
// provider action values and remain opaque to the transport.
func (c *OutboundClient) SendCard(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
	previousProviderMessageID string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.sender == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom outbound client is not initialized")}
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("wecom card reply: %w", err)
	}
	if reply.Channel != channels.ChannelWeCom || reply.ReplyKind() != channels.ReplyKindCard {
		return channels.ProviderReceipt{}, errWeComReplyCapability
	}
	if previousProviderMessageID != "" {
		return channels.ProviderReceipt{}, errWeComReplyCapability
	}
	target, err := decodeProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom provider target is invalid")}
	}
	sender, ok := c.sender.(cardMessageSender)
	if !ok {
		return channels.ProviderReceipt{}, errWeComReplyCapability
	}
	providerMessageID, err := sender.SendCard(ctx, target.Destination, encodeWeComCard(*reply.Card))
	if err != nil {
		return channels.ProviderReceipt{}, providerError(err)
	}
	if strings.TrimSpace(providerMessageID) == "" {
		return channels.ProviderReceipt{}, &ProviderSendError{Uncertain: true, cause: errWeComProviderMessageID}
	}
	return channels.ProviderReceipt{ProviderMessageID: providerMessageID}, nil
}

func encodeWeComCard(card channels.ReplyCard) map[string]any {
	cardBody := map[string]any{
		"card_type":  "text_notice",
		"main_title": map[string]any{"title": card.Title},
		"emphasis_content": map[string]any{
			"title": card.Body,
			"desc":  card.Status,
		},
	}
	if len(card.Actions) > 0 {
		buttons := make([]map[string]any, 0, len(card.Actions))
		for _, action := range card.Actions {
			buttons = append(buttons, map[string]any{
				"name":    action.Label,
				"keyname": action.ID,
				"value":   action.Value,
			})
		}
		cardBody["horizontal_content_list"] = buttons
	}
	return cardBody
}

// Close releases a sender-owned long connection.
func (c *OutboundClient) Close(ctx context.Context) error {
	if c == nil || c.sender == nil {
		return nil
	}
	if client, ok := c.sender.(*WebSocketClient); ok {
		return client.Close(ctx)
	}
	return nil
}

func providerError(err error) error {
	if err == nil {
		return nil
	}
	var protocolErr *protocolError
	if errors.As(err, &protocolErr) {
		return &ProviderSendError{
			Code:      protocolErr.code,
			Retryable: retryableWeComCode(protocolErr.code),
			Uncertain: protocolErr.code >= 50000,
			cause:     err,
		}
	}
	return &ProviderSendError{
		Retryable: !errors.Is(err, context.Canceled),
		Uncertain: !errors.Is(err, context.Canceled),
		cause:     err,
	}
}

func retryableWeComCode(code int) bool {
	return code == 45009 || code >= 50000
}

var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
