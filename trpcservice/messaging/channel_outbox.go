package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"go.opentelemetry.io/otel/propagation"
)

const (
	channelOutboxLease      = 30 * time.Second
	channelReplyEventPrefix = "channel_reply."
)

// ChannelOutboxDispatcher owns one durable-dispatch identity. It claims reply
// events before calling a channel and persists the provider receipt afterward.
// This removes concurrent duplicate sends. A crash after Send succeeds but
// before the receipt commits is deliberately at-least-once and is surfaced by
// the retained attempt count and last delivery error.
type ChannelOutboxDispatcher struct {
	store    storage.OutboxDeliveryStore
	resolver ChannelSenderResolver
	observer metrics.SenderObserver
	policy   ChannelDeliveryPolicy
	owner    string
	lease    time.Duration
	channels []channels.Channel
}

// WithDeliveryPolicy applies the platform-wide text segmentation and pacing
// rules to every IM sender resolved by this dispatcher.
func WithDeliveryPolicy(policy ChannelDeliveryPolicy) ChannelOutboxOption {
	return func(dispatcher *ChannelOutboxDispatcher) error {
		if policy == nil {
			return fmt.Errorf("channel delivery policy is required")
		}
		dispatcher.policy = policy
		return nil
	}
}

type ChannelOutboxOption func(*ChannelOutboxDispatcher) error

// WithSenderObserver attaches telemetry to the only external channel side
// effect. It must not record recipients, bodies, provider receipts, or keys.
func WithSenderObserver(observer metrics.SenderObserver) ChannelOutboxOption {
	return func(dispatcher *ChannelOutboxDispatcher) error {
		if observer == nil {
			return fmt.Errorf("channel sender observer is required")
		}
		dispatcher.observer = observer
		return nil
	}
}

// ChannelReplyEventType returns the durable outbox type for one channel. Using
// a channel-specific type lets independently owned connectors claim only the
// deliveries they can actually send (for example the single active WeCom
// WebSocket owner) without adding another scheduler or querying JSON payloads.
func ChannelReplyEventType(channel channels.Channel) string {
	return channelReplyEventPrefix + string(channel)
}

// WithChannels restricts a dispatcher to the listed channel reply types.
func WithChannels(values ...channels.Channel) ChannelOutboxOption {
	return func(dispatcher *ChannelOutboxDispatcher) error {
		if len(values) == 0 {
			return fmt.Errorf("at least one outbox channel is required")
		}
		seen := make(map[channels.Channel]struct{}, len(values))
		configured := make([]channels.Channel, 0, len(values))
		for _, channel := range values {
			if !channel.Supported() {
				return fmt.Errorf("unsupported outbox channel %q", channel)
			}
			if _, duplicate := seen[channel]; duplicate {
				continue
			}
			seen[channel] = struct{}{}
			configured = append(configured, channel)
		}
		dispatcher.channels = configured
		return nil
	}
}

type ChannelSenderResolver interface {
	ResolveSender(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error)
}

type ChannelArtifactMaterializer interface {
	MaterializeOutboundArtifacts(context.Context, string, string, uint64, []OutboundArtifactRef) ([]channels.OutboundFile, func(), error)
}

type channelReplyPayload struct {
	Channel            channels.Channel           `json:"channel"`
	BindingID          string                     `json:"binding_id,omitempty"`
	AppCode            string                     `json:"app_code,omitempty"`
	ConfigVersion      uint64                     `json:"config_version,omitempty"`
	ConversationID     string                     `json:"conversation_id"`
	ConversationScope  channels.ConversationScope `json:"conversation_scope,omitempty"`
	ProviderReplyToken string                     `json:"provider_reply_token,omitempty"`
	ProgressMessageID  string                     `json:"progress_message_id,omitempty"`
	WebOwnerID         string                     `json:"web_owner_id,omitempty"`
	Artifacts          []OutboundArtifactRef      `json:"artifacts,omitempty"`
	Text               string                     `json:"text"`
	TraceParent        string                     `json:"traceparent,omitempty"`
}

func NewChannelOutboxDispatcher(store storage.StateStore, senders map[channels.BindingKey]channels.Sender, options ...ChannelOutboxOption) (*ChannelOutboxDispatcher, error) {
	if len(senders) == 0 {
		return nil, fmt.Errorf("at least one outbox channel sender is required")
	}
	resolver := make(staticChannelSenderResolver, len(senders))
	for key, sender := range senders {
		if err := key.Validate(); err != nil || sender == nil {
			return nil, fmt.Errorf("outbox sender for binding %q/%q is invalid", key.Channel, key.BindingID)
		}
		resolver[key] = sender
	}
	return NewChannelOutboxDispatcherWithResolver(store, resolver, options...)
}

type staticChannelSenderResolver map[channels.BindingKey]channels.Sender

func (r staticChannelSenderResolver) ResolveSender(_ context.Context, _ string, _ string, _ uint64, key channels.BindingKey) (channels.Sender, error) {
	sender, ok := r[key]
	if !ok {
		return nil, fmt.Errorf("sender for outbox binding %q/%q is not configured", key.Channel, key.BindingID)
	}
	return sender, nil
}

func NewChannelOutboxDispatcherWithResolver(store storage.StateStore, resolver ChannelSenderResolver, options ...ChannelOutboxOption) (*ChannelOutboxDispatcher, error) {
	deliveryStore, ok := store.(storage.OutboxDeliveryStore)
	if !ok {
		return nil, fmt.Errorf("outbox state store must implement durable delivery leases")
	}
	if resolver == nil {
		return nil, fmt.Errorf("outbox channel sender resolver is required")
	}
	owner, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate outbox dispatcher identity: %w", err)
	}
	dispatcher := &ChannelOutboxDispatcher{
		store: deliveryStore, resolver: resolver, owner: owner.String(), lease: channelOutboxLease,
		channels: []channels.Channel{channels.Web, channels.Telegram, channels.WeCom, channels.Feishu},
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("channel outbox option is required")
		}
		if err := option(dispatcher); err != nil {
			return nil, err
		}
	}
	return dispatcher, nil
}

// Dispatch sends one already-claimed event. Callers should normally use
// DispatchTenant, which obtains the lease atomically from the state store.
func (d *ChannelOutboxDispatcher) Dispatch(ctx context.Context, event storage.OutboxEvent) (channels.SendReceipt, error) {
	if strings.TrimSpace(event.DeliveryOwner) != d.owner {
		return channels.SendReceipt{}, fmt.Errorf("outbox event is not claimed by this dispatcher")
	}
	var payload channelReplyPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return channels.SendReceipt{}, fmt.Errorf("decode channel outbox payload: %w", err)
	}
	if event.Type != ChannelReplyEventType(payload.Channel) {
		return channels.SendReceipt{}, fmt.Errorf("outbox event type %q does not match channel %q", event.Type, payload.Channel)
	}
	if strings.TrimSpace(payload.TraceParent) != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": payload.TraceParent})
	}
	if !payload.Channel.Supported() || strings.TrimSpace(payload.ConversationID) == "" || strings.TrimSpace(payload.Text) == "" {
		return channels.SendReceipt{}, fmt.Errorf("invalid channel outbox payload")
	}
	key := channels.BindingKey{Channel: payload.Channel, BindingID: payload.BindingID}
	if err := key.Validate(); err != nil {
		return channels.SendReceipt{}, fmt.Errorf("invalid channel outbox binding: %w", err)
	}
	sender, err := d.resolver.ResolveSender(ctx, event.TenantID, payload.AppCode, payload.ConfigVersion, key)
	if err != nil {
		return channels.SendReceipt{}, fmt.Errorf("resolve sender for outbox binding %q/%q: %w", payload.Channel, payload.BindingID, err)
	}
	sendContext, finish := ctx, func(error) {}
	if d.observer != nil {
		sendContext, finish = d.observer.StartSender(ctx, metrics.SenderAttributes{TenantID: event.TenantID, Channel: string(payload.Channel), BindingID: payload.BindingID})
	}
	idempotencyKey := strings.TrimSpace(event.RequestID)
	if idempotencyKey == "" {
		idempotencyKey = event.ID
	}
	var outboundFiles []channels.OutboundFile
	cleanupFiles := func() {}
	if payload.Channel != channels.Web && len(payload.Artifacts) > 0 {
		materializer, ok := d.resolver.(ChannelArtifactMaterializer)
		if !ok {
			return channels.SendReceipt{}, fmt.Errorf("outbox resolver cannot materialize artifacts")
		}
		outboundFiles, cleanupFiles, err = materializer.MaterializeOutboundArtifacts(ctx, event.TenantID, payload.AppCode, payload.ConfigVersion, payload.Artifacts)
		if err != nil {
			return channels.SendReceipt{}, fmt.Errorf("materialize outbound artifacts: %w", err)
		}
		defer cleanupFiles()
	}
	receipt, sendErr := d.sendWithLease(sendContext, event, sender, channels.ReplyTarget{
		TenantID: event.TenantID, Channel: payload.Channel, BindingID: payload.BindingID,
		ConversationID: payload.ConversationID, ConversationScope: payload.ConversationScope,
		ProviderReplyToken: payload.ProviderReplyToken, WebOwnerID: payload.WebOwnerID,
	}, channels.OutboundMessage{Text: payload.Text, Files: outboundFiles, UpdateMessageID: payload.ProgressMessageID, IdempotencyKey: idempotencyKey})
	finish(sendErr)
	if sendErr != nil {
		finalizeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.store.FailOutboxDelivery(finalizeCtx, event.TenantID, event.ID, d.owner, sendErr)
		return channels.SendReceipt{}, fmt.Errorf("send outbox channel reply: %w", sendErr)
	}
	if err := d.store.CompleteOutboxDelivery(ctx, event.TenantID, event.ID, d.owner, receipt.ExternalMessageID); err != nil {
		return channels.SendReceipt{}, fmt.Errorf("persist channel send receipt: %w", err)
	}
	return receipt, nil
}

func (d *ChannelOutboxDispatcher) sendWithLease(ctx context.Context, event storage.OutboxEvent, sender channels.Sender, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	if d.lease <= 0 {
		return channels.SendReceipt{}, fmt.Errorf("outbox dispatcher lease must be positive")
	}
	sendContext, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	stopped := make(chan struct{})
	renewed := make(chan struct{})
	renewalFailure := make(chan error, 1)
	interval := d.lease / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-sendContext.Done():
				return
			case <-ticker.C:
				if err := d.store.RenewOutboxDelivery(ctx, event.TenantID, event.ID, d.owner, d.lease); err != nil {
					select {
					case renewalFailure <- err:
					default:
					}
					cancelSend()
					return
				}
			}
		}
	}()
	segments := []string{message.Text}
	files := append([]channels.OutboundFile(nil), message.Files...)
	var err error
	if d.policy != nil {
		segments, err = d.policy.Segments(target.Channel, message.Text)
		if err != nil {
			cancelSend()
			close(stopped)
			<-renewed
			return channels.SendReceipt{}, err
		}
	}
	var receipt channels.SendReceipt
	for index, text := range segments {
		if d.policy != nil {
			if err = d.policy.Wait(sendContext, target); err != nil {
				break
			}
		}
		part := message
		part.Text = text
		part.Files = nil
		if len(segments) > 1 {
			part.IdempotencyKey = fmt.Sprintf("%s:part:%d", message.IdempotencyKey, index)
			if index > 0 {
				// Only the first segment replaces the ingress progress message.
				// Later segments are normal follow-up messages rather than repeated
				// overwrites of the same provider message.
				part.UpdateMessageID = ""
			}
		}
		receipt, err = sender.Send(sendContext, target, part)
		if err != nil {
			break
		}
	}
	if err == nil && len(files) > 0 {
		fileMessage := channels.OutboundMessage{Files: files, IdempotencyKey: message.IdempotencyKey + ":files"}
		receipt, err = sender.Send(sendContext, target, fileMessage)
	}
	close(stopped)
	<-renewed
	select {
	case renewErr := <-renewalFailure:
		return channels.SendReceipt{}, fmt.Errorf("renew outbox delivery lease: %w", renewErr)
	default:
	}
	if err != nil {
		return channels.SendReceipt{}, err
	}
	return receipt, nil
}

func (d *ChannelOutboxDispatcher) DispatchTenant(ctx context.Context, tenantID string, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outbox dispatch limit must be positive")
	}
	dispatched := 0
	for dispatched < limit {
		progressed := false
		for _, channel := range d.channels {
			if dispatched == limit {
				break
			}
			// Claim one event at a time. If a send fails, only that event is
			// leased/retried; unrelated sessions and channel owners remain free.
			events, err := d.store.ClaimPendingOutboxByType(ctx, tenantID, d.owner, d.lease, 1, ChannelReplyEventType(channel))
			if err != nil {
				return dispatched, err
			}
			if len(events) == 0 {
				continue
			}
			progressed = true
			if _, err := d.Dispatch(ctx, events[0]); err != nil {
				return dispatched, err
			}
			dispatched++
		}
		if !progressed {
			return dispatched, nil
		}
	}
	return dispatched, nil
}
