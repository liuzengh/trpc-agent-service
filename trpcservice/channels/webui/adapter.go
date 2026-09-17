package webui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Message struct {
	TenantID, ChannelBindingID, ExternalAccountID string
	ExternalUserID, ExternalChatID                string
	RequestID, ClientRequestID, ProviderMessageID string
	ContentRef, ContentDigest                     string
	ConfigVersion                                 int64
	CreatedAt                                     time.Time
}

type MessageQuery struct {
	TenantID, ChannelBindingID, ExternalAccountID string
	ExternalUserID, ExternalChatID                string
	After                                         time.Time
	Limit                                         int
}

type Mailbox interface {
	PutMessage(context.Context, Message) (Message, error)
	GetMessageByClientRequestID(context.Context, string, string) (Message, error)
}

type MessageReader interface {
	ListMessages(context.Context, MessageQuery) ([]Message, error)
}

type Adapter struct {
	Protocol Verifier
	Mailbox  Mailbox
}

func (*Adapter) ID() string { return "webui" }

func (*Adapter) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *Adapter) PublicRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := request.Query[routeKeyQuery]
	timestamp, nonce := header(request.Headers, headerTimestamp), header(request.Headers, headerNonce)
	if routeKey == "" || timestamp == "" || nonce == "" || header(request.Headers, headerSignature) == "" || len(request.Body) == 0 {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte(timestamp+"\x00"+nonce+"\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey, RouteKeyDigest: RouteKeyDigest(routeKey),
		IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (*Adapter) IsChallenge(channel.CallbackRequest) bool { return false }

func (*Adapter) PublicChallengeRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, runtime.ErrCapabilityUnsupported
}

func (*Adapter) VerifyChallenge(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.HTTPResponse, channel.VerificationReceipt, error) {
	return channel.HTTPResponse{}, channel.VerificationReceipt{}, runtime.ErrCapabilityUnsupported
}

func (*Adapter) CallbackACK() channel.HTTPResponse {
	return channel.HTTPResponse{ContentType: "application/json", Body: []byte(`{"accepted":true}`)}
}

func (a *Adapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	return handle.Verify(ctx, request, a.Protocol.Verify)
}

func (*Adapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	event, err := decodeMessage(callback)
	if err != nil {
		return nil, err
	}
	return []channel.ProviderEvent{event}, nil
}

func (a *Adapter) Deliver(ctx context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	if a == nil || a.Mailbox == nil || request.Event.TenantID == "" || request.Event.ChannelBindingID == "" ||
		request.Target.Channel != "webui" || request.Target.ExternalAccountID == "" || request.Target.ExternalUserID == "" ||
		request.Target.ExternalChatID == "" || request.ClientRequestID == "" || request.Event.RequestID == "" ||
		request.Event.ContentRef == "" || len(request.Content) == 0 {
		return channel.DeliveryResult{}, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(request.Content)
	if request.ContentDigest != hex.EncodeToString(sum[:]) {
		return channel.DeliveryResult{}, runtime.ErrVersionMismatch
	}
	providerID := providerMessageID(request.ClientRequestID)
	stored, err := a.Mailbox.PutMessage(ctx, Message{TenantID: request.Event.TenantID,
		ChannelBindingID: request.Event.ChannelBindingID, ExternalAccountID: request.Target.ExternalAccountID,
		ExternalUserID: request.Target.ExternalUserID, ExternalChatID: request.Target.ExternalChatID,
		RequestID: request.Event.RequestID, ClientRequestID: request.ClientRequestID, ProviderMessageID: providerID,
		ContentRef: request.Event.ContentRef, ContentDigest: request.ContentDigest, ConfigVersion: request.Event.ConfigVersion})
	if err != nil {
		return channel.DeliveryResult{}, err
	}
	if stored.ProviderMessageID != providerID {
		return channel.DeliveryResult{}, channel.AmbiguousDeliveryError{Err: runtime.ErrVersionMismatch}
	}
	return channel.DeliveryResult{ProviderMessageID: providerID, Delivered: true}, nil
}

func (a *Adapter) ReconcileDelivery(ctx context.Context, request channel.ReconciliationRequest) (channel.ReconciliationResult, error) {
	if a == nil || a.Mailbox == nil || request.Event.TenantID == "" || request.ClientRequestID == "" {
		return channel.ReconciliationResult{}, runtime.ErrInvariantViolation
	}
	message, err := a.Mailbox.GetMessageByClientRequestID(ctx, request.Event.TenantID, request.ClientRequestID)
	if errors.Is(err, runtime.ErrNotFound) {
		return channel.ReconciliationResult{Status: channel.ReconciliationNotDelivered}, nil
	}
	if err != nil {
		return channel.ReconciliationResult{}, err
	}
	if message.RequestID != request.Event.RequestID || message.ProviderMessageID == "" {
		return channel.ReconciliationResult{Status: channel.ReconciliationUnknown}, nil
	}
	return channel.ReconciliationResult{Status: channel.ReconciliationDelivered, ProviderMessageID: message.ProviderMessageID}, nil
}

func (*Adapter) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

func providerMessageID(clientRequestID string) string {
	sum := sha256.Sum256([]byte("webui-message-v1\x00" + clientRequestID))
	return "webui_" + hex.EncodeToString(sum[:16])
}

var (
	_ channel.Adapter            = (*Adapter)(nil)
	_ channel.HTTPAdapter        = (*Adapter)(nil)
	_ channel.DeliveryReconciler = (*Adapter)(nil)
)
