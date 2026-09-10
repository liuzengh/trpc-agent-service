// Package contract defines provider-neutral Channel events.
package contract

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

// AmbiguousDeliveryError means the provider might have accepted a delivery,
// so callers must reconcile it instead of blindly sending it again.
type AmbiguousDeliveryError struct{ Err error }

func (e AmbiguousDeliveryError) Error() string {
	if e.Err == nil {
		return "reply delivery outcome is ambiguous"
	}
	return e.Err.Error()
}
func (e AmbiguousDeliveryError) Unwrap() error { return e.Err }

// RetryableDeliveryError optionally carries a provider-requested retry delay.
type RetryableDeliveryError struct {
	Err        error
	RetryAfter time.Duration
}

func (e RetryableDeliveryError) Error() string {
	if e.Err == nil {
		return "reply delivery should be retried"
	}
	return e.Err.Error()
}
func (e RetryableDeliveryError) Unwrap() error { return e.Err }

// PermanentDeliveryError classifies a provider rejection that cannot succeed
// unchanged, such as an invalid recipient or an oversized payload.
type PermanentDeliveryError struct {
	Err   error
	Class string
}

func (e PermanentDeliveryError) Error() string {
	if e.Err == nil {
		return "permanent reply delivery failure"
	}
	return e.Err.Error()
}
func (e PermanentDeliveryError) Unwrap() error { return e.Err }

type MediaRef struct {
	ID          string `json:"id"`
	MessageID   string `json:"message_id,omitempty"`
	Kind        string `json:"kind"`
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
}
type CallbackRequest struct {
	Headers    map[string]string
	Query      map[string]string
	Body       []byte
	ReceivedAt time.Time
}

// PublicRouteHint contains provider-public routing material only. It never
// carries tenant, application, SecretRef, or credential values.
type PublicRouteHint struct {
	Channel, ExternalAccountHint, RouteKeyDigest string
	IngressAttemptID, TraceParent                string
}

type CandidateBindingContext struct {
	Channel, RouteKeyDigest, CandidateToken string
	BindingVersion                          int64
	Purpose                                 string
	IssuedAt, ExpiresAt                     time.Time
}

type VerifiedProtocolPayload struct {
	Body                   []byte
	Headers                map[string]string
	ProtocolIdentityDigest string
}

type VerifiedCallback struct {
	Body                   []byte
	Headers                map[string]string
	ReceivedAt             time.Time
	ProtocolIdentityDigest string
}

// ProtocolVerifier is invoked while the single-use handle owns the scoped
// verification secret. Implementations must not retain secret after return.
type ProtocolVerifier func(context.Context, CallbackRequest, []byte) (VerifiedProtocolPayload, error)

type VerificationReceipt struct {
	CandidateToken, ReceiptToken, Purpose, ProtocolIdentityDigest string
	VerifiedAt                                                    time.Time
}

type ScopedVerifierHandle interface {
	Verify(context.Context, CallbackRequest, ProtocolVerifier) (VerifiedCallback, VerificationReceipt, error)
	Close() error
}

type VerifiedBinding struct {
	TenantID, AgentAppID, ChannelBindingID, Channel, ExternalAccountID string
	TenantVersion, BindingVersion                                      int64
	IdentitySecretRef, SessionSecretRef                                secrets.SecretRef
}

type IngressBindingResolver interface {
	ResolveCandidate(context.Context, PublicRouteHint) (CandidateBindingContext, error)
	AcquireVerifier(context.Context, CandidateBindingContext) (ScopedVerifierHandle, error)
	PromoteVerified(context.Context, CandidateBindingContext, VerificationReceipt) (VerifiedBinding, error)
}
type Capabilities struct{ Text, StreamEdit, Markdown, Card, Image, File, Recall bool }

// TextSegmentLimit is optional. Adapters expose it only when the provider has
// a hard byte limit for one text message. Delivery uses it to create durable,
// independently idempotent segments before calling the provider.
type TextSegmentLimit interface {
	MaxTextBytes() int
}
type ProviderEvent struct {
	SchemaVersion                                 uint16
	Channel, ExternalAccountID, ExternalMessageID string
	ConversationType                              string
	ExternalUserID, ExternalChatID                string
	MessageType, Text                             string
	MediaRefs                                     []MediaRef
	OccurredAt                                    time.Time
	TraceParent                                   string
}
type ChannelEvent struct {
	SchemaVersion                                 uint16
	TenantID, AgentAppID, ChannelBindingID        string
	RequestID, UserID, SessionID                  string
	Channel, ExternalMessageID, MessageType, Text string
	MediaRefs                                     []MediaRef
	PayloadRef, TraceParent                       string
}
type ReplyEvent struct {
	SchemaVersion                                      uint16
	TenantID, RequestID, ChannelBindingID, DeliveryKey string
	ConfigVersion                                      int64
	EventSeq                                           uint64
	Kind, ContentRef, ContentType                      string
	Target                                             DeliveryTarget
	Final                                              bool
	TraceParent                                        string
}
type ReplyDestination struct {
	TenantID, Channel, ChannelBindingID, ExternalAccountID string
	ConfigVersion                                          int64
}
type ReplyPublisher interface {
	PublishReply(context.Context, ReplyDestination, ReplyEvent) error
}

type ReplyDelivery struct {
	ID          string
	Destination ReplyDestination
	Event       ReplyEvent
}

type ReplyConsumerOptions struct {
	ConsumerID string
	Limit      int
}

// ReplyQueue is the durable account-level handoff between Reply Relay and a
// Channel Adapter owner. A delivery is ACKed only after Delivery Ledger has
// reached a durable state that makes replay safe.
type ReplyQueue interface {
	ConsumeReplies(context.Context, ReplyDestination, ReplyConsumerOptions, func(context.Context, ReplyDelivery) error) error
	AckReply(context.Context, ReplyDestination, ReplyDelivery) error
	ReclaimReplies(context.Context, ReplyDestination, ReplyConsumerOptions) ([]ReplyDelivery, error)
}
type DeliveryRequest struct {
	Event                      ReplyEvent
	ClientRequestID            string
	Target                     DeliveryTarget
	Content                    []byte
	ContentDigest, ContentType string
}

type DeliveryTarget struct {
	Channel, ExternalAccountID, ExternalMessageID string
	ExternalChatID, ExternalUserID                string
}
type DeliveryResult struct {
	ProviderMessageID string
	Delivered         bool
}

type ReconciliationStatus string

const (
	ReconciliationDelivered    ReconciliationStatus = "delivered"
	ReconciliationNotDelivered ReconciliationStatus = "not_delivered"
	ReconciliationUnknown      ReconciliationStatus = "unknown"
)

type ReconciliationRequest struct {
	Event           ReplyEvent
	ClientRequestID string
}

type ReconciliationResult struct {
	Status            ReconciliationStatus
	ProviderMessageID string
}

// DeliveryReconciler is optional. Adapters without a provider query or stable
// idempotency facility leave ambiguous deliveries pending for manual repair.
type DeliveryReconciler interface {
	ReconcileDelivery(context.Context, ReconciliationRequest) (ReconciliationResult, error)
}

// HTTPResponse is a provider-defined successful callback response. Status is
// intentionally fixed by the HTTP transport so adapters cannot weaken common
// error handling.
type HTTPResponse struct {
	ContentType string
	Body        []byte
}

// HTTPAdapter extends Adapter with the provider's callback challenge and ACK
// contract. Challenge verification still uses the same single-use scoped
// verifier and candidate promotion boundary as normal callbacks.
type HTTPAdapter interface {
	Adapter
	IsChallenge(CallbackRequest) bool
	PublicChallengeRoute(context.Context, CallbackRequest) (PublicRouteHint, error)
	VerifyChallenge(context.Context, CallbackRequest, ScopedVerifierHandle) (HTTPResponse, VerificationReceipt, error)
	CallbackACK() HTTPResponse
}

type Adapter interface {
	ID() string
	Run(context.Context) error
	PublicRoute(context.Context, CallbackRequest) (PublicRouteHint, error)
	Verify(context.Context, CallbackRequest, ScopedVerifierHandle) (VerifiedCallback, VerificationReceipt, error)
	Decode(context.Context, VerifiedCallback) ([]ProviderEvent, error)
	Deliver(context.Context, DeliveryRequest) (DeliveryResult, error)
	Capabilities() Capabilities
}
