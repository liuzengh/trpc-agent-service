// Package messaging defines durable Inbox, Outbox and delivery contracts.
package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	ContentTypeText = "text/plain"
	ContentTypeCard = "application/vnd.trpc.card+json"
)

// NormalizeContentType is deliberately strict: plain text remains the
// compatibility default, cards must use the service-owned vendor type, and
// binary replies are limited to images until a file delivery policy exists.
func NormalizeContentType(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ContentTypeText, nil
	}
	if value == ContentTypeText || value == ContentTypeCard || strings.HasPrefix(value, "image/") && len(value) > len("image/") {
		return value, nil
	}
	return "", runtime.ErrCapabilityUnsupported
}

type InboxKey struct {
	TenantID, Channel, ExternalAccountID, ExternalMessageID string
}

type InboxState string

const (
	InboxPreprocessPending InboxState = "preprocess_pending"
	InboxDispatchPending   InboxState = "dispatch_pending"
	InboxDispatchReady     InboxState = "dispatch_ready"
	InboxTerminal          InboxState = "terminal"
)

type ClaimInboxRequest struct {
	InboxKey
	AgentAppID, SessionID          string
	ExternalChatID, ExternalUserID string
	PayloadDigest                  string
	KeyVersion                     int64
	InitialState                   InboxState
}

// StableInboxIdentity is the single service-owned identity derivation used by
// every Inbox repository implementation.
func StableInboxIdentity(key InboxKey) (requestID, payloadRef string) {
	h := sha256.New()
	for _, part := range []string{key.TenantID, key.Channel, key.ExternalAccountID, key.ExternalMessageID} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	requestID = "req_" + hex.EncodeToString(h.Sum(nil)[:16])
	return requestID, "inbound://" + key.TenantID + "/" + requestID
}

type InboxRecord struct {
	InboxKey
	RequestID, AgentAppID, SessionID string
	ExternalChatID, ExternalUserID   string
	InputSeq                         uint64
	State                            InboxState
	PayloadRef, PayloadDigest        string
	KeyVersion, Version              int64
	TerminalReason, ResultRef        string
	CreatedAt, UpdatedAt             time.Time
}

type InboxClaimer interface {
	ClaimInbox(context.Context, ClaimInboxRequest) (InboxRecord, error)
	GetInbox(context.Context, InboxKey) (InboxRecord, error)
}

type PayloadRecord struct {
	TenantID, RequestID, PayloadRef, ContentDigest string
	Content                                        []byte
	KeyVersion                                     int64
	CreatedAt                                      time.Time
}

// PayloadCipherKey is a short-lived tenant-scoped encryption key. Callers
// must clear Bytes as soon as the current crypto operation completes.
type PayloadCipherKey struct {
	Bytes   []byte
	Version int64
}

// PayloadKeyResolver resolves the exact key generation recorded beside an
// encrypted payload. Implementations must not fall back to current/latest.
type PayloadKeyResolver interface {
	ResolvePayloadKey(context.Context, string, int64) (PayloadCipherKey, error)
}

// PayloadStore persists normalized inbound payloads before durable ACK so a
// dispatcher can recover them after process restart.
type PayloadStore interface {
	PutPayload(context.Context, PayloadRecord) error
	GetPayload(context.Context, string, string) (PayloadRecord, error)
}

// PreparedPayloadRecord is the immutable, post-Preprocess input consumed by
// execution. SourcePayloadRef ties it back to the durable ingress payload;
// PayloadRef is a separate stable reference so the raw provider media ID can
// never be mistaken for executable content.
type PreparedPayloadRecord struct {
	TenantID, RequestID, PayloadRef, SourcePayloadRef, ContentDigest string
	Content                                                          []byte
	KeyVersion                                                       int64
	ArtifactRetention                                                time.Duration
	ArtifactReferences                                               []PreparedArtifactReference
	CreatedAt                                                        time.Time
}

// PreparedArtifactReference makes Artifact reachability queryable without
// decrypting PreparedPayload content. ArtifactRetention is measured from the
// durable PreparedPayload creation time, so retries preserve one deadline.
type PreparedArtifactReference struct {
	ArtifactID string
}

type PreparedPayloadStore interface {
	PutPreparedPayload(context.Context, PreparedPayloadRecord) error
	GetPreparedPayload(context.Context, string, string, string) (PayloadRecord, error)
}

type ResultRecord struct {
	TenantID, RequestID, ResultRef, ContentDigest, ContentType string
	Content                                                    []byte
	KeyVersion                                                 int64
	CreatedAt                                                  time.Time
}

// ResultStore owns immutable terminal response payloads referenced by reply
// Outbox records.
type ResultStore interface {
	PutResult(context.Context, ResultRecord) error
	GetResult(context.Context, string, string) (ResultRecord, error)
}

// ToolResultRecord is the encrypted, immutable output of one confirmed tool
// attempt. GrantID is its idempotency key; RequestID keeps the result bound to
// the execution which received the grant.
type ToolResultRecord struct {
	TenantID, GrantID, RequestID, ResultRef, ContentDigest string
	Content                                                []byte
	KeyVersion                                             int64
	CreatedAt                                              time.Time
}

// ToolResultStore makes a successful confirmed tool call resumable after the
// process dies. Implementations must encrypt Content and reject collisions.
type ToolResultStore interface {
	PutToolResult(context.Context, ToolResultRecord) error
	GetToolResult(context.Context, string, string) (ToolResultRecord, error)
}

// InteractionRecord stores a non-terminal reply such as a confirmation
// prompt. Unlike ResultRecord, multiple immutable interactions may belong to
// one request before its terminal result exists.
type InteractionRecord struct {
	TenantID, RequestID, ContentRef, ContentDigest, ContentType string
	Content                                                     []byte
	KeyVersion                                                  int64
	CreatedAt                                                   time.Time
}

type InteractionStore interface {
	PutInteraction(context.Context, InteractionRecord) error
	GetReplyContent(context.Context, string, string, string) (ResultRecord, error)
}

func ResolveReplyContent(ctx context.Context, store ResultStore, tenantID, requestID, contentRef string) (ResultRecord, error) {
	if store == nil || tenantID == "" || requestID == "" || contentRef == "" {
		return ResultRecord{}, runtime.ErrInvariantViolation
	}
	if interactions, ok := store.(InteractionStore); ok {
		return interactions.GetReplyContent(ctx, tenantID, requestID, contentRef)
	}
	result, err := store.GetResult(ctx, tenantID, requestID)
	if err != nil {
		return ResultRecord{}, err
	}
	if result.ResultRef != contentRef {
		return ResultRecord{}, runtime.ErrVersionMismatch
	}
	return result, nil
}

// ReplyCoordinate is the storage-owned logical identity of one reply event.
// Retries of the same committed stage and ordinal must resolve to the same ID.
type ReplyCoordinate struct {
	TenantID, RequestID, Stage string
	InputSeq                   uint64
	Ordinal                    uint64
}

// StableReplyID implements the canonical reply identity defined by the
// storage contract. It deliberately contains no Worker or delivery attempt.
func StableReplyID(in ReplyCoordinate) (string, error) {
	if in.TenantID == "" || in.RequestID == "" || in.Stage == "" || in.InputSeq < 1 {
		return "", runtime.ErrCommitConflict
	}
	h := sha256.New()
	for _, part := range []string{in.TenantID, in.RequestID, strconv.FormatUint(in.InputSeq, 10), in.Stage, strconv.FormatUint(in.Ordinal, 10)} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return "r1_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

type ReplyRoute struct {
	TenantID, RequestID, Channel, ChannelBindingID string
	ExternalAccountID, ExternalMessageID           string
	ExternalChatID, ExternalUserID                 string
	ConfigVersion                                  int64
}

// ReplyRouteStore resolves a reply against the configuration version frozen
// on the execution record. Implementations must never fall back to current.
type ReplyRouteStore interface {
	ResolveReplyRoute(context.Context, string, string) (ReplyRoute, error)
}

type OutboxState string

const (
	OutboxPending    OutboxState = "pending"
	OutboxClaimed    OutboxState = "claimed"
	OutboxRetryWait  OutboxState = "retry_wait"
	OutboxPublished  OutboxState = "published"
	OutboxDeadLetter OutboxState = "dead_letter"
)

type OutboxRecord struct {
	TenantID, OutboxID, Kind, AggregateID, IdempotencyKey, PayloadRef string
	EventSeq, Version                                                 uint64
	State                                                             OutboxState
	Attempt                                                           int
	NextAttemptAt, ClaimUntil, CreatedAt                              time.Time
	ClaimOwner, TraceParent                                           string
}

type OutboxStore interface {
	ClaimOutbox(context.Context, string, int, string, time.Time) ([]OutboxRecord, error)
	RenewOutboxClaim(context.Context, string, string, uint64, string, time.Time) (uint64, error)
	MarkPublished(context.Context, string, string, uint64) error
	MarkRetry(context.Context, string, string, uint64, time.Time) error
}

type ReconciliationIssueKind string

const (
	IssueStuckInbox         ReconciliationIssueKind = "stuck_inbox"
	IssueExpiredOutboxClaim ReconciliationIssueKind = "expired_outbox_claim"
	IssueParkedInput        ReconciliationIssueKind = "parked_input"
	IssueMissingReplyOutbox ReconciliationIssueKind = "missing_reply_outbox"
	IssueStuckDelivery      ReconciliationIssueKind = "stuck_delivery"
	IssueAmbiguousDelivery  ReconciliationIssueKind = "ambiguous_delivery"
)

type ReconciliationIssue struct {
	Kind                         ReconciliationIssueKind
	TenantID, AggregateID, RefID string
	Version                      uint64
}

type ReconciliationStore interface {
	FindReconciliationIssues(context.Context, time.Time, int) ([]ReconciliationIssue, error)
}

type DeliveryKey struct {
	TenantID, DeliveryKey string
	SegmentNo             int
}

type DeliveryState string

const (
	DeliveryPending   DeliveryState = "pending"
	DeliverySending   DeliveryState = "sending"
	DeliverySent      DeliveryState = "sent"
	DeliveryAmbiguous DeliveryState = "ambiguous"
	DeliveryRetryWait DeliveryState = "retry_wait"
	DeliveryFailed    DeliveryState = "failed"
)

type DeliveryPlan struct {
	RendererVersion, FormatVersion, ContentDigest string
	SegmentCount                                  int
}

type DeliveryRecord struct {
	DeliveryKey
	ProviderMessageID, LastErrorClass, ClientRequestID, ClaimOwner string
	State                                                          DeliveryState
	Plan                                                           DeliveryPlan
	Attempt, ReconcileAttempt                                      int
	Version                                                        int64
	NotBefore, ClaimUntil, UpdatedAt                               time.Time
}

type DeliveryClaim struct {
	Owner string
	TTL   time.Duration
}

type DeliveryLedger interface {
	GetDelivery(context.Context, DeliveryKey) (DeliveryRecord, error)
	ClaimDelivery(context.Context, DeliveryKey, DeliveryPlan, DeliveryClaim) (DeliveryRecord, bool, error)
	RenewDeliveryClaim(context.Context, DeliveryRecord, time.Duration) (DeliveryRecord, error)
	FinishDelivery(context.Context, DeliveryRecord, int64) (DeliveryRecord, error)
	DeferDeliveryReconciliation(context.Context, DeliveryRecord, int64) (DeliveryRecord, error)
	ReconcileDelivery(context.Context, DeliveryRecord, int64) (DeliveryRecord, error)
}

func StableDeliveryRequestID(key DeliveryKey) (string, error) {
	if key.TenantID == "" || key.DeliveryKey == "" || key.SegmentNo < 0 {
		return "", runtime.ErrTenantScope
	}
	h := sha256.New()
	for _, part := range []string{key.TenantID, key.DeliveryKey, strconv.Itoa(key.SegmentNo)} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return "d1_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}
