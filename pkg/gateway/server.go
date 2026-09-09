//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// Server is the gateway server that handles IM webhooks.
type Server struct {
	tenantService   tenant.Service
	adapterRegistry *channel.AdapterRegistry
	rateLimiter     *RateLimiter
	healthChecker   *health.HealthChecker
	inbox           reliable.Store
	audit           AuditLogger
}

// AuditLogger is the Gateway security-decision audit seam. Production uses a
// redacting telemetry Collector; tests use an in-memory adapter.
type AuditLogger interface {
	LogAudit(context.Context, *telemetry.AuditLog) error
}

// NewDurableServer creates a Gateway that persists canonical inbound work
// before acknowledging the webhook. It deliberately has no direct Worker
// dependency: Consumer owns all Agent execution.
func NewDurableServer(
	tenantService tenant.Service,
	adapterRegistry *channel.AdapterRegistry,
	redisClient *redis.Client,
	healthChecker *health.HealthChecker,
	inbox reliable.Store,
	audit AuditLogger,
) *Server {
	if isNilGatewayDependency(tenantService) {
		panic("tenant service is required")
	}
	if adapterRegistry == nil {
		panic("channel adapter registry is required")
	}
	if isNilGatewayDependency(inbox) {
		panic("durable Inbox store is required")
	}
	if redisClient == nil {
		panic("Redis client is required for fail-closed rate limiting")
	}
	if healthChecker == nil {
		panic("health checker is required")
	}
	if isNilGatewayDependency(audit) {
		panic("Gateway security audit logger is required")
	}
	return &Server{
		tenantService:   tenantService,
		adapterRegistry: adapterRegistry,
		rateLimiter:     NewRateLimiter(redisClient),
		healthChecker:   healthChecker,
		inbox:           inbox,
		audit:           audit,
	}
}

func isNilGatewayDependency(value interface{}) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// HandleWebhook handles incoming IM webhooks.
func (s *Server) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract the opaque webhook route key from the query. The public route is
	// intentionally fixed at /webhook; accepting a path key here would create a
	// second routing contract and make proxy normalization ambiguous.
	webhookToken, err := singleQueryValue(r, "token")
	if err != nil {
		http.Error(w, "missing webhook token", http.StatusBadRequest)
		return
	}

	// Resolve tenant by webhook token. The scoped capability is mandatory at
	// ingress so this process never materializes model, storage, or unrelated
	// channel credentials.
	t, err := resolveWebhookTenant(ctx, s.tenantService, webhookToken)
	if err != nil || t == nil {
		log.Printf("failed to resolve tenant: error=%s", stableErrorCode(err))
		http.Error(w, "invalid webhook token", http.StatusUnauthorized)
		return
	}
	// Keep the ingress boundary fail-closed even when a custom tenant service
	// implementation returns a stale or suspended snapshot. Tenant status is a
	// security decision, not merely a repository filter.
	if t.Status != tenant.TenantStatusActive {
		log.Printf("rejected webhook for inactive tenant")
		http.Error(w, "invalid webhook token", http.StatusUnauthorized)
		return
	}

	// Find matching channel binding
	var binding *tenant.ChannelBinding
	for i := range t.Channels {
		if t.Channels[i].EffectiveWebhookKey() == webhookToken {
			binding = &t.Channels[i]
			break
		}
	}

	if binding == nil {
		http.Error(w, "channel not found", http.StatusNotFound)
		return
	}

	// Get channel adapter
	adapter, ok := s.adapterRegistry.Get(channel.ChannelType(binding.Type))
	if !ok {
		log.Printf("unsupported channel type: %s", binding.Type)
		http.Error(w, "unsupported channel type", http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodGet {
		verifier, ok := adapter.(channel.URLVerifier)
		if !ok {
			http.Error(w, "channel does not support URL verification", http.StatusMethodNotAllowed)
			return
		}
		echo, err := verifier.VerifyURL(r, binding)
		if err != nil {
			log.Printf("channel URL verification failed: error=%s", stableErrorCode(err))
			http.Error(w, "invalid URL verification request", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(echo))
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the body once under a hard size cap, before any adapter consumes it.
	// The bytes are rewound after each read so signature verification, parsing
	// and the dedup payload hash all observe the identical payload.
	payload, err := readLimitedBody(r, MaxWebhookBodyBytes)
	if err != nil {
		switch {
		case errors.Is(err, ErrBodyTooLarge):
			log.Printf("rejected oversize webhook body for tenant %s: error=%s", t.ID, stableErrorCode(err))
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		case errors.Is(err, ErrEmptyBody):
			http.Error(w, "empty request body", http.StatusBadRequest)
		default:
			log.Printf("failed to read webhook body: error=%s", stableErrorCode(err))
			http.Error(w, "invalid request body", http.StatusBadRequest)
		}
		return
	}

	// Verify signature
	if err := adapter.VerifySignature(r, binding); err != nil {
		recordWebhook(t.ID, binding.Type, "signature_rejected")
		log.Printf("signature verification failed: error=%s", stableErrorCode(err))
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	// VerifySignature may consume the body; rewind before parsing.
	restoreBody(r, payload)
	// Parse/depth-check only authenticated payloads. The body is already hard
	// capped above, and adapters such as WeCom use XML rather than JSON.
	if err := validatePayload(payload, MaxJSONDepth); err != nil {
		log.Printf("rejected malformed webhook payload for tenant %s: error=%s", t.ID, stableErrorCode(err))
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	// Parse inbound message
	msg, err := adapter.ParseInbound(r, binding)
	if err != nil {
		if errors.Is(err, channel.ErrIgnoredInbound) {
			recordWebhook(t.ID, binding.Type, "ignored")
			w.WriteHeader(http.StatusOK)
			return
		}
		log.Printf("failed to parse message: error=%s", stableErrorCode(err))
		http.Error(w, "invalid message format", http.StatusBadRequest)
		return
	}
	if msg == nil {
		recordWebhook(t.ID, binding.Type, "invalid_message")
		log.Printf("channel adapter returned a nil inbound message for tenant %s", t.ID)
		http.Error(w, "invalid message format", http.StatusBadRequest)
		return
	}

	msg.TenantID = t.ID
	msg.ChannelAccountID = binding.EnsureAccountID()
	msg.ChannelType = binding.Type
	msg.Timestamp = msg.Timestamp.UTC()
	if err := validateInboundMessage(msg); err != nil {
		recordWebhook(t.ID, binding.Type, "invalid_message")
		log.Printf("rejected invalid inbound message for tenant %s: error=%s", t.ID, stableErrorCode(err))
		http.Error(w, "invalid message identity", http.StatusBadRequest)
		return
	}
	identity, err := channel.BuildSessionIdentity(msg)
	if err != nil {
		log.Printf("rejected inbound message with incomplete session identity for tenant %s", t.ID)
		http.Error(w, "invalid message identity", http.StatusBadRequest)
		return
	}
	msg.SessionOwnerID = identity.SessionOwnerID
	msgSession := identity.SessionID
	if len(msg.Content) > MaxMessageContentBytes {
		recordWebhook(t.ID, binding.Type, "content_too_large")
		log.Printf("rejected oversize message content for tenant %s: %d bytes", t.ID, len(msg.Content))
		http.Error(w, "message content too large", http.StatusRequestEntityTooLarge)
		return
	}

	digest := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(digest[:])
	if msg.MessageID == "" {
		// Some providers omit a source ID for selected event types. The payload
		// hash remains deterministic for redelivery of that event.
		msg.MessageID = "payload:" + payloadHash
	}
	if msg.ReplyToID == "" {
		msg.ReplyToID = msg.MessageID
	}
	// Limit every authenticated, well-formed provider source before applying
	// tenant access policy. Otherwise a valid signature from an unauthorized
	// identity can bypass quota and amplify synchronous denial auditing. A
	// marker hit is still accepted so provider redelivery does not spend quota.
	sourceKey := msg.ChannelType + "\x00" + msg.ChannelAccountID + "\x00" + msg.MessageID
	limitDecision, err := s.rateLimiter.allowSource(ctx, t.ID, msg.ExternalUserID, sourceKey)
	if err != nil {
		// Limiter failure is a security and cost-control failure. Reject the
		// request so unavailable Redis cannot become an unbounded model-spend
		// path; the IM provider can retry after recovery.
		log.Printf("rate limit check unavailable: error=%s", stableErrorCode(err))
		http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
		return
	}
	if !limitDecision.allowed {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	if err := channel.AuthorizeInbound(binding, msg); err != nil {
		recordWebhook(t.ID, binding.Type, "identity_rejected")
		// A provider can retry one source many times. The limit marker makes
		// those retries free, and this coalesces the durable security audit to
		// the first observed denial for that source in the same limit window.
		if limitDecision.newSource {
			if auditErr := s.audit.LogAudit(ctx, &telemetry.AuditLog{
				TenantID:    t.ID,
				ChannelType: binding.Type,
				UserID:      msg.ExternalUserID,
				SessionID:   msgSession,
				AgentName:   binding.AgentApp,
				Decision:    "deny",
				ErrorType:   "im_identity_denied",
			}); auditErr != nil {
				log.Printf("failed to persist channel identity denial audit: error=%s", stableErrorCode(auditErr))
			}
		}
		log.Printf("rejected unauthorized channel identity for tenant=%s channel=%s", t.ID, binding.Type)
		// The callback is authentic provider traffic, so acknowledge it to avoid
		// an unauthorized sender causing unbounded provider redelivery.
		w.WriteHeader(http.StatusOK)
		return
	}

	canonicalPayload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("failed to encode canonical message: error=%s", stableErrorCode(err))
		http.Error(w, "failed to persist message", http.StatusInternalServerError)
		return
	}
	agentApp := binding.AgentApp
	if agentApp == "" {
		http.Error(w, "channel has no agent app binding", http.StatusServiceUnavailable)
		return
	}
	inboxMessage := &reliable.InboxMessage{
		TenantID:          t.ID,
		ChannelType:       msg.ChannelType,
		ChannelAccountID:  msg.ChannelAccountID,
		AgentApp:          agentApp,
		ExternalMessageID: msg.MessageID,
		ConversationID:    msg.ConversationID,
		ReplyToID:         msg.ReplyToID,
		UserID:            msg.ExternalUserID,
		SessionID:         msgSession,
		IsGroupChat:       msg.IsGroupChat,
		SessionOwnerID:    identity.SessionOwnerID,
		RoutingVersion:    reliable.CurrentInboxRoutingVersion,
		PayloadHash:       payloadHash,
		Payload:           canonicalPayload,
		TraceParent:       telemetry.TraceParentFromContext(ctx),
	}
	var inserted bool
	if admission, ok := s.inbox.(reliable.QueueAdmissionStore); ok && !isNilGatewayDependency(admission) {
		inserted, err = admission.EnqueueInboxWithAdmission(ctx, inboxMessage)
	} else {
		inserted, err = s.inbox.EnqueueInbox(ctx, inboxMessage)
	}
	if errors.Is(err, reliable.ErrIdempotencyConflict) {
		recordWebhook(t.ID, msg.ChannelType, "idempotency_conflict")
		log.Printf("idempotency conflict for tenant=%s channel=%s", t.ID, msg.ChannelType)
		http.Error(w, "message identifier conflicts with an earlier payload", http.StatusConflict)
		return
	}
	if errors.Is(err, reliable.ErrTenantInactive) {
		recordWebhook(t.ID, msg.ChannelType, "tenant_inactive")
		log.Printf("acknowledging webhook rejected by tenant lifecycle fence tenant=%s channel=%s", t.ID, msg.ChannelType)
		// Signature and sender authorization already succeeded. Acknowledge the
		// provider so suspension does not create an unbounded redelivery storm.
		w.WriteHeader(http.StatusOK)
		return
	}
	if errors.Is(err, reliable.ErrTenantQueueFull) {
		recordWebhook(t.ID, msg.ChannelType, "queue_full")
		w.Header().Set("Retry-After", "1")
		http.Error(w, "tenant queue capacity reached", http.StatusTooManyRequests)
		return
	}
	if err != nil {
		if errors.Is(err, reliable.ErrInvalidInboxMessage) {
			recordWebhook(t.ID, msg.ChannelType, "invalid_message")
			http.Error(w, "invalid message format", http.StatusBadRequest)
			return
		}
		recordWebhook(t.ID, msg.ChannelType, "persistence_error")
		log.Printf("failed to enqueue durable message: error=%s", stableErrorCode(err))
		http.Error(w, "message persistence unavailable", http.StatusServiceUnavailable)
		return
	}
	if !inserted {
		recordWebhook(t.ID, msg.ChannelType, "duplicate")
		log.Printf("acknowledging duplicate message tenant=%s channel=%s", t.ID, msg.ChannelType)
	} else {
		recordWebhook(t.ID, msg.ChannelType, "accepted")
	}
	w.WriteHeader(http.StatusOK)
}

func resolveWebhookTenant(ctx context.Context, service tenant.Service, token string) (*tenant.Tenant, error) {
	if scoped, ok := service.(tenant.WebhookScopedReader); ok {
		return scoped.GetTenantByWebhookTokenScoped(ctx, token)
	}
	return nil, tenant.ErrScopedTenantReadUnavailable
}

// HealthCheck handles health check requests.
func (s *Server) HealthCheck(w http.ResponseWriter, r *http.Request) {
	body, statusCode := s.healthChecker.Report(r.Context())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(body)
}

// RateLimiter handles rate limiting using Redis.
type RateLimiter struct {
	redis                *redis.Client
	tenantLimitPerMinute int64
}

const defaultTenantRateLimitPerMinute int64 = 2000

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(redisClient *redis.Client) *RateLimiter {
	return &RateLimiter{
		redis:                redisClient,
		tenantLimitPerMinute: defaultTenantRateLimitPerMinute,
	}
}

// NewRateLimiterWithTenantLimit creates a rate limiter with an explicit
// tenant-wide fixed-window ceiling. The per-user ceiling remains unchanged.
// A tenant cap is an ingress guard, not a replacement for queue backpressure
// and provider/WAF limits at the deployment boundary.
func NewRateLimiterWithTenantLimit(redisClient *redis.Client, limitPerMinute int64) *RateLimiter {
	if limitPerMinute <= 0 {
		limitPerMinute = defaultTenantRateLimitPerMinute
	}
	return &RateLimiter{redis: redisClient, tenantLimitPerMinute: limitPerMinute}
}

// Allow checks if a request is allowed under rate limits.
// Allow preserves the original request-based limiter contract for callers that
// do not have a provider source identifier. Gateway webhooks should use
// AllowSource so redeliveries are charged only once.
func (r *RateLimiter) Allow(ctx context.Context, tenantID, userID string) (bool, error) {
	return r.AllowSource(ctx, tenantID, userID, fmt.Sprintf("legacy:%d", time.Now().UnixNano()))
}

// AllowSource applies the quota to a complete provider idempotency source.
func (r *RateLimiter) AllowSource(ctx context.Context, tenantID, userID, sourceKey string) (bool, error) {
	decision, err := r.allowSource(ctx, tenantID, userID, sourceKey)
	return decision.allowed, err
}

// sourceRateLimitDecision keeps the redelivery marker private to the Gateway
// package while preserving RateLimiter's boolean public interface for callers
// that only need an admission decision.
type sourceRateLimitDecision struct {
	allowed   bool
	newSource bool
}

// allowSource additionally reports whether the source consumed a quota token.
// Gateway uses that fact to coalesce durable denial audits for redeliveries.
func (r *RateLimiter) allowSource(ctx context.Context, tenantID, userID, sourceKey string) (sourceRateLimitDecision, error) {
	if r == nil || r.redis == nil {
		return sourceRateLimitDecision{}, fmt.Errorf("rate limiter Redis client is unavailable")
	}
	if tenantID == "" || userID == "" || sourceKey == "" {
		return sourceRateLimitDecision{}, fmt.Errorf("rate limiter tenant, user and source key are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := rateLimitHashKey("ratelimit", tenantID, userID)
	seenKey := rateLimitHashKey("ratelimit-source", tenantID, sourceKey)
	tenantKey := rateLimitHashKey("ratelimit-tenant", tenantID)
	tenantLimit := r.tenantLimitPerMinute
	if tenantLimit <= 0 {
		tenantLimit = defaultTenantRateLimitPerMinute
	}
	// INCR and first-use expiry must be atomic. A crash between separate INCR
	// and EXPIRE calls otherwise creates a permanent limiter key. This is a
	// deliberately simple fixed-window policy. The source marker prevents a
	// signed provider redelivery from consuming another user token. It records
	// only allowed sources. Once the counter is over the limit, every new source
	// is rejected from the counter itself; avoiding per-source rejected markers
	// prevents attacker-controlled high-cardinality Redis keys.
	const script = `
local counterTTL = redis.call('PTTL', KEYS[1])
if counterTTL == -1 then
  -- Repair a counter that lost its expiry (for example after an operator
  -- restore) before consulting source markers.
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  counterTTL = tonumber(ARGV[1])
elseif counterTTL <= 0 then
  -- A marker must never outlive the fixed-window counter. Clear stale state so
  -- a source from the new window cannot bypass its first quota token.
  redis.call('DEL', KEYS[2])
  counterTTL = 0
end
local tenantTTL = redis.call('PTTL', KEYS[3])
if tenantTTL == -1 then
  redis.call('PEXPIRE', KEYS[3], ARGV[1])
  tenantTTL = tonumber(ARGV[1])
elseif tenantTTL <= 0 then
  redis.call('DEL', KEYS[3])
  tenantTTL = 0
end
if counterTTL > 0 and redis.call('GET', KEYS[2]) == 'allowed' then return 0 end
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
	if current >= tonumber(ARGV[2]) then
	  return -1
	end
	local tenantCurrent = tonumber(redis.call('GET', KEYS[3]) or '0')
	if tenantCurrent >= tonumber(ARGV[3]) then
	  return -1
	end
	local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  counterTTL = tonumber(ARGV[1])
else
  counterTTL = redis.call('PTTL', KEYS[1])
  if counterTTL <= 0 then counterTTL = tonumber(ARGV[1]) end
end
if count > tonumber(ARGV[2]) then
  return -1
end
local tenantCount = redis.call('INCR', KEYS[3])
if tenantCount == 1 then
  redis.call('PEXPIRE', KEYS[3], ARGV[1])
  tenantTTL = tonumber(ARGV[1])
else
  tenantTTL = redis.call('PTTL', KEYS[3])
  if tenantTTL <= 0 then tenantTTL = tonumber(ARGV[1]) end
end
if tenantCount > tonumber(ARGV[3]) then
  return -1
end
local sourceTTL = counterTTL
if tenantTTL < sourceTTL then sourceTTL = tenantTTL end
if sourceTTL <= 0 then sourceTTL = tonumber(ARGV[1]) end
redis.call('SET', KEYS[2], 'allowed', 'PX', sourceTTL)
return 1`
	result, err := r.redis.Eval(ctx, script, []string{key, seenKey, tenantKey},
		int64(time.Minute/time.Millisecond), int64(rateLimitPerMinute), tenantLimit).Int64()
	if err != nil {
		return sourceRateLimitDecision{}, err
	}
	switch result {
	case -1:
		return sourceRateLimitDecision{}, nil
	case 0:
		return sourceRateLimitDecision{allowed: true}, nil
	case 1:
		return sourceRateLimitDecision{allowed: true, newSource: true}, nil
	default:
		return sourceRateLimitDecision{}, fmt.Errorf("rate limiter returned an invalid decision")
	}
}

const (
	maxInboundUserIDBytes    = 255
	maxInboundConversationID = 256
	maxInboundMessageID      = 256
	maxInboundReplyToID      = 256
	maxInboundMetadata       = 64
	maxInboundMetadataKey    = 128
	maxInboundMetadataValue  = 1024
	maxInboundAttachments    = 32
	maxInboundAttachmentURL  = 2048
	maxInboundAttachmentName = 256
	maxInboundAttachmentMIME = 128
)

func validateInboundMessage(msg *channel.InboundMessage) error {
	if msg == nil {
		return channel.ErrInvalidInboundMessage
	}
	if !utf8.ValidString(msg.Content) || strings.IndexByte(msg.Content, 0) >= 0 {
		return fmt.Errorf("%w: content is invalid", channel.ErrInvalidInboundMessage)
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"external user id", msg.ExternalUserID, maxInboundUserIDBytes},
		{"conversation id", msg.ConversationID, maxInboundConversationID},
		{"message id", msg.MessageID, maxInboundMessageID},
		{"reply target", msg.ReplyToID, maxInboundReplyToID},
	} {
		if field.value == "" && field.name != "message id" && field.name != "reply target" {
			return fmt.Errorf("%w: %s is required", channel.ErrInvalidInboundMessage, field.name)
		}
		if !validInboundIdentifier(field.value) || len(field.value) > field.max {
			return fmt.Errorf("%w: %s is invalid", channel.ErrInvalidInboundMessage, field.name)
		}
	}
	if len(msg.Metadata) > maxInboundMetadata || len(msg.Attachments) > maxInboundAttachments {
		return fmt.Errorf("%w: metadata or attachment count exceeds limit", channel.ErrInvalidInboundMessage)
	}
	for key, value := range msg.Metadata {
		if !validInboundIdentifier(key) || len(key) > maxInboundMetadataKey ||
			!validInboundIdentifier(value) || len(value) > maxInboundMetadataValue {
			return fmt.Errorf("%w: metadata entry is invalid", channel.ErrInvalidInboundMessage)
		}
	}
	for _, attachment := range msg.Attachments {
		typ := strings.ToLower(strings.TrimSpace(attachment.Type))
		switch typ {
		case "image", "file", "audio", "video":
		default:
			return fmt.Errorf("%w: attachment type is invalid", channel.ErrInvalidInboundMessage)
		}
		if attachment.URL == "" || !validInboundIdentifier(attachment.URL) ||
			len(attachment.URL) > maxInboundAttachmentURL ||
			!validInboundIdentifier(attachment.Name) || len(attachment.Name) > maxInboundAttachmentName ||
			!validInboundIdentifier(attachment.MimeType) || len(attachment.MimeType) > maxInboundAttachmentMIME ||
			attachment.Size < 0 {
			return fmt.Errorf("%w: attachment is invalid", channel.ErrInvalidInboundMessage)
		}
	}
	return nil
}

// singleQueryValue rejects duplicate or control-bearing routing parameters.
// Different proxies/frameworks choose first-versus-last semantics for
// duplicates; accepting either would make tenant resolution ambiguous.
func singleQueryValue(r *http.Request, name string) (string, error) {
	if r == nil || r.URL == nil {
		return "", fmt.Errorf("query parameter %q is missing", name)
	}
	values, ok := r.URL.Query()[name]
	if !ok || len(values) != 1 || values[0] == "" || strings.ContainsAny(values[0], "\x00\r\n") {
		return "", fmt.Errorf("query parameter %q is invalid", name)
	}
	return values[0], nil
}

func validInboundIdentifier(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

const rateLimitPerMinute = 20

func rateLimitHashKey(prefix string, values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
	}
	return prefix + ":" + hex.EncodeToString(h.Sum(nil))
}
