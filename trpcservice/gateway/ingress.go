package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admission"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/capacity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ratelimit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type WebhookAdapter interface {
	Verify(*http.Request, []byte) error
	Parse([]byte) (channels.Incoming, error)
}

type WebhookChallengeAdapter interface {
	Challenge([]byte) ([]byte, bool, error)
}

type WebhookIngress interface {
	Handle(context.Context, string, string, *http.Request, []byte) WebhookResult
}

type WebhookResult struct {
	Status      int
	ContentType string
	Body        []byte
	// RetryAfter is a bounded, server-owned retry hint in seconds. Zero
	// means no header. It is only set for the two overload outcomes
	// (rate_limited, capacity_exhausted) and is clamped by the ingress.
	RetryAfter time.Duration
}

// IngressAdmissionTelemetry is the narrow optional capacity observability
// surface. Outcome values are a bounded enum; no tenant, binding or chat
// identifier ever crosses it.
type IngressAdmissionTelemetry interface {
	IngressAdmission(outcome string)
}

// Bounded Retry-After contract: the limiter's own window remainder, clamped
// into [minRateLimitRetryAfter, maxRateLimitRetryAfter]; never a raw
// provider or backend value.
const (
	minRateLimitRetryAfter = time.Second
	maxRateLimitRetryAfter = time.Hour
	// capacityRetryAfter is the fixed, server-owned hint attached to
	// capacity_exhausted rejections.
	capacityRetryAfter = time.Second
)

// IngressConfig contains server-owned dependencies. Tenant, binding, and
// internal identity values are resolved from this boundary, never from body.
type IngressConfig struct {
	Resolver     tenant.TenantResolver
	Identity     tenant.IdentityResolver
	Audit        storage.BindingAuditRepository
	Claims       storage.ClaimStore
	Gateway      *Gateway
	ResolveAgent func(context.Context, tenant.TenantContext) (agent.AgentSpec, error)
	Adapters     map[string]WebhookAdapter
	OwnerID      string
	ClaimTTL     time.Duration
	JobTimeout   time.Duration
	Now          func() time.Time
	// RateLimiter is the optional server-owned three-dimensional Redis
	// quota gate (tenant, channel-binding, external chat). Nil keeps the
	// historical unthrottled behavior for compositions without Redis.
	RateLimiter *ratelimit.RateLimiter
	// Admission is the optional process-local bounded slot budget. Nil
	// keeps historical behavior; production compositions always wire it.
	Admission *admission.Gate
	// DurableBudget is the optional cross-process durable admission budget
	// (capacity.ScopeGuard, scope "ingress"). Unlike Admission it is shared
	// by every app instance through PostgreSQL, so it bounds the aggregate
	// concurrent admitted requests per tenant. Nil keeps historical
	// behavior; a tenant without an ingress budget row is not enforced
	// (fail-open). The reservation is held for the request duration and
	// released on every return path; a lost release is healed by the
	// reservation TTL.
	DurableBudget *capacity.ScopeGuard
	// Telemetry is the optional capacity observability surface. Nil
	// disables capacity metrics; it never changes outcomes.
	Telemetry IngressAdmissionTelemetry
}

type Ingress struct {
	resolver      tenant.TenantResolver
	identity      tenant.IdentityResolver
	audit         storage.BindingAuditRepository
	claims        storage.ClaimStore
	gateway       *Gateway
	resolveAgent  func(context.Context, tenant.TenantContext) (agent.AgentSpec, error)
	adapters      map[string]WebhookAdapter
	ownerID       string
	claimTTL      time.Duration
	jobTimeout    time.Duration
	now           func() time.Time
	rateLimiter   *ratelimit.RateLimiter
	admission     *admission.Gate
	durableBudget *capacity.ScopeGuard
	telemetry     IngressAdmissionTelemetry
}

func NewIngress(config IngressConfig) (*Ingress, error) {
	if config.Resolver == nil || config.Claims == nil || config.Gateway == nil || config.ResolveAgent == nil || config.OwnerID == "" {
		return nil, ErrInvalidRequest
	}
	if config.ClaimTTL == 0 {
		config.ClaimTTL = 15 * time.Minute
	}
	if config.JobTimeout == 0 {
		config.JobTimeout = 2 * time.Minute
	}
	if config.ClaimTTL <= 0 || config.JobTimeout <= 0 || config.JobTimeout > config.ClaimTTL {
		return nil, ErrInvalidRequest
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	if config.Identity == nil {
		config.Identity = tenant.RepositoryIdentityResolver{}
	}
	adapters := make(map[string]WebhookAdapter, len(config.Adapters))
	for channel, adapter := range config.Adapters {
		if strings.TrimSpace(channel) == "" || adapter == nil {
			return nil, ErrInvalidRequest
		}
		adapters[channel] = adapter
	}
	return &Ingress{resolver: config.Resolver, identity: config.Identity, audit: config.Audit, claims: config.Claims, gateway: config.Gateway, resolveAgent: config.ResolveAgent, adapters: adapters, ownerID: config.OwnerID, claimTTL: config.ClaimTTL, jobTimeout: config.JobTimeout, now: config.Now, rateLimiter: config.RateLimiter, admission: config.Admission, durableBudget: config.DurableBudget, telemetry: config.Telemetry}, nil
}

func (i *Ingress) Handle(ctx context.Context, channel, externalAppID string, request *http.Request, body []byte) WebhookResult {
	if i == nil || ctx == nil || request == nil || channel == "" || externalAppID == "" {
		return failureResult(http.StatusBadRequest)
	}
	adapter, ok := i.adapters[channel]
	if !ok {
		return failureResult(http.StatusNotFound)
	}
	if err := adapter.Verify(request, body); err != nil {
		return failureResult(http.StatusUnauthorized)
	}
	if challengeAdapter, ok := adapter.(WebhookChallengeAdapter); ok {
		challenge, isChallenge, err := challengeAdapter.Challenge(body)
		if err != nil {
			return failureResult(http.StatusUnauthorized)
		}
		if isChallenge {
			return WebhookResult{Status: http.StatusOK, ContentType: "application/json", Body: challenge}
		}
	}
	incoming, err := adapter.Parse(body)
	if err != nil || incoming.ID == "" || strings.TrimSpace(incoming.Text) == "" {
		return failureResult(http.StatusBadRequest)
	}
	if (incoming.Channel != "" && incoming.Channel != channel) || incoming.TenantID != "" {
		return failureResult(http.StatusBadRequest)
	}
	incoming.Channel = channel
	tc, err := i.resolver.Resolve(ctx, tenant.ResolveRequest{Channel: channel, ExternalAppID: externalAppID, ExternalUser: incoming.UserID, ExternalChat: incoming.ChatID, ExternalChatType: incoming.ChatType, ExternalThreadID: incoming.ThreadID, RequestID: incoming.ID, MessageID: incoming.ID, TraceID: incoming.ID})
	if err != nil {
		if errors.Is(err, storage.ErrBackendUnavailable) {
			return failureResult(http.StatusServiceUnavailable)
		}
		return failureResult(http.StatusUnauthorized)
	}
	if i.identity != nil {
		// Identity resolution is fail-closed. Audit is best effort and never
		// changes this security decision.
		resolvedIdentity, identityErr := i.identity.ResolveIdentity(ctx, tc)
		if errors.Is(identityErr, storage.ErrBackendUnavailable) {
			return failureResult(http.StatusServiceUnavailable)
		}
		if identityErr != nil || resolvedIdentity.Validate() != nil || resolvedIdentity.Identity.TenantID != tc.TenantID || resolvedIdentity.Identity.Channel != tc.Channel || resolvedIdentity.Identity.BindingID != tc.BindingID {
			return failureResult(http.StatusUnauthorized)
		}
		if i.audit != nil {
			_ = i.audit.AppendBindingEvent(ctx, tc, storage.BindingAuditEvent{TenantID: tc.TenantID, AuditID: auditEventID(tc, "identity_resolve"), BindingID: tc.BindingID, Channel: tc.Channel, Operation: "identity_resolve", Success: true, IdentityFingerprint: tenant.IdentityFingerprint(tc.ExternalUser), Version: resolvedIdentity.Identity.Version, CreatedAt: i.now()})
		}
		tc.InternalUser = resolvedIdentity.Identity.InternalUserID
	} else {
		tc.InternalUser = incoming.UserID
	}
	tc.SessionID = channels.SessionIDWithThread(tc.TenantID, channel, incoming.UserID, incoming.ChatID, incoming.ThreadID)
	if err := tc.Validate(); err != nil {
		return failureResult(http.StatusUnauthorized)
	}
	// P2-03 capacity boundary: both gates sit strictly after the server-owned
	// TenantContext resolution and strictly before the dedup claim and the
	// durable enqueue, so a rejected or overloaded request never claims, never
	// submits and never reaches a runner, tool or sender.
	//
	// Order is fixed: rate limit (shared Redis quota) first, then the
	// process-local admission budget. Admission acquisition is
	// global -> tenant -> binding and the release is deferred, so the slot is
	// held until the fast ACK (or any failure/panic/cancellation return path)
	// and released exactly once.
	if i.rateLimiter != nil {
		decision, limitErr := i.rateLimiter.Allow(ctx, ratelimit.LimitRequest{TenantID: tc.TenantID, BindingID: tc.BindingID, Channel: channel, ExternalChatID: incoming.ChatID, Cost: rateLimitRequestCost})
		i.observeAdmission(limitOutcome(limitErr))
		if limitErr != nil {
			if errors.Is(limitErr, ratelimit.ErrRateLimited) {
				// Quota exhausted: bounded retry hint, never a claim.
				return rateLimitedResult(clampRetryAfter(decision.RetryAfter))
			}
			// Backend unavailable (timeout, protocol error, cancel) is
			// dependency_unavailable: fail closed as 503, never as allowed
			// and never as an ordinary rate limit.
			return failureResult(http.StatusServiceUnavailable)
		}
	}
	if i.admission != nil {
		release, admitErr := i.admission.Acquire(ctx, tc.TenantID, channel+"/"+tc.BindingID)
		if admitErr != nil {
			i.observeAdmission(admissionOutcome(admitErr))
			if errors.Is(admitErr, admission.ErrContextDone) {
				return failureResult(http.StatusServiceUnavailable)
			}
			// Process budget exhausted: bounded fast reject before any claim.
			return capacityResult()
		}
		i.observeAdmission("allowed")
		defer release()
	}
	// WS-8 durable admission budget: after the process-local gate and
	// strictly before the dedup claim and durable enqueue. Exhaustion is
	// the same bounded capacity rejection as the admission gate; a budget
	// backend failure is fail-closed 503 (never an admission).
	if i.durableBudget != nil {
		releaseBudget, budgetErr := i.durableBudget.AcquireScope(ctx, tc.TenantID, capacity.ScopeIngress, i.ownerID, i.claimTTL)
		if budgetErr != nil {
			if errors.Is(budgetErr, capacity.ErrCapacityFull) {
				i.observeAdmission("capacity_exhausted")
				return capacityResult()
			}
			return failureResult(http.StatusServiceUnavailable)
		}
		if releaseBudget != nil {
			defer releaseBudget()
		}
	}
	agentSpec, err := i.resolveAgent(ctx, tc)
	if err != nil {
		return failureResult(http.StatusServiceUnavailable)
	}
	if agentSpec.TenantID != tc.TenantID || agentSpec.AgentAppID != tc.AgentAppID || agentSpec.Version != tc.ConfigVersion {
		return failureResult(http.StatusServiceUnavailable)
	}
	key := storage.DedupKey{TenantID: tc.TenantID, Channel: channel, BindingID: tc.BindingID, ExternalMessageID: incoming.ID}
	claim, err := i.claims.Claim(ctx, tc, key, i.claimTTL, i.ownerID)
	if err != nil {
		return failureResult(http.StatusServiceUnavailable)
	}
	if claim.Status == storage.ClaimCompleted || claim.OwnerID != i.ownerID {
		return acceptedResult(nil, true)
	}
	jobID, executionID := ingressJobIdentity(key)
	createdAt := claim.ClaimedAt
	if createdAt.IsZero() {
		createdAt = i.now()
	}
	accepted, err := i.gateway.SubmitWithIdentity(tenant.WithContext(ctx, tc), GatewayRequest{TenantContext: tc, Agent: agentSpec, Input: agent.Message{ID: incoming.ID, Role: "user", Content: incoming.Text, CreatedAt: createdAt}, CreatedAt: createdAt, Deadline: createdAt.Add(i.jobTimeout)}, jobID, executionID)
	if err != nil {
		return failureResult(http.StatusServiceUnavailable)
	}
	guard := storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}
	if err := i.claims.Complete(ctx, tc, key, i.ownerID, accepted.JobID, guard); err != nil {
		latest, claimErr := i.claims.Claim(ctx, tc, key, i.claimTTL, i.ownerID)
		if claimErr == nil && latest.Status == storage.ClaimCompleted {
			return acceptedResult(nil, true)
		}
		return failureResult(http.StatusServiceUnavailable)
	}
	return acceptedResult(&accepted, false)
}

func auditEventID(tc tenant.TenantContext, operation string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("p1-04-audit:"+tc.TenantID+"|"+tc.Channel+"|"+tc.BindingID+"|"+tc.MessageID+"|"+operation)).String()
}

func ingressJobIdentity(key storage.DedupKey) (string, string) {
	identity := strings.Join([]string{key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-job:"+identity)).String(), uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-execution:"+identity)).String()
}

func acceptedResult(accepted *Accepted, duplicate bool) WebhookResult {
	value := map[string]any{"accepted": true}
	if duplicate {
		value["duplicate"] = true
	}
	if accepted != nil {
		value["job_id"] = accepted.JobID
		value["execution_id"] = accepted.ExecutionID
		value["request_id"] = accepted.RequestID
		value["trace_id"] = accepted.TraceID
	}
	body, err := json.Marshal(value)
	if err != nil {
		return failureResult(http.StatusInternalServerError)
	}
	return WebhookResult{Status: http.StatusAccepted, ContentType: "application/json", Body: body}
}

func failureResult(status int) WebhookResult {
	return WebhookResult{Status: status, ContentType: "application/json", Body: []byte(`{"error":"webhook request rejected"}`)}
}

// rateLimitRequestCost is the bounded per-request quota cost. One webhook
// consumes exactly one unit in every dimension; body size is bounded
// separately by the transport limit.
const rateLimitRequestCost = int64(1)

func clampRetryAfter(value time.Duration) time.Duration {
	if value < minRateLimitRetryAfter {
		return minRateLimitRetryAfter
	}
	if value > maxRateLimitRetryAfter {
		return maxRateLimitRetryAfter
	}
	return value
}

func limitOutcome(err error) string {
	switch {
	case err == nil:
		return "allowed"
	case errors.Is(err, ratelimit.ErrRateLimited):
		return "rate_limited"
	default:
		return "backend_unavailable"
	}
}

func admissionOutcome(err error) string {
	switch {
	case err == nil:
		return "allowed"
	case errors.Is(err, admission.ErrContextDone):
		return "dependency_unavailable"
	default:
		return "capacity_exhausted"
	}
}

func (i *Ingress) observeAdmission(outcome string) {
	if i == nil || i.telemetry == nil {
		return
	}
	i.telemetry.IngressAdmission(outcome)
}

// rateLimitedResult is the quota outcome: HTTP 429 with a bounded
// server-owned Retry-After and a stable, non-sensitive body. The request
// never reached the dedup claim.
func rateLimitedResult(retryAfter time.Duration) WebhookResult {
	return WebhookResult{
		Status:      http.StatusTooManyRequests,
		ContentType: "application/json",
		Body:        []byte(`{"error":"webhook request rejected","reason":"rate_limited"}`),
		RetryAfter:  retryAfter,
	}
}

// capacityResult is the process-budget outcome: HTTP 429 with the fixed
// bounded capacity hint and a stable, non-sensitive body. The request never
// reached the dedup claim and no admission slot was taken.
func capacityResult() WebhookResult {
	return WebhookResult{
		Status:      http.StatusTooManyRequests,
		ContentType: "application/json",
		Body:        []byte(`{"error":"webhook request rejected","reason":"capacity_exhausted"}`),
		RetryAfter:  capacityRetryAfter,
	}
}
