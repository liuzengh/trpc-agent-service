package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// WebhookAdapter is the verification and parsing boundary for one channel.
// The adapter is never allowed to resolve tenant identity from request data.
type WebhookAdapter interface {
	Verify(*http.Request, []byte) error
	Parse([]byte) (channels.Incoming, error)
}

// WebhookChallengeAdapter handles provider URL verification without entering
// the asynchronous job path.
type WebhookChallengeAdapter interface {
	Challenge([]byte) ([]byte, bool, error)
}

// WebhookIngress is the HTTP-neutral async webhook submission boundary.
type WebhookIngress interface {
	Handle(context.Context, string, string, *http.Request, []byte) WebhookResult
}

// WebhookResult is a bounded HTTP result produced by the async ingress.
type WebhookResult struct {
	Status      int
	ContentType string
	Body        []byte
}

// IngressConfig contains all server-owned dependencies for production webhook
// submission. No field is populated from a webhook body.
type IngressConfig struct {
	Resolver     tenant.TenantResolver
	Claims       storage.ClaimStore
	Gateway      *Gateway
	ResolveAgent func(context.Context, tenant.TenantContext) (agent.AgentSpec, error)
	Adapters     map[string]WebhookAdapter
	OwnerID      string
	ClaimTTL     time.Duration
	JobTimeout   time.Duration
	Now          func() time.Time
}

// Ingress verifies a webhook, resolves its binding, claims its external
// identity, and durably submits a Job. It never executes an Agent or sends a
// provider reply in the request goroutine.
type Ingress struct {
	resolver     tenant.TenantResolver
	claims       storage.ClaimStore
	gateway      *Gateway
	resolveAgent func(context.Context, tenant.TenantContext) (agent.AgentSpec, error)
	adapters     map[string]WebhookAdapter
	ownerID      string
	claimTTL     time.Duration
	jobTimeout   time.Duration
	now          func() time.Time
}

func NewIngress(config IngressConfig) (*Ingress, error) {
	if config.Resolver == nil || config.Claims == nil || config.Gateway == nil || config.ResolveAgent == nil || config.OwnerID == "" || len(config.Adapters) == 0 {
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
	adapters := make(map[string]WebhookAdapter, len(config.Adapters))
	for channel, adapter := range config.Adapters {
		if strings.TrimSpace(channel) == "" || adapter == nil {
			return nil, ErrInvalidRequest
		}
		adapters[channel] = adapter
	}
	return &Ingress{
		resolver:     config.Resolver,
		claims:       config.Claims,
		gateway:      config.Gateway,
		resolveAgent: config.ResolveAgent,
		adapters:     adapters,
		ownerID:      config.OwnerID,
		claimTTL:     config.ClaimTTL,
		jobTimeout:   config.JobTimeout,
		now:          config.Now,
	}, nil
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
	incoming.Channel = channel
	tc, err := i.resolver.Resolve(ctx, tenant.ResolveRequest{
		Channel:          channel,
		ExternalAppID:    externalAppID,
		ExternalUser:     incoming.UserID,
		ExternalChat:     incoming.ChatID,
		ExternalThreadID: incoming.ThreadID,
		RequestID:        incoming.ID,
		MessageID:        incoming.ID,
		TraceID:          incoming.ID,
	})
	if err != nil {
		if errors.Is(err, storage.ErrBackendUnavailable) {
			return failureResult(http.StatusServiceUnavailable)
		}
		return failureResult(http.StatusUnauthorized)
	}
	tc.SessionID = channels.SessionID(tc.TenantID, channel, incoming.UserID, incoming.ChatID)
	if err := tc.Validate(); err != nil {
		return failureResult(http.StatusUnauthorized)
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
	accepted, err := i.gateway.SubmitWithIdentity(tenant.WithContext(ctx, tc), GatewayRequest{
		TenantContext: tc,
		Agent:         agentSpec,
		Input: agent.Message{
			ID:        incoming.ID,
			Role:      "user",
			Content:   incoming.Text,
			CreatedAt: createdAt,
		},
		CreatedAt: createdAt,
		Deadline:  createdAt.Add(i.jobTimeout),
	}, jobID, executionID)
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

func ingressJobIdentity(key storage.DedupKey) (string, string) {
	identity := strings.Join([]string{key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-job:"+identity)).String(),
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-execution:"+identity)).String()
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
