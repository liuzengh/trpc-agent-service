// Package pipeline contains the production Inbox consumer and IM delivery
// loops. It deliberately owns orchestration only; persistence remains in
// package reliable and Agent execution remains behind worker.Client.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

// ConsumerConfig bounds concurrency, leases and model execution.
type ConsumerConfig struct {
	Owner       string
	Concurrency int
	// FairQueue enables the optional operator-owned tenant scheduler. When
	// enabled, construction fails closed unless the durable Store implements
	// reliable.FairInboxClaimer; the default remains the compatible global FIFO
	// claimer.
	FairQueue           bool
	PollInterval        time.Duration
	LeaseDuration       time.Duration
	ProcessTimeout      time.Duration
	RetryBase           time.Duration
	RetryMaximum        time.Duration
	ExpiryReapInterval  time.Duration
	ExpiryReapBatchSize int
}

// Consumer turns durable Inbox rows into durable Outbox rows.
type Consumer struct {
	store   reliable.Store
	tenants tenant.Service
	worker  worker.Client
	config  ConsumerConfig
}

// NewConsumer validates dependencies and applies safe defaults.
func NewConsumer(store reliable.Store, tenants tenant.Service, workerClient worker.Client, config ConsumerConfig) (*Consumer, error) {
	if isNilPipelineDependency(store) || isNilPipelineDependency(tenants) || isNilPipelineDependency(workerClient) {
		return nil, fmt.Errorf("consumer requires store, tenant service and worker client")
	}
	if config.FairQueue {
		if _, ok := store.(reliable.FairInboxClaimer); !ok {
			return nil, fmt.Errorf("consumer: %w", reliable.ErrFairQueueUnavailable)
		}
	}
	if config.Owner == "" {
		return nil, fmt.Errorf("consumer owner is required")
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 4
	}
	if err := validateDerivedOwner(config.Owner, config.Concurrency); err != nil {
		return nil, fmt.Errorf("consumer owner: %w", err)
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 3 * time.Minute
	}
	if config.ProcessTimeout <= 0 {
		config.ProcessTimeout = 150 * time.Second
	}
	if config.LeaseDuration < minimumSafeLease(config.ProcessTimeout) {
		return nil, fmt.Errorf(
			"consumer lease duration %s must be at least %s for process timeout %s",
			config.LeaseDuration,
			minimumSafeLease(config.ProcessTimeout),
			config.ProcessTimeout,
		)
	}
	if config.RetryBase <= 0 {
		config.RetryBase = time.Second
	}
	if config.RetryMaximum <= 0 {
		config.RetryMaximum = 5 * time.Minute
	}
	if config.RetryMaximum < config.RetryBase {
		return nil, fmt.Errorf(
			"consumer retry maximum %s must be at least retry base %s",
			config.RetryMaximum, config.RetryBase,
		)
	}
	interval, batchSize, err := normalizeExpiryReapConfig(config.ExpiryReapInterval, config.ExpiryReapBatchSize)
	if err != nil {
		return nil, fmt.Errorf("consumer expiry reaper: %w", err)
	}
	config.ExpiryReapInterval = interval
	config.ExpiryReapBatchSize = batchSize
	return &Consumer{store: store, tenants: tenants, worker: workerClient, config: config}, nil
}

// Run starts a fixed-size worker pool. Cancelling ctx stops new claims; an
// already claimed invocation receives a fresh bounded context so SIGTERM drains
// it rather than abandoning it immediately.
func (c *Consumer) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var wg sync.WaitGroup
	for i := 0; i < c.config.Concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			c.loop(ctx, fmt.Sprintf("%s-%d", c.config.Owner, index))
		}(i)
	}
	if reaper, ok := c.store.(reliable.ExpiredWorkReaper); ok && !isNilPipelineDependency(reaper) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runExpiryReaper(ctx, "consumer", reaper, c.config.ExpiryReapInterval, c.config.ExpiryReapBatchSize)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (c *Consumer) loop(ctx context.Context, owner string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.claimInbox(ctx, owner)
		if errors.Is(err, reliable.ErrNoWork) {
			if !wait(ctx, c.config.PollInterval) {
				return
			}
			continue
		}
		if err != nil {
			log.Printf("consumer owner=%s claim failed: error=%s", owner, stablePipelineError(err))
			if !wait(ctx, c.config.PollInterval) {
				return
			}
			continue
		}

		// Preserve trace values but detach the invocation from intake
		// cancellation, then bound it with an explicit processing timeout.
		processCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.ProcessTimeout)
		c.processOne(processCtx, msg)
		cancel()
	}
}

func (c *Consumer) claimInbox(ctx context.Context, owner string) (*reliable.InboxMessage, error) {
	if c.config.FairQueue {
		return c.store.(reliable.FairInboxClaimer).ClaimInboxFair(ctx, owner, c.config.LeaseDuration)
	}
	return c.store.ClaimInbox(ctx, owner, c.config.LeaseDuration)
}

func (c *Consumer) processOne(ctx context.Context, msg *reliable.InboxMessage) {
	if msg == nil {
		log.Printf("consumer received a nil Inbox message")
		return
	}
	ctx = nonNilContext(ctx)
	started, result := time.Now(), "error"
	defer func() { observePipeline("consumer", result, msg.TenantID, started, msg.CreatedAt) }()
	ctx = telemetry.ContextWithTraceParent(ctx, msg.TraceParent)
	ctx, span := telemetry.StartOperation(ctx, telemetry.OperationInboxProcess)
	defer func() {
		if result == "error" {
			telemetry.EndOperation(span, errors.New("inbox processing failed"))
			return
		}
		telemetry.EndOperation(span, nil)
	}()
	status, err := resolveConsumerTenantStatus(ctx, c.tenants, msg.TenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			c.deadLetter(ctx, msg, fmt.Errorf("tenant no longer exists: %w", err))
			return
		}
		c.retry(ctx, msg, fmt.Errorf("tenant unavailable: %w", err))
		return
	}
	if status == tenant.TenantStatusSuspended {
		c.block(ctx, msg, fmt.Errorf("tenant is suspended"))
		return
	}
	if status == tenant.TenantStatusDeleted {
		c.deadLetter(ctx, msg, fmt.Errorf("tenant is deleted"))
		return
	}
	if status != tenant.TenantStatusActive {
		c.deadLetter(ctx, msg, fmt.Errorf("tenant has unsupported status %q", status))
		return
	}

	var inbound channel.InboundMessage
	if err := json.Unmarshal(msg.Payload, &inbound); err != nil {
		// The payload was durably accepted by Gateway, so retrying a malformed
		// canonical payload can never make progress and only creates an error
		// storm. Preserve the original bytes in the durable error field, bounded
		// by reliable.errorText, and require an audited replay after correction.
		c.deadLetter(ctx, msg, fmt.Errorf("decode canonical inbound payload: %w", err))
		return
	}
	hasGroupFlag, payloadGroup, hasPayloadOwner, payloadOwner, err := inboundRoutingFields(msg.Payload)
	if err != nil {
		c.deadLetter(ctx, msg, fmt.Errorf("decode canonical inbound routing: %w", err))
		return
	}
	// The Inbox routing columns are authoritative. Older payloads did not carry
	// all canonical identity fields, so fill only missing values from the
	// trusted row before validating the Runner session identity. A present value
	// that disagrees is treated as a corrupt/tampered payload and is not retried.
	if err := reconcileInboundIdentity(&inbound, msg); err != nil {
		c.deadLetter(ctx, msg, err)
		return
	}
	var ownerID string
	if msg.RoutingVersion >= reliable.CurrentInboxRoutingVersion {
		// Current rows carry the routing decision in durable columns. The
		// payload is only checked for an explicit matching projection; it is
		// never allowed to override the row after a partial/corrupt write.
		if !hasGroupFlag || payloadGroup != msg.IsGroupChat {
			c.deadLetter(ctx, msg, errors.New("inbox routing group flag is missing or inconsistent"))
			return
		}
		if msg.SessionOwnerID == "" {
			c.deadLetter(ctx, msg, errors.New("inbox routing is missing a session owner"))
			return
		}
		if hasPayloadOwner && payloadOwner != msg.SessionOwnerID {
			c.deadLetter(ctx, msg, errors.New("inbox routing session owner mismatch"))
			return
		}
		inbound.IsGroupChat = msg.IsGroupChat
		inbound.SessionOwnerID = msg.SessionOwnerID
		ownerID = msg.SessionOwnerID
		if err := channel.ValidateSessionIdentity(&inbound, msg.SessionID, ownerID); err != nil {
			c.deadLetter(ctx, msg, fmt.Errorf("validate session identity: %w", err))
			return
		}
	} else {
		// Legacy rows predate the durable routing columns. A missing group flag
		// is ambiguous (it could be a group silently decoded as direct), so do
		// not guess; require the original payload to prove the relationship.
		if !hasGroupFlag {
			c.deadLetter(ctx, msg, errors.New("legacy inbox routing is ambiguous without isGroupChat"))
			return
		}
		inbound.IsGroupChat = payloadGroup
		// Preserve the deterministic owner so replay does not silently fork the
		// Session. The payload owner is advisory for legacy rows and is not used
		// unless it independently matches the derived value below.
		ownerID, err = channel.SessionOwnerIDFor(&inbound, msg.SessionID)
		if err != nil {
			c.deadLetter(ctx, msg, fmt.Errorf("derive legacy session owner: %w", err))
			return
		}
		if hasPayloadOwner && payloadOwner != ownerID {
			c.deadLetter(ctx, msg, errors.New("legacy inbox routing session owner mismatch"))
			return
		}
		if err := channel.ValidateSessionIdentity(&inbound, msg.SessionID, ownerID); err != nil {
			c.deadLetter(ctx, msg, fmt.Errorf("validate legacy session identity: %w", err))
			return
		}
	}
	inbound.SessionOwnerID = ownerID

	// The Agent App is a durable routing decision made by Gateway. Never select
	// the tenant's current first Agent for a damaged or legacy row: that would
	// execute historical work against a mutable deployment and make replay
	// non-reproducible.
	if msg.AgentApp == "" {
		c.deadLetter(ctx, msg, errors.New("inbox routing is missing a pinned agent app"))
		return
	}

	req := &worker.Request{
		TenantID:         msg.TenantID,
		ChannelType:      msg.ChannelType,
		ConversationID:   msg.ConversationID,
		ChannelAccountID: msg.ChannelAccountID,
		UserID:           msg.UserID,
		SessionOwnerID:   ownerID,
		SessionID:        msg.SessionID,
		MessageID:        msg.ExternalMessageID,
		IdempotencyKey:   fmt.Sprintf("inbox:%d", msg.ID),
		PayloadHash:      msg.PayloadHash,
		Content:          inbound.Content,
		Attachments:      append([]channel.Attachment(nil), inbound.Attachments...),
		IsGroupChat:      inbound.IsGroupChat,
		Metadata: map[string]interface{}{
			"traceparent":        msg.TraceParent,
			"channel_account_id": msg.ChannelAccountID,
		},
	}
	req.AgentApp = msg.AgentApp

	response, err := c.worker.ProcessMessage(ctx, req)
	if err != nil {
		// A budget-aware Worker may have completed a side-effecting invocation
		// before a proxy or mixed-version hop stripped the response proof. The
		// durable result cannot be trusted from this boundary, so stop the
		// session FIFO and require reconciliation instead of retrying blindly.
		if errors.Is(err, worker.ErrWorkerExecutionOutcomeUnknown) {
			c.block(ctx, msg, err)
			return
		}
		// LocalClient returns this fail-closed Worker sentinel directly, while
		// HTTPClient receives the equivalent 423 response. Both transports must
		// stop automatic execution until an operator reconciles the transcript.
		if errors.Is(err, worker.ErrApprovalResumeUnsafe) {
			c.block(ctx, msg, err)
			return
		}
		if errors.Is(err, worker.ErrExecutionPreflightPermanent) {
			// A deterministic policy or request rejection occurred before the
			// Runner boundary. Retrying it would only consume attempts and block
			// the Session FIFO; preserve it in the Inbox DLQ instead.
			c.deadLetter(context.WithoutCancel(ctx), msg, fmt.Errorf("worker preflight rejected: %w", err))
			return
		}
		if pause, ok := worker.AsApprovalPause(err); ok {
			c.waitForApproval(ctx, msg, err, pause)
			return
		}
		var statusErr *worker.HTTPStatusError
		if errors.As(err, &statusErr) && statusErr != nil {
			switch {
			case statusErr.StatusCode == http.StatusLocked:
				c.block(ctx, msg, err)
				return
			case !statusErr.Retryable():
				// 4xx conflicts, auth failures and malformed requests are
				// deterministic. Retrying them cannot repair the request and
				// would keep a session FIFO blocked indefinitely.
				c.deadLetter(ctx, msg, fmt.Errorf("worker execution is not retryable: %w", err))
				return
			case statusErr.RetryAfter > 0:
				c.retryWithDelay(context.WithoutCancel(ctx), msg,
					fmt.Errorf("worker execution: %w", err), statusErr.RetryAfter)
				return
			}
		}
		c.retry(context.WithoutCancel(ctx), msg, fmt.Errorf("worker execution: %w", err))
		return
	}
	if response == nil {
		// A nil response without an error violates the Worker protocol. Do not
		// dereference it or leave the claimed Inbox row to lease expiry; this is
		// deterministic for the current attempt and must be visible in the DLQ.
		c.deadLetter(context.WithoutCancel(ctx), msg, errors.New("worker returned an empty response"))
		return
	}
	contentType := response.ContentType
	if contentType == "" {
		contentType = "text"
	}
	reply := reliable.OutboxReply{
		ContentType: contentType,
		Content:     response.Content,
		TraceParent: telemetry.TraceParentFromContext(ctx),
	}
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	var completionErr error
	if response.Summary != nil {
		if err := response.Summary.Validate(); err != nil {
			c.block(context.WithoutCancel(ctx), msg, fmt.Errorf("invalid worker summary receipt: %w", err))
			return
		}
		completer, ok := c.store.(reliable.SummaryInboxCompleter)
		if !ok {
			c.block(context.WithoutCancel(ctx), msg, errors.New("atomic summary completion is unavailable"))
			return
		}
		_, completionErr = completer.CompleteInboxWithSummary(
			persistCtx, msg.ID, msg.Lease, reply, *response.Summary,
		)
	} else {
		_, completionErr = c.store.CompleteInbox(persistCtx, msg.ID, msg.Lease, reply)
	}
	if completionErr != nil {
		// A stale fence means another worker owns recovery; never create a
		// second reply outside the atomic store operation.
		log.Printf("consumer inbox=%d completion failed: error=%s", msg.ID, stablePipelineError(completionErr))
		if errors.Is(completionErr, reliable.ErrInvalidInboxMessage) {
			// The Worker returned a response that cannot be persisted (for example,
			// an empty or malformed body). Retrying the same deterministic value
			// cannot make progress and would leave the Inbox in PROCESSING until its
			// lease expires. Dead-letter it under the original fence instead.
			c.deadLetter(context.WithoutCancel(ctx), msg, completionErr)
			return
		}
		if errors.Is(completionErr, reliable.ErrOutboxConflict) ||
			errors.Is(completionErr, reliable.ErrSummaryCompletionConflict) {
			// The store could not prove that the existing reply belongs to this
			// completion. Stop automatic retries and require operator reconciliation
			// before replaying the Inbox, otherwise session FIFO remains blocked.
			c.block(context.WithoutCancel(ctx), msg, completionErr)
			return
		}
		if errors.Is(completionErr, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("consumer").Inc()
		}
		return
	}
	result = "success"
}

func resolveConsumerTenantStatus(ctx context.Context, service tenant.Service, tenantID string) (tenant.TenantStatus, error) {
	if reader, ok := service.(tenant.TenantStatusReader); ok {
		return reader.GetTenantStatus(ctx, tenantID)
	}
	// Compatibility path for older Service implementations. Production's
	// TenantService uses the status-only SQL capability above.
	value, err := service.GetTenant(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", fmt.Errorf("tenant lookup returned nil")
	}
	return value.Status, nil
}

func reconcileInboundIdentity(inbound *channel.InboundMessage, msg *reliable.InboxMessage) error {
	if inbound == nil || msg == nil {
		return fmt.Errorf("canonical inbound identity is missing")
	}
	checks := []struct {
		name    string
		payload *string
		durable string
	}{
		{"tenant", &inbound.TenantID, msg.TenantID},
		{"channel", &inbound.ChannelType, msg.ChannelType},
		{"channel account", &inbound.ChannelAccountID, msg.ChannelAccountID},
		{"user", &inbound.ExternalUserID, msg.UserID},
		{"conversation", &inbound.ConversationID, msg.ConversationID},
		{"message", &inbound.MessageID, msg.ExternalMessageID},
	}
	for _, check := range checks {
		if *check.payload == "" {
			*check.payload = check.durable
			continue
		}
		if check.durable != "" && *check.payload != check.durable {
			return fmt.Errorf("canonical inbound %s identity mismatch", check.name)
		}
	}
	if inbound.ReplyToID == "" {
		inbound.ReplyToID = msg.ReplyToID
	} else if msg.ReplyToID != "" && inbound.ReplyToID != msg.ReplyToID {
		return fmt.Errorf("canonical inbound reply target mismatch")
	}
	return nil
}

// inboundRoutingFields reports field presence separately from the Go zero
// value. This is required for rolling upgrades: a legacy payload that omitted
// isGroupChat must not be silently interpreted as a direct message.
func inboundRoutingFields(payload []byte) (hasGroup bool, group bool, hasOwner bool, owner string, err error) {
	var raw struct {
		IsGroupChat    json.RawMessage `json:"isGroupChat"`
		SessionOwnerID json.RawMessage `json:"sessionOwnerId"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return false, false, false, "", err
	}
	if len(raw.IsGroupChat) != 0 && string(raw.IsGroupChat) != "null" {
		hasGroup = true
		if err := json.Unmarshal(raw.IsGroupChat, &group); err != nil {
			return false, false, false, "", fmt.Errorf("isGroupChat is invalid: %w", err)
		}
	}
	if len(raw.SessionOwnerID) != 0 && string(raw.SessionOwnerID) != "null" {
		hasOwner = true
		if err := json.Unmarshal(raw.SessionOwnerID, &owner); err != nil {
			return false, false, false, "", fmt.Errorf("sessionOwnerId is invalid: %w", err)
		}
	}
	return hasGroup, group, hasOwner, owner, nil
}

// waitForApproval persists a pre-execution approval pause without consuming
// the ordinary retry budget. The Store, rather than this Consumer node, owns
// validation of the expiry against its authoritative clock.
func (c *Consumer) waitForApproval(ctx context.Context, msg *reliable.InboxMessage, cause error, pause worker.ApprovalPause) {
	if pause.ExpiresAt.IsZero() {
		c.block(ctx, msg, fmt.Errorf("worker approval result has no valid expiry: %w", cause))
		return
	}
	approvalStore, ok := c.store.(reliable.ApprovalWaitStore)
	if !ok {
		c.block(ctx, msg, fmt.Errorf("approval wait persistence is unavailable: %w", cause))
		return
	}
	delay := pause.RetryAfter
	if delay <= 0 {
		delay = c.config.RetryBase
	}
	if delay > c.config.RetryMaximum {
		delay = c.config.RetryMaximum
	}
	persistCtx, cancelPersist := persistenceContext(ctx)
	waitErr := approvalStore.WaitInboxApproval(
		persistCtx, msg.ID, msg.Lease,
		errors.New("tool approval required"), delay, pause.ExpiresAt,
	)
	cancelPersist()
	if waitErr == nil {
		return
	}
	log.Printf("consumer inbox=%d approval wait failed: error=%s", msg.ID, stablePipelineError(waitErr))
	if errors.Is(waitErr, reliable.ErrApprovalDeadlineInvalid) {
		c.block(ctx, msg, fmt.Errorf("worker approval result has invalid expiry: %w", cause))
		return
	}
	if errors.Is(waitErr, reliable.ErrStaleLease) {
		pipelineFenceRejects.WithLabelValues("consumer").Inc()
	}
}

func (c *Consumer) retry(ctx context.Context, msg *reliable.InboxMessage, cause error) {
	c.retryWithDelay(ctx, msg, cause, 0)
}

func (c *Consumer) retryWithDelay(ctx context.Context, msg *reliable.InboxMessage, cause error, suggested time.Duration) {
	pipelineRetries.WithLabelValues("consumer", telemetry.MetricTenantLabel(msg.TenantID)).Inc()
	jitter := reliable.DeterministicJitter(msg.ID, c.config.RetryBase)
	delay := reliable.Backoff(msg.AttemptCount, c.config.RetryBase, c.config.RetryMaximum, jitter)
	if suggested > 0 {
		// Provider hints take precedence, but an untrusted Retry-After must
		// never hold a queue row forever. The local maximum is an operational
		// upper bound, not a promise that the provider will be ready then.
		if suggested > c.config.RetryMaximum {
			suggested = c.config.RetryMaximum
		}
		delay = suggested
	}
	clockSafe, ok := c.store.(reliable.RetryAfterStore)
	if !ok {
		// A node wall clock is not an authority for a distributed retry
		// deadline. Park the message for reconciliation instead of silently
		// scheduling it using a potentially skewed local clock.
		c.block(ctx, msg, fmt.Errorf("retry-after persistence is unavailable"))
		return
	}
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	err := clockSafe.RetryInboxAfter(persistCtx, msg.ID, msg.Lease, cause, delay)
	if err != nil {
		log.Printf("consumer inbox=%d fenced retry failed: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("consumer").Inc()
		}
	}
}

func (c *Consumer) block(ctx context.Context, msg *reliable.InboxMessage, cause error) {
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	if err := c.store.BlockInbox(persistCtx, msg.ID, msg.Lease, cause); err != nil {
		log.Printf("consumer inbox=%d reconciliation block failed: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("consumer").Inc()
		}
	}
}

func (c *Consumer) deadLetter(ctx context.Context, msg *reliable.InboxMessage, cause error) {
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	if err := c.store.DeadLetterInbox(persistCtx, msg.ID, msg.Lease, cause); err != nil {
		log.Printf("consumer inbox=%d dead-letter transition failed: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("consumer").Inc()
		}
		return
	}
	pipelineDeadLetters.WithLabelValues("consumer", telemetry.MetricTenantLabel(msg.TenantID)).Inc()
}

// minimumSafeLease leaves a bounded persistence margin after an external call
// times out. Without the margin, a response arriving at the deadline can lose
// its fence before RetryInbox or CompleteInbox commits.
func minimumSafeLease(operationTimeout time.Duration) time.Duration {
	if operationTimeout <= 0 {
		return 0
	}
	margin := operationTimeout / 10
	if margin < 5*time.Second {
		margin = 5 * time.Second
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if operationTimeout > maxDuration-margin {
		return maxDuration
	}
	return operationTimeout + margin
}

func persistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(nonNilContext(ctx)), 5*time.Second)
}

func wait(ctx context.Context, duration time.Duration) bool {
	ctx = nonNilContext(ctx)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func isNilPipelineDependency(value interface{}) bool {
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
