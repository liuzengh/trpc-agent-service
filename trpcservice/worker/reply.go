package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	defaultReplyPollInterval     = time.Second
	defaultReplyLease            = 30 * time.Second
	defaultReplyBatch            = 32
	defaultReplyMaxAttempts      = 8
	replyRetryInitial            = time.Second
	replyRetryMax                = time.Minute
	replyUncertainPersistTimeout = 5 * time.Second
)

var (
	// ErrReplyBindingInactive means a binding was disabled after a reply was
	// claimed. The sender returns the row to PENDING so re-enabling the binding
	// can resume delivery without invoking Runner again.
	ErrReplyBindingInactive = errors.New("reply binding is inactive")
	// ErrReplyBindingChanged means a reply was created against an older
	// binding authorization snapshot and must not be sent with the new target.
	ErrReplyBindingChanged = errors.New("reply binding authorization changed")
	// ErrReplyCapabilityUnavailable means the selected provider has no native
	// implementation for the requested rich reply kind.
	ErrReplyCapabilityUnavailable = errors.New("reply provider capability is unavailable")
)

// ReplyMode selects the IM presentation mode for new executions. Text is the
// compatibility default; stream and card use the same durable Reply Outbox.
type ReplyMode string

const (
	ReplyModeText   ReplyMode = "text"
	ReplyModeStream ReplyMode = "stream"
	ReplyModeCard   ReplyMode = "card"
)

func (m ReplyMode) Validate() error {
	switch m {
	case ReplyModeText, ReplyModeStream, ReplyModeCard:
		return nil
	default:
		return fmt.Errorf("reply mode %q is invalid", m)
	}
}

// Build returns one durable text reply for a completed execution event.
// Events without user-visible assistant text return no replies while the
// execution event itself remains durable.
func BuildReplyEvent(
	ctx context.Context,
	exec Execution,
	sequence int64,
	evt *event.Event,
) ([]channels.Reply, error) {
	return BuildReplyEventForMode(ctx, exec, sequence, evt, ReplyModeText)
}

// BuildReplyEventForMode projects model partials and terminal output into
// durable provider-neutral frames. The PostgreSQL journal later converts
// partial stream deltas into full snapshots atomically with event persistence.
func BuildReplyEventForMode(
	ctx context.Context,
	exec Execution,
	sequence int64,
	evt *event.Event,
	mode ReplyMode,
) ([]channels.Reply, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequence <= 0 {
		return nil, errors.New("reply event sequence must be positive")
	}
	if exec.RequestID == "" {
		return nil, errors.New("execution request_id is required")
	}
	if evt == nil {
		return nil, errors.New("runner event is required")
	}
	if evt.RequestID != "" && evt.RequestID != exec.RequestID {
		return nil, errors.New("runner event request_id does not match execution")
	}
	if err := mode.Validate(); err != nil {
		return nil, err
	}
	if exec.Tenant.Channel == "" || exec.Tenant.BindingID == "" {
		return nil, nil
	}
	if evt.IsTerminalError() {
		// Keep the final provider-facing boundary guarded even when callers
		// invoke the projector directly instead of going through the journal.
		text := safeReplyText(replyErrorText(evt))
		if mode == ReplyModeText {
			return []channels.Reply{newReply(exec, sequence, mode, text, "")}, nil
		}
		if mode == ReplyModeCard {
			return []channels.Reply{newCardReplyWithStatusAndPhase(exec, sequence, text, "FAILED", channels.StreamPhaseAbort)}, nil
		}
		return []channels.Reply{newReply(exec, sequence, mode, text, channels.StreamPhaseAbort)}, nil
	}
	if exec.ApprovalPending && evt.IsRunnerCompletion() {
		return nil, nil
	}
	if mode == ReplyModeText && !evt.IsRunnerCompletion() {
		// Text mode keeps the terminal projection contract: the runner
		// completion owns the one user-visible reply. The journal merges the
		// preceding final chat.completion into it when necessary.
		return nil, nil
	}
	phase := channels.StreamPhaseEnd
	text := visibleAssistantText(evt)
	isPartial := evt.Response != nil && evt.Response.IsPartial
	if mode == ReplyModeStream || mode == ReplyModeCard {
		if isPartial {
			if mode == ReplyModeCard && channels.Channel(exec.Tenant.Channel) == channels.ChannelWeCom {
				// WeCom has no card patch operation. Emit one final card instead
				// of creating a new card for every partial event.
				return nil, nil
			}
			text = visibleAssistantDeltaText(evt)
			phase = channels.StreamPhaseUpdate
		} else if !evt.IsRunnerCompletion() {
			return nil, nil
		}
	}
	text = safeReplyText(text)
	if text == "" && !evt.IsRunnerCompletion() {
		return nil, nil
	}
	if text == "" && evt.IsRunnerCompletion() && mode == ReplyModeText {
		// A text reply has no lifecycle row to close. Do not enqueue an invalid
		// empty ordinary message when the runner completed without visible text.
		return nil, nil
	}
	if mode == ReplyModeCard {
		status := "SUCCEEDED"
		if isPartial {
			status = "STREAMING"
		}
		return []channels.Reply{newCardReplyWithStatusAndPhase(exec, sequence, text, status, phase)}, nil
	}
	return []channels.Reply{newReply(exec, sequence, mode, text, phase)}, nil
}

func safeReplyText(text string) string {
	return guardrail.SanitizeOutput(text).Text
}

func newReply(exec Execution, sequence int64, mode ReplyMode, text string, phase channels.StreamPhase) channels.Reply {
	target := replyTarget(exec)
	reply := channels.Reply{
		TenantID:        exec.Tenant.TenantID,
		AppID:           exec.Tenant.AppID,
		RequestID:       exec.RequestID,
		SourceEventID:   fmt.Sprintf("%s:%d", exec.RequestID, sequence),
		Channel:         channels.Channel(exec.Tenant.Channel),
		BindingID:       exec.Tenant.BindingID,
		BindingRevision: exec.Tenant.BindingRevision,
		Revision:        sequence,
		Target:          target,
		Text:            text,
	}
	if mode == ReplyModeStream {
		reply.Kind = channels.ReplyKindStream
		reply.StreamID = exec.RequestID
		reply.StreamPhase = phase
		reply.StreamSequence = sequence
	}
	reply.ReplyID = reply.StableID()
	return reply
}

func newCardReply(exec Execution, sequence int64, text string) channels.Reply {
	return newCardReplyWithStatusAndPhase(exec, sequence, text, "SUCCEEDED", channels.StreamPhaseEnd)
}

func newCardReplyWithStatus(exec Execution, sequence int64, text, status string) channels.Reply {
	return newCardReplyWithStatusAndPhase(exec, sequence, text, status, channels.StreamPhaseEnd)
}

func newCardReplyWithStatusAndPhase(
	exec Execution,
	sequence int64,
	text, status string,
	phase channels.StreamPhase,
) channels.Reply {
	reply := newReply(exec, sequence, ReplyModeStream, "", phase)
	reply.Kind = channels.ReplyKindCard
	reply.Card = &channels.ReplyCard{
		Title:  "Agent 回复",
		Body:   text,
		Status: status,
	}
	reply.ReplyID = reply.StableID()
	return reply
}

func replyTarget(exec Execution) channels.ReplyTarget {
	target := channels.ReplyTarget{
		Kind:             channels.TargetKindUser,
		InternalEntityID: exec.Tenant.UserID,
	}
	if exec.Tenant.SessionPrincipalID != "" && exec.Tenant.SessionPrincipalID != exec.Tenant.UserID {
		target = channels.ReplyTarget{
			Kind:             channels.TargetKindConversation,
			InternalEntityID: exec.Tenant.SessionPrincipalID,
		}
	}
	return target
}

// ReplyOutbox persists and leases provider replies independently from the
// execution Dispatch Outbox.
type ReplyOutbox interface {
	ClaimReplies(context.Context, string, time.Duration, int) ([]ReplyDelivery, error)
	CompleteReply(context.Context, ReplyDelivery, channels.ProviderReceipt) error
	MarkReplyUncertain(context.Context, ReplyDelivery, string, error) error
	RetryReply(context.Context, ReplyDelivery, string, time.Duration, error) error
	FailReply(context.Context, ReplyDelivery, string, error) error
	RecoverReplyLeases(context.Context) error
}

// ReplyDelivery is one Reply Outbox row leased by a sender.
type ReplyDelivery struct {
	Reply             channels.Reply
	Attempt           int
	LeaseOwner        string
	LeaseUntil        time.Time
	ProviderMessageID string
	TraceID           string
	TraceParent       string
	TraceState        string
}

// Validate checks the identity and lease fields of a claimed reply.
func (d ReplyDelivery) Validate() error {
	if err := d.Reply.Validate(); err != nil {
		return fmt.Errorf("reply: %w", err)
	}
	if d.Attempt <= 0 || d.LeaseOwner == "" || d.LeaseUntil.IsZero() {
		return errors.New("reply delivery lease is incomplete")
	}
	return nil
}

// ReplyProvider is the provider client and optional rate limiter selected for
// one Reply Outbox delivery.
type ReplyProvider struct {
	Client  channels.ProviderOutboundClient
	Limiter ReplyRateLimiter
}

// ReplyRateLimiter delays or rejects one provider send within its bound
// provider account.
type ReplyRateLimiter interface {
	Allow(context.Context, ReplyDelivery) error
}

// ReplySenderOptions configures the finite Reply Outbox delivery loop.
type ReplySenderOptions struct {
	Owner        string
	Lease        time.Duration
	SendTimeout  time.Duration
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	Metrics      *platformmetrics.Recorder
}

// ReplySender claims Reply Outbox rows, sends each row once, and records the
// bounded result. It never invokes Runner.
type ReplySender struct {
	outbox       ReplyOutbox
	targets      func(context.Context, ReplyDelivery) (string, error)
	providers    func(context.Context, ReplyDelivery) (ReplyProvider, error)
	owner        string
	lease        time.Duration
	sendTimeout  time.Duration
	pollInterval time.Duration
	batchSize    int
	maxAttempts  int
	metrics      *platformmetrics.Recorder
}

// RetryableReplyError marks a reply dependency failure that is safe to retry
// without invoking the provider. It lets target resolution distinguish a
// backend outage from a permanently missing target.
type RetryableReplyError struct {
	Err       error
	ErrorKind string
}

func (e RetryableReplyError) Error() string {
	if e.Err == nil {
		return "retryable reply dependency error"
	}
	return e.Err.Error()
}

func (e RetryableReplyError) Unwrap() error     { return e.Err }
func (e RetryableReplyError) IsRetryable() bool { return true }
func (e RetryableReplyError) RetryErrorType() string {
	if e.ErrorKind == "" {
		return "reply_dependency"
	}
	return e.ErrorKind
}

func NewRetryableReplyError(err error, errorKind string) error {
	if err == nil {
		return nil
	}
	return RetryableReplyError{Err: err, ErrorKind: errorKind}
}

// NewReplySender creates a finite-retry Reply Outbox sender.
func NewReplySender(
	outbox ReplyOutbox,
	targets func(context.Context, ReplyDelivery) (string, error),
	providers func(context.Context, ReplyDelivery) (ReplyProvider, error),
	options ReplySenderOptions,
) (*ReplySender, error) {
	if outbox == nil || targets == nil || providers == nil {
		return nil, errors.New("reply sender dependencies are required")
	}
	if options.Owner == "" {
		return nil, errors.New("reply sender owner is required")
	}
	if options.Lease <= 0 {
		options.Lease = defaultReplyLease
	}
	if options.SendTimeout <= 0 {
		options.SendTimeout = options.Lease / 2
	}
	if options.SendTimeout <= 0 || options.SendTimeout >= options.Lease {
		return nil, errors.New("reply send timeout must be shorter than reply lease")
	}
	if options.PollInterval <= 0 {
		options.PollInterval = defaultReplyPollInterval
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultReplyBatch
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = defaultReplyMaxAttempts
	}
	return &ReplySender{
		outbox:       outbox,
		targets:      targets,
		providers:    providers,
		owner:        options.Owner,
		lease:        options.Lease,
		sendTimeout:  options.SendTimeout,
		pollInterval: options.PollInterval,
		batchSize:    options.BatchSize,
		maxAttempts:  options.MaxAttempts,
		metrics:      options.Metrics,
	}, nil
}

// SendBatch claims and processes up to the configured batch size once.
func (s *ReplySender) SendBatch(ctx context.Context) (int, error) {
	if s == nil || s.outbox == nil || s.targets == nil || s.providers == nil {
		return 0, errors.New("reply sender is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Claim one row at a time. If sending the first provider-attempted row
	// fails, later rows remain PENDING and therefore retryable; claiming the
	// whole batch first would incorrectly leave those rows in SENDING.
	processed := 0
	for processed < s.batchSize {
		deliveries, err := s.outbox.ClaimReplies(ctx, s.owner, s.lease, 1)
		if err != nil {
			return processed, fmt.Errorf("claim replies: %w", err)
		}
		if len(deliveries) == 0 {
			return processed, nil
		}
		if err := s.sendOne(ctx, deliveries[0]); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

// Run delivers replies until ctx is canceled. Backend outages do not stop the
// sender: they wait with bounded exponential backoff and jitter. Per-reply
// provider failures remain bounded by MaxAttempts in recordFailure.
func (s *ReplySender) Run(ctx context.Context) error {
	if s == nil {
		return errors.New("reply sender is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; ; {
		if _, err := s.SendBatch(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("reply sender batch failed: %s", platformlog.SafeError(err))
			if err := waitForReplySender(ctx, replyLoopRetryDelay(attempt)); err != nil {
				return err
			}
			attempt++
			continue
		}
		attempt = 0
		if err := waitForReplySender(ctx, s.pollInterval); err != nil {
			return err
		}
	}
}

func (s *ReplySender) sendOne(ctx context.Context, delivery ReplyDelivery) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	parentCtx := platformtelemetry.Extract(ctx, map[string]string{
		"traceparent": delivery.TraceParent,
		"tracestate":  delivery.TraceState,
	})
	replyCtx, span := platformtelemetry.StartSpan(parentCtx, "reply.send",
		attribute.String("tenant_id", delivery.Reply.TenantID),
		attribute.String("app_id", delivery.Reply.AppID),
		attribute.String("channel", string(delivery.Reply.Channel)),
		attribute.String("request_id", delivery.Reply.RequestID),
	)
	defer span.End()
	started := time.Now()
	sendCtx, cancel := context.WithTimeout(replyCtx, s.sendTimeout)
	defer cancel()
	provider, err := s.providers(sendCtx, delivery)
	if err != nil {
		retryable, errorType := classifyReplyError(err)
		if errors.Is(err, ErrReplyBindingInactive) {
			retryable = true
			errorType = "binding_inactive"
		} else if errorType == "provider_permanent" {
			errorType = "provider_resolution"
		}
		return s.recordFailure(ctx, delivery, err, retryable, errorType, started, span)
	}
	if provider.Client == nil {
		return s.recordFailure(ctx, delivery, errors.New("reply provider client is not initialized"), false, "provider_client", started, span)
	}
	if provider.Limiter != nil {
		if err := provider.Limiter.Allow(sendCtx, delivery); err != nil {
			return s.recordFailure(ctx, delivery, err, true, "rate_limit", started, span)
		}
	}
	providerTarget, err := s.targets(sendCtx, delivery)
	if err != nil {
		retryable, errorType := classifyReplyError(err)
		if errors.Is(err, channels.ErrBindingInactive) {
			retryable = true
			errorType = "binding_inactive"
		} else if errors.Is(err, channels.ErrIdentityInactive) {
			retryable = true
			errorType = "identity_inactive"
		} else if errorType == "provider_permanent" {
			errorType = "target_resolution"
		}
		return s.recordFailure(ctx, delivery, err, retryable, errorType, started, span)
	}
	receipt, err := sendReply(sendCtx, provider.Client, delivery, providerTarget)
	if err != nil {
		if isReplySideEffectUncertain(err) || errors.Is(err, context.Canceled) {
			if errors.Is(err, context.Canceled) {
				err = fmt.Errorf("reply provider send canceled: %w", err)
			}
			return s.recordUncertain(ctx, delivery, "", err, "provider_result_unknown", started, span)
		}
		retryable, errorType := classifyReplyError(err)
		return s.recordFailure(ctx, delivery, err, retryable, errorType, started, span)
	}
	if err := receipt.Validate(); err != nil {
		return s.recordUncertain(ctx, delivery, receipt.ProviderMessageID, err, "invalid_provider_receipt", started, span)
	}
	if err := s.outbox.CompleteReply(ctx, delivery, receipt); err != nil {
		return s.recordUncertain(ctx, delivery, receipt.ProviderMessageID, err, "reply_completion_unknown", started, span)
	}
	if s.metrics != nil {
		s.metrics.RecordReply(ctx, platformmetrics.Labels{
			TenantID: delivery.Reply.TenantID,
			AppID:    delivery.Reply.AppID,
			Channel:  string(delivery.Reply.Channel),
		}, time.Since(started), "")
	}
	return nil
}

func sendReply(
	ctx context.Context,
	client channels.ProviderOutboundClient,
	delivery ReplyDelivery,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	switch delivery.Reply.ReplyKind() {
	case channels.ReplyKindText:
		return client.SendOnce(ctx, delivery.Reply, providerTarget)
	case channels.ReplyKindStream:
		rich, ok := client.(channels.ProviderStreamOutboundClient)
		if !ok {
			return channels.ProviderReceipt{}, fmt.Errorf("%w: stream", ErrReplyCapabilityUnavailable)
		}
		return rich.SendStream(ctx, delivery.Reply, providerTarget, delivery.ProviderMessageID)
	case channels.ReplyKindCard:
		rich, ok := client.(channels.ProviderCardOutboundClient)
		if !ok {
			return channels.ProviderReceipt{}, fmt.Errorf("%w: card", ErrReplyCapabilityUnavailable)
		}
		return rich.SendCard(ctx, delivery.Reply, providerTarget, delivery.ProviderMessageID)
	default:
		return channels.ProviderReceipt{}, fmt.Errorf("%w: %s", ErrReplyCapabilityUnavailable, delivery.Reply.ReplyKind())
	}
}

func (s *ReplySender) recordUncertain(
	ctx context.Context,
	delivery ReplyDelivery,
	providerMessageID string,
	cause error,
	errorType string,
	started time.Time,
	span trace.Span,
) error {
	platformtelemetry.MarkError(span, errorType, cause)
	if s.metrics != nil {
		s.metrics.RecordReply(ctx, platformmetrics.Labels{
			TenantID: delivery.Reply.TenantID,
			AppID:    delivery.Reply.AppID,
			Channel:  string(delivery.Reply.Channel),
		}, time.Since(started), errorType)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replyUncertainPersistTimeout)
	defer cancel()
	if err := s.outbox.MarkReplyUncertain(persistCtx, delivery, providerMessageID, cause); err != nil {
		return fmt.Errorf("mark uncertain reply: %w", err)
	}
	return nil
}

func isReplySideEffectUncertain(err error) bool {
	var marker interface{ IsSideEffectUncertain() bool }
	return errors.As(err, &marker) && marker.IsSideEffectUncertain()
}

func (s *ReplySender) recordFailure(
	ctx context.Context,
	delivery ReplyDelivery,
	cause error,
	retryable bool,
	errorType string,
	started time.Time,
	span trace.Span,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if errorType == "" {
		errorType = "provider_send"
	}
	platformtelemetry.MarkError(span, errorType, cause)
	if s.metrics != nil {
		s.metrics.RecordReply(ctx, platformmetrics.Labels{
			TenantID: delivery.Reply.TenantID,
			AppID:    delivery.Reply.AppID,
			Channel:  string(delivery.Reply.Channel),
		}, time.Since(started), errorType)
	}
	if retryable && delivery.Attempt < s.maxAttempts {
		delay := retryDelay(delivery.Attempt)
		retryAfter := retryAfterDelay(cause)
		if retryAfter > replyRetryMax {
			retryAfter = replyRetryMax
		}
		if retryAfter > delay {
			delay = retryAfter
		}
		persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replyUncertainPersistTimeout)
		defer cancel()
		if err := s.outbox.RetryReply(persistCtx, delivery, errorType, delay, cause); err != nil {
			return fmt.Errorf("retry reply: %w", err)
		}
		if s.metrics != nil {
			s.metrics.RecordRetry(ctx, platformmetrics.Labels{
				TenantID: delivery.Reply.TenantID,
				AppID:    delivery.Reply.AppID,
				Channel:  string(delivery.Reply.Channel),
			}, "reply_"+errorType)
		}
		return nil
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replyUncertainPersistTimeout)
	defer cancel()
	if err := s.outbox.FailReply(persistCtx, delivery, errorType, cause); err != nil {
		return fmt.Errorf("fail reply: %w", err)
	}
	return nil
}

func classifyReplyError(err error) (bool, string) {
	if errors.Is(err, context.Canceled) {
		return true, "shutdown_canceled"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true, "transport_timeout"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, "transport_timeout"
	}
	var retryableErr interface{ IsRetryable() bool }
	if errors.As(err, &retryableErr) && retryableErr.IsRetryable() {
		var typed interface{ RetryErrorType() string }
		if errors.As(err, &typed) && typed.RetryErrorType() != "" {
			return true, typed.RetryErrorType()
		}
		return true, "provider_retryable"
	}
	return false, "provider_permanent"
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := replyRetryInitial
	for attempt > 1 && delay < replyRetryMax {
		delay *= 2
		attempt--
	}
	if delay > replyRetryMax {
		delay = replyRetryMax
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

type retryAfterError interface {
	RetryAfter() time.Duration
}

func retryAfterDelay(err error) time.Duration {
	var providerError retryAfterError
	if !errors.As(err, &providerError) {
		return 0
	}
	delay := providerError.RetryAfter()
	if delay < 0 {
		return 0
	}
	return delay
}

func replyLoopRetryDelay(attempt int) time.Duration {
	return retryDelay(attempt + 1)
}

func waitForReplySender(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = defaultReplyPollInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func visibleAssistantText(evt *event.Event) string {
	if !isUserVisibleAssistantEvent(evt) {
		return ""
	}
	var text string
	for _, choice := range evt.Response.Choices {
		message := choice.Message
		if message.Role != "" && message.Role != model.RoleAssistant {
			continue
		}
		text += message.Content
	}
	return text
}

func visibleAssistantDeltaText(evt *event.Event) string {
	if !isUserVisibleAssistantEvent(evt) {
		return ""
	}
	var text string
	for _, choice := range evt.Response.Choices {
		message := choice.Delta
		if message.Content == "" {
			// Some runners expose a cumulative partial snapshot in Message
			// instead of an incremental Delta.
			message = choice.Message
		}
		if message.Role != "" && message.Role != model.RoleAssistant {
			continue
		}
		text += message.Content
	}
	return text
}

func isUserVisibleAssistantEvent(evt *event.Event) bool {
	if evt == nil || evt.Response == nil || evt.Error != nil {
		return false
	}
	if evt.Response.IsToolCallResponse() || evt.Response.IsToolResultResponse() {
		return false
	}
	switch evt.Object {
	case "", model.ObjectTypeChatCompletion, model.ObjectTypeChatCompletionChunk, model.ObjectTypeRunnerCompletion:
		return true
	default:
		return false
	}
}

func replyErrorText(evt *event.Event) string {
	if evt != nil && evt.Error != nil && evt.Error.Type == unsupportedAttachmentErrorType {
		return "当前模型不支持该附件类型，请更换模型或移除附件。"
	}
	return "执行失败，请稍后重试。"
}
