// Package reply delivers pending outbound messages through Channel Adapters.
package reply

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	WorkerID     string
	BatchSize    int
	ClaimLease   time.Duration
	PollInterval time.Duration
	RetryDelay   time.Duration
	MaxAttempts  int
	Audit        audit.Writer
	Metrics      *platformmetrics.Recorder
}

// Sender claims outbound rows and delegates provider calls to an Adapter.
type Sender struct {
	journal    gateway.Journal
	repository controlplane.Repository
	registry   *channels.Registry
	opts       Options
}

func New(
	journal gateway.Journal,
	repository controlplane.Repository,
	registry *channels.Registry,
	opts Options,
) (*Sender, error) {
	if journal == nil || repository == nil || registry == nil {
		return nil, fmt.Errorf("reply Sender dependencies are required")
	}
	if opts.WorkerID == "" || opts.BatchSize <= 0 || opts.ClaimLease <= 0 ||
		opts.PollInterval <= 0 || opts.RetryDelay <= 0 || opts.MaxAttempts <= 0 {
		return nil, fmt.Errorf("reply Sender options are invalid")
	}
	return &Sender{journal: journal, repository: repository, registry: registry, opts: opts}, nil
}

func (s *Sender) ProcessOnce(ctx context.Context) (int, error) {
	items, err := s.journal.ClaimOutbound(
		ctx, s.opts.WorkerID, s.opts.BatchSize, s.opts.ClaimLease,
	)
	if err != nil {
		return 0, err
	}
	sent := 0
	var sendErr error
	contexts := make([]context.Context, len(items))
	stops := make([]func(), len(items))
	for i, item := range items {
		contexts[i], stops[i] = s.protectOutbound(ctx, item)
		defer stops[i]()
	}
	for i, item := range items {
		err := s.sendOne(contexts[i], item)
		stops[i]()
		if err != nil {
			sendErr = errors.Join(sendErr, err)
			continue
		}
		sent++
	}
	return sent, sendErr
}

func (s *Sender) sendOne(ctx context.Context, item gateway.OutboundItem) error {
	ctx = background.ContextWithTraceParent(ctx, item.TraceParent)
	ctx, span := otel.Tracer("trpc-agent-service/reply").Start(ctx, "reply.send")
	span.SetAttributes(
		attribute.String("tenant.id", item.TenantID),
		attribute.String("messaging.message.id", item.ID),
		attribute.String("gen_ai.request.id", item.RequestID),
	)
	defer span.End()
	started := time.Now()
	binding, err := s.repository.GetChannelBinding(
		ctx, item.TenantID, item.ChannelBindingID,
	)
	if err != nil {
		return s.fail(ctx, item, "", true, started, err)
	}
	if binding.Status != controlplane.StatusActive {
		return s.fail(ctx, item, binding.ChannelType, true, started, fmt.Errorf("channel binding is disabled"))
	}
	tenant, err := s.repository.GetTenant(ctx, binding.TenantID)
	if err != nil {
		return s.fail(ctx, item, binding.ChannelType, false, started, fmt.Errorf("tenant delivery check unavailable"))
	}
	app, err := s.repository.GetAgentApp(ctx, binding.TenantID, binding.AppID)
	if err != nil {
		return s.fail(ctx, item, binding.ChannelType, false, started, fmt.Errorf("app delivery check unavailable"))
	}
	if tenant.Status != controlplane.StatusActive || app.Status != controlplane.StatusActive {
		return s.fail(ctx, item, binding.ChannelType, true, started, fmt.Errorf("tenant or app delivery is disabled"))
	}
	adapter, err := s.registry.Get(binding.ChannelType)
	if err != nil {
		return s.fail(ctx, item, binding.ChannelType, true, started, err)
	}
	parts := splitText(item.Text, adapter.Capabilities().MaxTextRunes)
	providerIDs := make([]string, 0, len(parts))
	for index, part := range parts {
		current, err := s.repository.GetChannelBinding(ctx, binding.TenantID, binding.ID)
		if err != nil || current.Version != binding.Version || current.Status != controlplane.StatusActive {
			return s.fail(ctx, item, binding.ChannelType, true, started, errors.New("binding changed during multipart delivery"))
		}
		identity, _ := json.Marshal([]any{binding.TenantID, binding.ID, binding.Version, binding.ChannelType, item.ReplyTarget, item.Text, len(parts), index})
		digest := sha256.Sum256(identity)
		attempt, owner, err := s.journal.BeginPart(ctx, item, s.opts.WorkerID, index, len(parts), hex.EncodeToString(digest[:]))
		if err != nil {
			return s.fail(ctx, item, binding.ChannelType, true, started, &channels.DeliveryError{Cause: err, Unknown: true})
		}
		if !owner {
			if attempt.Status == "sent" {
				if attempt.ProviderID != "" {
					providerIDs = append(providerIDs, attempt.ProviderID)
				}
				continue
			}
			return s.fail(ctx, item, binding.ChannelType, true, started, &channels.DeliveryError{Cause: errors.New("outbound part requires reconciliation"), Unknown: attempt.Status != "rejected"})
		}
		receipt, sendErr := adapter.Send(ctx, binding, channels.OutboundMessage{
			OutboundID:  partID(item.ID, index, len(parts)),
			RequestID:   item.RequestID,
			Text:        part,
			ReplyTarget: item.ReplyTarget,
		})
		status := "sent"
		if sendErr != nil {
			status = "rejected"
			var deliveryErr *channels.DeliveryError
			if errors.As(sendErr, &deliveryErr) {
				if deliveryErr.Unknown {
					status = "unknown"
				} else if deliveryErr.Retryable {
					status = "pending"
				}
			}
		}
		finalize, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		persistErr := s.journal.FinishPart(finalize, attempt, status, receipt.ProviderMessageID)
		cancel()
		if persistErr != nil {
			return s.fail(ctx, item, binding.ChannelType, true, started, &channels.DeliveryError{Cause: errors.New("part outcome persistence failed"), Unknown: true})
		}
		if sendErr != nil {
			return s.fail(
				ctx, item, binding.ChannelType,
				deliveryTerminal(sendErr, item.AttemptCount, s.opts.MaxAttempts),
				started, sendErr,
			)
		}
		if receipt.ProviderMessageID != "" {
			providerIDs = append(providerIDs, receipt.ProviderMessageID)
		}
	}
	if err := s.journal.MarkOutboundSent(
		ctx, item.ID, s.opts.WorkerID, strings.Join(providerIDs, ","), item.AttemptCount,
	); err != nil {
		return err
	}
	s.opts.Metrics.RecordDelivery(
		ctx, binding.TenantID, binding.ChannelType, "sent", time.Since(started),
	)
	if s.opts.Audit != nil {
		if err := s.opts.Audit.Record(ctx, audit.Event{
			TenantID:         binding.TenantID,
			Channel:          binding.ChannelType,
			ChannelBindingID: binding.ID,
			RequestID:        item.RequestID,
			TraceID:          audit.TraceID(ctx),
			Decision:         "reply_sent",
			Latency:          time.Since(started),
			Details: map[string]any{
				"outbound_id": item.ID,
				"parts":       len(parts),
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sender) fail(
	ctx context.Context,
	item gateway.OutboundItem,
	channelType string,
	terminal bool,
	started time.Time,
	cause error,
) error {
	retryAt := time.Now().Add(s.opts.RetryDelay)
	var deliveryErr *channels.DeliveryError
	unknown := errors.As(cause, &deliveryErr) && deliveryErr.Unknown
	if unknown {
		terminal = true
	}
	if errors.As(cause, &deliveryErr) && deliveryErr.RetryAfter > 0 {
		retryAt = time.Now().Add(deliveryErr.RetryAfter)
	}
	markErr := s.journal.MarkOutboundFailed(
		ctx, item.ID, s.opts.WorkerID, retryAt, terminal, cause, item.AttemptCount,
	)
	status, decision, errorType := "failed", "reply_failed", "channel_delivery"
	if unknown {
		status, decision, errorType = "unknown", "reply_delivery_unknown", "channel_delivery_unknown"
	}
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, "")
	span.SetAttributes(attribute.String("error.type", errorType))
	details := map[string]any{"outbound_id": item.ID, "terminal": terminal}
	if deliveryErr != nil && deliveryErr.Diagnostics != nil {
		d := deliveryErr.Diagnostics.Safe()
		details["delivery_error_kind"], details["delivery_phase"] = d.Kind, d.Phase
		details["delivery_http_status"] = d.HTTPStatus
		span.SetAttributes(attribute.String("delivery.error.kind", d.Kind), attribute.String("delivery.phase", d.Phase))
		if d.HTTPStatus != 0 {
			span.SetAttributes(attribute.Int("http.response.status_code", d.HTTPStatus))
		}
	}
	s.opts.Metrics.RecordDelivery(ctx, item.TenantID, channelType, status, time.Since(started))
	var auditErr error
	if s.opts.Audit != nil {
		auditErr = s.opts.Audit.Record(ctx, audit.Event{
			TenantID:         item.TenantID,
			Channel:          channelType,
			ChannelBindingID: item.ChannelBindingID,
			RequestID:        item.RequestID,
			TraceID:          audit.TraceID(ctx),
			Decision:         decision,
			Latency:          time.Since(started),
			ErrorType:        errorType,
			Details:          details,
		})
	}
	return errors.Join(cause, markErr, auditErr)
}

func deliveryTerminal(err error, attempt int, maxAttempts int) bool {
	if attempt >= maxAttempts {
		return true
	}
	var deliveryErr *channels.DeliveryError
	if errors.As(err, &deliveryErr) {
		return !deliveryErr.Retryable
	}
	return false
}

func splitText(text string, maxRunes int) []string {
	if maxRunes <= 0 {
		maxRunes = 4000
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	result := make([]string, 0, (len(runes)+maxRunes-1)/maxRunes)
	for start := 0; start < len(runes); start += maxRunes {
		end := start + maxRunes
		if end > len(runes) {
			end = len(runes)
		}
		result = append(result, string(runes[start:end]))
	}
	return result
}

func partID(outboundID string, index int, count int) string {
	if count <= 1 {
		return outboundID
	}
	return fmt.Sprintf("%s-part-%d", outboundID, index+1)
}

func (s *Sender) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		_, _ = s.ProcessOnce(ctx)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
