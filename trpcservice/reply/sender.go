// Package reply delivers pending outbound messages through Channel Adapters.
package reply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type Options struct {
	WorkerID     string
	BatchSize    int
	ClaimLease   time.Duration
	PollInterval time.Duration
	RetryDelay   time.Duration
	MaxAttempts  int
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
		return nil, fmt.Errorf("Reply Sender dependencies are required")
	}
	if opts.WorkerID == "" || opts.BatchSize <= 0 || opts.ClaimLease <= 0 ||
		opts.PollInterval <= 0 || opts.RetryDelay <= 0 || opts.MaxAttempts <= 0 {
		return nil, fmt.Errorf("Reply Sender options are invalid")
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
	for _, item := range items {
		if err := s.sendOne(ctx, item); err != nil {
			sendErr = errors.Join(sendErr, err)
			continue
		}
		sent++
	}
	return sent, sendErr
}

func (s *Sender) sendOne(ctx context.Context, item gateway.OutboundItem) error {
	binding, err := s.repository.GetChannelBinding(
		ctx, item.TenantID, item.ChannelBindingID,
	)
	if err != nil {
		return s.fail(ctx, item, true, err)
	}
	if binding.Status != controlplane.StatusActive {
		return s.fail(ctx, item, true, fmt.Errorf("channel binding is disabled"))
	}
	adapter, err := s.registry.Get(binding.ChannelType)
	if err != nil {
		return s.fail(ctx, item, true, err)
	}
	parts := splitText(item.Text, adapter.Capabilities().MaxTextRunes)
	providerIDs := make([]string, 0, len(parts))
	for index, part := range parts {
		receipt, sendErr := adapter.Send(ctx, binding, channels.OutboundMessage{
			OutboundID: partID(item.ID, index, len(parts)),
			RequestID:  item.RequestID,
			Text:       part,
		})
		if sendErr != nil {
			return s.fail(ctx, item, deliveryTerminal(sendErr, item.AttemptCount, s.opts.MaxAttempts), sendErr)
		}
		providerIDs = append(providerIDs, receipt.ProviderMessageID)
	}
	if err := s.journal.MarkOutboundSent(
		ctx, item.ID, s.opts.WorkerID, strings.Join(providerIDs, ","),
	); err != nil {
		return err
	}
	return nil
}

func (s *Sender) fail(
	ctx context.Context,
	item gateway.OutboundItem,
	terminal bool,
	cause error,
) error {
	retryAt := time.Now().Add(s.opts.RetryDelay)
	var deliveryErr *channels.DeliveryError
	if errors.As(cause, &deliveryErr) && deliveryErr.RetryAfter > 0 {
		retryAt = time.Now().Add(deliveryErr.RetryAfter)
	}
	markErr := s.journal.MarkOutboundFailed(
		ctx, item.ID, s.opts.WorkerID, retryAt, terminal, cause,
	)
	return errors.Join(cause, markErr)
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
