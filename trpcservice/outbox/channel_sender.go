package outbox

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// ChannelSender adapts the channel package's routing/transport sender to the
// existing Outbox Dispatcher contract. It never receives a repository and
// cannot perform durable mutations itself.
type ChannelSender struct {
	delegate channels.Sender
}

func NewChannelSender(delegate channels.Sender) (*ChannelSender, error) {
	if delegate == nil {
		return nil, ErrInvalidConfig
	}
	return &ChannelSender{delegate: delegate}, nil
}

// ChannelAwareSender is the descriptive alias used by callers that want to
// make the routing boundary explicit.
type ChannelAwareSender = ChannelSender

func NewChannelAwareSender(delegate channels.Sender) (*ChannelAwareSender, error) {
	return NewChannelSender(delegate)
}

func (s *ChannelSender) Send(ctx context.Context, message storage.OutboxMessage) SenderOutcome {
	if s == nil || s.delegate == nil {
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
	outcome := s.delegate.Send(ctx, message)
	switch outcome.Class {
	case channels.OutcomeDelivered:
		return SenderOutcome{Class: OutcomeDelivered}
	case channels.OutcomeRetryableFailure:
		return SenderOutcome{Class: OutcomeRetryableFailure, Code: outcome.Code}
	case channels.OutcomePermanentFailure:
		return SenderOutcome{Class: OutcomePermanentFailure, Code: outcome.Code}
	case channels.OutcomeUnknown:
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	default:
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
}

var _ Sender = (*ChannelSender)(nil)
