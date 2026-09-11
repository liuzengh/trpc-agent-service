package messaging

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

type observedProducer struct {
	delegate Producer
	observer metrics.InboundObserver
}

func NewObservedProducer(delegate Producer, observer metrics.InboundObserver) (Producer, error) {
	if delegate == nil || observer == nil {
		return nil, fmt.Errorf("observed Kafka producer dependencies are required")
	}
	return &observedProducer{delegate: delegate, observer: observer}, nil
}

func (p *observedProducer) Publish(ctx context.Context, envelope Envelope) error {
	payload, err := DecodeInboundPayload(envelope)
	if err != nil {
		return p.delegate.Publish(ctx, envelope)
	}
	ctx, finish := p.observer.StartInbound(ctx, metrics.InboundAttributes{
		TenantID: envelope.TenantID,
		AppCode:  payload.AppCode,
		Channel:  string(payload.Inbound.Channel),
	})
	err = p.delegate.Publish(ctx, envelope)
	finish(err)
	return err
}

var _ Producer = (*observedProducer)(nil)
