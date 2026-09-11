package natsadapter

import (
	"context"
	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/nats-io/nats.go"
)

type ManifestPublisher struct{ js nats.JetStreamContext }

func NewManifestPublisher(nc *nats.Conn) (*ManifestPublisher, error) {
	if nc == nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	return &ManifestPublisher{js}, nil
}
func (p *ManifestPublisher) PublishManifest(ctx context.Context, id string, payload []byte) error {
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(payload)
	if err != nil || event.EventID != id {
		return application.ErrManifestDistributionUnavailable
	}
	msg := nats.NewMsg(controleventsv1.ManifestSubject)
	msg.Data = payload
	msg.Header.Set(nats.MsgIdHdr, id)
	ack, err := p.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(controleventsv1.ManifestStream))
	if err != nil || ack == nil || ack.Stream != controleventsv1.ManifestStream || ack.Sequence == 0 {
		return application.ErrManifestDistributionUnavailable
	}
	return nil
}
