package natsadapter

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	"github.com/nats-io/nats.go"
)

type Publisher struct{ js nats.JetStreamContext }

func NewPublisher(nc *nats.Conn) (*Publisher, error) {
	if nc == nil {
		return nil, application.ErrDependencyUnavailable
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, application.ErrDependencyUnavailable
	}
	return &Publisher{js}, nil
}
func (p *Publisher) PublishRoute(ctx context.Context, eventID string, payload []byte) error {
	if !domain.ValidID(eventID) || len(payload) == 0 || len(payload) > domain.MaxRouteEventBytes {
		return application.ErrRoutePublishUnavailable
	}
	msg := nats.NewMsg(domain.RouteSubject)
	msg.Data = payload
	msg.Header.Set(nats.MsgIdHdr, eventID)
	ack, err := p.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(domain.RouteStream))
	if err != nil || ack == nil || ack.Stream != domain.RouteStream || ack.Sequence == 0 {
		return application.ErrRoutePublishUnavailable
	}
	return nil
}

var _ application.RoutePublisher = (*Publisher)(nil)
