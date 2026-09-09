// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.)
// into tRPC-Agent-Go Runner inputs, following the OpenClaw Channel model.
package channels

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

var ErrIngressConflict = errors.New("ingress message conflict")

type UnavailableAdapter struct {
	BindingID string
}

func (a *UnavailableAdapter) Name() string { return a.BindingID }

func (a *UnavailableAdapter) Start(ctx context.Context, _ IngressSink) error {
	if ctx == nil {
		ctx = context.Background()
	}
	<-ctx.Done()
	return nil
}

func (a *UnavailableAdapter) Ready(context.Context) error {
	return errors.New("channel adapter credentials are unavailable")
}

func (a *UnavailableAdapter) Send(context.Context, message.OutboundMessage) error {
	return errors.New("channel adapter credentials are unavailable")
}

func (a *UnavailableAdapter) Close() error { return nil }

// AcceptResult describes the durable Inbox/Task-Stream outcome. Adapters use
// it to advance platform offsets only after a message is safely accepted.
type AcceptResult struct {
	TaskID    string
	RequestID string
	TraceID   string
	Duplicate bool
	Terminal  bool
}

type IngressSink interface {
	Accept(context.Context, message.InboundMessage) (AcceptResult, error)
}

type Adapter interface {
	Name() string
	Start(context.Context, IngressSink) error
	Ready(context.Context) error
	Send(context.Context, message.OutboundMessage) error
	Close() error
}
