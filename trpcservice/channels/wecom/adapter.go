package wecom

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// Adapter is this package's half of a text channel: it hands the durable
// pipeline what the Client accepted, and puts one final reply back on the
// connection that accepted it.
//
// It is deliberately thin. Everything durable — recording, ordering, executing
// once, one send attempt — belongs to the shared consumer, which this package
// does not import and knows nothing about; everything protocol-shaped stays
// here. The seam between them is a value contract, so nothing in this package
// reaches a Runner, a Store or a telemetry exporter.
type Adapter struct {
	client *Client
	// identity is derived once, from the binding the Client already validated
	// and authorized. It is never recomputed and never read from a frame.
	identity channels.BindingIdentity
}

// Adapter is what the shared consumer takes. The assertion is here so that a
// change to either side is a compile error in this package rather than a
// process that starts and cannot answer.
var _ channels.TextAdapter = (*Adapter)(nil)

// NewAdapter wraps a configured Client.
//
// The identity comes from the Client's own binding rather than from a second
// argument: New already validated it and entitled its credential reference, so
// there is no way to build an adapter whose identity and whose connection
// disagree.
func NewAdapter(client *Client) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: an adapter needs a client", ErrConfig)
	}
	return &Adapter{
		client: client,
		identity: channels.BindingIdentity{
			TenantID:   client.binding.TenantID,
			AgentAppID: client.binding.AgentAppID,
			BindingID:  client.binding.BindingID,
			Channel:    channels.ChannelWeCom,
		},
	}, nil
}

// Identity is the static trust anchor this adapter runs on.
func (a *Adapter) Identity() channels.BindingIdentity { return a.identity }

// ReplyTextLimit is the platform's own stream reply limit. The consumer bounds
// an answer to it before storing, which is why the same number also bounds what
// SendFinalText will accept: the stored text is transmitted as it stands.
func (a *Adapter) ReplyTextLimit() int { return maxReplyTextBytes }

// Serve records what the Client accepted, in arrival order.
//
// It reads the Client's bounded inbound buffer rather than the socket, so a
// database write here cannot stall the read loop that answers heartbeats. It
// returns when ctx ends, when accept refuses — which is terminal, because this
// protocol has no inbound acknowledgement and nothing will redeliver — or when
// a message cannot be put in an envelope at all. Every call to accept is
// finished before it returns.
func (a *Adapter) Serve(ctx context.Context, accept channels.AcceptFunc) error {
	if accept == nil {
		return fmt.Errorf("%w: serve needs somewhere to record what it accepted", ErrConfig)
	}
	messages := a.client.Messages()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			envelope, err := a.envelope(message)
			if err != nil {
				// The reply address of an accepted message could not be
				// written down, so this message could never be answered and
				// neither could the next one on this connection.
				return err
			}
			if err := accept(ctx, envelope); err != nil {
				return err
			}
		}
	}
}

// Send makes one attempt to put one recorded answer on the wire.
//
// The text is the stored text, sent verbatim or refused: this adapter does not
// shorten, split or decorate an answer that has already been recorded as the
// thing the user would see.
func (a *Adapter) Send(
	ctx context.Context,
	target channels.DeliveryTarget,
	message channels.OutboundMessage,
) channels.DeliveryReport {
	reply, err := decodeTarget(a.client.binding, target)
	if err != nil {
		// A reply address this binding cannot read is one no connection of this
		// process could answer on, which is the same condition as a target from
		// a connection that is gone.
		return channels.DeliveryReport{Outcome: channels.DeliveryTargetStale}
	}
	return channels.DeliveryReport{
		Outcome: deliveryOutcome(a.client.SendFinalText(ctx, reply, message.Text)),
	}
}

// deliveryOutcome maps what this protocol reports onto the closed vocabulary
// the pipeline records.
//
// A spent reply address, an expired one and one from a connection that no
// longer exists are the same thing to a caller: the channel worked as designed
// and there is no later attempt that could succeed. A refusal is not — the
// platform saw the message and said no. Anything else is unknown, which is the
// fail-closed direction: the pipeline records an unknown outcome as a duplicate
// risk and, with one attempt allowed, ends the part rather than sending again.
func deliveryOutcome(err error) channels.DeliveryOutcome {
	switch {
	case err == nil:
		return channels.Delivered
	case errors.Is(err, ErrReplyTargetExpired),
		errors.Is(err, ErrNotConnected),
		errors.Is(err, ErrReplyAlreadySent):
		return channels.DeliveryTargetStale
	case errors.Is(err, ErrReplyRejected),
		errors.Is(err, ErrTextTooLong),
		errors.Is(err, ErrTextInvalid):
		return channels.DeliveryRejected
	default:
		return channels.DeliveryUnknown
	}
}

// envelope maps one accepted message onto the durable inbound envelope. Every
// identity field comes from the static Binding, never from the frame.
func (a *Adapter) envelope(message DirectText) (channels.InboundEnvelope, error) {
	target, err := encodeTarget(a.client.binding, message.Reply)
	if err != nil {
		return channels.InboundEnvelope{}, err
	}
	return channels.InboundEnvelope{
		TenantID:         a.identity.TenantID,
		Channel:          a.identity.Channel,
		ChannelBindingID: a.identity.BindingID,
		AgentAppID:       a.identity.AgentAppID,
		PrincipalID:      message.PrincipalID,
		SessionID:        message.SessionID,
		ExternalEventID:  message.ExternalMessageID,
		ReceivedAt:       message.ReceivedAt,
		Message:          channels.InboundMessage{Text: message.Text},
		DeliveryTarget:   target,
	}, nil
}
