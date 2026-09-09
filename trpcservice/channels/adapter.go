package channels

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// This file is the contract between one platform adapter and the shared text
// pipeline that answers what the adapter accepted.
//
// The split is the point. An adapter owns its protocol: authentication, event
// parsing, the reply address it minted, the text limit its API enforces, and
// what that API reported about one attempt. The pipeline owns everything
// durable: acceptance, deduplication, ordering, executing a Run once, and one
// send attempt per final reply. Neither half can be written in terms of the
// other, so a second channel is a second implementation of this interface and
// not a branch inside the pipeline.
//
// Nothing here names a platform. Send reports use a closed vocabulary: a
// protocol error travelling into the pipeline would put
// third-party text into a process log the moment anything wrapped it.

// BindingIdentity is the static, server-side trust anchor of one binding: the
// four values every durable row of this channel is written under.
//
// It is not the platform's own binding. A bot account and a credential
// reference are the adapter's business; these four are the pipeline's, and they
// come from process configuration rather than from an inbound event, because an
// inbound channel decides which tenant an unsolicited message is charged to.
type BindingIdentity struct {
	TenantID   string
	AgentAppID string
	BindingID  string
	Channel    ChannelType
}

// Validate rejects an identity that could not own a durable row.
func (b BindingIdentity) Validate() error {
	if err := tenant.ValidateResourceID("tenant id", b.TenantID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("app id", b.AgentAppID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("channel binding id", b.BindingID); err != nil {
		return err
	}
	return b.Channel.Validate()
}

// Scope is the tenant this identity may read and write inside.
func (b BindingIdentity) Scope() tenant.TenantContext {
	return tenant.TenantContext{TenantID: b.TenantID}
}

// Owns reports whether an envelope belongs to this binding, on all four fields.
//
// It is checked on every accepted event, and not only when the consumer is
// built. The envelope comes from the adapter, which is the component this
// package trusts least: a mis-wired or third-party adapter that named another
// tenant, another app, another binding or another channel would otherwise
// record a conversation nobody configured, under a target nobody can answer.
func (b BindingIdentity) Owns(envelope InboundEnvelope) bool {
	return envelope.TenantID == b.TenantID &&
		envelope.AgentAppID == b.AgentAppID &&
		envelope.ChannelBindingID == b.BindingID &&
		envelope.Channel == b.Channel
}

// AcceptFunc records one inbound event durably.
//
// A nil return means the event is in the Store, newly recorded or recognised
// as one already there. It does not guarantee execution or delivery succeeds.
// A protocol may acknowledge durable acceptance at that point and not before.
// Any other return is terminal for the caller: the pipeline is no longer recording, and
// an adapter that kept reading would take messages off the wire that this
// process cannot promise to answer.
type AcceptFunc func(ctx context.Context, envelope InboundEnvelope) error

// DeliveryOutcome is what an adapter learned from one send attempt. It is a
// closed vocabulary rather than an error because it crosses into both a durable
// row and an operator's metrics, and neither may carry protocol text.
type DeliveryOutcome string

const (
	// Delivered means the platform confirmed the message.
	Delivered DeliveryOutcome = "delivered"
	// DeliveryTargetStale means the reply address cannot be used again: it is
	// spent, expired, or belongs to a connection that no longer exists. The
	// channel is working as designed and no later attempt could succeed.
	DeliveryTargetStale DeliveryOutcome = "target_stale"
	// DeliveryRejected means the platform refused this message, and would
	// refuse it again.
	DeliveryRejected DeliveryOutcome = "rejected"
	// DeliveryUnknown means the attempt neither confirmed nor definitely
	// failed. The zero value and unrecognised outcomes are also treated as
	// unknown by SendResult.
	DeliveryUnknown DeliveryOutcome = "unknown"
)

// DeliveryReport is one attempt, as the adapter saw it.
type DeliveryReport struct {
	Outcome DeliveryOutcome
	// ExternalMessageID is the platform's id for the delivered message, when
	// the protocol returns one. Only a delivered attempt may carry it.
	ExternalMessageID string
}

// SendResult converts one report into what the Store stores.
//
// The two are not the same judgement: the Store only asks whether a part may be
// tried again, while the report also says whether anything reached the
// platform. This is the only mapping between them, so the durable outcome and
// the operator's outcome cannot drift apart.
//
// Everything the adapter states definitely is permanent, including a stale
// target: a reply address that is spent has no later attempt that could
// succeed. Anything else — including a report this package does not recognise,
// and one that is malformed — is unknown, which is the fail-closed direction:
// an unknown outcome is recorded as a duplicate risk and, with one attempt
// allowed, ends the part rather than sending it again.
func (r DeliveryReport) SendResult() SendResult {
	unknown := SendResult{Outcome: SendUnknown, ErrorType: ErrorOutcomeUnknown}
	var result SendResult
	switch r.Outcome {
	case Delivered:
		result = SendResult{
			Outcome:           SendSucceeded,
			ErrorType:         ErrorNone,
			ExternalMessageID: r.ExternalMessageID,
		}
	case DeliveryTargetStale, DeliveryRejected:
		result = SendResult{Outcome: SendPermanent, ErrorType: ErrorPermanent}
	default:
		return unknown
	}
	// Checked here rather than at the Store, so that an adapter returning an
	// oversized or malformed platform id fails one send closed instead of
	// failing the write that records it.
	if err := result.Validate(); err != nil {
		return unknown
	}
	return result
}

// TextAdapter is one platform's half of a text channel.
//
// Connection lifecycle is separate: dialling, credentials and reconnection
// belong to the adapter's own client and to the composition root
// that owns it, because those are the parts an operator configures and the
// parts that differ most between protocols.
type TextAdapter interface {
	// Identity is the trust anchor this adapter runs on. It is immutable and
	// derived from validated configuration, never from an inbound event.
	Identity() BindingIdentity

	// ReplyTextLimit is the largest final reply this platform will take, in
	// UTF-8 bytes. The pipeline bounds an answer to it before storing, so the
	// stored text and the sent text are the same bytes.
	ReplyTextLimit() int

	// Serve delivers accepted events until ctx ends.
	//
	// It calls accept once per event, in the order the protocol promised, and
	// returns only after every callback it started has finished — a caller that
	// returned early would leave a goroutine writing to the Store behind it. It
	// returns accept's error unchanged when accept fails, and it always
	// responds to cancellation.
	Serve(ctx context.Context, accept AcceptFunc) error

	// Send makes at most one attempt to put one recorded answer on the wire.
	//
	// The pipeline decides whether an attempt may happen at all; this is only
	// the attempt. message is the stored text, which is transmitted verbatim or
	// refused, and target is the address the adapter itself minted.
	Send(ctx context.Context, target DeliveryTarget, message OutboundMessage) DeliveryReport
}
