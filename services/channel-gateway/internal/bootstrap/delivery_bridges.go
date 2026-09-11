package bootstrap

import (
	"context"
	"errors"

	admissiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	wecomsender "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// These bridges only translate owner read models/ports. They do not own a send
// loop, select a recipient, query another module's SQL, or decide to retry.
type admissionSnapshotSource interface {
	ReadReplySnapshot(context.Context, string, string) (admissiondomain.ReplySnapshot, error)
}
type admissionDeliveryReader struct{ source admissionSnapshotSource }

func (b admissionDeliveryReader) ReadReplyTarget(ctx context.Context, admissionID, runID string) (d.Target, error) {
	s, err := b.source.ReadReplySnapshot(ctx, admissionID, runID)
	if err != nil {
		if errors.Is(err, admissiondomain.ErrReplyNotFound) {
			return d.Target{}, d.ErrNotFound
		}
		return d.Target{}, d.ErrUnavailable
	}
	if s.AdmissionID != admissionID || s.RunID != runID {
		return d.Target{}, d.ErrUnauthorized
	}
	target := d.Target{TenantID: s.TenantID, Provider: s.Provider, AccountID: s.AccountID, ManifestDigest: s.ManifestDigest, ConversationID: s.ConversationID, ThreadID: s.ThreadID, SourceMessageID: s.SourceMessageID, SourceEventID: s.SourceEventID, CallbackRequestID: s.CallbackRequestID, ReceivedAt: s.ReceivedAt}
	if s.Origin != nil {
		target.Origin = &d.ReplyOrigin{InstanceID: s.Origin.InstanceID, Epoch: s.Origin.Epoch, Revision: s.Origin.Revision, SocketGeneration: s.Origin.SocketGeneration}
	}
	// Old WeCom rows carry no transferable original socket authority.
	if target.Provider == "wecom" && target.Origin == nil {
		return d.Target{}, d.ErrUnsupported
	}
	return target, nil
}

type connectionSenderSource interface {
	ReserveFinal(context.Context, connection.SenderOrigin) (connection.ReservedSender, error)
}
type connectionDeliverySource struct{ source connectionSenderSource }

func (b connectionDeliverySource) ReserveOriginal(ctx context.Context, t d.Target) (wecomsender.Session, error) {
	if t.Origin == nil {
		return nil, d.ErrNotFound
	}
	h, err := b.source.ReserveFinal(ctx, connection.SenderOrigin{AccountID: t.AccountID, InstanceID: t.Origin.InstanceID, Epoch: t.Origin.Epoch, Revision: t.Origin.Revision, RequestID: t.CallbackRequestID, SocketGeneration: t.Origin.SocketGeneration})
	if err != nil {
		switch {
		case errors.Is(err, connection.ErrStaleOrigin):
			return nil, d.ErrNotFound
		case errors.Is(err, connection.ErrInvalidSender):
			return nil, d.ErrInvalid
		default:
			return nil, d.ErrUnavailable
		}
	}
	if h == nil {
		return nil, d.ErrUnavailable
	}
	return connectionDeliverySession{h}, nil
}

type connectionDeliverySession struct{ source connection.ReservedSender }

func (s connectionDeliverySession) Release() { s.source.Release() }
func (s connectionDeliverySession) SendFinal(ctx context.Context, streamID, text string) d.Result {
	r := s.source.SendFinal(ctx, connection.FinalCommand{StreamID: streamID, Content: text})
	return wecomsender.FromConnectionResult(string(r.Certainty), string(r.Code), r.ProviderCode)
}
