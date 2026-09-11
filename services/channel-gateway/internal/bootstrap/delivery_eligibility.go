package bootstrap

import (
	"context"

	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	deliveryapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// This composition bridge reads a real local grant; it never fabricates one from
// a status display, immutable ReplyOrigin, or configured process identity alone.
type localConnectionOwner interface {
	LocalOwner(string) (connectiondomain.OwnerGrant, bool)
}
type connectionDeliveryEligibility struct {
	source     localConnectionOwner
	instanceID string
}

func (b connectionDeliveryEligibility) InspectAccount(ctx context.Context, k deliveryapp.AccountKey) (deliveryapp.SendEligibility, error) {
	if ctx == nil || k.Provider != "wecom" || !accountIDPattern.MatchString(k.AccountID) {
		return deliveryapp.SendEligibility{}, d.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return deliveryapp.SendEligibility{}, err
	}
	if b.source == nil {
		return deliveryapp.SendEligibility{}, d.ErrUnavailable
	}
	g, ok := b.source.LocalOwner(k.AccountID)
	if !ok {
		return deliveryapp.SendEligibility{}, nil
	}
	if g.Validate() != nil || g.AccountID != k.AccountID || g.InstanceID != b.instanceID {
		return deliveryapp.SendEligibility{}, d.ErrInvalid
	}
	return deliveryapp.SendEligibility{Eligible: true, Owner: &d.OwnerFence{InstanceID: g.InstanceID, Epoch: g.Epoch, Revision: g.Revision}}, nil
}
