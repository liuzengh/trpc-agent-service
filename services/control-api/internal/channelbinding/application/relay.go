package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

var ErrOutboxLeaseLost = errors.New("channel outbox lease lost")
var ErrRoutePublishUnavailable = errors.New("channel route publish unavailable")

type RouteClaim struct {
	TenantID, EventID, AccountID, SchemaVersion, PayloadDigest, ClaimToken string
	Generation                                                             int64
	Attempt                                                                int
	Payload                                                                []byte
}
type RouteOutbox interface {
	ClaimRoute(context.Context) (RouteClaim, bool, error)
	FinishRoute(context.Context, RouteClaim, bool, string, time.Duration) error
}

// PublishRoute returns only after a durable acknowledgement of the exact stream.
// Transport adapters sanitize broker errors; no broker text is stored in Outbox.
type RoutePublisher interface {
	PublishRoute(context.Context, string, []byte) error
}
type RouteRelay struct {
	outbox    RouteOutbox
	publisher RoutePublisher
}

func NewRouteRelay(outbox RouteOutbox, publisher RoutePublisher) (*RouteRelay, error) {
	if outbox == nil || publisher == nil {
		return nil, ErrDependencyUnavailable
	}
	return &RouteRelay{outbox, publisher}, nil
}
func (r *RouteRelay) Step(ctx context.Context) (bool, error) {
	claim, found, err := r.outbox.ClaimRoute(ctx)
	if err != nil || !found {
		return found, err
	}
	projection, err := domain.ValidateStoredRoute(claim.Payload, claim.PayloadDigest)
	if err != nil || projection.EventID != claim.EventID || projection.Route.AccountID != claim.AccountID || projection.Route.Generation != claim.Generation || claim.SchemaVersion != "1" || (projection.Enabled && projection.Route.TenantID != claim.TenantID) {
		return true, r.outbox.FinishRoute(ctx, claim, false, "CHANNEL_OUTBOX_INTEGRITY", 0)
	}
	raw, _, err := projection.Encode()
	if err != nil {
		return true, r.outbox.FinishRoute(ctx, claim, false, "CHANNEL_OUTBOX_INTEGRITY", 0)
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = r.publisher.PublishRoute(publishCtx, claim.EventID, raw)
	cancel()
	if err != nil {
		delay := time.Second << min(max(claim.Attempt-1, 0), 6)
		return true, r.outbox.FinishRoute(ctx, claim, false, "CHANNEL_ROUTE_PUBLISH_UNAVAILABLE", min(delay, time.Minute))
	}
	return true, r.outbox.FinishRoute(ctx, claim, true, "", 0)
}
func (r *RouteRelay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		found, err := r.Step(ctx)
		if err == nil && found {
			continue
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
