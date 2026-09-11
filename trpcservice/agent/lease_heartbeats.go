package agent

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/leaseheartbeat"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type emptyLease struct{}

type claimHeartbeat = leaseheartbeat.Heartbeat[emptyLease]
type idempotencyLeaseHeartbeat = leaseheartbeat.Heartbeat[emptyLease]
type sessionLeaseHeartbeat = leaseheartbeat.Heartbeat[storage.SessionExecutionLease]
type usageReservationHeartbeat = leaseheartbeat.Heartbeat[emptyLease]

// startClaimHeartbeat keeps a long-running execution claim fresh. Completion
// still carries a trace fence, so a lost claim cannot be committed by its old
// owner even if a renewal races with takeover.
func startClaimHeartbeat(parent context.Context, store storage.ExecutionDedupStore, tenantID, channel, bindingID, messageID, traceID string, ttl time.Duration) *claimHeartbeat {
	return leaseheartbeat.Start(parent, "claim heartbeat", emptyLease{}, max(ttl/3, time.Second), finalizationTimeout,
		func(ctx context.Context, value emptyLease) (emptyLease, error) {
			return value, store.Renew(ctx, tenantID, channel, bindingID, messageID, traceID)
		})
}

func startIdempotencyLeaseHeartbeat(parent context.Context, store storage.IdempotencyStore, lease storage.Lease, ttl time.Duration) *idempotencyLeaseHeartbeat {
	return leaseheartbeat.Start(parent, "idempotency lease heartbeat", emptyLease{}, max(ttl/3, 100*time.Millisecond), finalizationTimeout,
		func(ctx context.Context, value emptyLease) (emptyLease, error) {
			return value, store.Renew(ctx, lease, ttl)
		})
}

func startSessionLeaseHeartbeat(parent context.Context, store storage.SessionExecutionLeaser, lease storage.SessionExecutionLease, ttl time.Duration) *sessionLeaseHeartbeat {
	return leaseheartbeat.Start(parent, "session lease heartbeat", lease, max(ttl/3, time.Second), finalizationTimeout,
		func(ctx context.Context, current storage.SessionExecutionLease) (storage.SessionExecutionLease, error) {
			return store.RenewSessionExecutionLease(ctx, current, ttl)
		})
}

func startUsageReservationHeartbeat(parent context.Context, governor governance.UsageGovernor, reservation governance.UsageReservation, ttl time.Duration) *usageReservationHeartbeat {
	return leaseheartbeat.Start(parent, "usage reservation heartbeat", emptyLease{}, max(ttl/3, time.Second), finalizationTimeout,
		func(ctx context.Context, value emptyLease) (emptyLease, error) {
			return value, governor.Renew(ctx, reservation, ttl)
		})
}
