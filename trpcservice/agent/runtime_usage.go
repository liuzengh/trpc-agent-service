package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

// runtimeUsageLease owns one model-usage reservation from acquisition through
// heartbeat and final settlement. Runtime.Handle only decides the outcome;
// reservation cleanup stays behind this seam.
type runtimeUsageLease struct {
	governor    governance.UsageGovernor
	reservation governance.UsageReservation
	heartbeat   *usageReservationHeartbeat
	settled     bool
}

func reserveRuntimeUsage(
	ctx context.Context,
	governor governance.UsageGovernor,
	request governance.UsageReservationRequest,
	ttl time.Duration,
) (context.Context, *runtimeUsageLease, error) {
	reservation, err := governor.Reserve(ctx, request)
	if err != nil {
		return ctx, nil, err
	}
	heartbeat := startUsageReservationHeartbeat(ctx, governor, reservation, ttl)
	return heartbeat.Context(), &runtimeUsageLease{
		governor: governor, reservation: reservation, heartbeat: heartbeat,
	}, nil
}

func (l *runtimeUsageLease) stopHeartbeat() error {
	if l == nil || l.heartbeat == nil {
		return nil
	}
	l.heartbeat.Stop()
	err := l.heartbeat.Err()
	l.heartbeat = nil
	return err
}

func (l *runtimeUsageLease) settleUnknown() error {
	if l == nil || l.settled {
		return nil
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	if err := l.governor.SettleUnknown(finalizeCtx, l.reservation); err != nil {
		return err
	}
	l.settled = true
	return nil
}

// SettleUnknown finalizes an interrupted or otherwise unpriced model call.
// Heartbeat loss does not replace the caller's primary run error; successful
// paths surface heartbeat loss through Settle below.
func (l *runtimeUsageLease) SettleUnknown() error {
	if l == nil {
		return nil
	}
	_ = l.stopHeartbeat()
	return l.settleUnknown()
}

func (l *runtimeUsageLease) Settle(known bool, usage governance.SettledUsage) error {
	if l == nil {
		return nil
	}
	if heartbeatErr := l.stopHeartbeat(); heartbeatErr != nil {
		settleErr := l.settleUnknown()
		return errors.Join(fmt.Errorf("model usage reservation lease lost: %w", heartbeatErr), settleErr)
	}
	if l.settled {
		return nil
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	var err error
	if known {
		err = l.governor.SettleKnown(finalizeCtx, l.reservation, usage)
	} else {
		err = l.governor.SettleUnknown(finalizeCtx, l.reservation)
	}
	if err != nil {
		return err
	}
	l.settled = true
	return nil
}

// Close is the fail-safe path. If Runtime exits before an explicit settlement,
// retain the reserved amount as unknown rather than leaking the reservation.
func (l *runtimeUsageLease) Close() {
	if l == nil {
		return
	}
	_ = l.stopHeartbeat()
	if !l.settled {
		_ = l.settleUnknown()
	}
}
