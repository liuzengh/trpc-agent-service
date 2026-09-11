package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

type recordingRuntimeUsageGovernor struct {
	reservation governance.UsageReservation
	known       []governance.SettledUsage
	unknown     int
}

func (g *recordingRuntimeUsageGovernor) Reserve(context.Context, governance.UsageReservationRequest) (governance.UsageReservation, error) {
	return g.reservation, nil
}

func (*recordingRuntimeUsageGovernor) Renew(context.Context, governance.UsageReservation, time.Duration) error {
	return nil
}

func (g *recordingRuntimeUsageGovernor) SettleKnown(_ context.Context, _ governance.UsageReservation, usage governance.SettledUsage) error {
	g.known = append(g.known, usage)
	return nil
}

func (g *recordingRuntimeUsageGovernor) SettleUnknown(context.Context, governance.UsageReservation) error {
	g.unknown++
	return nil
}

func TestRuntimeUsageLeaseSettlesKnownExactlyOnce(t *testing.T) {
	governor := &recordingRuntimeUsageGovernor{reservation: governance.UsageReservation{ID: "usage-1"}}
	_, lease, err := reserveRuntimeUsage(context.Background(), governor, governance.UsageReservationRequest{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := governance.SettledUsage{PromptTokens: 10, CompletionTokens: 4, TotalTokens: 14, CostMicros: 9}
	if err := lease.Settle(true, want); err != nil {
		t.Fatal(err)
	}
	lease.Close()
	if len(governor.known) != 1 || governor.known[0] != want {
		t.Fatalf("known settlements = %#v, want %#v", governor.known, want)
	}
	if governor.unknown != 0 {
		t.Fatalf("unknown settlements = %d, want 0", governor.unknown)
	}
}

func TestRuntimeUsageLeaseCloseSettlesUnknown(t *testing.T) {
	governor := &recordingRuntimeUsageGovernor{reservation: governance.UsageReservation{ID: "usage-2"}}
	_, lease, err := reserveRuntimeUsage(context.Background(), governor, governance.UsageReservationRequest{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	lease.Close()
	lease.Close()
	if governor.unknown != 1 {
		t.Fatalf("unknown settlements = %d, want 1", governor.unknown)
	}
	if len(governor.known) != 0 {
		t.Fatalf("known settlements = %#v, want none", governor.known)
	}
}
