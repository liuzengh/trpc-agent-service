package inmemory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

func TestConcurrentReserveNeverOversellsAndReplayIsStable(t *testing.T) {
	store := New(100, 100)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for index := 0; index < 100; index++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Reserve(context.Background(), governance.ReserveRequest{TenantID: "tenant", RequestID: string(rune(i + 1)), ResourceID: "model", AttemptClass: "model", PolicyVersion: 1, MaxCostMicros: 10, MaxTokens: 10})
			if err == nil {
				allowed.Add(1)
			}
		}(index)
	}
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatalf("allowed=%d", allowed.Load())
	}
	replayStore := New(0, 0)
	in := governance.ReserveRequest{TenantID: "tenant", RequestID: "stable", ResourceID: "model", AttemptClass: "model", PolicyVersion: 1, MaxCostMicros: 0, MaxTokens: 0}
	first, err := replayStore.Reserve(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replayStore.Reserve(context.Background(), in)
	if err != nil || first != second {
		t.Fatalf("replay=%#v err=%v", second, err)
	}
	settled, err := replayStore.Settle(context.Background(), governance.SettleRequest{TenantID: "tenant", ReservationID: first.ReservationID, RequestID: "stable", Stage: "model", UsageKind: "tokens", ExpectedVersion: 1, Usage: governance.Usage{InputTokens: 20, OutputTokens: 5}, ActualCostMicros: 0})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := replayStore.Settle(context.Background(), governance.SettleRequest{TenantID: "tenant", ReservationID: first.ReservationID, RequestID: "stable", Stage: "model", UsageKind: "tokens", ExpectedVersion: 1, Usage: governance.Usage{InputTokens: 20, OutputTokens: 5}, ActualCostMicros: 0})
	if err != nil || replay != settled {
		t.Fatalf("settle replay=%#v err=%v", replay, err)
	}
}
