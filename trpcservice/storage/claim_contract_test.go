package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestFakeCoordinationClaimContract(t *testing.T) {
	s := NewFakeCoordinationStore()
	tc := validContext("tenant-a")
	key := DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: "message-a"}
	first, err := s.Claim(context.Background(), tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatalf("single claim: %v", err)
	}
	if first.Status != ClaimAcquired || first.OwnerID != "owner-1" || first.Epoch == 0 || first.FenceToken == 0 {
		t.Fatalf("invalid first claim: %+v", first)
	}
	type result struct {
		requested, returned string
		claim               Claim
		err                 error
	}
	const n = 100
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		owner := "owner-" + string(rune('a'+i))
		wg.Add(1)
		go func(requested string) {
			defer wg.Done()
			c, e := s.Claim(context.Background(), tc, key, time.Second, requested)
			results <- result{requested: requested, returned: c.OwnerID, claim: c, err: e}
		}(owner)
	}
	wg.Wait()
	close(results)
	owners := map[string]int{}
	statuses := map[ClaimStatus]int{}
	attempts := map[int]int{}
	fences := map[uint64]int{}
	epochs := map[Epoch]int{}
	errorsByClass := map[string]int{}
	for r := range results {
		if r.err != nil {
			class := "unexpected"
			if errors.Is(r.err, ErrInvalidArgument) || errors.Is(r.err, ErrInvalidOwner) || errors.Is(r.err, ErrTenantMismatch) {
				class = "parameter/tenant"
			}
			errorsByClass[class]++
			continue
		}
		owners[r.returned]++
		statuses[r.claim.Status]++
		attempts[r.claim.Attempt]++
		fences[r.claim.FenceToken]++
		epochs[r.claim.Epoch]++
	}
	t.Logf("owners=%v attempts=%v fences=%v epochs=%v statuses=%v errors=%v", owners, attempts, fences, epochs, statuses, errorsByClass)
	if len(errorsByClass) != 0 {
		t.Fatalf("unexpected error classifications: %v", errorsByClass)
	}
	if len(owners) != 1 {
		t.Fatalf("multiple owners: %v", owners)
	}
	if _, ok := owners[first.OwnerID]; !ok {
		t.Fatalf("stable owner=%q not returned: %v", first.OwnerID, owners)
	}
	if len(attempts) != 1 || len(fences) != 1 || len(epochs) != 1 {
		t.Fatalf("unstable claim metadata: attempts=%v fences=%v epochs=%v", attempts, fences, epochs)
	}
	winner := first
	g := OperationGuard{Backend: BackendPostgres, Epoch: winner.Epoch, OwnerID: winner.OwnerID, FenceToken: winner.FenceToken}
	if err := s.Complete(context.Background(), tc, key, winner.OwnerID, "response", g); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(context.Background(), tc, key, "old", "again", g); err == nil {
		t.Fatal("stale owner accepted")
	}
}
