package postgresadapter_test

import (
	"context"
	"errors"
	"fmt"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"sync"
	"testing"
)

func TestResolveObservedClosesOnlyRecoveredOriginalAttempt(t *testing.T) {
	s, _, pool := ledgers(t, pg.Options{})
	in := prepared("observed-resolution")
	accept(t, s, in)
	a := calling(t, s, oneClaim(t, s, claimRequest()))
	if e := s.Observe(context.Background(), observation(a, "ack-before-recovery")); e != nil {
		t.Fatal(e)
	}
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
		t.Fatal(e)
	}
	if n, e := s.RecoverStaleCalling(context.Background(), 100); e != nil || n != 1 {
		t.Fatalf("recover=%d %v", n, e)
	}
	changed, e := s.ResolveObserved(context.Background(), a.ID)
	if e != nil || !changed {
		t.Fatalf("authenticated late result did not close UNKNOWN: changed=%v err=%v", changed, e)
	}
	if partState(t, s, in.Intent.ID, 0).State != domain.Accepted {
		t.Fatal("explicit resolution did not finish original part")
	}
	assertCount(t, pool, "gateway_delivery_attempts", 1)
}

func TestResolveObservedDoesNotAdvanceCallingOrConflictingEvidence(t *testing.T) {
	for _, kind := range []string{"calling", "unknown-only", "not-sent", "conflict", "rejected", "old-attempt"} {
		t.Run(kind, func(t *testing.T) {
			s, _, pool := ledgers(t, pg.Options{})
			in := prepared("resolve-" + kind)
			accept(t, s, in)
			a := calling(t, s, oneClaim(t, s, claimRequest()))
			obs := observation(a, "evidence-1")
			switch kind {
			case "unknown-only":
				obs.Result = domain.Result{Certainty: domain.CertaintyUnknown}
			case "not-sent":
				obs.Result = domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}
			case "rejected":
				obs.Result = domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}
			}
			if e := s.Observe(context.Background(), obs); e != nil {
				t.Fatal(e)
			}
			if kind == "old-attempt" {
				if e := s.Finish(context.Background(), a, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorTemporary}); e != nil {
					t.Fatal(e)
				}
				if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
					t.Fatal(e)
				}
				newer := calling(t, s, oneClaim(t, s, claimRequest()))
				if newer.Number != 2 {
					t.Fatal("missing successor")
				}
			}
			if kind != "calling" {
				if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
					t.Fatal(e)
				}
				if _, e := s.RecoverStaleCalling(context.Background(), 100); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "conflict" {
				other := observation(a, "evidence-2")
				other.Result = domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}
				if e := s.Observe(context.Background(), other); e != nil {
					t.Fatal(e)
				}
			}
			before := partState(t, s, in.Intent.ID, 0)
			changed, e := s.ResolveObserved(context.Background(), a.ID)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "rejected" {
				if !changed || partState(t, s, in.Intent.ID, 0).State != domain.Rejected {
					t.Fatal("unique rejected evidence not resolved")
				}
			} else if changed || partState(t, s, in.Intent.ID, 0) != before {
				t.Fatalf("invalid evidence advanced %s", kind)
			}
		})
	}
}

func TestObservationCapacityAllowsReplayAndConcurrentResolutionIsSingleWriter(t *testing.T) {
	s, other, pool := ledgers(t, pg.Options{})
	in := prepared("observation-capacity")
	accept(t, s, in)
	a := calling(t, s, oneClaim(t, s, claimRequest()))
	for i := range 16 {
		if e := s.Observe(context.Background(), observation(a, fmt.Sprintf("evidence-%d", i))); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.Observe(context.Background(), observation(a, "excess")); !errors.Is(e, domain.ErrCapacity) {
		t.Fatalf("unbounded observations: %v", e)
	}
	if e := s.Observe(context.Background(), observation(a, "evidence-0")); e != nil {
		t.Fatalf("capacity blocked idempotent replay: %v", e)
	}
	if _, e := pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, a.PartID); e != nil {
		t.Fatal(e)
	}
	if _, e := s.RecoverStaleCalling(context.Background(), 100); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 16)
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Go(func() {
			store := s
			if i%2 == 1 {
				store = other
			}
			changed, e := store.ResolveObserved(context.Background(), a.ID)
			results <- changed
			errs <- e
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	n := 0
	for changed := range results {
		if changed {
			n++
		}
	}
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if n != 1 {
		t.Fatalf("resolution writers=%d", n)
	}
	var provenance string
	if e := pool.QueryRow(context.Background(), `SELECT resolved_observation_id FROM gateway_delivery_attempts WHERE attempt_id=$1`, a.ID).Scan(&provenance); e != nil || provenance == "" {
		t.Fatalf("resolution lost evidence provenance: %q %v", provenance, e)
	}
}
