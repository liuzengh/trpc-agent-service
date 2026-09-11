package channels

import (
	"context"
	"errors"
	"testing"
	"time"
)

// failingCoordinator simulates a Redis outage: every coordination call errors
// out immediately, which is what a stopped Redis looked like in the D4 drill.
type failingCoordinator struct{}

func (failingCoordinator) Claim(context.Context, string, time.Duration) (bool, error) {
	return false, errors.New("coordinator down")
}

func (failingCoordinator) Lock(context.Context, string, time.Duration) (func(), error) {
	return nil, errors.New("coordinator down")
}

// TestCoordinatorOutageFallsBackToInProcess pins the D4 lesson: a coordinator
// that cannot answer must degrade to the in-process dedup/lock instead of
// rejecting or hanging the message. The drill saw the reject path look
// exactly like a hang — the adapter had already answered 202, so nothing
// retried and nothing replied — which is why this is a test and not just a
// comment on claim.
func TestCoordinatorOutageFallsBackToInProcess(t *testing.T) {
	g := NewGateway(nil).WithCoordinator(failingCoordinator{})
	ctx := context.Background()

	fresh, err := g.claim(ctx, "m-1")
	if err != nil || !fresh {
		t.Fatalf("claim on outage = (%v, %v), want a fresh in-process claim", fresh, err)
	}
	fresh, err = g.claim(ctx, "m-1")
	if err != nil || fresh {
		t.Fatalf("second claim = (%v, %v), want the in-process table to catch the duplicate", fresh, err)
	}

	release, err := g.lockSession(ctx, "s-1", time.Second)
	if err != nil {
		t.Fatalf("lockSession on outage: %v, want the in-process lock", err)
	}
	if release == nil {
		t.Fatal("lockSession on outage returned a nil release")
	}
	release()

	// Empty ids are never deduped, outage or not.
	if fresh, err := g.claim(ctx, ""); err != nil || !fresh {
		t.Fatalf("empty-id claim = (%v, %v), want true", fresh, err)
	}
}
