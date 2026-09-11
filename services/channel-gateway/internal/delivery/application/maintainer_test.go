package application_test

import (
	"context"
	"errors"
	"fmt"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
)

type maintenanceFixture struct {
	claims  func(context.Context, int) (int, error)
	calling func(context.Context, int) (int, error)
	expire  func(context.Context, int) (int, error)
	list    func(context.Context, app.ObservedAttemptQuery) (app.ObservedAttemptPage, error)
	resolve func(context.Context, string) (bool, error)
}

func (f maintenanceFixture) RecoverExpiredClaims(c context.Context, n int) (int, error) {
	if f.claims != nil {
		return f.claims(c, n)
	}
	return 0, nil
}
func (f maintenanceFixture) RecoverStaleCalling(c context.Context, n int) (int, error) {
	if f.calling != nil {
		return f.calling(c, n)
	}
	return 0, nil
}
func (f maintenanceFixture) ExpirePending(c context.Context, n int) (int, error) {
	if f.expire != nil {
		return f.expire(c, n)
	}
	return 0, nil
}
func (f maintenanceFixture) ListResolvableObserved(c context.Context, q app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) {
	if f.list != nil {
		return f.list(c, q)
	}
	return app.ObservedAttemptPage{Exhausted: true}, nil
}
func (f maintenanceFixture) ResolveObserved(c context.Context, id string) (bool, error) {
	if f.resolve != nil {
		return f.resolve(c, id)
	}
	return false, nil
}

func TestMaintainerSweepsWithoutAnySender(t *testing.T) {
	var calls []string
	step := func(name string, result int) func(context.Context, int) (int, error) {
		return func(ctx context.Context, n int) (int, error) {
			if ctx == nil || n != 2 {
				t.Fatal("missing bounded operation")
			}
			calls = append(calls, name)
			return result, nil
		}
	}
	m, err := app.NewMaintainer(maintenanceFixture{
		claims: step("claims", 1), calling: step("calling", 2), expire: step("expiry", 1),
		list: func(ctx context.Context, q app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) {
			calls = append(calls, "list")
			return app.ObservedAttemptPage{AttemptIDs: []string{"attempt-a"}, NextAttemptID: "attempt-a", Exhausted: true}, nil
		},
		resolve: func(ctx context.Context, id string) (bool, error) { calls = append(calls, id); return true, nil },
	}, app.MaintenanceOptions{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"claims", "calling", "expiry", "list", "attempt-a"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("order %v want %v", calls, want)
	}
	if got.RecoveredClaims != 1 || got.RecoveredCalling != 2 || got.ExpiredPending != 1 || got.ScannedObservations != 1 || got.ResolvedObservations != 1 {
		t.Fatalf("result %+v", got)
	}
}

func TestMaintainerAdvancesPastUnresolvableEvidenceAcrossSweeps(t *testing.T) {
	var cursors, resolved []string
	m, err := app.NewMaintainer(maintenanceFixture{
		list: func(ctx context.Context, q app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) {
			cursors = append(cursors, q.AfterAttemptID)
			if q.AfterAttemptID == "" {
				return app.ObservedAttemptPage{AttemptIDs: []string{"attempt-a"}, NextAttemptID: "attempt-a"}, nil
			}
			return app.ObservedAttemptPage{AttemptIDs: []string{"attempt-b"}, NextAttemptID: "attempt-b", Exhausted: true}, nil
		},
		resolve: func(ctx context.Context, id string) (bool, error) {
			resolved = append(resolved, id)
			return id == "attempt-b", nil
		},
	}, app.MaintenanceOptions{BatchSize: 1, MaxPagesPerSweep: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = m.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(cursors, []string{"", "attempt-a", ""}) || !reflect.DeepEqual(resolved, []string{"attempt-a", "attempt-b", "attempt-a"}) {
		t.Fatalf("cursor %v resolved %v", cursors, resolved)
	}
}

func TestMaintainerFailureDoesNotStarveIndependentExpiryOrLaterEvidence(t *testing.T) {
	expired := 0
	var resolved []string
	m, err := app.NewMaintainer(maintenanceFixture{
		claims: func(context.Context, int) (int, error) { return 0, fmt.Errorf("sensitive dependency error") },
		expire: func(context.Context, int) (int, error) { expired++; return 0, nil },
		list: func(ctx context.Context, q app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) {
			if q.AfterAttemptID == "" {
				return app.ObservedAttemptPage{AttemptIDs: []string{"attempt-a"}, NextAttemptID: "attempt-a"}, nil
			}
			return app.ObservedAttemptPage{AttemptIDs: []string{"attempt-b"}, NextAttemptID: "attempt-b", Exhausted: true}, nil
		},
		resolve: func(ctx context.Context, id string) (bool, error) {
			resolved = append(resolved, id)
			if id == "attempt-a" {
				return false, fmt.Errorf("sensitive evidence error")
			}
			return true, nil
		},
	}, app.MaintenanceOptions{BatchSize: 1, MaxPagesPerSweep: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err = m.Sweep(context.Background()); !errors.Is(err, d.ErrUnavailable) {
			t.Fatalf("error %v", err)
		}
	}
	if expired != 2 || !reflect.DeepEqual(resolved, []string{"attempt-a", "attempt-b"}) {
		t.Fatalf("expiry %d resolved %v", expired, resolved)
	}
}

func TestMaintainerRejectsMalformedPageBeforeResolving(t *testing.T) {
	for name, page := range map[string]app.ObservedAttemptPage{
		"duplicate":    {AttemptIDs: []string{"a", "a"}, NextAttemptID: "a", Exhausted: true},
		"oversized":    {AttemptIDs: []string{"a", "b", "c"}, NextAttemptID: "c", Exhausted: true},
		"nonadvancing": {NextAttemptID: ""},
		"wrong_cursor": {AttemptIDs: []string{"a"}, NextAttemptID: "z"},
		"invalid_id":   {AttemptIDs: []string{"bad id"}, NextAttemptID: "bad id", Exhausted: true},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			m, err := app.NewMaintainer(maintenanceFixture{list: func(context.Context, app.ObservedAttemptQuery) (app.ObservedAttemptPage, error) { return page, nil }, resolve: func(context.Context, string) (bool, error) { calls++; return true, nil }}, app.MaintenanceOptions{BatchSize: 2})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Sweep(context.Background()); !errors.Is(err, d.ErrInvalid) || calls != 0 {
				t.Fatalf("error %v resolve calls %d", err, calls)
			}
		})
	}
}

func TestMaintainerBoundsLifecycleAndCancellation(t *testing.T) {
	for name, o := range map[string]app.MaintenanceOptions{
		"negative_batch": {BatchSize: -1}, "huge_batch": {BatchSize: 1001}, "negative_pages": {MaxPagesPerSweep: -1}, "huge_pages": {MaxPagesPerSweep: 101},
		"tiny_poll": {PollInterval: time.Nanosecond}, "huge_poll": {PollInterval: 2 * time.Hour}, "tiny_operation": {OperationTimeout: time.Nanosecond}, "huge_operation": {OperationTimeout: 2 * time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := app.NewMaintainer(maintenanceFixture{}, o); err != d.ErrInvalid {
				t.Fatal(err)
			}
		})
	}
	if _, err := app.NewMaintainer((*maintenanceFixture)(nil), app.MaintenanceOptions{}); err != d.ErrUnavailable {
		t.Fatal(err)
	}
	started := make(chan struct{})
	m, _ := app.NewMaintainer(maintenanceFixture{claims: func(ctx context.Context, n int) (int, error) {
		if n != 100 {
			t.Error("default batch")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded operation")
		}
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}}, app.MaintenanceOptions{})
	if _, err := m.Sweep(nil); err != d.ErrInvalid {
		t.Fatal(err)
	}
	if err := m.Run(nil); err != d.ErrInvalid {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	receive(t, started)
	if _, err := m.Sweep(context.Background()); err != d.ErrUnavailable {
		t.Fatalf("overlapping sweep %v", err)
	}
	if err := m.Run(context.Background()); err != app.ErrRuntimeStarted {
		t.Fatal(err)
	}
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if m.Snapshot().Running || m.Snapshot().LastFailure != "canceled" {
		t.Fatalf("snapshot %+v", m.Snapshot())
	}
	if err := m.Run(context.Background()); err != app.ErrRuntimeStarted {
		t.Fatal(err)
	}
}

func TestMaintainerRunRecoversAfterDependencyFailure(t *testing.T) {
	var calls atomic.Int32
	succeeded := make(chan struct{}, 1)
	m, _ := app.NewMaintainer(maintenanceFixture{claims: func(context.Context, int) (int, error) {
		if calls.Add(1) == 1 {
			return 0, fmt.Errorf("hidden secret")
		}
		select {
		case succeeded <- struct{}{}:
		default:
		}
		return 1, nil
	}}, app.MaintenanceOptions{PollInterval: time.Millisecond})
	if _, err := m.Sweep(context.Background()); err != d.ErrUnavailable || m.Snapshot().LastFailure != "unavailable" {
		t.Fatalf("failure %v snapshot %+v", err, m.Snapshot())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	receive(t, succeeded)
	// Wait for the successful public Sweep result before canceling the next poll.
	deadline := time.Now().Add(time.Second)
	for m.Snapshot().LastResult.RecoveredClaims != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.Snapshot().LastResult.RecoveredClaims != 1 {
		t.Fatal("maintenance did not recover")
	}
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}
