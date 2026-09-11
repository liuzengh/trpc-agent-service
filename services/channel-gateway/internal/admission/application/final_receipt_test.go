package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

func TestFinalReceiptReplaysWinnerAfterConcurrentResolveFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var mu sync.Mutex
	var winner domain.Receipt
	var digest string
	finds, commits := 0, 0
	ledger := ledgerStub{
		find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			finds++
			return winner, digest, winner.Decision != "", nil
		},
		commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
			mu.Lock()
			defer mu.Unlock()
			commits++
			winner = c.Receipt
			digest = c.Input.SourceDigest
			return winner, nil
		},
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	loser := New(ledger, resolveFunc(func(ctx context.Context, _, _ string) (domain.RouteSnapshot, error) {
		close(entered)
		select {
		case <-resume:
			return domain.RouteSnapshot{}, errors.New("route disabled after the winner committed")
		case <-ctx.Done():
			return domain.RouteSnapshot{}, ctx.Err()
		}
	}))
	type outcome struct {
		receipt domain.Receipt
		err     error
	}
	done := make(chan outcome, 1)
	go func() { r, err := loser.AcceptInbound(ctx, input()); done <- outcome{r, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	expected, err := New(ledger, routes()).AcceptInbound(ctx, input())
	if err != nil {
		t.Fatal(err)
	}
	close(resume)
	select {
	case got := <-done:
		if got.err != nil || got.receipt != expected {
			t.Fatalf("concurrent winner was not replayed: got=%+v error=%v expected=%+v", got.receipt, got.err, expected)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if finds != 3 || commits != 1 {
		t.Fatalf("finds=%d commits=%d; expected A/B first lookup plus one final lookup and one commit", finds, commits)
	}
}

func TestFinalReceiptDecisionMatrix(t *testing.T) {
	winner := domain.Receipt{Decision: "admit-run", AdmissionID: "winner-admission", RunID: "winner-run"}
	databaseFailure := errors.New("final database read failed")
	for _, tc := range []struct {
		name                 string
		invalidRoute, found  bool
		digest               string
		findError, wantError error
	}{
		{name: "route error winner", found: true, digest: input().SourceDigest},
		{name: "invalid route winner", invalidRoute: true, found: true, digest: input().SourceDigest},
		{name: "different digest", found: true, digest: "different", wantError: domain.ErrConflict},
		{name: "no winner", wantError: domain.ErrUnavailable},
		{name: "PG failure", findError: databaseFailure, wantError: databaseFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finds := 0
			service := New(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
				finds++
				if finds == 1 {
					return domain.Receipt{}, "", false, nil
				}
				return winner, tc.digest, tc.found, tc.findError
			}, commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) {
				t.Fatal("preparation failure reached Commit")
				return domain.Receipt{}, nil
			}}, funcResolver(func() (domain.RouteSnapshot, error) {
				if tc.invalidRoute {
					r := route()
					r.AccountID = "different-account"
					return r, nil
				}
				return domain.RouteSnapshot{}, errors.New("mutable route unavailable")
			}))
			got, err := service.AcceptInbound(context.Background(), input())
			if tc.wantError == nil {
				if err != nil || got != winner {
					t.Fatalf("got=%+v err=%v", got, err)
				}
			} else if !errors.Is(err, tc.wantError) || got != (domain.Receipt{}) {
				t.Fatalf("got=%+v err=%v want=%v", got, err, tc.wantError)
			}
			if finds != 2 {
				t.Fatalf("finds=%d; final recheck must be exactly one", finds)
			}
		})
	}
}

func TestFinalReceiptUsesOriginalDeadline(t *testing.T) {
	finds := 0
	var firstDeadline time.Time
	service, err := NewWithOptions(ledgerStub{find: func(ctx context.Context, _ domain.EventKey) (domain.Receipt, string, bool, error) {
		finds++
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("lookup has no deadline")
		}
		if finds == 1 {
			firstDeadline = deadline
			return domain.Receipt{}, "", false, nil
		}
		if deadline != firstDeadline {
			t.Fatal("final lookup reset request deadline")
		}
		<-ctx.Done()
		return domain.Receipt{}, "", false, ctx.Err()
	}}, funcResolver(func() (domain.RouteSnapshot, error) { return domain.RouteSnapshot{}, domain.ErrUnavailable }), Options{MaxConcurrent: 1, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.AcceptInbound(context.Background(), input()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("final read lost deadline: %v", err)
	}
	if finds != 2 {
		t.Fatalf("finds=%d", finds)
	}
}

func TestFinalReceiptNeverQueriesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finds := 0
	service := New(ledgerStub{find: func(ctx context.Context, _ domain.EventKey) (domain.Receipt, string, bool, error) {
		finds++
		if ctx.Err() != nil {
			t.Fatal("query after context cancellation")
		}
		return domain.Receipt{}, "", false, nil
	}}, funcResolver(func() (domain.RouteSnapshot, error) { cancel(); return domain.RouteSnapshot{}, domain.ErrUnavailable }))
	if _, err := service.AcceptInbound(ctx, input()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation: %v", err)
	}
	if finds != 1 {
		t.Fatalf("finds=%d; expired request started final lookup", finds)
	}
}

func TestFinalReceiptDoesNotRetryInitialReadOrCommitFailure(t *testing.T) {
	failure := errors.New("database failure")
	for _, initialFailure := range []bool{true, false} {
		finds := 0
		resolver := routes()
		if initialFailure {
			resolver = noRoutes(t)
		}
		service := New(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
			finds++
			if initialFailure {
				return domain.Receipt{}, "", false, failure
			}
			return domain.Receipt{}, "", false, nil
		}, commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) { return domain.Receipt{}, failure }}, resolver)
		if _, err := service.AcceptInbound(context.Background(), input()); !errors.Is(err, failure) {
			t.Fatal(err)
		}
		if finds != 1 {
			t.Fatalf("unrelated failure triggered receipt fallback: finds=%d", finds)
		}
	}
}

func TestFinalReceiptCanReplayAfterStopButDoesNotCommit(t *testing.T) {
	finds := 0
	winner := domain.Receipt{Decision: "admit-run", AdmissionID: "winner", RunID: "run"}
	var service *Service
	service = New(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
		finds++
		if finds == 1 {
			return domain.Receipt{}, "", false, nil
		}
		return winner, input().SourceDigest, true, nil
	}, commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) {
		t.Fatal("Stop admitted new work")
		return domain.Receipt{}, nil
	}}, funcResolver(func() (domain.RouteSnapshot, error) { service.Stop(); return route(), nil }))
	if got, err := service.AcceptInbound(context.Background(), input()); err != nil || got != winner {
		t.Fatalf("stopped winner replay: %+v %v", got, err)
	}
	if finds != 2 {
		t.Fatal(finds)
	}
}

func TestFinalReceiptRespectsLookupConcurrencyBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resolving, resumeResolve := make(chan struct{}), make(chan struct{})
	occupying, resumeLookup := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	finds := 0
	service, err := NewWithOptions(ledgerStub{find: func(ctx context.Context, k domain.EventKey) (domain.Receipt, string, bool, error) {
		mu.Lock()
		finds++
		mu.Unlock()
		if k.EventID == "occupy" {
			close(occupying)
			select {
			case <-resumeLookup:
				return domain.Receipt{Decision: "ignore"}, input().SourceDigest, true, nil
			case <-ctx.Done():
				return domain.Receipt{}, "", false, ctx.Err()
			}
		}
		return domain.Receipt{}, "", false, nil
	}}, resolveFunc(func(ctx context.Context, _, _ string) (domain.RouteSnapshot, error) {
		close(resolving)
		select {
		case <-resumeResolve:
			return domain.RouteSnapshot{}, domain.ErrUnavailable
		case <-ctx.Done():
			return domain.RouteSnapshot{}, ctx.Err()
		}
	}), Options{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := service.AcceptInbound(ctx, input()); done <- err }()
	select {
	case <-resolving:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	otherDone := make(chan error, 1)
	other := input()
	other.Key.EventID = "occupy"
	go func() { _, err := service.AcceptInbound(ctx, other); otherDone <- err }()
	select {
	case <-occupying:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(resumeResolve)
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrUnavailable) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("final lookup queued behind occupied capacity")
	}
	close(resumeLookup)
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if finds != 2 {
		t.Fatalf("lookup bound bypassed: finds=%d", finds)
	}
}

func TestFinalReceiptDoesNotQueryAfterPreparationDeadline(t *testing.T) {
	finds := 0
	service, err := NewWithOptions(ledgerStub{find: func(ctx context.Context, _ domain.EventKey) (domain.Receipt, string, bool, error) {
		finds++
		if ctx.Err() != nil {
			t.Fatal("query started after request deadline")
		}
		return domain.Receipt{}, "", false, nil
	}}, resolveFunc(func(ctx context.Context, _, _ string) (domain.RouteSnapshot, error) {
		<-ctx.Done()
		return domain.RouteSnapshot{}, domain.ErrUnavailable
	}), Options{MaxConcurrent: 1, Timeout: 25 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.AcceptInbound(context.Background(), input()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired preparation result: %v", err)
	}
	if finds != 1 {
		t.Fatalf("finds=%d; deadline must forbid final query", finds)
	}
}
