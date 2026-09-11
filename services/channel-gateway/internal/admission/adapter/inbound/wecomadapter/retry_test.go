package wecomadapter_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	adapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

type acceptFunc func(context.Context, domain.Inbound) (domain.Receipt, error)

func (f acceptFunc) AcceptInbound(ctx context.Context, in domain.Inbound) (domain.Receipt, error) {
	return f(ctx, in)
}

func TestTransientAdmissionRetriesSameNormalizedInput(t *testing.T) {
	var first domain.Inbound
	calls := 0
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(ctx context.Context, in domain.Inbound) (domain.Receipt, error) {
		calls++
		if calls == 1 {
			first = in
			return domain.Receipt{}, domain.ErrUnavailable
		}
		if in.Key != first.Key || in.SourceDigest != first.SourceDigest || !in.ReceivedAt.Equal(first.ReceivedAt) || !bytes.Equal(in.ReplyContext, first.ReplyContext) || *in.ConnectionFence != *first.ConnectionFence {
			t.Fatal("retry changed normalized input or owner")
		}
		return domain.Receipt{Decision: "admit-run"}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Handle(context.Background(), event()); err != nil {
		t.Fatalf("temporary failure prevented recovery: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2", calls)
	}
}

func TestRetryBudgetExhaustionAndDefaultPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy []adapter.RetryPolicy
		want   int
	}{
		{"default", nil, 6},
		{"configured", []adapter.RetryPolicy{{MaxAttempts: 3, Timeout: time.Second, Backoff: time.Millisecond}}, 3},
		{"single attempt", []adapter.RetryPolicy{{MaxAttempts: 1, Timeout: time.Second, Backoff: time.Millisecond}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(context.Context, domain.Inbound) (domain.Receipt, error) {
				calls++
				return domain.Receipt{}, domain.ErrUnavailable
			}), tc.policy...)
			if err != nil {
				t.Fatal(err)
			}
			err = h.Handle(context.Background(), event())
			if !adapter.IsRetryable(err) || !errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("budget exhaustion classification=%v", err)
			}
			if calls != tc.want {
				t.Fatalf("attempts=%d want=%d", calls, tc.want)
			}
		})
	}
}

func TestRetryNeverRetriesDeterministicRejection(t *testing.T) {
	for _, failure := range []error{domain.ErrConflict, domain.ErrInvalidInput, fmt.Errorf("rejected: %w", domain.ErrConflict)} {
		calls := 0
		h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(context.Context, domain.Inbound) (domain.Receipt, error) {
			calls++
			return domain.Receipt{}, failure
		}), adapter.RetryPolicy{MaxAttempts: 4, Timeout: time.Second, Backoff: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		err = h.Handle(context.Background(), event())
		if !errors.Is(err, failure) || !adapter.IsRejected(err) || adapter.IsRetryable(err) || calls != 1 {
			t.Fatalf("rejected result=%v calls=%d", err, calls)
		}
	}
}

func TestRetryCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	var calls atomic.Int32
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(context.Context, domain.Inbound) (domain.Receipt, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		return domain.Receipt{}, domain.ErrUnavailable
	}), adapter.RetryPolicy{MaxAttempts: 20, Timeout: time.Second, Backoff: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Handle(ctx, event()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no first attempt")
	}
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) || adapter.IsRetryable(err) {
			t.Fatalf("cancel became restartable: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel ignored during backoff")
	}
	if calls.Load() != 1 {
		t.Fatalf("new retry after cancel: %d", calls.Load())
	}
}

func TestRetryDeadlineDifferentiatesRetentionFromOwnerContext(t *testing.T) {
	for _, parentExpires := range []bool{false, true} {
		name := "retention"
		if parentExpires {
			name = "owner context"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cancel := func() {}
			timeout := 25 * time.Millisecond
			if parentExpires {
				ctx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
				timeout = time.Second
			}
			defer cancel()
			calls := 0
			var originalDeadline time.Time
			h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(ctx context.Context, _ domain.Inbound) (domain.Receipt, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatal("retry attempt has no deadline")
				}
				originalDeadline = deadline
				<-ctx.Done()
				return domain.Receipt{}, ctx.Err()
			}), adapter.RetryPolicy{MaxAttempts: 20, Timeout: timeout, Backoff: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			err = h.Handle(ctx, event())
			if !errors.Is(err, context.DeadlineExceeded) || adapter.IsRetryable(err) == parentExpires || calls != 1 || originalDeadline.IsZero() {
				t.Fatalf("deadline result=%v retryable=%v calls=%d", err, adapter.IsRetryable(err), calls)
			}
		})
	}
}

func TestRetryPolicyRejectsInvalidBounds(t *testing.T) {
	dst := acceptFunc(func(context.Context, domain.Inbound) (domain.Receipt, error) { return domain.Receipt{}, nil })
	for _, p := range []adapter.RetryPolicy{{MaxAttempts: -1}, {MaxAttempts: 21}, {Timeout: -time.Second}, {Timeout: 31 * time.Second}, {Backoff: -time.Second}, {Backoff: 2 * time.Second}} {
		if _, err := adapter.NewHandler("account-1", "bot-1", fence(), dst, p); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatalf("invalid policy accepted: %+v err=%v", p, err)
		}
	}
	if _, err := adapter.NewHandler("account-1", "bot-1", fence(), dst, adapter.RetryPolicy{}, adapter.RetryPolicy{}); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("multiple policies accepted")
	}
}

func TestRetryKeepsOneDeadlineAcrossAttempts(t *testing.T) {
	var deadline time.Time
	calls := 0
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(ctx context.Context, _ domain.Inbound) (domain.Receipt, error) {
		calls++
		got, ok := ctx.Deadline()
		if !ok {
			t.Fatal("missing retention deadline")
		}
		if calls == 1 {
			deadline = got
		} else if !deadline.Equal(got) {
			t.Fatal("retry reset the retention deadline")
		}
		return domain.Receipt{}, domain.ErrUnavailable
	}), adapter.RetryPolicy{MaxAttempts: 3, Timeout: time.Second, Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Handle(context.Background(), event()); !adapter.IsRetryable(err) || calls != 3 {
		t.Fatalf("result=%v calls=%d", err, calls)
	}
}

func TestRetryDoesNotQueryWithAlreadyCanceledOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), acceptFunc(func(context.Context, domain.Inbound) (domain.Receipt, error) { calls++; return domain.Receipt{}, nil }))
	if err != nil {
		t.Fatal(err)
	}
	err = h.Handle(ctx, event())
	if !errors.Is(err, context.Canceled) || adapter.IsRetryable(err) || calls != 0 {
		t.Fatalf("canceled callback result=%v calls=%d", err, calls)
	}
}
