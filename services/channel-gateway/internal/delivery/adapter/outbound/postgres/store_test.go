package postgresadapter

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"testing"
	"time"
)

var _ application.Ledger = (*Store)(nil)

type rollbackProbe struct {
	pgx.Tx
	calls   int
	inspect func(context.Context)
}

func (p *rollbackProbe) Rollback(ctx context.Context) error {
	p.calls++
	p.inspect(ctx)
	return errors.New("fixture rollback failure")
}
func TestRollbackHasIndependentBoundedContext(t *testing.T) {
	p := &rollbackProbe{inspect: func(ctx context.Context) {
		if ctx.Err() != nil {
			t.Fatal("canceled cleanup")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
			t.Fatal("unbounded cleanup")
		}
	}}
	rollback(p)
	if p.calls != 1 {
		t.Fatal("missing rollback")
	}
}
func TestInvalidOptionsAndBoundsFailBeforeDatabase(t *testing.T) {
	if _, e := NewStore(nil, nil, Options{}); !errors.Is(e, domain.ErrInvalid) {
		t.Fatal(e)
	}
	for _, o := range []Options{{MaxIntents: -1}, {MaxParts: -1}, {MaxAttempts: 4}, {MaxIntents: 1000001}, {MaxParts: 10000001}} {
		if _, e := NewStore(&pgxpool.Pool{}, nil, o); !errors.Is(e, domain.ErrInvalid) {
			t.Fatalf("options=%+v err=%v", o, e)
		}
	}
	s, _ := NewStore(&pgxpool.Pool{}, nil, Options{})
	ctx := context.Background()
	for _, call := range []func() error{
		func() error { _, e := s.ClaimDue(ctx, domain.ClaimRequest{}); return e }, func() error { _, e := s.MarkCalling(ctx, domain.CallingRequest{}); return e }, func() error { return s.Finish(ctx, domain.Attempt{}, domain.Result{}) }, func() error { return s.FinishPreparation(ctx, domain.Claim{}, domain.Result{}) }, func() error { return s.Observe(ctx, domain.Observation{}) }, func() error { _, e := s.ResolveObserved(ctx, ""); return e }, func() error { _, e := s.RecoverExpiredClaims(ctx, 0); return e }, func() error { _, e := s.RecoverStaleCalling(ctx, 1001); return e },
	} {
		if e := call(); !errors.Is(e, domain.ErrInvalid) {
			t.Fatal(e)
		}
	}
}
