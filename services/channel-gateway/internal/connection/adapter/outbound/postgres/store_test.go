package postgresadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type rollbackProbe struct {
	pgx.Tx
	inspect func(context.Context)
}

func (p rollbackProbe) Rollback(ctx context.Context) error {
	p.inspect(ctx)
	return pgx.ErrTxClosed
}

func TestRollbackHasIndependentBoundedDeadline(t *testing.T) {
	called := false
	rollbackBounded(rollbackProbe{inspect: func(ctx context.Context) {
		called = true
		if err := ctx.Err(); err != nil {
			t.Fatalf("cleanup context already canceled: %v", err)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
			t.Fatalf("cleanup deadline is not bounded to 5s: %v %v", deadline, ok)
		}
	}})
	if !called {
		t.Fatal("rollback was not attempted")
	}
}

func TestInvalidInputsAreRejectedWithoutDatabase(t *testing.T) {
	s := NewStore(nil)
	ctx := context.Background()
	a := domain.Account{ID: "a", BotID: "b", CredentialRef: "REF", Revision: 1, Enabled: true}
	g := domain.OwnerGrant{AccountID: "a", BotID: "b", InstanceID: "i", Epoch: 1, Revision: 1}
	for name, call := range map[string]func() error{
		"account":                   func() error { _, err := s.ApplyAndAcquire(ctx, domain.Account{}, "i", time.Second); return err },
		"instance":                  func() error { _, err := s.ApplyAndAcquire(ctx, a, "", time.Second); return err },
		"acquire ttl":               func() error { _, err := s.ApplyAndAcquire(ctx, a, "i", 0); return err },
		"acquire missing pool":      func() error { _, err := s.ApplyAndAcquire(ctx, a, "i", time.Second); return err },
		"renew identity":            func() error { _, err := s.Renew(ctx, domain.OwnerGrant{}, time.Second); return err },
		"renew ttl":                 func() error { _, err := s.Renew(ctx, g, 0); return err },
		"release identity":          func() error { return s.Release(ctx, domain.OwnerGrant{}) },
		"check identity":            func() error { return s.Check(ctx, domain.OwnerGrant{}) },
		"replace identity":          func() error { return s.MarkReplaced(ctx, domain.OwnerGrant{}) },
		"guard identity":            func() error { return s.VerifyOwner(ctx, nil, "", "i", 1, 1) },
		"guard missing transaction": func() error { return s.VerifyOwner(ctx, nil, "a", "i", 1, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}
