package postgresadapter_test

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	connectionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"sync/atomic"
	"testing"
	"time"
)

type guardFunc func(context.Context, pgx.Tx, string, string, int64, int64) error

func (f guardFunc) VerifyOwner(ctx context.Context, tx pgx.Tx, a, i string, e, r int64) error {
	return f(ctx, tx, a, i, e, r)
}
func wecomLedger(t *testing.T, ttl time.Duration) (*pg.Store, *connectionpg.Store, *pgxpool.Pool, connectiondomain.OwnerGrant, domain.Prepared) {
	t.Helper()
	pool, _ := deliveryDatabase(t)
	owners := connectionpg.NewStore(pool)
	g, e := owners.ApplyAndAcquire(context.Background(), connectiondomain.Account{ID: "account-1", BotID: "bot-1", CredentialRef: "FIXTURE_SECRET", Revision: 1, Enabled: true}, "instance-1", ttl)
	if e != nil {
		t.Fatal(e)
	}
	ledger, e := pg.NewStore(pool, owners, pg.Options{})
	if e != nil {
		t.Fatal(e)
	}
	p := prepared("wecom")
	p.Target.Provider = "wecom"
	p.Target.ConversationID = "conversation-1"
	p.Target.CallbackRequestID = "callback-req-1"
	p.Target.Origin = &domain.ReplyOrigin{InstanceID: g.InstanceID, Epoch: g.Epoch, Revision: g.Revision, SocketGeneration: 1}
	refresh(&p)
	accept(t, ledger, p)
	return ledger, owners, pool, g, p
}
func ownerRequest(g connectiondomain.OwnerGrant) domain.ClaimRequest {
	r := claimRequest()
	r.Provider = "wecom"
	r.Owner = &domain.OwnerFence{InstanceID: g.InstanceID, Epoch: g.Epoch, Revision: g.Revision}
	return r
}
func ownerOperation(t *testing.T, s *pg.Store, g connectiondomain.OwnerGrant, operation string) (domain.Claim, domain.Attempt, func(*pg.Store, context.Context) error) {
	t.Helper()
	var c domain.Claim
	var at domain.Attempt
	if operation != "claim" {
		c = oneClaim(t, s, ownerRequest(g))
	}
	if operation == "finish" {
		at = calling(t, s, c)
	}
	run := func(store *pg.Store, ctx context.Context) error {
		switch operation {
		case "claim":
			_, e := store.ClaimDue(ctx, ownerRequest(g))
			return e
		case "calling":
			_, e := store.MarkCalling(ctx, callRequest(c))
			return e
		case "finish":
			return store.Finish(ctx, at, acceptedResult())
		case "preparation":
			return store.FinishPreparation(ctx, c, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorPermanent})
		}
		return errors.New("invalid test operation")
	}
	return c, at, run
}
func TestWeComTransitionsHoldOwnerLockThroughTheirCommit(t *testing.T) {
	for _, operation := range []string{"claim", "calling", "finish", "preparation"} {
		t.Run(operation, func(t *testing.T) {
			s, owners, pool, g, _ := wecomLedger(t, 10*time.Second)
			_, _, run := ownerOperation(t, s, g, operation)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			verified := make(chan int32, 1)
			proceed := make(chan struct{})
			var released atomic.Bool
			defer func() {
				if released.CompareAndSwap(false, true) {
					close(proceed)
				}
			}()
			guard := guardFunc(func(ctx context.Context, tx pgx.Tx, a, i string, e, r int64) error {
				if err := owners.VerifyOwner(ctx, tx, a, i, e, r); err != nil {
					return err
				}
				var pid int32
				if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
					return err
				}
				verified <- pid
				select {
				case <-proceed:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			guarded, e := pg.NewStore(pool, guard, pg.Options{})
			if e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- run(guarded, ctx) }()
			var pid int32
			select {
			case pid = <-verified:
			case e := <-done:
				t.Fatalf("operation failed before guard %v", e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			release := make(chan error, 1)
			go func() { release <- owners.Release(ctx, g) }()
			waitDeliveryBlocked(t, ctx, pool, pid)
			if released.CompareAndSwap(false, true) {
				close(proceed)
			}
			if e = <-done; e != nil {
				t.Fatal(e)
			}
			if e = <-release; e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestWeComChecksOwnerAfterWaitingAndRejectsExpiredLease(t *testing.T) {
	for _, operation := range []string{"claim", "calling", "finish", "preparation"} {
		t.Run(operation, func(t *testing.T) {
			s, _, pool, g, in := wecomLedger(t, 100*time.Millisecond)
			_, _, run := ownerOperation(t, s, g, operation)
			before := partState(t, s, in.Intent.ID, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, e := pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback(context.Background())
			if _, e = tx.Exec(ctx, `SELECT 1 FROM gateway_connection_accounts WHERE account_id=$1 FOR UPDATE`, g.AccountID); e != nil {
				t.Fatal(e)
			}
			var pid int32
			if e = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- run(s, ctx) }()
			waitDeliveryBlocked(t, ctx, pool, pid)
			time.Sleep(130 * time.Millisecond)
			if e = tx.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			if e = <-done; !errors.Is(e, domain.ErrClaimLost) {
				t.Fatalf("expired owner advanced %s: %v", operation, e)
			}
			if after := partState(t, s, in.Intent.ID, 0); after != before {
				t.Fatalf("failed owner guard mutated part: before=%+v after=%+v", before, after)
			}
		})
	}
}
func TestOwnerTakeoverRejectsOldStateWriterButRetainsLateObservation(t *testing.T) {
	s, owners, pool, g, in := wecomLedger(t, time.Second)
	at := calling(t, s, oneClaim(t, s, ownerRequest(g)))
	if e := owners.Release(context.Background(), g); e != nil {
		t.Fatal(e)
	}
	newer, e := owners.ApplyAndAcquire(context.Background(), connectiondomain.Account{ID: g.AccountID, BotID: g.BotID, CredentialRef: "FIXTURE_SECRET", Revision: 1, Enabled: true}, "instance-2", time.Second)
	if e != nil || newer.Epoch <= g.Epoch {
		t.Fatalf("new owner=%+v %v", newer, e)
	}
	if e = s.Finish(context.Background(), at, acceptedResult()); !errors.Is(e, domain.ErrClaimLost) {
		t.Fatalf("old owner Finish=%v", e)
	}
	if e = s.Observe(context.Background(), observation(at, "old-owner-ack")); e != nil {
		t.Fatal(e)
	}
	if partState(t, s, in.Intent.ID, 0).State != domain.Calling {
		t.Fatal("late observation overwrote active state")
	}
	if _, e = pool.Exec(context.Background(), `UPDATE gateway_delivery_parts SET calling_until=clock_timestamp()-interval '1 second' WHERE part_id=$1`, at.PartID); e != nil {
		t.Fatal(e)
	}
	if n, e := s.RecoverStaleCalling(context.Background(), 100); e != nil || n != 1 {
		t.Fatalf("recovery %d %v", n, e)
	}
	if e = s.Observe(context.Background(), observation(at, "old-owner-ack")); e != nil {
		t.Fatal(e)
	}
	if partState(t, s, in.Intent.ID, 0).State != domain.Unknown {
		t.Fatal("observation changed UNKNOWN")
	}
}
func TestTelegramBypassesOwnerAtEveryDeliveryGate(t *testing.T) {
	pool, _ := deliveryDatabase(t)
	var calls atomic.Int32
	s, e := pg.NewStore(pool, guardFunc(func(context.Context, pgx.Tx, string, string, int64, int64) error {
		calls.Add(1)
		return errors.New("must not be called")
	}), pg.Options{})
	if e != nil {
		t.Fatal(e)
	}
	in := prepared("telegram-send")
	accept(t, s, in)
	at := calling(t, s, oneClaim(t, s, claimRequest()))
	if e = s.Finish(context.Background(), at, acceptedResult()); e != nil {
		t.Fatal(e)
	}
	if e = s.Observe(context.Background(), observation(at, "telegram-obs")); e != nil {
		t.Fatal(e)
	}
	in = prepared("telegram-prepare")
	accept(t, s, in)
	c := oneClaim(t, s, claimRequest())
	if e = s.FinishPreparation(context.Background(), c, domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: domain.ErrorPermanent}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.RecoverExpiredClaims(context.Background(), 100); e != nil {
		t.Fatal(e)
	}
	if _, e = s.RecoverStaleCalling(context.Background(), 100); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 0 {
		t.Fatalf("Telegram called Connection guard %d times", calls.Load())
	}
}
