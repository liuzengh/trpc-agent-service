package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func setup(t *testing.T) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DATABASE_URL for real PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"gateway_connection_" + hex.EncodeToString(random[:])}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return postgresadapter.NewStore(pool), pool
}

func account() domain.Account {
	return domain.Account{ID: "account-1", BotID: "wecom-bot-1", CredentialRef: "WECOM_TEST_SECRET", Revision: 1, Enabled: true}
}

func TestIntegrationConcurrentAcquireHasOneWinnerIncludingSameInstance(t *testing.T) {
	store, _ := setup(t)
	ctx := context.Background()
	type result struct {
		grant domain.OwnerGrant
		err   error
	}
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, e := store.ApplyAndAcquire(ctx, account(), fmt.Sprintf("instance-%d", i), 5*time.Second)
			results <- result{g, e}
		}()
	}
	wg.Wait()
	close(results)
	var winner domain.OwnerGrant
	wins := 0
	for result := range results {
		if result.err == nil {
			winner = result.grant
			wins++
		} else if !errors.Is(result.err, domain.ErrHeld) {
			t.Fatal(result.err)
		}
	}
	if wins != 1 {
		t.Fatalf("got %d winners, want one", wins)
	}
	if winner.Epoch != 1 || winner.Revision != 1 || winner.AccountID != account().ID || winner.BotID != account().BotID || winner.LeaseUntil.Sub(winner.ObservedAt) != 5*time.Second {
		t.Fatalf("invalid grant: %+v", winner)
	}
	if _, err := store.ApplyAndAcquire(ctx, account(), winner.InstanceID, 5*time.Second); !errors.Is(err, domain.ErrHeld) {
		t.Fatalf("same instance reused active grant: %v", err)
	}
	if err := store.Check(ctx, winner); err != nil {
		t.Fatal(err)
	}
}

func acquire(t *testing.T, s *postgresadapter.Store, a domain.Account, instance string, ttl time.Duration) domain.OwnerGrant {
	t.Helper()
	g, err := s.ApplyAndAcquire(context.Background(), a, instance, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestIntegrationExpiryAndOldEpochCannotRenewOrReleaseNewOwner(t *testing.T) {
	s, pool := setup(t)
	ctx := context.Background()
	g1 := acquire(t, s, account(), "instance-1", 100*time.Millisecond)
	// The real clock crosses the lease boundary; caller clocks cannot extend it.
	time.Sleep(150 * time.Millisecond)
	g1.LeaseUntil = time.Now().Add(24 * time.Hour)
	g1.ObservedAt = g1.LeaseUntil
	if err := s.Check(ctx, g1); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	if _, err := s.Renew(ctx, g1, time.Second); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	if err := s.MarkReplaced(ctx, g1); !errors.Is(err, domain.ErrLost) {
		t.Fatalf("expired grant installed replacement block: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.VerifyOwner(ctx, tx, g1.AccountID, g1.InstanceID, g1.Epoch, g1.Revision); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	g2 := acquire(t, s, account(), "instance-1", time.Second)
	if g2.Epoch != g1.Epoch+1 {
		t.Fatalf("same-instance new claim did not increment epoch: %+v", g2)
	}
	if err = s.Release(ctx, g1); !errors.Is(err, domain.ErrLost) {
		t.Fatalf("old epoch released new owner: %v", err)
	}
	if _, err = s.Renew(ctx, g1, time.Second); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	g3, err := s.Renew(ctx, g2, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if g3.Epoch != g2.Epoch || g3.LeaseUntil.Sub(g3.ObservedAt) != 2*time.Second || g3.ObservedAt.Before(g2.ObservedAt) {
		t.Fatalf("renew grant: %+v", g3)
	}
	if err = s.Release(ctx, g3); err != nil {
		t.Fatal(err)
	}
	if err = s.Release(ctx, g3); err != nil {
		t.Fatalf("exact release should be idempotent: %v", err)
	}
	if err = s.Check(ctx, g3); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	g4 := acquire(t, s, account(), "instance-2", time.Second)
	if g4.Epoch != g3.Epoch+1 {
		t.Fatalf("Release reset epoch: %+v", g4)
	}
}

func TestIntegrationNewRevisionCommitsWhileHeldAndOldClientMayRelease(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	a := account()
	g1 := acquire(t, s, a, "instance-1", 5*time.Second)
	a.Revision = 2
	a.CredentialRef = "WECOM_ROTATED_SECRET"
	if _, err := s.ApplyAndAcquire(ctx, a, "instance-2", time.Second); !errors.Is(err, domain.ErrHeld) {
		t.Fatal(err)
	}
	if err := s.Check(ctx, g1); !errors.Is(err, domain.ErrStaleRevision) {
		t.Fatalf("ErrHeld rolled back desired revision: %v", err)
	}
	if _, err := s.Renew(ctx, g1, time.Second); !errors.Is(err, domain.ErrStaleRevision) {
		t.Fatal(err)
	}
	if _, err := s.ApplyAndAcquire(ctx, account(), "instance-3", time.Second); !errors.Is(err, domain.ErrStaleRevision) {
		t.Fatal(err)
	}
	conflicting := a
	conflicting.CredentialRef = "DIFFERENT_SAME_REVISION"
	if _, err := s.ApplyAndAcquire(ctx, conflicting, "instance-3", time.Second); !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
	if err := s.Release(ctx, g1); err != nil {
		t.Fatalf("old revision could not release its own epoch: %v", err)
	}
	g2 := acquire(t, s, a, "instance-2", 5*time.Second)
	if g2.Revision != 2 || g2.Epoch != 2 {
		t.Fatal(g2)
	}
	a.Revision = 3
	a.Enabled = false
	if _, err := s.ApplyAndAcquire(ctx, a, "instance-3", time.Second); !errors.Is(err, domain.ErrDisabled) {
		t.Fatal(err)
	}
	if err := s.Check(ctx, g2); !errors.Is(err, domain.ErrDisabled) {
		t.Fatal(err)
	}
	if _, err := s.Renew(ctx, g2, time.Second); !errors.Is(err, domain.ErrDisabled) {
		t.Fatal(err)
	}
	if err := s.Release(ctx, g2); err != nil {
		t.Fatalf("disabled old owner could not release: %v", err)
	}
	a.Revision = 4
	a.Enabled = true
	g3 := acquire(t, s, a, "instance-3", time.Second)
	if g3.Epoch != 3 || g3.Revision != 4 {
		t.Fatal(g3)
	}
	if err := s.Release(ctx, g2); !errors.Is(err, domain.ErrLost) {
		t.Fatal(err)
	}
	if err := s.Check(ctx, g3); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationDisabledAccountIsDurableAndBotIdentityIsNeverReused(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	a := account()
	a.Enabled = false
	if _, err := s.ApplyAndAcquire(ctx, a, "instance-1", time.Second); !errors.Is(err, domain.ErrDisabled) {
		t.Fatal(err)
	}
	changedBot := a
	changedBot.BotID = "different-bot"
	changedBot.Revision = 2
	if _, err := s.ApplyAndAcquire(ctx, changedBot, "instance-2", time.Second); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stable account ID was reassigned: %v", err)
	}
	otherID := a
	otherID.ID = "account-2"
	otherID.Enabled = true
	if _, err := s.ApplyAndAcquire(ctx, otherID, "instance-2", time.Second); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("disabled account lost permanent physical identity: %v", err)
	}
	a.Revision = 2
	a.Enabled = true
	g := acquire(t, s, a, "instance-1", time.Second)
	if g.Epoch != 1 {
		t.Fatalf("disabled desired updates should not consume claims: %+v", g)
	}
	if err := s.Release(ctx, g); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyAndAcquire(ctx, otherID, "instance-2", time.Second); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("Release deleted identity: %v", err)
	}
}

func TestIntegrationConcurrentPhysicalBotRegistrationHasOneWinner(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a := account()
			a.ID = fmt.Sprintf("account-%d", i)
			_, err := s.ApplyAndAcquire(ctx, a, fmt.Sprintf("instance-%d", i), time.Second)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	wins, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			wins++
		} else if errors.Is(err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
}

func TestIntegrationReplacementBlocksAllReplicasUntilNewRevision(t *testing.T) {
	s, pool := setup(t)
	ctx := context.Background()
	a := account()
	g1 := acquire(t, s, a, "instance-1", time.Second)
	if err := s.MarkReplaced(ctx, g1); err != nil {
		t.Fatal(err)
	}
	restarted := postgresadapter.NewStore(pool)
	for _, instance := range []string{"instance-1", "instance-2", "instance-3"} {
		if _, err := restarted.ApplyAndAcquire(ctx, a, instance, time.Second); !errors.Is(err, domain.ErrReplaced) {
			t.Fatalf("%s reacquired replaced revision: %v", instance, err)
		}
	}
	if err := s.Check(ctx, g1); !errors.Is(err, domain.ErrReplaced) {
		t.Fatal(err)
	}
	if _, err := s.Renew(ctx, g1, time.Second); !errors.Is(err, domain.ErrReplaced) {
		t.Fatal(err)
	}
	a.Revision = 2
	g2 := acquire(t, restarted, a, "instance-2", time.Second)
	if g2.Epoch != 2 {
		t.Fatalf("replacement reset epoch: %+v", g2)
	}
	if err := s.MarkReplaced(ctx, g1); !errors.Is(err, domain.ErrStaleRevision) {
		t.Fatalf("old revision blocked new revision: %v", err)
	}
	if err := restarted.Check(ctx, g2); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, g2); err != nil {
		t.Fatal(err)
	}
	g3 := acquire(t, restarted, a, "instance-3", time.Second)
	if err := s.MarkReplaced(ctx, g2); !errors.Is(err, domain.ErrLost) {
		t.Fatalf("old epoch blocked new owner: %v", err)
	}
	if err := restarted.Check(ctx, g3); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationOwnerGuardSerializesEveryLeaseMutation(t *testing.T) {
	for _, mutation := range []string{"renew", "release", "revision", "replacement"} {
		t.Run(mutation, func(t *testing.T) {
			s, pool := setup(t)
			ctx := context.Background()
			a := account()
			g := acquire(t, s, a, "instance-1", 5*time.Second)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if err = s.VerifyOwner(ctx, tx, g.AccountID, g.InstanceID, g.Epoch, g.Revision); err != nil {
				t.Fatal(err)
			}
			limited, cancel := context.WithTimeout(ctx, 75*time.Millisecond)
			defer cancel()
			switch mutation {
			case "renew":
				_, err = s.Renew(limited, g, time.Second)
			case "release":
				err = s.Release(limited, g)
			case "revision":
				a.Revision = 2
				_, err = s.ApplyAndAcquire(limited, a, "instance-2", time.Second)
			case "replacement":
				err = s.MarkReplaced(limited, g)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("mutation escaped the admission SHARE lock: %v", err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = s.Check(ctx, g); err != nil {
				t.Fatalf("timed-out mutation changed lease facts: %v", err)
			}
		})
	}
}

func TestIntegrationOwnerGuardChecksDatabaseTimeAfterLockWait(t *testing.T) {
	s, pool := setup(t)
	ctx := context.Background()
	g := acquire(t, s, account(), "instance-1", 150*time.Millisecond)
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT account_id FROM gateway_connection_accounts WHERE account_id=$1 FOR UPDATE`, g.AccountID); err != nil {
		t.Fatal(err)
	}
	guardTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer guardTx.Rollback(ctx)
	pid := guardTx.Conn().PgConn().PID()
	result := make(chan error, 1)
	go func() {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		result <- s.VerifyOwner(call, guardTx, g.AccountID, g.InstanceID, g.Epoch, g.Revision)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var waiting bool
		if err = pool.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("guard never waited on the account row lock")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, domain.ErrLost) {
		t.Fatalf("pre-lock timestamp authorized an expired lease: %v", err)
	}
}
