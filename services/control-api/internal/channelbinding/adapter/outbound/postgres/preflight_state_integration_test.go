package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
)

type recordingPreflightAuthorizer struct {
	preflightTestAuthorizer
	users  []string
	events []string
	pid    chan int
}

func (a *recordingPreflightAuthorizer) LockRequesterUsers(ctx context.Context, tx pgx.Tx, users []string) error {
	a.users = slices.Clone(users)
	a.events = append(a.events, "identity-users")
	if a.pid != nil {
		var pid int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		a.pid <- pid
	}
	return a.preflightTestAuthorizer.LockRequesterUsers(ctx, tx, users)
}

func (a *recordingPreflightAuthorizer) AuthorizeOwner(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	a.events = append(a.events, "session-owner")
	return a.preflightTestAuthorizer.AuthorizeOwner(ctx, tx, tenant, user)
}

func (a *recordingPreflightAuthorizer) AuthorizeRequester(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	a.events = append(a.events, "requester:"+user)
	return a.preflightTestAuthorizer.AuthorizeRequester(ctx, tx, tenant, user)
}

func (a *recordingPreflightAuthorizer) AuthorizeActiveTenant(ctx context.Context, tx pgx.Tx, tenant string) (bool, error) {
	a.events = append(a.events, "tenant")
	return a.preflightTestAuthorizer.AuthorizeActiveTenant(ctx, tx, tenant)
}

func TestPreflightCreateRequesterLockOrderingAgainstPostgreSQL(t *testing.T) {
	_, app, pool := preflightPG(t)
	account := createAccount(t, app).Account.ID
	for _, tc := range []struct {
		name, current, extra string
		want                 []string
	}{
		{"reverse", "usr_owner", "usr_other", []string{"usr_other", "usr_owner"}},
		{"duplicate", "usr_owner", "usr_owner", []string{"usr_owner"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &recordingPreflightAuthorizer{}
			store, err := NewPreflightStore(pool, auth, Options{ScopeID: testScope, SourceEpoch: testEpoch})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = store.WithTransaction(ctx, "tenant:tnt_a", func(tx application.PreflightTransaction) error {
				facts, err := tx.LockAccount(ctx, "tnt_a", account, tc.current, true, tc.extra)
				if err != nil {
					return err
				}
				if !facts.RequesterOwner || !facts.RequesterOwners[tc.current] || len(facts.RequesterOwners) != len(tc.want) {
					return errors.New("requester facts mismatch")
				}
				if tc.extra == "usr_other" && facts.RequesterOwners[tc.extra] {
					return errors.New("other tenant owner became an owner here")
				}
				return nil
			})
			if err != nil || !slices.Equal(auth.users, tc.want) || len(auth.events) < 3 || auth.events[0] != "identity-users" || auth.events[1] != "session-owner" {
				t.Fatalf("identity lock ordering: users=%v events=%v err=%v", auth.users, auth.events, err)
			}
		})
	}
	t.Run("blocked-old-identity-precedes-tenant-and-account", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollback(blocker)
		if _, err = blocker.Exec(ctx, `SELECT id FROM user_accounts WHERE id='usr_other' FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		auth := &recordingPreflightAuthorizer{pid: make(chan int, 1)}
		store, err := NewPreflightStore(pool, auth, Options{ScopeID: testScope, SourceEpoch: testEpoch})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- store.WithTransaction(ctx, "tenant:tnt_a", func(tx application.PreflightTransaction) error {
				facts, err := tx.LockAccount(ctx, "tnt_a", account, "usr_owner", true, "usr_other")
				if err != nil {
					return err
				}
				if !facts.RequesterOwners["usr_owner"] || facts.RequesterOwners["usr_other"] {
					return errors.New("owner facts changed after barrier release")
				}
				return nil
			})
		}()
		var pid int
		select {
		case pid = <-auth.pid:
		case <-ctx.Done():
			t.Fatal("diagnostic transaction did not begin")
		}
		for {
			var blocked bool
			if err = pool.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				break
			}
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal("diagnostic transaction did not wait on held Identity row")
			}
		}
		probe, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollback(probe)
		if _, err = probe.Exec(ctx, `SELECT id FROM tenants WHERE id='tnt_a' FOR UPDATE NOWAIT`); err != nil {
			t.Fatal("waiting preflight already locked Tenant before old Identity", err)
		}
		if _, err = probe.Exec(ctx, `SELECT id FROM channel_accounts WHERE tenant_id='tnt_a' AND id=$1 FOR UPDATE NOWAIT`, account); err != nil {
			t.Fatal("waiting preflight already locked Account before old Identity", err)
		}
		if err = probe.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err = blocker.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal("diagnostic transaction did not resume", err)
		}
		if !slices.Equal(auth.users, []string{"usr_other", "usr_owner"}) || auth.events[0] != "identity-users" {
			t.Fatal("actual lock barrier did not preserve sorted identities")
		}
	})
}

func modifyStoredPreflight(t *testing.T, store *PreflightStore, record application.PreflightRecord, change func(*application.PreflightRecord)) {
	t.Helper()
	ctx := context.Background()
	err := store.WithTransaction(ctx, "account:"+record.View.AccountID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, record.View.TenantID, record.View.AccountID, record.View.RequestedBy, false); err != nil {
			return err
		}
		got, found, err := tx.Load(ctx, record.View.PreflightID)
		if err != nil || !found {
			t.Fatalf("load for mutation: found=%v err=%v", found, err)
		}
		change(&got)
		return tx.Save(ctx, got)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func markPreflightRunning(r *application.PreflightRecord) {
	origin := "https://gateway.example.com"
	digest, _ := channelv1.PreflightConfigDigest(r.ScopeID, r.SourceEpoch, &origin, "PUBLIC_ORIGIN_STATIC_VALID")
	started := r.View.RequestedAt.Add(time.Second)
	expiry := started.Add(30 * time.Second)
	unconfirmed := "UNCONFIRMED"
	r.View.State, r.View.ReasonCode = "RUNNING", "CHANNEL_PREFLIGHT_RUNNING"
	r.View.StartedAt, r.LeaseExpiresAt = &started, &expiry
	r.View.GatewayConfigDigest, r.View.ExpectedPublicOrigin, r.View.GatewayConfigFreshness = &digest, &origin, &unconfirmed
	r.OriginStatus, r.LeaseEpoch = "PUBLIC_ORIGIN_STATIC_VALID", 1
	r.PrincipalID, r.InstanceID, r.InstanceEpoch = "spiffe://test/gateway/one", "gateway_one", testEpoch
	r.ClaimTokenHash = "sha256:" + strings.Repeat("c", 64)
}

func TestPreflightCandidateLeaseAndMaintenanceAgainstPostgreSQL(t *testing.T) {
	s, app, _ := preflightPG(t)
	a := createAccount(t, app)
	ctx := context.Background()
	r := insertPreflight(t, s, a.Account.ID, "cpf_candidate", time.Minute)
	key, found, err := s.Candidate(ctx, testScope)
	if err != nil || !found || key.ID != r.View.PreflightID {
		t.Fatal("queued candidate", found, err)
	}
	modifyStoredPreflight(t, s, r, markPreflightRunning)
	key, found, err = s.Candidate(ctx, testScope)
	if err != nil || !found || key.ID != r.View.PreflightID {
		t.Fatal("expired lease not candidate", found, err)
	}
	modifyStoredPreflight(t, s, r, func(r *application.PreflightRecord) {
		r.LeaseEpoch++
		expires := time.Now().UTC().Truncate(time.Microsecond).Add(20 * time.Second)
		r.LeaseExpiresAt = &expires
		r.ClaimTokenHash = "sha256:" + strings.Repeat("d", 64)
	})
	_, found, err = s.Candidate(ctx, testScope)
	if err != nil || found {
		t.Fatal("live lease offered", found, err)
	}
	keys, err := s.MaintenanceCandidates(ctx, testScope, 64)
	if err != nil || len(keys) != 1 || keys[0].ID != r.View.PreflightID {
		t.Fatal("maintenance ignores live lease", len(keys), err)
	}
	modifyStoredPreflight(t, s, r, func(r *application.PreflightRecord) {
		r.View.State, r.View.ReasonCode = "TIMED_OUT", "CHANNEL_PREFLIGHT_EXECUTION_TIMEOUT"
		r.View.Freshness = "EXPIRED"
	})
	keys, err = s.MaintenanceCandidates(ctx, testScope, 64)
	if err != nil || len(keys) != 0 {
		t.Fatal("terminal re-entered maintenance", len(keys), err)
	}
}

func TestPreflightImmutableTerminalAndStoredIntegrityAgainstPostgreSQL(t *testing.T) {
	s, app, pool := preflightPG(t)
	a := createAccount(t, app)
	r := insertPreflight(t, s, a.Account.ID, "cpf_terminal", 0)
	modifyStoredPreflight(t, s, r, func(r *application.PreflightRecord) {
		r.View.State, r.View.ReasonCode, r.View.Freshness = "STALE", "CHANNEL_PREFLIGHT_ACCOUNT_CHANGED", "STALE"
	})
	ctx := context.Background()
	err := s.WithTransaction(ctx, "account:"+a.Account.ID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, false); err != nil {
			return err
		}
		got, found, err := tx.Load(ctx, r.View.PreflightID)
		if err != nil || !found {
			t.Fatal(err)
		}
		got.View.State, got.View.ReasonCode, got.View.Freshness = "QUEUED", "CHANNEL_PREFLIGHT_QUEUED", "NOT_CHECKED"
		return tx.Save(ctx, got)
	})
	if err == nil {
		t.Fatal("terminal task resurrected")
	}
	// Adapter reads reject even well-formed JSON containing an unrecognized
	// field, rather than normalizing and discarding untrusted stored material.
	if _, err = pool.Exec(ctx, `UPDATE channel_preflights SET record_jsonb=record_jsonb||'{"unknown_field":true}'::jsonb WHERE id=$1`, r.View.PreflightID); err != nil {
		t.Fatal(err)
	}
	err = s.WithTransaction(ctx, "account:"+a.Account.ID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, false); err != nil {
			return err
		}
		_, _, err := tx.Load(ctx, r.View.PreflightID)
		return err
	})
	if err == nil {
		t.Fatal("stored unknown field accepted")
	}
}

func TestPreflightRequesterRowLockAndRevocationFactsAgainstPostgreSQL(t *testing.T) {
	s, app, pool := preflightPG(t)
	a := createAccount(t, app)
	ctx := context.Background()
	locked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- s.WithTransaction(ctx, "account:"+a.Account.ID, func(tx application.PreflightTransaction) error {
			facts, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, false)
			if err != nil {
				close(locked)
				return err
			}
			if !facts.RequesterOwner {
				return errors.New("active owner was rejected")
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err := pool.Exec(bounded, `UPDATE user_accounts SET status='DISABLED' WHERE id='usr_owner'`)
	cancel()
	close(release)
	if txErr := <-done; txErr != nil {
		t.Fatal(txErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("requester revocation did not serialize with diagnostic facts", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE user_accounts SET status='DISABLED' WHERE id='usr_owner'`); err != nil {
		t.Fatal(err)
	}
	err = s.WithTransaction(ctx, "account:"+a.Account.ID, func(tx application.PreflightTransaction) error {
		facts, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, false)
		if err != nil || facts.RequesterOwner || !facts.TenantActive || facts.SessionOwner {
			t.Fatalf("revocation facts: requester=%v tenant=%v session=%v err=%v", facts.RequesterOwner, facts.TenantActive, facts.SessionOwner, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal("facts must remain readable for historical receipt acknowledgement", err)
	}
}

func TestPreflightExpiredReceiptsBoundedCleanupAndRateAgainstPostgreSQL(t *testing.T) {
	s, _, pool := preflightPG(t)
	ctx := context.Background()
	principal := "spiffe://test/gateway/one"
	key := func(i int) string { return fmt.Sprintf("sha256:%064x", i+1) }
	err := s.WithTransaction(ctx, "instance:"+principal, func(tx application.PreflightTransaction) error {
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < 70; i++ {
			created := now.Add(-10 * time.Minute)
			r := application.PreflightRequest{Kind: "claim", Key: key(i), Digest: key(i), PrincipalID: principal, InstanceID: "gateway_one", InstanceEpoch: testEpoch, CreatedAt: created, ExpiresAt: created.Add(5 * time.Minute)}
			if err = tx.SaveRequest(ctx, r); err != nil {
				return err
			}
		}
		if _, found, err := tx.FindRequest(ctx, "claim", key(69)); err != nil || found {
			t.Fatal("expired receipt was replayed", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM channel_preflight_requests`).Scan(&count); err != nil || count != 6 {
		t.Fatal("cleanup not bounded to 64", count, err)
	}
	err = s.WithTransaction(ctx, "instance:"+principal, func(tx application.PreflightTransaction) error {
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		for i, epoch := range []string{testEpoch, "11111111-1111-4111-8111-111111111111"} {
			r := application.PreflightRequest{Kind: "claim", Key: key(68 + i), Digest: key(68 + i), PrincipalID: principal, InstanceID: "gateway_one", InstanceEpoch: epoch, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute)}
			if err = tx.SaveRequest(ctx, r); err != nil {
				return err
			}
		}
		count, err := tx.ClaimCount(ctx, principal, "gateway_one", now.Add(-time.Second))
		if err != nil || count != 2 {
			t.Fatal("restart bypassed database rate counter", count, err)
		}
		other, err := tx.ClaimCount(ctx, principal, "gateway_two", now.Add(-time.Second))
		if err != nil || other != 0 {
			t.Fatal("rate scope crossed instances", other, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal("expired key reuse", err)
	}
	if err = s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM channel_preflight_requests`).Scan(&count); err != nil || count != 2 {
		t.Fatal("cleanup deleted current receipts", count, err)
	}
}

func TestPreflightTaskCleanupRetainsActiveAndReferencedAgainstPostgreSQL(t *testing.T) {
	s, app, pool := preflightPG(t)
	a := createAccount(t, app)
	old := insertPreflight(t, s, a.Account.ID, "cpf_cleanup_old", 25*time.Hour)
	modifyStoredPreflight(t, s, old, func(r *application.PreflightRecord) {
		r.View.State, r.View.ReasonCode, r.View.Freshness = "TIMED_OUT", "CHANNEL_PREFLIGHT_NO_EXECUTOR", "EXPIRED"
	})
	active := insertPreflight(t, s, a.Account.ID, "cpf_cleanup_active", 25*time.Hour)
	ctx := context.Background()
	err := s.WithTransaction(ctx, "tenant:tnt_a", func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, true); err != nil {
			return err
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(channelv1.PreflightCreated{PreflightID: old.View.PreflightID, TenantID: "tnt_a", AccountID: a.Account.ID, RequestedAt: old.View.RequestedAt, JobDeadlineAt: old.View.JobDeadlineAt, StatusURL: "/v1/tenants/tnt_a/channel-accounts/" + a.Account.ID + "/preflights/" + old.View.PreflightID})
		hash := "sha256:" + strings.Repeat("e", 64)
		return tx.SaveRequest(ctx, application.PreflightRequest{Kind: "create", Key: hash, Digest: hash, TenantID: "tnt_a", AccountID: a.Account.ID, RequestedBy: testActor.UserID, PreflightID: old.View.PreflightID, CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour), Response: raw})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM channel_preflights`).Scan(&count); err != nil || count != 2 {
		t.Fatal("cleanup deleted active/referenced task", count, err)
	}
	// Only in this isolated schema: remove the expired-reference fixture to
	// prove terminal cleanup, without depending on a 24-hour test delay.
	if _, err = pool.Exec(ctx, `DELETE FROM channel_preflight_requests`); err != nil {
		t.Fatal(err)
	}
	if err = s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.Lookup(ctx, testScope, old.View.PreflightID); err != nil || found {
		t.Fatal("old terminal not collected", found, err)
	}
	if _, found, err := s.Lookup(ctx, testScope, active.View.PreflightID); err != nil || !found {
		t.Fatal("active task collected", found, err)
	}
}
