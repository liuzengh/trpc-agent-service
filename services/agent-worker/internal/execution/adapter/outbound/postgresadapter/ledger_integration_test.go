package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestWorkerLedgerV1Postgres(t *testing.T) {
	adminURL, migrationURL, runtimeURL := os.Getenv("WORKER_TEST_ADMIN_URL"), os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if adminURL == "" || migrationURL == "" || runtimeURL == "" {
		t.Skip("requires dedicated Worker PostgreSQL fixture")
	}
	if os.Getenv("WORKER_TEST_ALLOW_RESET") != "1" {
		t.Fatal("dedicated fixture reset must be explicit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	open := func(url string) *pgxpool.Pool {
		p, e := pgxpool.New(ctx, url)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		return p
	}
	admin, migration, runtime := open(adminURL), open(migrationURL), open(runtimeURL)
	if err := migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	l := New(runtime)
	reset := func(t *testing.T) {
		t.Helper()
		_, err := admin.Exec(ctx, `TRUNCATE worker.execution_sessions CASCADE;TRUNCATE worker.execution_rejections`)
		if err != nil {
			t.Fatal(err)
		}
	}
	policy := domain.Policy{Version: "test-v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 3 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	request := func(id string) domain.Requested {
		return domain.Requested{EventID: "evt_" + id, RunID: "run_" + id, AdmissionID: "adm_" + id, EventDigest: domain.Digest([]byte("event" + id)), RunDigest: domain.Digest([]byte("run" + id)), Route: domain.Route{TenantID: "tenant_a", Provider: "telegram", AccountID: "account_a", BindingID: "binding_a", DeploymentRevisionID: "revision_a", ManifestRef: "manifest_a", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "43", Text: "hello", ReceivedAt: time.Now().UTC()}}
	}
	accept := func(t *testing.T, r domain.Requested, p domain.Policy) domain.Receipt {
		t.Helper()
		v, e := l.Accept(ctx, r, p, domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 100000})
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	claim := func(t *testing.T, r domain.Requested, worker string) domain.Grant {
		t.Helper()
		g, e := l.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: worker, MaxRunSeconds: 120, MaxActive: 3})
		if e != nil {
			t.Fatal(e)
		}
		return g
	}
	finish := func(g domain.Grant) domain.Finish {
		return domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate_" + g.AttemptID, Digest: domain.Digest([]byte("snapshot" + g.AttemptID)), Parent: g.Parent}, FinalText: "已完成"}
	}

	t.Run("tenant_quota_is_shared_across_concurrent_workers", func(t *testing.T) {
		reset(t)
		one, two := request("quota-one"), request("quota-two")
		two.Input.ConversationID = "different-session"
		usage := domain.UsagePolicy{
			Enabled: true, Revision: 1, MaxConcurrentRuns: 1,
			TokenPeriodSeconds: 3600, TokenLimit: 1000,
			TokenReservationPerRun: 100,
		}
		one.UsagePolicy, two.UsagePolicy = usage, usage
		accept(t, one, policy)
		accept(t, two, policy)
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for i, run := range []domain.Requested{one, two} {
			wg.Add(1)
			go func(worker int, requested domain.Requested) {
				defer wg.Done()
				_, err := l.Claim(ctx, domain.ClaimRequest{TenantID: requested.Route.TenantID, RunID: requested.RunID, WorkerID: fmt.Sprintf("quota-worker-%d", worker), MaxRunSeconds: 120, MaxActive: 3})
				errs <- err
			}(i, run)
		}
		wg.Wait()
		close(errs)
		succeeded, limited := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, domain.ErrCapacity):
				limited++
			default:
				t.Fatal(err)
			}
		}
		if succeeded != 1 || limited != 1 {
			t.Fatalf("claims succeeded=%d limited=%d", succeeded, limited)
		}
		var reservations int
		if err := runtime.QueryRow(ctx, `SELECT count(*) FROM worker.worker_tenant_usage_reservations_v1 WHERE tenant_id='tenant_a'`).Scan(&reservations); err != nil {
			t.Fatal(err)
		}
		if reservations != 1 {
			t.Fatalf("reservations=%d", reservations)
		}
	})

	t.Run("backlog_observation_tracks_durable_work", func(t *testing.T) {
		reset(t)
		one, two := request("observe1"), request("observe2")
		accept(t, one, policy)
		accept(t, two, policy)
		s, err := l.ObserveStorage(ctx)
		if err != nil || s.Queued != 2 || s.Running != 0 || s.ReplyPending != 0 || s.OldestRunSeconds < 0 {
			t.Fatalf("queued gauges %+v %v", s, err)
		}
		g := claim(t, one, "worker")
		s, err = l.ObserveStorage(ctx)
		if err != nil || s.Queued != 1 || s.Running != 1 {
			t.Fatalf("running gauges %+v %v", s, err)
		}
		c, err := l.Complete(ctx, finish(g))
		if err != nil {
			t.Fatal(err)
		}
		s, err = l.ObserveStorage(ctx)
		if err != nil || s.Queued != 1 || s.Running != 0 || s.ReplyPending != 1 || s.OldestReplySeconds < 0 {
			t.Fatalf("reply gauges %+v %v", s, err)
		}
		items, err := l.PendingReplies(ctx, 1)
		if err != nil || len(items) != 1 || items[0].IntentID != c.FinalIntentID {
			t.Fatal(items, err)
		}
		if err = l.MarkReplyPublished(ctx, items[0].IntentID, items[0].Digest); err != nil {
			t.Fatal(err)
		}
		s, err = l.ObserveStorage(ctx)
		if err != nil || s.ReplyPending != 0 || s.Queued != 1 {
			t.Fatalf("published history counted in backlog %+v %v", s, err)
		}
	})

	t.Run("concurrent_intake_and_durable_conflicts", func(t *testing.T) {
		reset(t)
		r := request("one")
		var wg sync.WaitGroup
		errs := make(chan error, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, e := l.Accept(ctx, r, policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 100000})
				if e == nil && v.Sequence != 1 {
					e = fmt.Errorf("sequence=%d", v.Sequence)
				}
				errs <- e
			}()
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		different := r
		different.EventDigest = domain.Digest([]byte("changed"))
		if _, e := l.Accept(ctx, different, policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 100000}); !errors.Is(e, domain.ErrConflict) {
			t.Fatal(e)
		}
		different.EventID = "evt_alias"
		different.RunDigest = domain.Digest([]byte("changed-run"))
		if _, e := l.Accept(ctx, different, policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 100000}); !errors.Is(e, domain.ErrConflict) {
			t.Fatal(e)
		}
		alias := r
		alias.EventID = "evt_same_run"
		alias.EventDigest = domain.Digest([]byte("alias"))
		v, e := l.Accept(ctx, alias, policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 100000})
		if e != nil || v.Sequence != 1 {
			t.Fatalf("alias %+v %v", v, e)
		}
		var n int
		if e := runtime.QueryRow(ctx, `SELECT count(*) FROM execution_runs`).Scan(&n); e != nil || n != 1 {
			t.Fatalf("runs=%d %v", n, e)
		}
		if e := runtime.QueryRow(ctx, `SELECT count(*) FROM execution_rejections`).Scan(&n); e != nil || n != 2 {
			t.Fatalf("rejections=%d %v", n, e)
		}
		if _, e := l.Accept(ctx, request("full"), policy, domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 100000}); !errors.Is(e, domain.ErrCapacity) {
			t.Fatal(e)
		}
	})
	t.Run("ordered_claim_completion_and_proof", func(t *testing.T) {
		reset(t)
		one, two := request("one"), request("two")
		accept(t, one, policy)
		accept(t, two, policy)
		if _, e := l.Claim(ctx, domain.ClaimRequest{TenantID: "tenant_a", RunID: two.RunID, WorkerID: "w1", MaxRunSeconds: 120, MaxActive: 3}); !errors.Is(e, domain.ErrNotReady) {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		grants := make(chan domain.Grant, 2)
		errs := make(chan error, 2)
		for _, w := range []string{"w1", "w2"} {
			wg.Add(1)
			go func(w string) {
				defer wg.Done()
				g, e := l.Claim(ctx, domain.ClaimRequest{TenantID: "tenant_a", RunID: one.RunID, WorkerID: w, MaxRunSeconds: 120, MaxActive: 3})
				if e == nil {
					grants <- g
				} else {
					errs <- e
				}
			}(w)
		}
		wg.Wait()
		close(grants)
		close(errs)
		if len(grants) != 1 {
			t.Fatalf("grants=%d", len(grants))
		}
		for e := range errs {
			if !errors.Is(e, domain.ErrNotReady) {
				t.Fatal(e)
			}
		}
		g := <-grants
		if _, e := l.ActiveToken(ctx, g.WorkerID, g.Token, "manifest_a", one.Route.ManifestDigest); e != nil {
			t.Fatal(e)
		}
		if e := l.MarkExecuting(ctx, g); e != nil {
			t.Fatal(e)
		}
		if _, e := l.Renew(ctx, g); e != nil {
			t.Fatal(e)
		}
		f := finish(g)
		c, e := l.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		again, e := l.Complete(ctx, f)
		if e != nil || c.CompletionID != again.CompletionID {
			t.Fatalf("uncertain retry %+v %v", again, e)
		}
		f.FinalText = "tamper"
		if _, e = l.Complete(ctx, f); !errors.Is(e, domain.ErrConflict) {
			t.Fatal(e)
		}
		if _, e = l.ActiveToken(ctx, g.WorkerID, g.Token, "manifest_a", one.Route.ManifestDigest); !errors.Is(e, domain.ErrFenced) {
			t.Fatal(e)
		}
		proof, e := l.Final(ctx, c.FinalIntentID)
		if e != nil || proof.ManifestDigest != one.Route.ManifestDigest || proof.RunID != one.RunID {
			t.Fatalf("proof %+v %v", proof, e)
		}
		g2 := claim(t, two, "w2")
		if g2.Parent != c.Candidate {
			t.Fatalf("next run did not read accepted head: %+v", g2.Parent)
		}
		if e = l.FailAttempt(ctx, g2, "MODEL_RETRY", true); e != nil {
			t.Fatal(e)
		}
		time.Sleep(3 * time.Millisecond)
		g3 := claim(t, two, "w1")
		if g3.AttemptID == g2.AttemptID || g3.Generation != g2.Generation+1 || !g3.Run.ExecutionDeadline.Equal(*g2.Run.ExecutionDeadline) {
			t.Fatal("retry identities/deadline changed incorrectly")
		}
	})
	t.Run("bounded_scan_skips_live_sessions", func(t *testing.T) {
		reset(t)
		for i := 0; i < 2; i++ {
			r := request(fmt.Sprintf("active%d", i))
			r.Input.ConversationID = fmt.Sprint(100 + i)
			accept(t, r, policy)
			claim(t, r, "w1")
		}
		next := request("ready")
		next.Input.ConversationID = "200"
		accept(t, next, policy)
		rows, err := l.Ready(ctx, 1)
		if err != nil || len(rows) != 1 || rows[0].Request.RunID != next.RunID {
			t.Fatalf("bounded scan starved independent session: %+v %v", rows, err)
		}
	})
	t.Run("reply_deadline_does_not_undo_session_completion", func(t *testing.T) {
		reset(t)
		p := policy
		p.MaxReplyAge = time.Millisecond
		r := request("latefinal")
		r.Input.ReceivedAt = time.Now().Add(-time.Second)
		accept(t, r, p)
		g := claim(t, r, "w1")
		c, e := l.Complete(ctx, finish(g))
		if e != nil || c.Status != domain.Succeeded || c.Candidate.Ref == "" || c.FinalIntentID != "" || c.ReplyDisposition != "NONE" {
			t.Fatalf("late final %+v %v", c, e)
		}
		rows, e := l.PendingReplies(ctx, 10)
		if e != nil || len(rows) != 0 {
			t.Fatal(rows, e)
		}
	})
	t.Run("completion_atomic_rollback", func(t *testing.T) {
		reset(t)
		r := request("atomic")
		accept(t, r, policy)
		g := claim(t, r, "w1")
		_, e := admin.Exec(ctx, `CREATE FUNCTION worker.test_reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test outbox failure'; END $$; CREATE TRIGGER test_reject_outbox BEFORE INSERT ON worker.execution_reply_outbox FOR EACH ROW EXECUTE FUNCTION worker.test_reject_outbox()`)
		if e != nil {
			t.Fatal(e)
		}
		defer func() {
			_, e := admin.Exec(ctx, `DROP TRIGGER test_reject_outbox ON worker.execution_reply_outbox; DROP FUNCTION worker.test_reject_outbox()`)
			if e != nil {
				t.Error(e)
			}
		}()
		if _, e = l.Complete(ctx, finish(g)); e == nil {
			t.Fatal("expected injected failure")
		}
		if _, e = l.FindCompletion(ctx, "tenant_a", r.RunID); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal(e)
		}
		var head string
		var settled int64
		if e = runtime.QueryRow(ctx, `SELECT accepted_ref,settled_sequence FROM execution_sessions`).Scan(&head, &settled); e != nil || head != "" || settled != 0 {
			t.Fatalf("partial commit %s %d %v", head, settled, e)
		}
		current, e := l.FindRun(ctx, "tenant_a", r.RunID)
		if e != nil || current.Status != domain.Running {
			t.Fatalf("run %+v %v", current, e)
		}
	})
	t.Run("lock_wait_fence_and_exhaustion", func(t *testing.T) {
		reset(t)
		p := policy
		p.LeaseTTL = 250 * time.Millisecond
		p.RenewalInterval = 50 * time.Millisecond
		p.MaxAttempts = 1
		r := request("fenced")
		accept(t, r, p)
		g := claim(t, r, "old")
		tx, e := admin.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer rollback(tx)
		if _, e = tx.Exec(ctx, `SELECT 1 FROM worker.execution_sessions FOR UPDATE`); e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { _, e := l.Complete(ctx, finish(g)); done <- e }()
		if _, e = tx.Exec(ctx, `SELECT pg_sleep(0.35)`); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		if e = <-done; !errors.Is(e, domain.ErrFenced) {
			t.Fatalf("late completion: %v", e)
		}
		ok, e := l.Terminalize(ctx, "tenant_a", r.RunID, "")
		if e != nil || !ok {
			t.Fatalf("terminalize %v %v", ok, e)
		}
		c, e := l.FindCompletion(ctx, "tenant_a", r.RunID)
		if e != nil || c.Kind != "SYSTEM_TERMINATION" || c.FinalIntentID != "" || c.Candidate.Ref != "" {
			t.Fatalf("completion %+v %v", c, e)
		}
		if _, e = l.Renew(ctx, g); !errors.Is(e, domain.ErrFenced) {
			t.Fatal(e)
		}
		r2 := request("after")
		accept(t, r2, policy)
		g2 := claim(t, r2, "new")
		if g2.Parent.Ref != "" {
			t.Fatal("unaccepted candidate leaked")
		}
	})
	t.Run("expired_queue_and_future_clock", func(t *testing.T) {
		reset(t)
		r := request("expired")
		r.Input.ReceivedAt = time.Now().Add(-2 * time.Hour)
		accept(t, r, policy)
		ok, e := l.Terminalize(ctx, "tenant_a", r.RunID, "")
		if e != nil || !ok {
			t.Fatal(ok, e)
		}
		r2 := request("future")
		r2.Input.ReceivedAt = time.Now().Add(2 * time.Hour)
		accept(t, r2, policy)
		ok, e = l.Terminalize(ctx, "tenant_a", r2.RunID, "")
		if e != nil || !ok {
			t.Fatal(ok, e)
		}
		var n int
		if e = runtime.QueryRow(ctx, `SELECT count(*) FROM execution_attempts`).Scan(&n); e != nil || n != 0 {
			t.Fatal(n, e)
		}
		if _, e = l.FindRun(ctx, "tenant_other", r.RunID); !errors.Is(e, domain.ErrNotFound) {
			t.Fatal(e)
		}
	})
	t.Run("runtime_role_is_not_migrator", func(t *testing.T) {
		if _, e := runtime.Exec(ctx, `CREATE TABLE worker.should_not_exist(id int)`); e == nil {
			t.Fatal("runtime created table")
		}
		if _, e := runtime.Exec(ctx, `DELETE FROM worker_schema_migrations`); e == nil {
			t.Fatal("runtime modified migration ledger")
		}
		var role string
		if e := runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); e != nil || !strings.Contains(role, "worker_runtime") {
			t.Fatal(role, e)
		}
	})
	t.Log("WORKER_LEDGER_V1=PASS concurrency, receipt conflict, session order, fence-after-lock, atomic completion, accepted head, immutable proof, retries and system terminalization")
}
