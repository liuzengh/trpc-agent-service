package postgresadapter

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

// This fixture owns its explicitly opted-in disposable Worker schema. Business
// terminal states are reached through Claim/Complete, never by test SQL updates.
func TestWorkerRetainedRunCapacityPostgres(t *testing.T) {
	adminURL, migrationURL, runtimeURL := os.Getenv("WORKER_TEST_ADMIN_URL"), os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if adminURL == "" || migrationURL == "" || runtimeURL == "" {
		t.Skip("requires dedicated Worker PostgreSQL fixture")
	}
	if os.Getenv("WORKER_TEST_ALLOW_RESET") != "1" {
		t.Fatal("dedicated fixture reset must be explicit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	ledger := New(runtime)
	reset := func(t *testing.T) {
		t.Helper()
		if _, err := admin.Exec(ctx, `TRUNCATE worker.execution_sessions CASCADE;TRUNCATE worker.execution_rejections`); err != nil {
			t.Fatal(err)
		}
	}
	policy := domain.Policy{Version: "retained-test", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 3 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	request := func(id string) domain.Requested {
		return domain.Requested{EventID: "evt_" + id, RunID: "run_" + id, AdmissionID: "adm_" + id, EventDigest: domain.Digest([]byte("event" + id)), RunDigest: domain.Digest([]byte("run" + id)), Route: domain.Route{TenantID: "tenant_a", Provider: "telegram", AccountID: "account_a", BindingID: "binding_a", DeploymentRevisionID: "revision_a", ManifestRef: "manifest_a", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "43", Text: "hello", ReceivedAt: time.Now().UTC()}}
	}
	accept := func(t *testing.T, r domain.Requested, limits domain.IntakeLimits) domain.Receipt {
		t.Helper()
		v, e := ledger.Accept(ctx, r, policy, limits)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	claim := func(t *testing.T, r domain.Requested) domain.Grant {
		t.Helper()
		g, e := ledger.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: "worker", MaxRunSeconds: 120, MaxActive: 2})
		if e != nil {
			t.Fatal(e)
		}
		return g
	}
	finish := func(g domain.Grant) domain.Finish {
		return domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate_" + g.AttemptID, Digest: domain.Digest([]byte("snapshot" + g.AttemptID)), Parent: g.Parent}, FinalText: "completed"}
	}
	complete := func(t *testing.T, r domain.Requested) {
		t.Helper()
		if _, e := ledger.Complete(ctx, finish(claim(t, r))); e != nil {
			t.Fatal(e)
		}
	}
	counts := func(t *testing.T) [10]int64 {
		t.Helper()
		var v [10]int64
		err := runtime.QueryRow(ctx, `SELECT (SELECT count(*) FROM execution_runs),(SELECT count(*) FROM execution_sessions),(SELECT count(*) FROM execution_receipts),(SELECT count(*) FROM execution_attempts),(SELECT count(*) FROM execution_completions),(SELECT count(*) FROM execution_session_commits),(SELECT count(*) FROM execution_reply_outbox),(SELECT COALESCE(sum(next_sequence),0) FROM execution_sessions),(SELECT COALESCE(sum(settled_sequence),0) FROM execution_sessions),(SELECT count(*) FROM execution_rejections)`).Scan(&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9])
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	t.Run("completed_history_full_without_side_effects", func(t *testing.T) {
		reset(t)
		limits := domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 2}
		first, second := request("first"), request("second")
		accept(t, first, limits)
		complete(t, first)
		accept(t, second, limits)
		// Count both successful and failed retained terminal outcomes.
		if _, err := ledger.Complete(ctx, domain.Finish{Grant: claim(t, second), Status: domain.Failed, Reason: "TEST_FAILURE", FinalText: "failed"}); err != nil {
			t.Fatal(err)
		}
		before := counts(t)
		if before[0] != 2 || before[4] != 2 || before[8] != 2 {
			t.Fatalf("terminal history=%v", before)
		}
		backlog, err := ledger.ObserveStorage(ctx)
		if err != nil || backlog.Queued+backlog.Running+backlog.RetryWait != 0 {
			t.Fatal(backlog, err)
		}
		for _, conversation := range []string{"42", "new-session"} {
			next := request("next-" + conversation)
			next.Input.ConversationID = conversation
			if _, err := ledger.Accept(ctx, next, policy, limits); !errors.Is(err, domain.ErrCapacity) {
				t.Fatalf("completed history admitted new Run: %v", err)
			}
			if after := counts(t); after != before {
				t.Fatalf("capacity rejection mutated facts: before=%v after=%v", before, after)
			}
			if _, err := ledger.FindRun(ctx, next.Route.TenantID, next.RunID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatal("rejected Run visible", err)
			}
		}
	})
	t.Run("full_capacity_preserves_replay_alias_and_execution_final", func(t *testing.T) {
		reset(t)
		limits := domain.IntakeLimits{MaxQueuedRuns: 2, MaxRetainedRuns: 1}
		req := request("recovery")
		receipt := accept(t, req, limits)
		if _, err := ledger.Accept(ctx, request("full"), policy, limits); !errors.Is(err, domain.ErrCapacity) {
			t.Fatal(err)
		}
		if again := accept(t, req, limits); again != receipt {
			t.Fatal("receipt changed", again, receipt)
		}
		alias := req
		alias.EventID = "alias"
		alias.EventDigest = domain.Digest([]byte("alias"))
		if again := accept(t, alias, limits); again.RunID != receipt.RunID || again.Sequence != receipt.Sequence {
			t.Fatal("alias allocated another Run", again)
		}
		g := claim(t, req)
		renewed, err := ledger.Renew(ctx, g)
		if err != nil {
			t.Fatal(err)
		}
		if err = ledger.MarkExecuting(ctx, renewed); err != nil {
			t.Fatal(err)
		}
		f := finish(renewed)
		c, err := ledger.Complete(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ledger.Complete(ctx, f)
		if err != nil || !reflect.DeepEqual(c, again) {
			t.Fatal("completion replay changed", again, err)
		}
		final, err := ledger.Final(ctx, c.FinalIntentID)
		if err != nil || final.RunID != req.RunID {
			t.Fatal(final, err)
		}
		pending, err := ledger.PendingReplies(ctx, 1)
		if err != nil || len(pending) != 1 || pending[0].IntentID != final.IntentID {
			t.Fatal(pending, err)
		}
		if err = ledger.MarkReplyPublished(ctx, final.IntentID, final.Digest); err != nil {
			t.Fatal(err)
		}
		replay, err := ledger.Final(ctx, c.FinalIntentID)
		if err != nil || !reflect.DeepEqual(replay, final) {
			t.Fatal("Final changed after publish", err)
		}
		if pending, err = ledger.PendingReplies(ctx, 1); err != nil || len(pending) != 0 {
			t.Fatal(pending, err)
		}
		altered := req
		altered.EventDigest = domain.Digest([]byte("conflict"))
		for range 2 {
			if _, err = ledger.Accept(ctx, altered, policy, limits); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("capacity blocked conflict", err)
			}
		}
		expected := [10]int64{1, 1, 2, 1, 1, 1, 1, 1, 1, 1}
		if got := counts(t); got != expected {
			t.Fatalf("recovery facts=%v want=%v", got, expected)
		}
	})
	t.Run("independent_pools_compete_for_last_retained_slot", func(t *testing.T) {
		reset(t)
		limits := domain.IntakeLimits{MaxQueuedRuns: 2, MaxRetainedRuns: 2}
		seed := request("seed")
		accept(t, seed, limits)
		complete(t, seed)
		// A separate pgx pool models another Worker process; no local shared mutex.
		other := New(open(runtimeURL))
		start := make(chan struct{})
		type result struct {
			req domain.Requested
			err error
		}
		results := make(chan result, 2)
		for i, l := range []*Ledger{ledger, other} {
			req := request([]string{"racer-a", "racer-b"}[i])
			req.Input.ConversationID = req.RunID
			go func(l *Ledger, req domain.Requested) {
				<-start
				_, err := l.Accept(ctx, req, policy, limits)
				results <- result{req, err}
			}(l, req)
		}
		close(start)
		success, full := 0, 0
		for range 2 {
			r := <-results
			switch {
			case r.err == nil:
				success++
			case errors.Is(r.err, domain.ErrCapacity):
				full++
				if _, e := ledger.FindRun(ctx, r.req.Route.TenantID, r.req.RunID); !errors.Is(e, domain.ErrNotFound) {
					t.Fatal("loser visible", e)
				}
			default:
				t.Fatal(r.err)
			}
		}
		if success != 1 || full != 1 {
			t.Fatalf("success=%d full=%d", success, full)
		}
		expected := [10]int64{2, 2, 2, 1, 1, 1, 1, 2, 1, 0}
		if got := counts(t); got != expected {
			t.Fatalf("racing intake facts=%v want=%v", got, expected)
		}
	})
	t.Run("lower_and_raise_capacity_preserve_retained_facts", func(t *testing.T) {
		reset(t)
		limits := domain.IntakeLimits{MaxQueuedRuns: 1, MaxRetainedRuns: 2}
		first := request("lower-first")
		receipt := accept(t, first, limits)
		complete(t, first)
		second := request("lower-second")
		accept(t, second, limits)
		complete(t, second)
		limits.MaxRetainedRuns = 1
		before := counts(t)
		next := request("after-raise")
		if _, err := ledger.Accept(ctx, next, policy, limits); !errors.Is(err, domain.ErrCapacity) {
			t.Fatal(err)
		}
		if again := accept(t, first, limits); again != receipt {
			t.Fatal("lowered limit broke replay")
		}
		if got := counts(t); got != before {
			t.Fatal("lowered limit mutated history", got, before)
		}
		limits.MaxRetainedRuns = 3
		accepted := accept(t, next, limits)
		if accepted.Sequence != 3 {
			t.Fatal("capacity rejection consumed sequence", accepted)
		}
		complete(t, next)
		if got := counts(t); got[0] != 3 || got[4] != 3 || got[8] != 3 {
			t.Fatal("raised capacity did not preserve history", got)
		}
	})
}
