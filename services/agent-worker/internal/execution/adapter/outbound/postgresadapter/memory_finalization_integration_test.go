package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestMemoryFinalizationPostgres(t *testing.T) {
	migrateURL, runtimeURL := os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if migrateURL == "" || runtimeURL == "" {
		t.Skip("requires disposable Worker PostgreSQL roles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	open := func(url string) *pgxpool.Pool {
		p, e := pgxpool.New(ctx, url)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		return p
	}
	migrator, pool := open(migrateURL), open(runtimeURL)
	if err := migrations.ApplyForRuntime(ctx, migrator, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	ledger := New(pool)
	sequence := 0
	newRun := func(t *testing.T, replyAge time.Duration) (domain.Requested, domain.Grant, domain.Finish) {
		t.Helper()
		sequence++
		id := fmt.Sprintf("memory_gate_%d_%d", time.Now().UnixNano(), sequence)
		r := domain.Requested{EventID: "event_" + id, EventDigest: domain.Digest([]byte(id)), RunDigest: domain.Digest([]byte("run" + id)), RunID: "run_" + id, AdmissionID: "admission_" + id, Route: domain.Route{TenantID: "tenant_" + id, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "100", Text: "remember tea", ReceivedAt: time.Now().UTC()}}
		policy := domain.Policy{Version: "memory-test", MaxRunAge: time.Minute, MaxReplyAge: replyAge, MaxFutureSkew: time.Second, LeaseTTL: 10 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 2}
		if _, e := ledger.Accept(ctx, r, policy, domain.IntakeLimits{MaxQueuedRuns: 10000, MaxRetainedRuns: 100000}); e != nil {
			t.Fatal(e)
		}
		g, e := ledger.Claim(ctx, domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: r.RunID, WorkerID: "worker", MaxRunSeconds: 30, MaxActive: 100})
		if e != nil {
			t.Fatal(e)
		}
		f := domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate_" + g.AttemptID, Digest: domain.Digest([]byte("snapshot")), Parent: g.Parent}, FinalText: "MEMORY_SUCCESS_CANARY", MemoryDigest: domain.Digest([]byte("sealed-memory"))}
		return r, g, f
	}
	follower := func(t *testing.T, r domain.Requested) domain.Requested {
		t.Helper()
		r.EventID += "_next"
		r.RunID += "_next"
		r.AdmissionID += "_next"
		r.EventDigest = domain.Digest([]byte(r.EventID))
		r.RunDigest = domain.Digest([]byte(r.RunID))
		r.Input.ReceivedAt = time.Now().UTC()
		p := domain.Policy{Version: "memory-test", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: 10 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 2}
		if _, e := ledger.Accept(ctx, r, p, domain.IntakeLimits{MaxQueuedRuns: 10000, MaxRetainedRuns: 100000}); e != nil {
			t.Fatal(e)
		}
		return r
	}
	assertHead := func(t *testing.T, r domain.Requested, c domain.Completion) {
		t.Helper()
		var ref, digest, status string
		e := pool.QueryRow(ctx, `SELECT s.accepted_ref,s.accepted_digest,r.status FROM execution_runs r JOIN execution_sessions s ON s.tenant_id=r.tenant_id AND s.session_id=r.session_id WHERE r.tenant_id=$1 AND r.run_id=$2`, r.Route.TenantID, r.RunID).Scan(&ref, &digest, &status)
		if e != nil || ref != c.Candidate.Ref || digest != c.Candidate.Digest || status != "SUCCEEDED" {
			t.Fatalf("accepted facts ref=%s digest=%s status=%s err=%v", ref, digest, status, e)
		}
	}
	assertHidden := func(t *testing.T, c domain.Completion) {
		t.Helper()
		if _, e := ledger.Final(ctx, c.FinalIntentID); !errors.Is(e, domain.ErrNotFound) {
			t.Fatalf("pending Final proof leaked: %v", e)
		}
		items, e := ledger.PendingReplies(ctx, 10000)
		if e != nil {
			t.Fatal(e)
		}
		for _, item := range items {
			if item.IntentID == c.FinalIntentID {
				t.Fatal("pending Final reached relay")
			}
		}
		var digest string
		var ready bool
		if e = pool.QueryRow(ctx, `SELECT digest,ready FROM execution_reply_outbox WHERE intent_id=$1`, c.FinalIntentID).Scan(&digest, &ready); e != nil || ready {
			t.Fatal("gate", ready, e)
		}
		if e = ledger.MarkReplyPublished(ctx, c.FinalIntentID, digest); !errors.Is(e, domain.ErrConflict) {
			t.Fatal("pending publish marked", e)
		}
	}
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprintf("accepted_%t_hidden_then_exact_final", success), func(t *testing.T) {
			r, _, f := newRun(t, time.Minute)
			c, e := ledger.Complete(ctx, f)
			if e != nil {
				t.Fatal(e)
			}
			if c.MemoryDigest != f.MemoryDigest || c.MemoryStatus != "PENDING" {
				t.Fatal(c)
			}
			assertHead(t, r, c)
			assertHidden(t, c)
			second := follower(t, r)
			claim := domain.ClaimRequest{TenantID: r.Route.TenantID, RunID: second.RunID, WorkerID: "worker-two", MaxRunSeconds: 30, MaxActive: 100}
			if _, e = ledger.Claim(ctx, claim); !errors.Is(e, domain.ErrNotReady) {
				t.Fatal("follower escaped pending Memory gate", e)
			}
			ready, e := ledger.Ready(ctx, 10000)
			if e != nil {
				t.Fatal(e)
			}
			for _, v := range ready {
				if v.Request.RunID == second.RunID {
					t.Fatal("pending follower is scheduled")
				}
			}
			// A different Session is not globally blocked by this Memory application.
			other := r
			other.Input.ConversationID = "other-chat"
			other.RunID += "_other"
			other.EventID += "_other"
			other.AdmissionID += "_other"
			other = follower(t, other)
			otherGrant, e := ledger.Claim(ctx, domain.ClaimRequest{TenantID: other.Route.TenantID, RunID: other.RunID, WorkerID: "other-worker", MaxRunSeconds: 30, MaxActive: 100})
			if e != nil {
				t.Fatal("different Session blocked", e)
			}
			if e = ledger.FailAttempt(ctx, otherGrant, "TEST_DONE", false); e != nil {
				t.Fatal(e)
			}
			if e = ledger.FinalizeMemory(ctx, c, success); e != nil {
				t.Fatal(e)
			}
			// Same decision represents an uncertain-commit retry; never reopens output.
			if e = ledger.FinalizeMemory(ctx, c, success); e != nil {
				t.Fatal("idempotent finalization", e)
			}
			if e = ledger.FinalizeMemory(ctx, c, !success); !errors.Is(e, domain.ErrConflict) {
				t.Fatal("opposite decision rewrote Final", e)
			}
			final, e := ledger.Final(ctx, c.FinalIntentID)
			if e != nil {
				t.Fatal(e)
			}
			event, e := codec.DecodeReplyIntent(final.Payload)
			if e != nil {
				t.Fatal(e)
			}
			want := memoryFailureFinal
			if success {
				want = f.FinalText
			}
			if event.Content.Text != want {
				t.Fatalf("text=%q want=%q", event.Content.Text, want)
			}
			if !success && strings.Contains(string(final.Payload), f.FinalText) {
				t.Fatal("successful Memory statement leaked after Apply failure")
			}
			verified, e := ledger.FindCompletion(ctx, c.TenantID, c.RunID)
			if e != nil {
				t.Fatal(e)
			}
			wantStatus := "FAILED"
			if success {
				wantStatus = "APPLIED"
			}
			if verified.Status != domain.Succeeded || verified.MemoryStatus != wantStatus || verified.ResultDigest != c.ResultDigest {
				t.Fatal(verified)
			}
			assertHead(t, r, c)
			next, e := ledger.Claim(ctx, claim)
			if e != nil || next.Parent != c.Candidate {
				t.Fatal("follower not released with accepted Session", next, e)
			}
			if e = ledger.FailAttempt(ctx, next, "TEST_DONE", false); e != nil {
				t.Fatal(e)
			}
			t.Logf("MEMORY_FINAL_GATE=PASS applied=%t pending_final_hidden=true follower_blocked=true different_session_allowed=true accepted_head_unchanged=true same_decision_idempotent=true", success)
		})
	}
	t.Run("wrong_acceptance_identity_never_releases", func(t *testing.T) {
		_, _, f := newRun(t, time.Minute)
		c, e := ledger.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		mutations := []func(*domain.Completion){func(v *domain.Completion) { v.AttemptID += "wrong" }, func(v *domain.Completion) { v.ResultDigest = domain.Digest([]byte("wrong")) }, func(v *domain.Completion) { v.MemoryDigest = domain.Digest([]byte("wrong")) }, func(v *domain.Completion) { v.Candidate.Ref += "wrong" }, func(v *domain.Completion) { v.CompletionID += "wrong" }, func(v *domain.Completion) { v.TenantID += "wrong" }, func(v *domain.Completion) { v.FinalIntentID += "wrong" }, func(v *domain.Completion) { v.CompletedAt = v.CompletedAt.Add(time.Second) }}
		for i, mutate := range mutations {
			wrong := c
			mutate(&wrong)
			if e = ledger.FinalizeMemory(ctx, wrong, true); e == nil {
				t.Fatalf("identity mutation %d accepted", i)
			}
			assertHidden(t, c)
		}
		if e = ledger.FinalizeMemory(ctx, c, true); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("old_lease_cannot_stage_an_accepted_memory_gate", func(t *testing.T) {
		r, g, f := newRun(t, time.Minute)
		g.LeaseEpoch++
		f.Grant = g
		if _, e := ledger.Complete(ctx, f); !errors.Is(e, domain.ErrFenced) {
			t.Fatal(e)
		}
		var n int
		if e := pool.QueryRow(ctx, `SELECT count(*) FROM execution_completions WHERE tenant_id=$1 AND run_id=$2`, r.Route.TenantID, r.RunID).Scan(&n); e != nil || n != 0 {
			t.Fatal(n, e)
		}
	})
	t.Run("failed_finalization_transaction_leaks_no_success", func(t *testing.T) {
		r, _, f := newRun(t, time.Minute)
		c, e := ledger.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		var original []byte
		var originalDigest string
		if e = pool.QueryRow(ctx, `SELECT payload,digest FROM execution_reply_outbox WHERE intent_id=$1`, c.FinalIntentID).Scan(&original, &originalDigest); e != nil {
			t.Fatal(e)
		}
		fn := fmt.Sprintf("memory_gate_reject_%d", time.Now().UnixNano())
		_, e = migrator.Exec(ctx, `CREATE FUNCTION `+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='`+r.RunID+`' THEN RAISE EXCEPTION 'fixture memory decision failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER `+fn+` BEFORE UPDATE OF memory_status ON execution_completions FOR EACH ROW EXECUTE FUNCTION `+fn+`()`)
		if e != nil {
			t.Fatal(e)
		}
		defer func() {
			_, e := migrator.Exec(context.Background(), `DROP TRIGGER IF EXISTS `+fn+` ON execution_completions; DROP FUNCTION IF EXISTS `+fn+`() `)
			if e != nil {
				t.Error(e)
			}
		}()
		if e = ledger.FinalizeMemory(ctx, c, false); e == nil {
			t.Fatal("injected decision failure returned success")
		}
		assertHidden(t, c)
		assertHead(t, r, c)
		var after []byte
		var digest, status string
		if e = pool.QueryRow(ctx, `SELECT o.payload,o.digest,c.memory_status FROM execution_reply_outbox o JOIN execution_completions c ON c.tenant_id=o.tenant_id AND c.run_id=o.run_id WHERE o.intent_id=$1`, c.FinalIntentID).Scan(&after, &digest, &status); e != nil || string(after) != string(original) || digest != originalDigest || status != "PENDING" {
			t.Fatal("partial visibility transaction", status, e)
		}
		if _, e = migrator.Exec(ctx, `DROP TRIGGER `+fn+` ON execution_completions; DROP FUNCTION `+fn+`() `); e != nil {
			t.Fatal(e)
		}
		if e = ledger.FinalizeMemory(ctx, c, false); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("expired_reply_still_finalizes_memory_without_outbox", func(t *testing.T) {
		r, _, f := newRun(t, time.Millisecond)
		var expired bool
		for !expired {
			if e := pool.QueryRow(ctx, `SELECT clock_timestamp()>=reply_deadline FROM execution_runs WHERE tenant_id=$1 AND run_id=$2`, r.Route.TenantID, r.RunID).Scan(&expired); e != nil {
				t.Fatal(e)
			}
		}
		c, e := ledger.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		if c.ReplyDisposition != "NONE" || c.FinalIntentID != "" || c.MemoryStatus != "PENDING" {
			t.Fatal(c)
		}
		if e = ledger.FinalizeMemory(ctx, c, true); e != nil {
			t.Fatal(e)
		}
		assertHead(t, r, c)
	})
	t.Run("no_memory_keeps_original_final_visibility", func(t *testing.T) {
		_, _, f := newRun(t, time.Minute)
		f.MemoryDigest = ""
		c, e := ledger.Complete(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		if c.MemoryStatus != "" || c.MemoryDigest != "" {
			t.Fatal(c)
		}
		if _, e = ledger.Final(ctx, c.FinalIntentID); e != nil {
			t.Fatal(e)
		}
		if e = ledger.FinalizeMemory(ctx, c, true); !errors.Is(e, domain.ErrInvalid) {
			t.Fatal(e)
		}
	})
}
