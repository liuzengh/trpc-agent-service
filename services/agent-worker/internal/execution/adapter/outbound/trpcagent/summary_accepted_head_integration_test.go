package trpcagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// This test crosses the actual Execution transaction, unlike the independently
// selected heads in TestSummaryUsesSamePostgresSessionCandidate. Only the SDK's
// model is a fixture; candidate INSERT, lease/Claim, Complete, formal commit and
// next accepted-parent selection all use the real PostgreSQL implementations.
func TestSummaryAcceptedHeadPostgresTransaction(t *testing.T) {
	names := []string{"WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SUMMARY_TEST_MIGRATION_URL", "WORKER_SUMMARY_TEST_RUNTIME_URL"}
	for _, name := range names {
		if os.Getenv(name) == "" {
			t.Skip("requires dedicated Worker and Session PostgreSQL fixture")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	open := func(name string) *pgxpool.Pool {
		pool, err := pgxpool.New(ctx, os.Getenv(name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	workerMigration, worker := open(names[0]), open(names[1])
	sessionMigration := open(names[2])
	if err := migrations.ApplyForRuntime(ctx, workerMigration, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	if err := sessionmigrations.Apply(ctx, sessionMigration); err != nil {
		t.Fatal(err)
	}
	storeURL := os.Getenv(names[3])
	cfg, err := pgxpool.ParseConfig(storeURL)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(storeURL)
	if err != nil {
		t.Fatal(err)
	}
	target := sessionstore.Target{Host: cfg.ConnConfig.Host, Port: cfg.ConnConfig.Port, Database: cfg.ConnConfig.Database, Username: cfg.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}
	store, err := sessionstore.Open(ctx, storeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	ledger := postgresadapter.New(worker)
	tenant := fmt.Sprintf("summary-accepted-%d", time.Now().UnixNano())
	policy := domain.Policy{Version: "summary-accepted-test", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Minute, LeaseTTL: 3 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	request := func(label string) domain.Requested {
		id := tenant + "-" + label
		return domain.Requested{EventID: "evt_" + id, RunID: "run_" + id, AdmissionID: "adm_" + id,
			EventDigest: domain.Digest([]byte("event-" + id)), RunDigest: domain.Digest([]byte("run-" + id)),
			Route: domain.Route{TenantID: tenant, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1},
			Input: domain.Input{ConversationID: "same-summary-chat", SenderID: "sender", Text: label, ReceivedAt: time.Now().UTC()}}
	}
	claim := func(r domain.Requested) domain.Grant {
		g, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: tenant, RunID: r.RunID, WorkerID: "summary-worker", MaxRunSeconds: 60, MaxActive: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err = ledger.MarkExecuting(ctx, g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	admit := func(label string) domain.Grant {
		r := request(label)
		if _, err := ledger.Accept(ctx, r, policy, domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
			t.Fatal(err)
		}
		return claim(r)
	}
	stage := func(g domain.Grant, payload []byte) domain.Candidate {
		h, err := store.Put(ctx, sessionstore.Candidate{Identity: sessionstore.Identity{TenantID: tenant, SessionID: g.Run.SessionID, RunID: g.Run.Request.RunID, AttemptID: g.AttemptID},
			Parent: sessionstore.Head{Ref: g.Parent.Ref, Digest: g.Parent.Digest}, ContentVersion: sessionstore.ContentVersion, Snapshot: payload})
		if err != nil {
			t.Fatal(err)
		}
		return domain.Candidate{Ref: h.Ref, Digest: h.Digest, Parent: g.Parent}
	}
	finish := func(g domain.Grant, c domain.Candidate) domain.Finish {
		return domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: c, FinalText: "Green tea."}
	}
	head := func() domain.Head {
		var h domain.Head
		if err := worker.QueryRow(ctx, `SELECT accepted_ref,accepted_digest FROM execution_sessions WHERE tenant_id=$1`, tenant).Scan(&h.Ref, &h.Digest); err != nil {
			t.Fatal(err)
		}
		return h
	}
	assertFormal := func(expected domain.Head, count int) {
		if actual := head(); actual != expected {
			t.Fatalf("accepted head changed: actual=%+v expected=%+v", actual, expected)
		}
		var n int
		if err := worker.QueryRow(ctx, `SELECT count(*) FROM execution_session_commits WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != count {
			t.Fatalf("formal commits=%d want=%d err=%v", n, count, err)
		}
	}
	g := admit("accepted-summary")
	payload := summaryLedgerSnapshot(t, ctx, g, nil, "User prefers green tea.", false)
	candidate := stage(g, payload)
	assertFormal(domain.Head{}, 0) // An INSERT alone never accepts SDK summary state.
	completion, err := ledger.Complete(ctx, finish(g, candidate))
	if err != nil {
		t.Fatal(err)
	}
	accepted := domain.Head{Ref: candidate.Ref, Digest: candidate.Digest}
	if completion.Status != domain.Succeeded || completion.Candidate != accepted {
		t.Fatalf("summary completion %+v", completion)
	}
	assertFormal(accepted, 1)
	if replay, err := ledger.Complete(ctx, finish(g, candidate)); err != nil || replay.CompletionID != completion.CompletionID {
		t.Fatalf("completion retry %+v %v", replay, err)
	}
	assertFormal(accepted, 1)
	// Close/reopen the actual Session pool, then use the next actual Claim parent,
	// never the test's candidate ref, to choose what is loaded.
	store.Close()
	store, err = sessionstore.Open(ctx, storeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	load := func(next domain.Grant) []byte {
		if next.Parent != accepted {
			t.Fatalf("next Claim chose nonaccepted parent %+v", next.Parent)
		}
		loaded, err := store.Load(ctx, tenant, next.Run.SessionID, sessionstore.Head{Ref: next.Parent.Ref, Digest: next.Parent.Digest})
		if err != nil || !bytes.Equal(loaded.Snapshot, payload) {
			t.Fatalf("formal parent snapshot differs: %v", err)
		}
		return loaded.Snapshot
	}
	if !t.Run("next_attempt_load_and_sdk_consumption", func(t *testing.T) {
		next := admit("read-accepted-summary")
		loaded := load(next)
		summaryLedgerConsume(t, ctx, next, loaded)
		// This invocation is a readback probe: settle failure without accepting its
		// in-memory continuation so all subsequent negatives use the same parent.
		if err := ledger.FailAttempt(ctx, next, "READBACK_PROBE", false); err != nil {
			t.Fatal(err)
		}
		assertFormal(accepted, 1)
	}) {
		return
	}
	if !t.Run("summary_failure_has_no_candidate", func(t *testing.T) {
		next := admit("failed-summary")
		loaded := load(next)
		if result := summaryLedgerSnapshot(t, ctx, next, loaded, "UNACCEPTED failed summary", true); result != nil {
			t.Fatal("failed summary produced snapshot")
		}
		var n int
		if err := sessionMigration.QueryRow(ctx, `SELECT count(*) FROM runtime_session.session_candidates WHERE run_id=$1`, next.Run.Request.RunID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("failed candidate count=%d err=%v", n, err)
		}
		if err := ledger.FailAttempt(ctx, next, "SUMMARY_MODEL_FAILED", false); err != nil {
			t.Fatal(err)
		}
		assertFormal(accepted, 1)
	}) {
		return
	}
	for _, mode := range []string{"failed_completion", "rejected_parent", "stale_lease"} {
		if !t.Run(mode, func(t *testing.T) {
			next := admit(mode)
			loaded := load(next)
			unaccepted := summaryLedgerSnapshot(t, ctx, next, loaded, "UNACCEPTED "+mode, false)
			if bytes.Equal(unaccepted, loaded) {
				t.Fatal("negative summary candidate did not change snapshot")
			}
			staged := stage(next, unaccepted)
			assertFormal(accepted, 1)
			rejected := finish(next, staged)
			want := domain.ErrFenced
			switch mode {
			case "failed_completion":
				rejected.Status, rejected.Reason = domain.Failed, "MODEL_FAILED"
				want = domain.ErrInvalid
			case "rejected_parent":
				rejected.Candidate.Parent = domain.Head{}
			case "stale_lease":
				// Wait on the authoritative DB predicate; never rewrite a lease,
				// timestamp, accepted head or Run status to manufacture expiry.
				for {
					var expired bool
					if err := worker.QueryRow(ctx, `SELECT lease_until<=clock_timestamp() FROM execution_attempts WHERE tenant_id=$1 AND attempt_id=$2`, tenant, next.AttemptID).Scan(&expired); err != nil {
						t.Fatal(err)
					}
					if expired {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
				next = claim(next.Run.Request)
				if next.AttemptID == rejected.Grant.AttemptID {
					t.Fatal("expiry did not create a fresh Attempt")
				}
				load(next)
			}
			if _, err := ledger.Complete(ctx, rejected); !errors.Is(err, want) {
				t.Fatalf("rejected summary Complete=%v want=%v", err, want)
			}
			assertFormal(accepted, 1)
			if _, err := ledger.FindCompletion(ctx, tenant, next.Run.Request.RunID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("rejected Complete left a durable completion: %v", err)
			}
			if err := ledger.FailAttempt(ctx, next, "REJECTED_SUMMARY_PROBE", false); err != nil {
				t.Fatal(err)
			}
			assertFormal(accepted, 1)
			// An orphan candidate remains immutable/readable but is not the parent.
			orphan, err := store.Load(ctx, tenant, next.Run.SessionID, sessionstore.Head{Ref: staged.Ref, Digest: staged.Digest})
			if err != nil || !bytes.Equal(orphan.Snapshot, unaccepted) {
				t.Fatalf("orphan candidate changed: %v", err)
			}
		}) {
			return
		}
	}
	follower := admit("fresh-follower")
	summaryLedgerConsume(t, ctx, follower, load(follower))
	if err := ledger.FailAttempt(ctx, follower, "READBACK_PROBE", false); err != nil {
		t.Fatal(err)
	}
	assertFormal(accepted, 1)
	t.Log("SUMMARY_ACCEPTED_HEAD_PG=PASS sdk_summary=true store_insert_not_accept=true complete_accepts_summary=true idempotent_completion=true next_claim_parent=true pool_reopen_load=true next_sdk_consumes=true summary_failure_no_candidate=true failed_complete_rejected=true parent_mismatch_rejected=true old_lease_rejected=true fresh_follower_parent_unchanged=true formal_commits=1")
}

type ledgerSummaryModel struct {
	text string
	fail bool
}

func (*ledgerSummaryModel) Info() model.Info { return model.Info{Name: "summary-fixture"} }
func (m *ledgerSummaryModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.fail {
		return nil, errors.New("explicit summary fixture failure")
	}
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage(m.text)}}}
	close(responses)
	return responses, nil
}

func summaryLedgerSnapshot(t *testing.T, ctx context.Context, g domain.Grant, accepted []byte, text string, fail bool) []byte {
	t.Helper()
	m := &ledgerSummaryModel{text: text, fail: fail}
	summarizer, err := BuildManifestSummarizer(summaryManifestFixture(), map[string]model.Model{"summarizer": m})
	if err != nil {
		t.Fatal(err)
	}
	o, err := newSummaryOverlay(g.Run.Request.Route.TenantID, g.Run.SessionID, accepted, 1<<20, summarizer)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	sess, err := o.GetSession(ctx, o.key)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"I prefer green tea", "Please remember", "Thank you"} {
		e := summaryFixtureEvent(fmt.Sprintf("%s-%d", g.AttemptID, i), text)
		e.FilterKey, e.Version = o.key.AppName, event.CurrentVersion
		if err = o.AppendEvent(ctx, sess, e); err != nil {
			t.Fatal(err)
		}
	}
	err = o.CreateSessionSummary(ctx, sess, o.key.AppName, true)
	if fail {
		if err == nil {
			t.Fatal("summary generator error was swallowed")
		}
		if b, err := o.Snapshot(); err == nil || b != nil {
			t.Fatal("failed Summary generated an acceptable candidate")
		}
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := o.GetSessionSummaryText(ctx, sess, session.WithSummaryFilterKey(o.key.AppName)); !ok || got != text {
		t.Fatalf("SDK summary=%q present=%v", got, ok)
	}
	b, err := o.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func summaryLedgerConsume(t *testing.T, ctx context.Context, g domain.Grant, accepted []byte) {
	t.Helper()
	if strings.Contains(string(accepted), "UNACCEPTED") {
		t.Fatal("rejected Summary polluted formal Session")
	}
	c := summaryManifestFixture()
	summarizer, err := BuildManifestSummarizer(c, map[string]model.Model{"summarizer": &summaryFixtureModel{}})
	if err != nil {
		t.Fatal(err)
	}
	o, err := newSummaryOverlay(g.Run.Request.Route.TenantID, g.Run.SessionID, accepted, 1<<20, summarizer)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	consumer := &summaryConsumerModel{}
	yes := true
	node := c.AgentPlan.Nodes[c.AgentPlan.Root]
	node.AddSessionSummary = &yes
	c.AgentPlan.Nodes[c.AgentPlan.Root] = node
	options, err := BuildManifestCapabilityOptions(c, c.AgentPlan.Root, CapabilityServices{})
	if err != nil {
		t.Fatal(err)
	}
	a := llmagent.New(c.AgentPlan.Root, append([]llmagent.Option{llmagent.WithModel(consumer), llmagent.WithEnableCodeExecutionResponseProcessor(false), llmagent.WithCodeExecutor(nil)}, options.Agent...)...)
	r := runner.NewRunner(o.key.AppName, a, append(options.Runner, runner.WithSessionService(o))...)
	defer r.Close()
	events, err := r.Run(ctx, o.key.UserID, o.key.SessionID, model.NewUserMessage("What drink do I prefer?"), agent.WithDetachedCancel(false))
	if err != nil {
		t.Fatal(err)
	}
	for e := range events {
		if e.Response != nil && e.Error != nil {
			t.Fatal(e.Error)
		}
	}
	if !consumer.seen.Load() {
		t.Fatal("real SDK did not consume accepted summary")
	}
}
