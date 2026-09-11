package trpcagent

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Real PostgreSQL stores the complete Session candidate, summaries included.
// Explicit heads model caller selection; this test does not execute the Worker
// accepted-head transaction or production factory, nor an external LLM.
func TestSummaryUsesSamePostgresSessionCandidate(t *testing.T) {
	migrationURL, runtimeURL := os.Getenv("WORKER_SUMMARY_TEST_MIGRATION_URL"), os.Getenv("WORKER_SUMMARY_TEST_RUNTIME_URL")
	if migrationURL == "" || runtimeURL == "" {
		t.Skip("isolated summary PostgreSQL URLs are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	migration, err := pgxpool.New(ctx, migrationURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migration.Close()
	if err = sessionmigrations.Apply(ctx, migration); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(runtimeURL)
	target := sessionstore.Target{Host: config.ConnConfig.Host, Port: config.ConnConfig.Port, Database: config.ConnConfig.Database, Username: config.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}
	store, err := sessionstore.Open(ctx, runtimeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	base, err := newOverlay("tenant-summary", "session-summary", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := base.GetSession(ctx, base.key)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"I prefer green tea", "Please remember", "Thank you"} {
		e := summaryFixtureEvent(string(rune('a'+i)), text)
		// SDK Runner defaults the root event filter to effective AppName.
		e.FilterKey = base.key.AppName
		e.Version = event.CurrentVersion
		if err = base.AppendEvent(ctx, sess, e); err != nil {
			t.Fatal(err)
		}
	}
	baseBytes, err := base.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	identity := sessionstore.Identity{TenantID: "tenant-summary", SessionID: "session-summary", RunID: "run-base", AttemptID: "attempt-base"}
	previous := sessionstore.Candidate{Identity: identity, ContentVersion: sessionstore.ContentVersion, Snapshot: baseBytes}
	parent, err := store.Put(ctx, previous)
	if err != nil {
		t.Fatal(err)
	}
	c := summaryManifestFixture()
	m := &summaryFixtureModel{}
	summarizer, err := BuildManifestSummarizer(c, map[string]model.Model{"summarizer": m})
	if err != nil {
		t.Fatal(err)
	}
	current, err := newSummaryOverlay(identity.TenantID, identity.SessionID, baseBytes, 1<<20, summarizer)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	sess, err = current.GetSession(ctx, current.key)
	if err != nil {
		t.Fatal(err)
	}
	if err = current.EnqueueSummaryJob(ctx, sess, current.key.AppName, false); err != nil {
		t.Fatal(err)
	}
	text, ok := current.GetSessionSummaryText(ctx, sess, session.WithSummaryFilterKey(base.key.AppName))
	if !ok || text != "User prefers green tea." || m.calls.Load() != 1 {
		t.Fatalf("summary %q present=%v calls=%d", text, ok, m.calls.Load())
	}
	payload, err := current.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	identity.RunID = "run-summary"
	identity.AttemptID = "attempt-summary"
	candidate := sessionstore.Candidate{Identity: identity, Parent: parent, ContentVersion: sessionstore.ContentVersion, Snapshot: payload}
	head, err := store.Put(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := store.Put(ctx, candidate); err != nil || replay != head {
		t.Fatal("candidate replay mismatch", err)
	}
	// Writing a candidate never changes the previously selected immutable head.
	unchanged, err := store.Load(ctx, identity.TenantID, identity.SessionID, parent)
	if err != nil || !bytes.Equal(unchanged.Snapshot, baseBytes) {
		t.Fatal("parent was changed", err)
	}
	store.Close()
	store, err = sessionstore.Open(ctx, runtimeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, identity.TenantID, identity.SessionID, head)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := newSummaryOverlay(identity.TenantID, identity.SessionID, loaded.Snapshot, 1<<20, summarizer)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view, err := reopened.GetSession(ctx, reopened.key)
	if err != nil {
		t.Fatal(err)
	}
	if text, ok = reopened.GetSessionSummaryText(ctx, view, session.WithSummaryFilterKey(base.key.AppName)); !ok || text != "User prefers green tea." {
		t.Fatal("summary lost on pool reopen")
	}
	// The next SDK invocation consumes the summary recovered from PostgreSQL.
	consume := &summaryConsumerModel{}
	enabled := true
	node := c.AgentPlan.Nodes[c.AgentPlan.Root]
	node.AddSessionSummary = &enabled
	c.AgentPlan.Nodes[c.AgentPlan.Root] = node
	options, err := BuildManifestCapabilityOptions(c, c.AgentPlan.Root, CapabilityServices{})
	if err != nil {
		t.Fatal(err)
	}
	a := llmagent.New(c.AgentPlan.Root, append([]llmagent.Option{llmagent.WithModel(consume), llmagent.WithEnableCodeExecutionResponseProcessor(false), llmagent.WithCodeExecutor(nil)}, options.Agent...)...)
	r := runner.NewRunner(reopened.key.AppName, a, append(options.Runner, runner.WithSessionService(reopened))...)
	events, err := r.Run(ctx, reopened.key.UserID, reopened.key.SessionID, model.NewUserMessage("What drink do I prefer?"), agent.WithDetachedCancel(false))
	if err != nil {
		t.Fatal(err)
	}
	for e := range events {
		if e.Response != nil && e.Error != nil {
			t.Fatal(e.Error)
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if !consume.seen.Load() {
		t.Fatal("restored summary not used by SDK model")
	}
	if _, err = store.Load(ctx, "different-tenant", identity.SessionID, head); !errors.Is(err, sessionstore.ErrNotFound) {
		t.Fatal("tenant isolation", err)
	}
	if _, err = store.Load(ctx, identity.TenantID, "different-session", head); !errors.Is(err, sessionstore.ErrNotFound) {
		t.Fatal("session isolation", err)
	}
	failedModel := &summaryFixtureModel{fail: true}
	failedSummarizer, err := BuildManifestSummarizer(c, map[string]model.Model{"summarizer": failedModel})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := newSummaryOverlay(identity.TenantID, identity.SessionID, baseBytes, 1<<20, failedSummarizer)
	if err != nil {
		t.Fatal(err)
	}
	defer failed.Close()
	failedView, err := failed.GetSession(ctx, failed.key)
	if err != nil {
		t.Fatal(err)
	}
	if err = failed.CreateSessionSummary(ctx, failedView, failed.key.AppName, true); err == nil {
		t.Fatal("model failure ignored")
	}
	if _, err = failed.Snapshot(); err == nil {
		t.Fatal("failed summary yielded candidate snapshot")
	}
	var count int
	if err = migration.QueryRow(ctx, "SELECT count(*) FROM runtime_session.session_candidates").Scan(&count); err != nil || count != 2 {
		t.Fatalf("rows=%d err=%v", count, err)
	}
	t.Log("SUMMARY_SESSION_PG=PASS sdk_summarizer=true same_candidate=true pool_reopen=true sdk_next_run_consumes=true parent_unchanged=true failure_no_candidate=true tenant_session_isolation=true accepted_transaction=NOT_RUN")
}

type summaryConsumerModel struct{ seen atomic.Bool }

func (*summaryConsumerModel) Info() model.Info { return model.Info{Name: "summary-consumer"} }
func (m *summaryConsumerModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, message := range req.Messages {
		if strings.Contains(message.Content, "User prefers green tea.") {
			m.seen.Store(true)
		}
	}
	if !m.seen.Load() {
		return nil, errors.New("summary absent from model context")
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("Green tea.")}}}
	close(ch)
	return ch, nil
}
