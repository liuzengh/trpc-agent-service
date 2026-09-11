package postgresadapter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func traceLedger(t *testing.T) (*Ledger, *pgxpool.Pool, context.Context) {
	t.Helper()
	url := os.Getenv("TRACING_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRACING_TEST_DATABASE_URL requires isolated PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("trace_commit_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return New(pool), pool, ctx
}
func traceRequest(id string) domain.Requested {
	return domain.Requested{EventID: "evt_" + id, RunID: "run_" + id, AdmissionID: "adm_" + id, EventDigest: domain.Digest([]byte("event" + id)), RunDigest: domain.Digest([]byte("run" + id)), Route: domain.Route{TenantID: "tenant", Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: id, SenderID: "43", Text: "body canary", ReceivedAt: time.Now().UTC()}}
}
func tracePolicy() domain.Policy {
	return domain.Policy{Version: "test-v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 3 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
}
func attrValue(s tracetest.SpanStub, key string) string {
	for _, a := range s.Attributes {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}
func TestSessionCommitTraceMatchesActualTransaction(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			ledger, pool, ctx := traceLedger(t)
			ex := tracetest.NewInMemoryExporter()
			p := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer p.Shutdown(context.Background())
			ledger.Tracer = p.Tracer("worker")
			ctx, parent := ledger.Tracer.Start(ctx, "worker.run.attempt")
			defer parent.End()
			req := traceRequest("formal")
			if _, err := ledger.Accept(ctx, req, tracePolicy(), domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
				t.Fatal(err)
			}
			g, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: "tenant", RunID: req.RunID, WorkerID: "worker", MaxRunSeconds: 60, MaxActive: 1})
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				if _, err = pool.Exec(ctx, `CREATE FUNCTION trace_reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture body canary'; END $$; CREATE CONSTRAINT TRIGGER reject_session_commit AFTER INSERT ON execution_session_commits DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION trace_reject_commit()`); err != nil {
					t.Fatal(err)
				}
			}
			finish := domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate", Digest: domain.Digest([]byte("snapshot")), Parent: g.Parent}, FinalText: "reply canary"}
			_, err = ledger.Complete(ctx, finish)
			if (err != nil) != failed {
				t.Fatal(err)
			}
			spans := ex.GetSpans()
			var commit, complete *tracetest.SpanStub
			for i := range spans {
				switch spans[i].Name {
				case "worker.session.commit":
					commit = &spans[i]
				case "worker.run.complete":
					complete = &spans[i]
				}
			}
			if commit == nil || complete == nil || commit.Parent.SpanID() != complete.SpanContext.SpanID() || commit.SpanContext.TraceID() != parent.SpanContext().TraceID() {
				t.Fatal("commit span relationship missing")
			}
			var accepted string
			var completions int
			if e := pool.QueryRow(ctx, `SELECT accepted_ref FROM execution_sessions WHERE tenant_id='tenant'`).Scan(&accepted); e != nil {
				t.Fatal(e)
			}
			if e := pool.QueryRow(ctx, `SELECT count(*) FROM execution_completions`).Scan(&completions); e != nil {
				t.Fatal(e)
			}
			if failed {
				if accepted != "" || completions != 0 || commit.Status.Code != codes.Error || attrValue(*commit, "app.outcome") != "failed" {
					t.Fatal("failed commit became accepted", accepted, completions, commit.Status)
				}
			} else {
				if accepted != "candidate" || completions != 1 || attrValue(*commit, "app.outcome") != "accepted" {
					t.Fatal("accepted mismatch")
				}
				before := len(spans)
				if _, e := ledger.Complete(ctx, finish); e != nil {
					t.Fatal(e)
				}
				for _, s := range ex.GetSpans()[before:] {
					if s.Name == "worker.session.commit" {
						t.Fatal("replay fabricated second head commit")
					}
				}
				if _, e := ledger.FindCompletion(ctx, "tenant", req.RunID); e != nil {
					t.Fatal(e)
				}
			}
			t.Logf("SESSION_COMMIT_TRACE=PASS actual_pg=true rollback=%t accepted_head_matches=true replay_no_duplicate=true", failed)
		})
	}
}
func TestTerminalTraceRestoresRunAndSkipsNoop(t *testing.T) {
	ledger, pool, ctx := traceLedger(t)
	ex := tracetest.NewInMemoryExporter()
	p := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer p.Shutdown(context.Background())
	ledger.Tracer = p.Tracer("worker")
	parentCtx, parent := ledger.Tracer.Start(ctx, "worker.run.attempt")
	carrier := tracecontext.Capture(parentCtx)
	parent.End()
	req := traceRequest("terminal")
	if _, err := ledger.Accept(parentCtx, req, tracePolicy(), domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
		t.Fatal(err)
	}
	before := len(ex.GetSpans())
	if changed, err := ledger.Terminalize(ctx, "tenant", req.RunID, ""); err != nil || changed {
		t.Fatal(err)
	}
	if len(ex.GetSpans()) != before {
		t.Fatal("no-op terminal poll emitted span")
	}
	if _, err := pool.Exec(ctx, `UPDATE execution_runs SET run_deadline=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if changed, err := ledger.Terminalize(ctx, "tenant", req.RunID, ""); err != nil || !changed {
		t.Fatal(err)
	}
	spans := ex.GetSpans()
	s := spans[len(spans)-1]
	if s.Name != "worker.run.terminalize" || s.Parent.SpanID() != parent.SpanContext().SpanID() || s.SpanContext.TraceID() != parent.SpanContext().TraceID() || attrValue(s, string(attribute.Key("app.run.status"))) != "FAILED" {
		t.Fatal("terminal context/state lost", carrier, s.Name)
	}
	t.Log("TERMINAL_TRACE=PASS actual_pg=true durable_parent=true no_empty_poll_span=true")
}
