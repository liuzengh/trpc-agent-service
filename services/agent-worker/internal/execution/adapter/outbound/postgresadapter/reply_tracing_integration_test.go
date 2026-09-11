package postgresadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

func TestReplyCreationTransactionAndReplay(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "rollback"}[failed], func(t *testing.T) {
			l, pool, ctx := traceLedger(t)
			ex := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer tp.Shutdown(context.Background())
			l.Tracer = tp.Tracer("worker")
			ctx, root := l.Tracer.Start(ctx, "worker.run.attempt")
			defer root.End()
			req := traceRequest("reply")
			if _, err := l.Accept(ctx, req, tracePolicy(), domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
				t.Fatal(err)
			}
			g, err := l.Claim(ctx, domain.ClaimRequest{TenantID: "tenant", RunID: req.RunID, WorkerID: "worker", MaxRunSeconds: 60, MaxActive: 1})
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				if _, err = pool.Exec(ctx, `CREATE FUNCTION reject_reply() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE CONSTRAINT TRIGGER reject_reply AFTER INSERT ON execution_reply_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_reply()`); err != nil {
					t.Fatal(err)
				}
			}
			f := domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate", Digest: domain.Digest([]byte("snapshot")), Parent: g.Parent}, FinalText: "reply"}
			_, err = l.Complete(ctx, f)
			if (err != nil) != failed {
				t.Fatal(err)
			}
			var creation, complete, session tracetest.SpanStub
			for _, s := range ex.GetSpans() {
				switch s.Name {
				case "create execution.reply-intent.v1":
					creation = s
				case "worker.run.complete":
					complete = s
				case "worker.session.commit":
					session = s
				}
			}
			if !creation.SpanContext.IsValid() || creation.Parent.SpanID() != complete.SpanContext.SpanID() || creation.Parent.SpanID() != session.Parent.SpanID() || creation.SpanKind != trace.SpanKindProducer {
				t.Fatal("creation must be sibling of session commit")
			}
			rows, err := New(pool).PendingTracedReplies(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				if len(rows) != 0 || attrValue(creation, "app.outcome") != "failed" {
					t.Fatal("rolled back reply appeared")
				}
				return
			}
			if len(rows) != 1 || trace.SpanContextFromContext(rows[0].Carrier.Restore(ctx)).SpanID() != creation.SpanContext.SpanID() {
				t.Fatal("durable creation context missing")
			}
			original := rows[0]
			other, cancel := context.WithCancel(context.Background())
			other = original.Carrier.Restore(other)
			cancel()
			if other.Err() != context.Canceled {
				t.Fatal("lifecycle lost")
			}
			_, err = l.Complete(context.Background(), f)
			if err != nil {
				t.Fatal(err)
			}
			rows, err = l.PendingTracedReplies(ctx, 10)
			if err != nil || len(rows) != 1 || rows[0].Carrier != original.Carrier || string(rows[0].Payload) != string(original.Payload) {
				t.Fatal("replay changed immutable outbox")
			}
			creations := 0
			for _, s := range ex.GetSpans() {
				if s.Name == creation.Name {
					creations++
				}
			}
			if creations != 1 {
				t.Fatal("duplicate creation")
			}
			if _, err = pool.Exec(ctx, `UPDATE execution_reply_outbox SET traceparent=NULL,tracestate=NULL`); err != nil {
				t.Fatal(err)
			}
			rows, err = l.PendingTracedReplies(ctx, 10)
			if err != nil || rows[0].Carrier != (tracecontext.Carrier{}) {
				t.Fatal("legacy NULL row")
			}
			t.Log("REPLY_CREATION=PASS transaction=true replay_immutable=true sibling=true legacy_null=true")
		})
	}
}

func TestFailedAttemptFinalHasDurableCreation(t *testing.T) {
	l, pool, ctx := traceLedger(t)
	ex := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	l.Tracer = tp.Tracer("worker")
	ctx, parent := l.Tracer.Start(ctx, "worker.run.attempt")
	defer parent.End()
	req := traceRequest("failed-reply")
	if _, err := l.Accept(ctx, req, tracePolicy(), domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}); err != nil {
		t.Fatal(err)
	}
	g, err := l.Claim(ctx, domain.ClaimRequest{TenantID: "tenant", RunID: req.RunID, WorkerID: "worker", MaxRunSeconds: 60, MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = l.FailAttempt(ctx, g, "MODEL_FAILED", false); err != nil {
		t.Fatal(err)
	}
	rows, err := New(pool).PendingTracedReplies(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Carrier.Traceparent == "" {
		t.Fatal("failure Final missing durable carrier", err)
	}
	c, err := l.FindCompletion(ctx, "tenant", req.RunID)
	if err != nil || c.Status != domain.Failed || c.ReplyDisposition != "FINAL" {
		t.Fatal(c, err)
	}
	creates := 0
	for _, s := range ex.GetSpans() {
		if s.Name == "worker.session.commit" {
			t.Fatal("failure committed Session")
		}
		if s.Name == "create execution.reply-intent.v1" {
			creates++
			if s.SpanContext.TraceID() != parent.SpanContext().TraceID() {
				t.Fatal("wrong failure parent")
			}
		}
	}
	if creates != 1 {
		t.Fatal(creates)
	}
}
