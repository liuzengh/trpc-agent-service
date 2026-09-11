package postgresadapter

import (
	"context"
	"fmt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestDurableRunTraceScheduledRestore(t *testing.T) {
	dsn := os.Getenv("TRACING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRACING_TEST_DATABASE_URL requires isolated PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("trace_worker_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	ledger := New(pool)
	req := domain.Requested{EventID: "evt_trace", RunID: "run_trace", AdmissionID: "adm_trace", EventDigest: domain.Digest([]byte("event")), RunDigest: domain.Digest([]byte("run")), Route: domain.Route{TenantID: "tenant", Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "43", Text: "hello", ReceivedAt: time.Now().UTC()}}
	policy := domain.Policy{Version: "test-v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 3 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	limits := domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 1000}
	original := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00", Tracestate: "vendor=value"}
	oldCtx, stop := context.WithCancel(original.Restore(ctx))
	if _, err = ledger.Accept(oldCtx, req, policy, limits); err != nil {
		t.Fatal(err)
	}
	stop()
	pool.Close()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunTraceRecoveryProcess$", "-test.v")
	child.Env = append(os.Environ(), "TRACING_RECOVERY_SCHEMA="+schema)
	output, childErr := child.CombinedOutput()
	if childErr != nil || !strings.Contains(string(output), "RECOVERY_PROCESS=PASS") {
		t.Fatalf("recovery child: %v %s", childErr, output)
	}
	t.Log(strings.TrimSpace(string(output)))
	// Close every runtime connection and rebuild the adapter; no in-memory handoff.
	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ledger = New(pool)
	rows, err := ledger.Scheduled(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatal(err, len(rows))
	}
	if rows[0].Carrier != original || rows[0].Request.RunDigest != req.RunDigest {
		t.Fatal("durable metadata/digest mismatch")
	}
	current, finish := context.WithCancel(ctx)
	restored := rows[0].Carrier.Restore(current)
	if restored.Err() != nil {
		t.Fatal("old canceled context revived")
	}
	finish()
	if restored.Err() != context.Canceled {
		t.Fatal("new task cancellation lost")
	}
	other := tracecontext.Carrier{Traceparent: "00-1123456789abcdef0123456789abcdef-1123456789abcdef-01"}
	if _, err = ledger.Accept(other.Restore(ctx), req, policy, limits); err != nil {
		t.Fatal(err)
	}
	rows, err = ledger.Scheduled(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Carrier != original {
		t.Fatal("duplicate replaced first context", err)
	}
	// An initially untraced accepted Run is not backfilled by replay.
	if _, err = pool.Exec(ctx, `UPDATE execution_runs SET traceparent=NULL,tracestate=NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.Accept(other.Restore(ctx), req, policy, limits); err != nil {
		t.Fatal(err)
	}
	rows, err = ledger.Scheduled(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Carrier != (tracecontext.Carrier{}) {
		t.Fatal("NULL context backfilled", err)
	}
	t.Log("RUN_TRACE=PASS actual_pg=true connections_reopened=true unsampled_preserved=true replay_immutable=true lifecycle_restored=true")
}

// This child uses a fresh OS process and runtime pool. It is a recovery adapter
// gate, not a claim that the complete deployed Worker/Telegram loop restarted.
func TestRunTraceRecoveryProcess(t *testing.T) {
	schema := os.Getenv("TRACING_RECOVERY_SCHEMA")
	if schema == "" {
		t.Skip("spawned only by durable trace integration gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TRACING_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := New(pool).Scheduled(ctx, 1)
	if err != nil || len(rows) != 1 {
		t.Fatal(err, len(rows))
	}
	carrier := rows[0].Carrier
	if carrier.Traceparent != "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00" {
		t.Fatal("fresh process lost immutable carrier")
	}
	parent := trace.SpanContextFromContext(carrier.Restore(ctx))
	p := sdktrace.NewTracerProvider()
	defer p.Shutdown(context.Background())
	_, span := p.Tracer("worker").Start(carrier.Restore(ctx), "worker.run.attempt")
	defer span.End()
	if span.SpanContext().TraceID() != parent.TraceID() || span.SpanContext().SpanID() == parent.SpanID() || span.SpanContext().IsSampled() {
		t.Fatal("fresh process violated parent/sampling")
	}
	t.Logf("RECOVERY_PROCESS=PASS pid=%d durable_parent=true unsampled=true", os.Getpid())
}
