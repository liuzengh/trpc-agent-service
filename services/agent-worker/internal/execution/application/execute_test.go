package application_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type fixedManifest struct{ plan domain.Plan }

func (m fixedManifest) Resolve(context.Context, domain.Route) (domain.Plan, error) {
	return m.plan, nil
}

type fakeRuntimeFactory struct {
	calls      atomic.Int32
	prepareErr error
	attempt    *fakeRuntime
}

func (f *fakeRuntimeFactory) Prepare(_ context.Context, g domain.Grant, _ domain.Plan, _ func(context.Context) error) (application.AttemptRuntime, error) {
	f.calls.Add(1)
	if f.prepareErr != nil {
		return nil, f.prepareErr
	}
	f.attempt = &fakeRuntime{grant: g}
	return f.attempt, nil
}

type fakeRuntime struct {
	grant      domain.Grant
	executions int
	closed     bool
}

func (*fakeRuntime) Load(context.Context, domain.Head) ([]byte, error) { return nil, nil }
func (r *fakeRuntime) Execute(context.Context, []byte) (domain.RuntimeResult, error) {
	r.executions++
	return domain.RuntimeResult{FinalText: "verified final", Snapshot: []byte(`{"snapshot":"complete"}`)}, nil
}
func (r *fakeRuntime) Stage(context.Context, []byte) (domain.Candidate, error) {
	return domain.Candidate{Ref: domain.StableID("sc1", r.grant.AttemptID), Digest: domain.Digest([]byte("candidate")), Parent: r.grant.Parent}, nil
}
func (r *fakeRuntime) Close() { r.closed = true }

type commitLossLedger struct {
	application.Ledger
	completed     atomic.Bool
	proofCalls    atomic.Int32
	proofCanceled atomic.Bool
	tamperProof   bool
}

func (l *commitLossLedger) Complete(ctx context.Context, f domain.Finish) (domain.Completion, error) {
	c, err := l.Ledger.Complete(ctx, f)
	if err != nil {
		return c, err
	}
	if l.completed.CompareAndSwap(false, true) {
		// Wait for the renewal loop to see the committed terminal status and cancel
		// the Attempt. The commit response is then lost in exactly that window.
		<-ctx.Done()
		return domain.Completion{}, errors.New("injected lost commit response")
	}
	return c, nil
}
func (l *commitLossLedger) FindCompletion(ctx context.Context, tenant, run string) (domain.Completion, error) {
	l.proofCalls.Add(1)
	if ctx.Err() != nil {
		l.proofCanceled.Store(true)
	}
	c, err := l.Ledger.FindCompletion(ctx, tenant, run)
	if l.tamperProof {
		c.ResultDigest = domain.Digest([]byte("changed final body"))
	}
	return c, err
}

func TestProcessorPostgresCommitResponseLossAfterRenewalCancellation(t *testing.T) {
	for _, tamper := range []bool{false, true} {
		t.Run(fmt.Sprintf("tampered_proof_%t", tamper), func(t *testing.T) { runCommitLossTest(t, tamper) })
	}
}
func runCommitLossTest(t *testing.T, tamper bool) {
	url := os.Getenv("WORKER_PROCESSOR_TEST_RUNTIME_URL")
	if url == "" {
		t.Skip("WORKER_PROCESSOR_TEST_RUNTIME_URL requires prepared disposable Worker schema")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ledger := postgresadapter.New(pool)
	id := fmt.Sprintf("proof_loss_%d", time.Now().UnixNano())
	requested := domain.Requested{EventID: "evt_" + id, EventDigest: domain.Digest([]byte(id)), RunDigest: domain.Digest([]byte("run" + id)), RunID: "run_" + id, AdmissionID: "adm_" + id, Route: domain.Route{TenantID: "tenant_" + id, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "conversation", SenderID: "sender_" + id, Text: "Hello", ReceivedAt: time.Now().UTC()}}
	policy := domain.Policy{Version: "processor-test-v1", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: time.Second, RenewalInterval: 20 * time.Millisecond, RetryBackoff: time.Millisecond, MaxAttempts: 2}
	if _, err = ledger.Accept(ctx, requested, policy, domain.IntakeLimits{MaxQueuedRuns: 1000, MaxRetainedRuns: 100000}); err != nil {
		t.Fatal(err)
	}
	run, err := ledger.FindRun(ctx, requested.Route.TenantID, requested.RunID)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &commitLossLedger{Ledger: ledger, tamperProof: tamper}
	runtime := &fakeRuntimeFactory{}
	p := domain.Plan{TenantID: requested.Route.TenantID, ManifestID: requested.Route.ManifestRef, ManifestDigest: requested.Route.ManifestDigest, DeploymentRevisionID: requested.Route.DeploymentRevisionID, MaxRunSeconds: 5}
	processor, err := application.NewProcessor(wrapped, fixedManifest{p}, runtime, "worker-test", 10)
	if err != nil {
		t.Fatal(err)
	}
	err = processor.Advance(ctx, run)
	if (!tamper && err != nil) || (tamper && !errors.Is(err, domain.ErrFenced)) {
		t.Fatalf("committed result proof reconciliation: tamper=%t err=%v", tamper, err)
	}
	if wrapped.proofCalls.Load() == 0 || wrapped.proofCanceled.Load() {
		t.Fatalf("proof calls=%d canceled=%t", wrapped.proofCalls.Load(), wrapped.proofCanceled.Load())
	}
	if runtime.calls.Load() != 1 || runtime.attempt.executions != 1 || !runtime.attempt.closed {
		t.Fatalf("runtime reexecuted or not closed: calls=%d attempt=%+v", runtime.calls.Load(), runtime.attempt)
	}
	completion, err := ledger.FindCompletion(ctx, requested.Route.TenantID, requested.RunID)
	if err != nil || completion.Status != domain.Succeeded {
		t.Fatalf("completion=%+v err=%v", completion, err)
	}
	var outbox, commits int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM execution_reply_outbox WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM execution_session_commits WHERE tenant_id=$1 AND run_id=$2)`, requested.Route.TenantID, requested.RunID).Scan(&outbox, &commits); err != nil || outbox != 1 || commits != 1 {
		t.Fatalf("outbox=%d commits=%d err=%v", outbox, commits, err)
	}
	if tamper {
		t.Log("PROCESSOR TAMPERED PROOF PASS: mismatched full result digest rejected; no second model execution")
	} else {
		t.Log("PROCESSOR COMMIT LOSS PASS: committed proof reconciled after renewal canceled Attempt; one model execution, one accepted head, one outbox; runtime closed")
	}
}

func TestProcessorPostgresPreparationErrorIsExplicit(t *testing.T) {
	url := os.Getenv("WORKER_PROCESSOR_TEST_RUNTIME_URL")
	if url == "" {
		t.Skip("WORKER_PROCESSOR_TEST_RUNTIME_URL requires prepared disposable Worker schema")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ledger := postgresadapter.New(pool)
	id := fmt.Sprintf("preparation_%d", time.Now().UnixNano())
	digest := domain.Digest([]byte("manifest"))
	requested := domain.Requested{EventID: "evt_" + id, EventDigest: domain.Digest([]byte(id)), RunDigest: domain.Digest([]byte("run" + id)), RunID: "run_" + id, AdmissionID: "adm_" + id, Route: domain.Route{TenantID: "tenant_" + id, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: digest, Generation: 1}, Input: domain.Input{ConversationID: "conversation", SenderID: "sender_" + id, Text: "Hello", ReceivedAt: time.Now().UTC()}}
	policy := domain.Policy{Version: "processor-test-v1", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: time.Second, RenewalInterval: 20 * time.Millisecond, RetryBackoff: time.Millisecond, MaxAttempts: 2}
	if _, err = ledger.Accept(ctx, requested, policy, domain.IntakeLimits{MaxQueuedRuns: 1000, MaxRetainedRuns: 100000}); err != nil {
		t.Fatal(err)
	}
	run, err := ledger.FindRun(ctx, requested.Route.TenantID, requested.RunID)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntimeFactory{prepareErr: application.ErrSessionPreparation}
	plan := domain.Plan{TenantID: requested.Route.TenantID, ManifestID: "manifest", ManifestDigest: digest, DeploymentRevisionID: "revision", MaxRunSeconds: 5}
	processor, err := application.NewProcessor(ledger, fixedManifest{plan}, runtime, "worker-test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Advance(ctx, run); !errors.Is(err, application.ErrSessionPreparation) {
		t.Fatal(err)
	}
	var status, reason string
	var outbox int
	err = pool.QueryRow(ctx, `SELECT r.status,a.reason,(SELECT count(*) FROM execution_reply_outbox WHERE tenant_id=r.tenant_id AND run_id=r.run_id) FROM execution_runs r JOIN execution_attempts a ON a.tenant_id=r.tenant_id AND a.attempt_id=r.current_attempt_id WHERE r.tenant_id=$1 AND r.run_id=$2`, requested.Route.TenantID, requested.RunID).Scan(&status, &reason, &outbox)
	if err != nil || status != "RETRY_WAIT" || reason != "SESSION_PREPARATION_REQUIRED" || outbox != 0 || runtime.attempt != nil {
		t.Fatalf("status=%s reason=%s outbox=%d err=%v", status, reason, outbox, err)
	}
	t.Log("PROCESSOR PREPARATION PASS: explicit SESSION_PREPARATION_REQUIRED persisted; retry waiting; no model execution or Final")
}
