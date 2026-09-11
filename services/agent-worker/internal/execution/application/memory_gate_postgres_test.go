package application_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

type memoryProcessLedger struct {
	application.Ledger
	mode                                   string
	completes, renews, finalizes, failures atomic.Int32
}

func (l *memoryProcessLedger) Complete(ctx context.Context, f domain.Finish) (domain.Completion, error) {
	call := l.completes.Add(1)
	if f.MemoryDigest != "" {
		if l.mode == "rejected" {
			return domain.Completion{}, domain.ErrFenced
		}
		if l.mode == "retry_success" && call == 1 {
			return domain.Completion{}, errors.New("fixture response before commit")
		}
	}
	c, e := l.Ledger.Complete(ctx, f)
	if e == nil && f.MemoryDigest != "" && call == 1 && (l.mode == "response_loss" || l.mode == "wrong_proof") {
		// Actual committed Attempt becomes terminal; actual renewal then cancels its
		// old context. The Processor must not use that context for Memory writes.
		<-ctx.Done()
		return domain.Completion{}, errors.New("fixture lost accepted response")
	}
	return c, e
}
func (l *memoryProcessLedger) FindCompletion(ctx context.Context, tenant, run string) (domain.Completion, error) {
	c, e := l.Ledger.FindCompletion(ctx, tenant, run)
	if l.mode == "wrong_proof" {
		c.ResultDigest = domain.Digest([]byte("wrong"))
	}
	return c, e
}
func (l *memoryProcessLedger) Renew(ctx context.Context, g domain.Grant) (domain.Grant, error) {
	l.renews.Add(1)
	return l.Ledger.Renew(ctx, g)
}
func (l *memoryProcessLedger) FailAttempt(ctx context.Context, g domain.Grant, reason string, retry bool) error {
	l.failures.Add(1)
	return l.Ledger.FailAttempt(ctx, g, reason, retry)
}
func (l *memoryProcessLedger) FinalizeMemory(ctx context.Context, c domain.Completion, success bool) error {
	call := l.finalizes.Add(1)
	e := l.Ledger.FinalizeMemory(ctx, c, success)
	if e == nil && l.mode == "finalize_response_loss" && call == 1 {
		return errors.New("fixture lost finalization response")
	}
	return e
}

type memoryProcessFactory struct {
	mode              string
	entered, release  chan struct{}
	applies, prepares atomic.Int32
	persisted         atomic.Bool
	mu                sync.Mutex
	loaded            []bool
	firstContext      context.Context
}

func (f *memoryProcessFactory) Prepare(ctx context.Context, g domain.Grant, _ domain.Plan, _ func(context.Context) error) (application.AttemptRuntime, error) {
	first := f.prepares.Add(1) == 1
	if first {
		f.firstContext = ctx
	}
	return &memoryProcessAttempt{factory: f, grant: g, first: first}, nil
}

type memoryProcessAttempt struct {
	factory *memoryProcessFactory
	grant   domain.Grant
	first   bool
}

func (a *memoryProcessAttempt) Load(context.Context, domain.Head) ([]byte, error) {
	a.factory.mu.Lock()
	defer a.factory.mu.Unlock()
	a.factory.loaded = append(a.factory.loaded, a.factory.persisted.Load())
	return nil, nil
}
func (a *memoryProcessAttempt) Execute(context.Context, []byte) (domain.RuntimeResult, error) {
	if a.first && a.factory.mode == "execute_failure" {
		return domain.RuntimeResult{}, application.ErrRuntimeFailed
	}
	r := domain.RuntimeResult{FinalText: "PRIVATE_MEMORY_SUCCESS", Snapshot: []byte(`{"fixture":"session"}`)}
	if a.first {
		r.MemoryDigest = domain.Digest([]byte("sealed-memory"))
		r.MemoryTimeout = 3 * time.Second
	}
	return r, nil
}
func (a *memoryProcessAttempt) Stage(context.Context, []byte) (domain.Candidate, error) {
	return domain.Candidate{Ref: "candidate_" + a.grant.AttemptID, Digest: domain.Digest([]byte(a.grant.AttemptID)), Parent: a.grant.Parent}, nil
}
func (a *memoryProcessAttempt) ApplyAccepted(ctx context.Context, c domain.Completion) error {
	if !a.first || c.AttemptID != a.grant.AttemptID || c.Status != domain.Succeeded || c.MemoryStatus != "PENDING" || ctx.Err() != nil {
		return errors.New("bad accepted Memory context")
	}
	if a.factory.applies.Add(1) != 1 {
		return errors.New("duplicate Memory Apply")
	}
	close(a.factory.entered)
	select {
	case <-a.factory.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if a.factory.mode == "apply_failure" {
		return application.ErrDependency
	}
	a.factory.persisted.Store(true)
	return nil
}
func (*memoryProcessAttempt) Close() {}

func TestProcessorMemoryPostgres(t *testing.T) {
	migrateURL, runtimeURL := os.Getenv("WORKER_TEST_MIGRATION_URL"), os.Getenv("WORKER_TEST_RUNTIME_URL")
	if migrateURL == "" || runtimeURL == "" {
		t.Skip("requires disposable Worker PostgreSQL roles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	migration, e := pgxpool.New(ctx, migrateURL)
	if e != nil {
		t.Fatal(e)
	}
	defer migration.Close()
	if e = migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); e != nil {
		t.Fatal(e)
	}
	pool, e := pgxpool.New(ctx, runtimeURL)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	actual := postgresadapter.New(pool)
	for _, mode := range []string{"success", "response_loss", "retry_success", "finalize_response_loss", "apply_failure", "rejected", "wrong_proof", "execute_failure"} {
		t.Run(mode, func(t *testing.T) {
			id := fmt.Sprintf("memory_processor_%s_%d", mode, time.Now().UnixNano())
			r := domain.Requested{EventID: "event_" + id, RunID: "run_" + id, AdmissionID: "admission_" + id, EventDigest: domain.Digest([]byte(id)), RunDigest: domain.Digest([]byte("run" + id)), Route: domain.Route{TenantID: "tenant_" + id, Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "42", SenderID: "100", Text: "remember", ReceivedAt: time.Now().UTC()}}
			policy := domain.Policy{Version: "memory-test", MaxRunAge: time.Minute, MaxReplyAge: time.Minute, MaxFutureSkew: time.Second, LeaseTTL: time.Second, RenewalInterval: 5 * time.Millisecond, RetryBackoff: time.Millisecond, MaxAttempts: 2}
			accept := func(request domain.Requested) domain.Run {
				t.Helper()
				if _, e := actual.Accept(ctx, request, policy, domain.IntakeLimits{MaxQueuedRuns: 10000, MaxRetainedRuns: 100000}); e != nil {
					t.Fatal(e)
				}
				v, e := actual.FindRun(ctx, request.Route.TenantID, request.RunID)
				if e != nil {
					t.Fatal(e)
				}
				return v
			}
			run := accept(r)
			wrapped := &memoryProcessLedger{Ledger: actual, mode: mode}
			factory := &memoryProcessFactory{mode: mode, entered: make(chan struct{}), release: make(chan struct{})}
			p := domain.Plan{TenantID: r.Route.TenantID, ManifestID: r.Route.ManifestRef, ManifestDigest: r.Route.ManifestDigest, DeploymentRevisionID: r.Route.DeploymentRevisionID, MaxRunSeconds: 30}
			processor, e := application.NewProcessor(wrapped, fixedManifest{p}, factory, "memory-worker", 100)
			if e != nil {
				t.Fatal(e)
			}
			noApply := mode == "rejected" || mode == "wrong_proof" || mode == "execute_failure"
			if noApply {
				e = processor.Advance(ctx, run)
				if e == nil {
					t.Fatal("rejected execution returned success")
				}
				if factory.applies.Load() != 0 || wrapped.finalizes.Load() != 0 || factory.persisted.Load() {
					t.Fatal("unaccepted candidate applied")
				}
				if mode == "wrong_proof" {
					c, err := actual.FindCompletion(ctx, r.Route.TenantID, r.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = actual.Final(ctx, c.FinalIntentID); !errors.Is(err, domain.ErrNotFound) {
						t.Fatal("unproven candidate Final released", err)
					}
				}
				return
			}
			result := make(chan error, 1)
			go func() { result <- processor.Advance(ctx, run) }()
			released := false
			defer func() {
				if !released {
					close(factory.release)
					select {
					case <-result:
					case <-time.After(5 * time.Second):
						t.Error("Processor leaked")
					}
				}
			}()
			select {
			case <-factory.entered:
			case e := <-result:
				t.Fatalf("Processor ended before Memory Apply: %v", e)
			case <-time.After(8 * time.Second):
				t.Fatal("Memory Apply not reached")
			}
			c, e := actual.FindCompletion(ctx, r.Route.TenantID, r.RunID)
			if e != nil || c.MemoryStatus != "PENDING" || c.Status != domain.Succeeded {
				t.Fatal(c, e)
			}
			if _, e = actual.Final(ctx, c.FinalIntentID); !errors.Is(e, domain.ErrNotFound) {
				t.Fatal("success Final visible before Memory persistence", e)
			}
			if mode == "response_loss" && factory.firstContext.Err() == nil {
				t.Fatal("commit-loss fixture did not cancel original Attempt")
			}
			renewalCount := wrapped.renews.Load()
			next := r
			next.RunID += "_next"
			next.EventID += "_next"
			next.AdmissionID += "_next"
			next.RunDigest = domain.Digest([]byte(next.RunID))
			next.EventDigest = domain.Digest([]byte(next.EventID))
			next.Input.ReceivedAt = time.Now().UTC()
			nextRun := accept(next)
			if e = processor.Advance(ctx, nextRun); !errors.Is(e, domain.ErrNotReady) {
				t.Fatal("next Run did not wait for Memory", e)
			}
			if factory.prepares.Load() != 1 {
				t.Fatal("blocked next Run loaded runtime/Memory")
			}
			time.Sleep(15 * time.Millisecond)
			if wrapped.renews.Load() != renewalCount {
				t.Fatal("renewal continued during post-accept Memory Apply")
			}
			close(factory.release)
			released = true
			e = <-result
			if mode == "apply_failure" {
				if !errors.Is(e, application.ErrMemoryApply) {
					t.Fatal("Apply failure swallowed", e)
				}
			} else if e != nil {
				t.Fatal(e)
			}
			if factory.applies.Load() != 1 || wrapped.failures.Load() != 0 {
				t.Fatal("Apply duplicated or accepted Attempt failed")
			}
			wantFinalizes := int32(1)
			if mode == "finalize_response_loss" {
				wantFinalizes = 2
			}
			if wrapped.finalizes.Load() != wantFinalizes {
				t.Fatal("wrong same-decision retries", wrapped.finalizes.Load())
			}
			final, e := actual.Final(ctx, c.FinalIntentID)
			if e != nil {
				t.Fatal(e)
			}
			event, e := codec.DecodeReplyIntent(final.Payload)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "apply_failure" {
				if event.Content.Text != "本次执行未完成，请稍后重试。" {
					t.Fatal("successful Memory text leaked")
				}
			} else if event.Content.Text != "PRIVATE_MEMORY_SUCCESS" {
				t.Fatal(event.Content.Text)
			}
			if e = processor.Advance(ctx, nextRun); e != nil {
				t.Fatal("released follower failed", e)
			}
			factory.mu.Lock()
			reads := append([]bool(nil), factory.loaded...)
			factory.mu.Unlock()
			expectedPersisted := mode != "apply_failure"
			if len(reads) != 2 || reads[0] || reads[1] != expectedPersisted {
				t.Fatal("follower Memory visibility order", reads)
			}
			settled, e := actual.FindCompletion(ctx, c.TenantID, c.RunID)
			if e != nil || settled.Status != domain.Succeeded || settled.Candidate != c.Candidate {
				t.Fatal("accepted execution altered", settled, e)
			}
			t.Logf("MEMORY_PROCESSOR_GATE=PASS mode=%s accepted_before_apply=true pending_follower_no_load=true next_load_applied=%t one_apply=true no_fail_attempt=true", mode, expectedPersisted)
		})
	}
}
