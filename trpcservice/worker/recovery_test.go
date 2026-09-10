package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type countedModel struct {
	model.Model
	calls atomic.Int64
}

func (m *countedModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	m.calls.Add(1)
	return m.Model.GenerateContent(ctx, req)
}

type completionFault struct {
	*gateway.MemoryJournal
	fail, afterCommit bool
}

func (j *completionFault) CompleteRun(ctx context.Context, task workqueue.AgentTask, result gateway.RunResult) error {
	if j.fail {
		j.fail = false
		if j.afterCommit {
			if err := j.MemoryJournal.CompleteRun(ctx, task, result); err != nil {
				return err
			}
		}
		return errors.New("injected completion acknowledgement loss")
	}
	return j.MemoryJournal.CompleteRun(ctx, task, result)
}

func recoveryTask(t *testing.T, j *gateway.MemoryJournal) workqueue.AgentTask {
	t.Helper()
	_, err := j.Accept(context.Background(), gateway.InboundRequest{
		Scope: runtimecontext.TutorialScope(), ExternalMessageID: "recovery-message",
		UserID: "fixture-user", SessionID: "fixture-session", ChatType: "direct", Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	return j.Tasks()[0]
}

func newRecoveryWorker(t *testing.T, q workqueue.Queue, j gateway.Journal, r Runtime, id string) *Worker {
	t.Helper()
	w, err := New(q, j, r, Options{WorkerID: id, MaxAttempts: 3, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestCompletionFailureReplaysCachedModelResult(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "before-commit"
		if committed {
			name = "commit-response-lost"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			j := &completionFault{MemoryJournal: gateway.NewMemoryJournal(), fail: true, afterCommit: committed}
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(j)
			q := workqueue.NewMemoryQueue(4)
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(q)
			m := &countedModel{Model: agentruntime.NewTutorialModel()}
			r, err := agentruntime.NewRuntime(m, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(r)
			task := recoveryTask(t, j.MemoryJournal)
			if err := q.Publish(ctx, task); err != nil {
				t.Fatal(err)
			}
			w := newRecoveryWorker(t, q, j, r, "worker")
			if processed, err := w.ProcessOne(ctx); !processed || err == nil {
				t.Fatal("completion fault was not exercised")
			}
			if processed, err := w.ProcessOne(ctx); !processed || err != nil {
				t.Fatalf("repair failed: %v", err)
			}
			if m.calls.Load() != 1 {
				t.Fatal("repair called model again despite cached result")
			}
			items, err := j.ClaimOutbound(ctx, "sender", 10, time.Minute)
			if err != nil || len(items) != 1 {
				t.Fatal("repair did not retain exactly one outbound")
			}
		})
	}
}

type cancellationRuntime struct{ started chan struct{} }

type jobSubmissionFault struct {
	background.Repository
	calls int
}

func (j *jobSubmissionFault) Enqueue(ctx context.Context, input background.EnqueueRequest) (background.EnqueueResult, error) {
	j.calls++
	if j.calls == 2 {
		return background.EnqueueResult{}, errors.New("injected second job submission failure")
	}
	return j.Repository.Enqueue(ctx, input)
}

func TestCompletedRedeliveryAfterExpiryOnlyRepairsBookkeeping(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	dedupe, err := idempotency.NewRedisStore(idempotency.RedisOptions{URL: "redis://" + server.Addr(), KeyPrefix: "expiry", ProcessingTTL: time.Minute, CompletedTTL: 24 * time.Hour, RenewInterval: time.Second, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	m := &countedModel{Model: agentruntime.NewTutorialModel()}
	r, err := agentruntime.NewRuntimeWithServices(m, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), dedupe, false)
	if err != nil {
		t.Fatal(err)
	}
	j := gateway.NewMemoryJournal()
	q := workqueue.NewMemoryQueue(4)
	jobs := &jobSubmissionFault{Repository: background.NewMemoryRepository()}
	t.Cleanup(func() { _ = r.Close(); _ = j.Close(); _ = q.Close(); _ = jobs.Close() })
	task := recoveryTask(t, j)
	w := newRecoveryWorker(t, q, j, r, "worker")
	w.opts.MaxAttempts = 1
	w.opts.Jobs = jobs
	if err = q.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	if _, err = w.ProcessOne(ctx); err == nil {
		t.Fatal("job failure not exercised")
	}
	stored, found, err := j.LoadCompletedRun(ctx, task)
	if err != nil || !found || stored.Finalized {
		t.Fatal("completed outcome lost or unfinished jobs marked final")
	}
	server.FastForward(25 * time.Hour)
	if _, err = w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	stored, found, err = j.LoadCompletedRun(ctx, task)
	if err != nil || !found || !stored.Finalized || m.calls.Load() != 1 || jobs.calls != 4 {
		t.Fatal("expiry caused execution or skipped pending bookkeeping")
	}
	// Even attempts beyond the normal execution limit only ACK finalized runs.
	task.Attempt = 99
	server.FastForward(25 * time.Hour)
	if err = q.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	if _, err = w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if m.calls.Load() != 1 || jobs.calls != 4 {
		t.Fatal("finalized run repeated model or job submission")
	}
}

func (r cancellationRuntime) ChatWithScope(ctx context.Context, _ agentruntime.ChatInput) (agentruntime.ChatResult, error) {
	close(r.started)
	<-ctx.Done()
	return agentruntime.ChatResult{}, context.Cause(ctx)
}

func TestCanceledWorkerLeavesDeliveryForAnotherConsumer(t *testing.T) {
	server := miniredis.RunT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queue := func(id string) *workqueue.RedisQueue {
		q, err := workqueue.NewRedisQueue(ctx, workqueue.RedisOptions{
			URL: "redis://" + server.Addr() + "/0", KeyPrefix: "recovery", Stream: "runs", Group: "workers",
			Consumer: id, BlockTimeout: 10 * time.Millisecond, ClaimMinIdle: 20 * time.Millisecond, MaxLen: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = q.Close() })
		return q
	}
	first, second := queue("first"), queue("second")
	j := gateway.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(j)
	task := recoveryTask(t, j)
	if err := first.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	r := cancellationRuntime{started: make(chan struct{})}
	w := newRecoveryWorker(t, first, j, r, "first")
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	done := make(chan error, 1)
	go func() { done <- w.Run(firstCtx) }()
	select {
	case <-r.started:
	case <-ctx.Done():
		t.Fatal("first worker did not start")
	}
	stopFirst()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("worker did not join on cancellation")
	}
	// No new publish: the original unacknowledged Redis Streams delivery must
	// be reclaimed by a different consumer after its idle interval.
	time.Sleep(30 * time.Millisecond)
	next := agentruntime.NewDemoRuntime()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(next)
	if processed, err := newRecoveryWorker(t, second, j, next, "second").ProcessOne(ctx); !processed || err != nil {
		t.Fatalf("reclaim: processed=%t error=%v", processed, err)
	}
	status, _, _ := j.RunStatus(task.RequestID)
	if status != "completed" {
		t.Fatalf("status=%s", status)
	}
	items, err := j.ClaimOutbound(ctx, "sender", 10, time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatal("recovered task must produce only one reply")
	}
}

type unavailableQueue struct {
	workqueue.Queue
	calls  int
	cancel context.CancelFunc
}

func (q *unavailableQueue) Receive(context.Context) (workqueue.Delivery, error) {
	q.calls++
	if q.calls == 3 {
		q.cancel()
	}
	return nil, errors.New("injected unavailable backend")
}

func TestWorkerBackendFailureBacksOffAndCancels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	q := &unavailableQueue{cancel: cancel}
	j := gateway.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(j)
	r := agentruntime.NewDemoRuntime()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(r)
	start := time.Now()
	err := newRecoveryWorker(t, q, j, r, "worker").Run(ctx)
	if !errors.Is(err, context.Canceled) || q.calls != 3 || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("backend failure spun or ignored cancellation: calls=%d err=%v", q.calls, err)
	}
}
