//go:build integration

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// runtimeStackSummary returns a bounded, identifier-free summary of goroutine
// blocking states for the soak failure diagnostics.
func runtimeStackSummary() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	counts := map[string]int{}
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		if strings.Contains(line, "select") || strings.Contains(line, "chan receive") || strings.Contains(line, "chan send") || strings.Contains(line, "IO wait") || strings.Contains(line, "semacquire") {
			counts[line]++
		}
	}
	top := ""
	for line, count := range counts {
		top += strconv.Itoa(count) + "x[" + strings.TrimSpace(strings.Split(line, " ")[0]) + "] "
		if len(top) > 300 {
			break
		}
	}
	return top
}

// TestP204BoundedSoak is the B6 bounded soak: a FIXED number of webhook
// operations at a FIXED concurrency against the real production assembly
// (PostgreSQL durable queue/claims/completion/outbox, real rate limiter,
// admission budget, deterministic runner, recording sender) with periodic
// resource sampling. It proves boundary, classification, recovery and
// no-leak behavior only; it is never a production throughput or capacity
// claim.
//
// Fixed budget: 120 operations, 6 concurrent workers, 45s wall deadline.
// Sampling: goroutines/heap every 20 operations. End state: active slots 0,
// no goroutine growth, all durable facts accounted, external requests 0.
func TestP204BoundedSoak(t *testing.T) {
	f := newProductionTestFixture(t)
	const (
		totalOps       = 120
		concurrency    = 6
		wallDeadline   = 45 * time.Second
		chatQuotaBound = 1000
	)
	// Session-serialized execution is the platform contract: one worker at a
	// time holds the single seeded session lease, so the soak runs the worker
	// pool at one to exercise the lease/fence path without synthetic
	// same-session contention.
	t.Setenv("WORKER_CONCURRENCY", "1")
	t.Setenv("DISPATCHER_CONCURRENCY", "2")
	t.Setenv("INGRESS_MAX_INFLIGHT", "16")
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_TENANT", "8")
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_BINDING", "8")
	t.Setenv("RATE_LIMIT_REDIS_URL", os.Getenv("TEST_REDIS_URL"))
	t.Setenv("RATE_LIMIT_TENANT_LIMIT", "100000")
	t.Setenv("RATE_LIMIT_BINDING_LIMIT", "100000")
	t.Setenv("RATE_LIMIT_CHAT_LIMIT", strconv.Itoa(chatQuotaBound))

	factory := &deterministicAgentFactory{result: "soak reply", started: make(chan agent.AgentInput, totalOps)}
	larkSender := &recordingProductionSender{channel: "lark", calls: make(chan storage.OutboxMessage, totalOps)}
	telegramSender := &recordingProductionSender{channel: "telegram", calls: make(chan storage.OutboxMessage, totalOps)}
	hooks := newProductionRuntimeHooks()
	hooks.releaseDispatcher()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	startResult := startProductionRuntime(t, runtimeValue, hooks)
	defer finishProductionStart(t, startResult, hooks)
	// The fixture hook channels are small and only existing tests await a
	// bounded number of signals; the soak drains each channel independently
	// and counts the observed completions.
	var completionCount, outboxCount atomic.Int64
	hookDrained := make(chan struct{})
	go func() {
		defer close(hookDrained)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < totalOps; i++ {
				select {
				case <-hooks.completion:
					completionCount.Add(1)
				case <-time.After(120 * time.Second):
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < totalOps; i++ {
				select {
				case <-hooks.outboxCompleted:
					outboxCount.Add(1)
				case <-time.After(120 * time.Second):
					return
				}
			}
		}()
		wg.Wait()
	}()

	var goroutineHighWater int64
	var heapHighWater uint64
	outcomes := make([]int, totalOps)
	var nextUpdate atomic.Int64
	nextUpdate.Store(50000)

	deadline := time.Now().Add(wallDeadline)
	sample := func() {
		if ms := runtime.NumGoroutine(); int64(ms) > atomic.LoadInt64(&goroutineHighWater) {
			atomic.StoreInt64(&goroutineHighWater, int64(ms))
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.HeapAlloc > atomic.LoadUint64(&heapHighWater) {
			atomic.StoreUint64(&heapHighWater, mem.HeapAlloc)
		}
	}

	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				index := int(nextUpdate.Add(1)) - 50001
				if index >= totalOps || time.Now().After(deadline) {
					return
				}
				// Every operation reuses the seeded session identity (user/chat/thread)
				// so the durable session lease path is exercised; the chat quota is set
				// high enough that the soak measures admission/worker bounds instead of
				// quota rejections.
				body := telegramProductionEvent(f.telegramUpdateID+int64(index)+30000, 10000001, -10000001, "soak input")
				request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/"+f.telegramExternal, bytes.NewReader(body))
				request.Header.Set("X-Telegram-Bot-Api-Secret-Token", os.Getenv("P009GC_TELEGRAM_WEBHOOK_SECRET"))
				recorder := serveProductionWebhook(t, server, "/webhook/telegram/"+f.telegramExternal, request, body)
				outcomes[index] = recorder.Code
				if index%20 == 19 {
					sample()
				}
			}
		}()
	}
	wg.Wait()
	sample()

	accepted := 0
	for _, code := range outcomes {
		switch code {
		case http.StatusAccepted, http.StatusTooManyRequests, http.StatusServiceUnavailable:
		default:
			t.Fatalf("uncategorized soak outcome: %d", code)
		}
		if code == http.StatusAccepted {
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatalf("soak accepted nothing")
	}

	// Durable invariants: one queue row per accepted request, no duplicates.
	var queueRows int
	if err := f.base.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+".job_queue").Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if queueRows != accepted {
		t.Fatalf("soak durable enqueues %d != accepted %d", queueRows, accepted)
	}

	// Release every deterministic execution; the recording senders prove the
	// reply path stays local (zero external requests).
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for i := 0; i < accepted; i++ {
			<-factory.started
		}
	}()
	select {
	case <-drained:
	case <-time.After(60 * time.Second):
		var jobState, leaseState, execCount string
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(distinct status||':a'||attempt::text, ','),'none') FROM "+f.schema+".job_queue").Scan(&jobState)
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(owner_id||':e'||epoch::text||':f'||fencing_token::text, ','),'none') FROM "+f.schema+".session_lease").Scan(&leaseState)
		_ = f.base.QueryRow(context.Background(), "SELECT count(*)::text FROM "+f.schema+".execution_result").Scan(&execCount)
		var activity string
		actRows, actErr := f.base.Query(context.Background(), "SELECT coalesce(string_agg(state || ':' || coalesce(wait_event_type,'-') || ':' || left(regexp_replace(query, '[\\n\\r]+', ' ', 'g'), 60), ' | '), 'none') FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")
		if actErr == nil {
			for actRows.Next() {
				_ = actRows.Scan(&activity)
			}
			actRows.Close()
		}
		var totalJobs int
		_ = f.base.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+".job_queue").Scan(&totalJobs)
		t.Fatalf("soak executions never finished: runner_calls=%d accepted=%d total_jobs=%d jobs=%s lease=%s results=%s activity=%s stacks=%s", factory.Calls(), accepted, totalJobs, jobState, leaseState, execCount, activity, runtimeStackSummary())
	}

	// Bounded settle: the dispatcher claims at most its free concurrency
	// slots per claim interval (bounded dispatch), so draining 120 replies at
	// concurrency 2 takes on the order of a minute. The wait stays bounded.
	settle := time.Now().Add(90 * time.Second)
	for time.Now().Before(settle) && telegramSender.Calls() != accepted {
		time.Sleep(50 * time.Millisecond)
	}
	if telegramSender.Calls() != accepted || larkSender.Calls() != 0 {
		var outboxStates, dlq string
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(status||':a'||attempt::text, ','), 'none') FROM (SELECT status, attempt FROM "+f.schema+".outbox_message LIMIT 8) x").Scan(&outboxStates)
		_ = f.base.QueryRow(context.Background(), "SELECT count(*)::text FROM "+f.schema+".dead_letter").Scan(&dlq)
		t.Fatalf("soak sender routing: telegram=%d lark=%d accepted=%d outbox_states=%s dlq=%s", telegramSender.Calls(), larkSender.Calls(), accepted, outboxStates, dlq)
	}

	// End state: no goroutine growth beyond a bounded factor of the high
	// water, admission slots released (high water within the configured
	// bound), durable facts consistent.
	if runtimeValue.admission == nil {
		t.Fatal("admission gate not wired")
	}
	highGlobal, highTenant, highBinding := runtimeValue.admission.HighWater()
	if highGlobal > 16 || highTenant > 8 || highBinding > 8 {
		t.Fatalf("admission high water above bounds: %d/%d/%d", highGlobal, highTenant, highBinding)
	}
	if runtimeValue.admission.ActiveGlobal() != 0 {
		t.Fatalf("admission slots leaked: %d", runtimeValue.admission.ActiveGlobal())
	}
	for i := 0; i < 50 && int64(runtime.NumGoroutine()) > atomic.LoadInt64(&goroutineHighWater)+16; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if int64(runtime.NumGoroutine()) > atomic.LoadInt64(&goroutineHighWater)+16 {
		t.Fatalf("goroutine leak after soak: current=%d high_water=%d", runtime.NumGoroutine(), atomic.LoadInt64(&goroutineHighWater))
	}
	select {
	case <-hookDrained:
	case <-time.After(120 * time.Second):
		t.Fatalf("hook drainer never finished: completions=%d outbox=%d", completionCount.Load(), outboxCount.Load())
	}
	if completionCount.Load() < int64(accepted) || outboxCount.Load() < int64(accepted) {
		t.Fatalf("hook completions incomplete: completions=%d outbox=%d accepted=%d", completionCount.Load(), outboxCount.Load(), accepted)
	}
	_ = heapHighWater
	t.Logf("soak evidence ops=%d accepted=%d high_water_goroutines=%d heap_high_water_bytes=%d", totalOps, accepted, goroutineHighWater, heapHighWater)
}
