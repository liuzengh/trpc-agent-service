//go:build integration

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// TestCapacityEnvConfigFailsClosed proves the bounded capacity configuration
// contract: invalid, negative, oversized and relationship-breaking values
// fail closed instead of silently weakening protection.
func TestCapacityEnvConfigFailsClosed(t *testing.T) {
	t.Run("admission gate validation", func(t *testing.T) {
		t.Setenv("INGRESS_MAX_INFLIGHT", "4")
		t.Setenv("INGRESS_MAX_INFLIGHT_PER_TENANT", "8")
		if _, err := admissionGateFromEnv(); err == nil {
			t.Fatalf("per-tenant above global accepted")
		}
		t.Setenv("INGRESS_MAX_INFLIGHT_PER_TENANT", "2")
		t.Setenv("INGRESS_MAX_INFLIGHT_PER_BINDING", "4")
		if _, err := admissionGateFromEnv(); err == nil {
			t.Fatalf("per-binding above per-tenant accepted")
		}
		t.Setenv("INGRESS_MAX_INFLIGHT_PER_BINDING", "2")
		t.Setenv("INGRESS_MAX_INFLIGHT", "not-a-number")
		if _, err := admissionGateFromEnv(); err == nil {
			t.Fatalf("non-integer admission limit accepted")
		}
		t.Setenv("INGRESS_MAX_INFLIGHT", "-4")
		if _, err := admissionGateFromEnv(); err == nil {
			t.Fatalf("negative admission limit accepted")
		}
		t.Setenv("INGRESS_MAX_INFLIGHT", "99999999")
		if _, err := admissionGateFromEnv(); err == nil {
			t.Fatalf("oversized admission limit accepted")
		}
		t.Setenv("INGRESS_MAX_INFLIGHT", "")
		if _, err := admissionGateFromEnv(); err != nil {
			t.Fatalf("defaults rejected: %v", err)
		}
	})
	t.Run("rate limiter env", func(t *testing.T) {
		t.Setenv("RATE_LIMIT_REDIS_URL", "not-a-redis-url")
		if _, _, err := rateLimiterFromEnv(); err == nil {
			t.Fatalf("invalid redis URL accepted")
		}
		t.Setenv("RATE_LIMIT_REDIS_URL", "redis://127.0.0.1:6379/0")
		t.Setenv("RATE_LIMIT_TENANT_LIMIT", "0")
		if _, _, err := rateLimiterFromEnv(); err == nil {
			t.Fatalf("zero tenant limit accepted")
		}
		t.Setenv("RATE_LIMIT_TENANT_LIMIT", "")
		t.Setenv("RATE_LIMIT_WINDOW", "700ms")
		if _, _, err := rateLimiterFromEnv(); err == nil {
			t.Fatalf("sub-second window accepted")
		}
		t.Setenv("RATE_LIMIT_WINDOW", "")
		if limiter, cleanup, err := rateLimiterFromEnv(); err != nil || limiter == nil {
			t.Fatalf("valid rate limiter config rejected: %v", err)
		} else {
			cleanup()
		}
	})
	t.Run("rate limiter disabled without url", func(t *testing.T) {
		t.Setenv("RATE_LIMIT_REDIS_URL", "")
		limiter, cleanup, err := rateLimiterFromEnv()
		if err != nil || limiter != nil {
			t.Fatalf("unset URL must keep rate limiting disabled: %v", err)
		}
		cleanup()
	})
	t.Run("bounded integers", func(t *testing.T) {
		t.Setenv("P203_TEST_INT", "7")
		value, err := boundedIntEnv("P203_TEST_INT", 4, 1, 10)
		if err != nil || value != 7 {
			t.Fatalf("valid int rejected: %v %d", err, value)
		}
		t.Setenv("P203_TEST_INT", "11")
		if _, err := boundedIntEnv("P203_TEST_INT", 4, 1, 10); err == nil {
			t.Fatalf("oversized int accepted")
		}
		t.Setenv("P203_TEST_INT", "")
		if value, err := boundedIntEnv("P203_TEST_INT", 4, 1, 10); err != nil || value != 4 {
			t.Fatalf("default not applied: %v %d", err, value)
		}
	})
}

// TestRateLimitBackendUnavailableFailsClosed proves the composition-level
// fail-closed rule: when the limiter backend is unreachable the webhook is
// rejected as dependency unavailable (503) before any dedup claim, and no
// request is reported accepted.
func TestRateLimitBackendUnavailableFailsClosed(t *testing.T) {
	f := newProductionTestFixture(t)
	t.Setenv("RATE_LIMIT_REDIS_URL", "redis://127.0.0.1:1/0") // closed loopback port
	t.Setenv("INGRESS_MAX_INFLIGHT", "8")
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_TENANT", "4")
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_BINDING", "2")
	factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
	larkSender := &recordingProductionSender{channel: "lark", calls: make(chan storage.OutboxMessage, 1)}
	telegramSender := &recordingProductionSender{channel: "telegram", calls: make(chan storage.OutboxMessage, 1)}
	hooks := newProductionRuntimeHooks()
	hooks.releaseDispatcher()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	if runtimeValue.ingress == nil {
		t.Fatal("ingress not assembled")
	}
	body := telegramProductionEvent(f.telegramUpdateID, 20000001, -20000001, "backend down input")
	request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/"+f.telegramExternal, bytes.NewReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", os.Getenv("P009GC_TELEGRAM_WEBHOOK_SECRET"))
	recorder := serveProductionWebhook(t, server, "/webhook/telegram/"+f.telegramExternal, request, body)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("backend unavailable status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte("accepted")) {
		t.Fatalf("backend unavailable returned accepted: %s", recorder.Body.String())
	}
	var dedup int
	if err := f.base.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+".message_dedup").Scan(&dedup); err != nil {
		t.Fatal(err)
	}
	if dedup != 0 {
		t.Fatalf("rejected request reached the dedup claim: %d", dedup)
	}
	if factory.Calls() != 0 || telegramSender.Calls() != 0 {
		t.Fatalf("rejected request reached runner or sender: runner=%d sender=%d", factory.Calls(), telegramSender.Calls())
	}
}

// TestProductionCapacityDrill is the deterministic bounded capacity drill:
// fixed request count, fixed concurrency, fixed admission and quota bounds,
// event-synchronized saturation, and full invariant verification. It is
// bounded local protection evidence, never a production throughput claim.
func TestProductionCapacityDrill(t *testing.T) {
	f := newProductionTestFixture(t)
	const (
		totalRequests  = 16
		globalBound    = 4
		perTenantBound = 2
		chatQuota      = 4
	)
	t.Setenv("WORKER_CONCURRENCY", "1")
	t.Setenv("DISPATCHER_CONCURRENCY", "1")
	t.Setenv("INGRESS_MAX_INFLIGHT", strconv.Itoa(globalBound))
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_TENANT", strconv.Itoa(perTenantBound))
	t.Setenv("INGRESS_MAX_INFLIGHT_PER_BINDING", strconv.Itoa(perTenantBound))
	t.Setenv("RATE_LIMIT_REDIS_URL", os.Getenv("TEST_REDIS_URL"))
	t.Setenv("RATE_LIMIT_TENANT_LIMIT", "1000")
	t.Setenv("RATE_LIMIT_BINDING_LIMIT", "1000")
	t.Setenv("RATE_LIMIT_CHAT_LIMIT", strconv.Itoa(chatQuota))

	factory := &deterministicAgentFactory{result: "capacity drill reply", started: make(chan agent.AgentInput, 1)}
	larkSender := &recordingProductionSender{channel: "lark", calls: make(chan storage.OutboxMessage, totalRequests)}
	telegramSender := &recordingProductionSender{channel: "telegram", calls: make(chan storage.OutboxMessage, totalRequests)}
	hooks := newProductionRuntimeHooks()
	hooks.releaseDispatcher()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	startResult := startProductionRuntime(t, runtimeValue, hooks)
	defer finishProductionStart(t, startResult, hooks)

	type outcome struct {
		status     int
		body       string
		retryAfter string
	}
	results := make([]outcome, totalRequests)
	var wg sync.WaitGroup
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// All requests share the seeded session identity (user/chat/thread)
			// so the durable lease path is exercised deterministically, and the
			// shared external chat makes the chat quota rejection exact.
			body := telegramProductionEvent(f.telegramUpdateID+int64(index)+10, 10000001, -10000001, "capacity drill input")
			request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/"+f.telegramExternal, bytes.NewReader(body))
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", os.Getenv("P009GC_TELEGRAM_WEBHOOK_SECRET"))
			recorder := serveProductionWebhook(t, server, "/webhook/telegram/"+f.telegramExternal, request, body)
			results[index] = outcome{status: recorder.Code, body: recorder.Body.String(), retryAfter: recorder.Header().Get("Retry-After")}
		}(i)
	}
	wg.Wait()

	accepted, rateLimited, capacityRejected, other := 0, 0, 0, 0
	acceptedBodies := ""
	for _, result := range results {
		if result.status == http.StatusAccepted {
			acceptedBodies += result.body + " | "
		}
	}
	for _, result := range results {
		switch {
		case result.status == http.StatusAccepted:
			accepted++
		case result.status == http.StatusTooManyRequests && strings.Contains(result.body, "rate_limited"):
			rateLimited++
			if result.retryAfter == "" {
				t.Fatalf("rate limited response missing bounded Retry-After")
			}
		case result.status == http.StatusTooManyRequests && strings.Contains(result.body, "capacity_exhausted"):
			capacityRejected++
		default:
			other++
		}
	}
	if other != 0 {
		t.Fatalf("uncategorized webhook outcomes: %d (%+v)", other, results)
	}
	if accepted > chatQuota {
		t.Fatalf("accepted above the chat quota: %d > %d", accepted, chatQuota)
	}
	if accepted == 0 {
		t.Fatalf("drill accepted nothing; quota/rate fixtures misconfigured")
	}
	// Durable invariant: every accepted response has exactly one durable
	// enqueue; no duplicates, no lost facts.
	var queueRows int
	if err := f.base.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+".job_queue").Scan(&queueRows); err != nil {
		t.Fatal(err)
	}
	if queueRows != accepted {
		var dedupRows int
		_ = f.base.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+".message_dedup").Scan(&dedupRows)
		t.Fatalf("durable enqueues %d != accepted %d (dedup=%d)", queueRows, accepted, dedupRows)
	}
	highGlobal, highTenant, highBinding := runtimeValue.admission.HighWater()
	if highGlobal > globalBound || highTenant > perTenantBound || highBinding > perTenantBound {
		t.Fatalf("admission high water above bounds: global=%d tenant=%d binding=%d", highGlobal, highTenant, highBinding)
	}
	if highGlobal == 0 {
		t.Fatalf("admission high water never observed")
	}
	// Release the saturated deterministic runner so all accepted executions
	// complete; the recording sender proves no external request happens.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for i := 0; i < accepted; i++ {
			<-factory.started
		}
	}()
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		var jobStatus, dedupStatus string
		row := f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(distinct status, ','),'none') FROM "+f.schema+".job_queue")
		_ = row.Scan(&jobStatus)
		row2 := f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(distinct status, ','),'none') FROM "+f.schema+".message_dedup")
		_ = row2.Scan(&dedupStatus)
		var proconfig, bypass, availView, roleView string
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(proconfig::text,'none') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='"+f.schema+"' AND p.proname='trpc_queue_claim_next'").Scan(&proconfig)
		_ = f.base.QueryRow(context.Background(), "SELECT rolbypassrls::text FROM pg_roles WHERE rolname='trpc_claim_owner'").Scan(&bypass)
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(status||'@in'||extract(epoch from (available_at-clock_timestamp()))::int::text||'s@a'||attempt::text, ','),'none') FROM "+f.schema+".job_queue").Scan(&availView)
		_ = f.base.QueryRow(context.Background(), "SELECT coalesce(string_agg(status, ','),'none') FROM (SELECT status FROM "+f.schema+".job_queue WHERE status='queued' AND available_at <= clock_timestamp() LIMIT 3) x").Scan(&roleView)
		t.Fatalf("accepted executions never finished: runner_calls=%d job_status=%s dedup_status=%s proconfig=%s bypassrls=%s avail=%s eligible=%s", factory.Calls(), jobStatus, dedupStatus, proconfig, bypass, availView, roleView)
	}
	awaitSignal(t, hooks.outboxCompleted, "capacity drill outbox completion")
	// Bounded polling: the dispatcher completes the remaining reply
	// asynchronously; wait for it instead of racing a fixed sleep.
	settle := time.Now().Add(15 * time.Second)
	for time.Now().Before(settle) && telegramSender.Calls() != accepted {
		time.Sleep(50 * time.Millisecond)
	}
	if telegramSender.Calls() != accepted || larkSender.Calls() != 0 {
		t.Fatalf("sender routing: telegram=%d lark=%d accepted=%d", telegramSender.Calls(), larkSender.Calls(), accepted)
	}
}
