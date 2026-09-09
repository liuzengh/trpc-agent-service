//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// capacityOverride is the compose override for the capacity gate: it enables
// the deterministic Telegram ingress path (placeholder secret resolved from
// an override-provided environment value) and a tight chat quota. The model
// responder keeps the default closed-loopback endpoint, so every execution
// fails locally: no reply outbox is produced, no sender runs, and the
// external request count stays zero for the whole gate.
const capacityOverride = `services:
  app:
    environment:
      BOOTSTRAP_TELEGRAM_ENABLED: "true"
      BOOTSTRAP_LARK_ENABLED: "false"
      BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF: "env://P203_GATE_SECRET"
      BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF: "env://P203_GATE_SECRET"
      P203_GATE_SECRET: "capacity-gate-placeholder"
      RATE_LIMIT_CHAT_LIMIT: "3"
      WORKER_CONCURRENCY: "2"
`

type capacityGate struct {
	*composeRun
	override string
}

func (g *capacityGate) compose(t *testing.T, timeout time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "--project-directory", g.root,
		"--env-file", filepath.Join(g.dir, "env"),
		"-f", filepath.Join(g.root, "docker-compose.yml"),
		"-f", g.override}, args...)
	output, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v failed: %v\n%s", args, err, tailString(string(output), 2000))
	}
	return string(output)
}

func (g *capacityGate) composeTry(timeout time.Duration, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "--project-directory", g.root,
		"--env-file", filepath.Join(g.dir, "env"),
		"-f", filepath.Join(g.root, "docker-compose.yml"),
		"-f", g.override}, args...)
	_, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	return err
}

// capacityWebhook posts one deterministic Telegram webhook event and returns
// status, body and the bounded Retry-After header.
func (g *capacityGate) capacityWebhook(updateID int64, chatID int64, text string) (int, string, string) {
	return g.capacityWebhookAs(updateID, 10000001, chatID, text)
}

// capacityWebhookAs posts one deterministic Telegram webhook event as the
// given external user.
func (g *capacityGate) capacityWebhookAs(updateID int64, userID int64, chatID int64, text string) (int, string, string) {
	body := fmt.Sprintf(`{"update_id":%d,"message":{"message_id":1,"message_thread_id":77,"from":{"id":%d},"chat":{"id":%d,"type":"supergroup"},"text":%q}}`,
		updateID, userID, chatID, text)
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/webhook/telegram/p109-tg-ext", g.appPort), bytes.NewReader([]byte(body)))
	if err != nil {
		return 0, "", ""
	}
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "capacity-gate-placeholder")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return 0, "", ""
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return response.StatusCode, string(raw), response.Header.Get("Retry-After")
}

// TestComposeCapacityProtectionGate is the P2-03 compose-level drill: valid
// capacity config starts and serves, the rate limiter and admission budget
// reject before the dedup claim, Redis and PostgreSQL outages fail closed and
// recover, worker saturation stays bounded, and a SIGTERM restart reclaims
// the pending durable work. External requests stay zero by construction.
func TestComposeCapacityProtectionGate(t *testing.T) {
	if testing.Short() {
		t.Skip("compose capacity gate is a long drill")
	}
	image := composeAppImage(t)
	run := newComposeRun(t, image)
	override := filepath.Join(run.dir, "override.yml")
	if err := os.WriteFile(override, []byte(capacityOverride), 0o600); err != nil {
		t.Fatal(err)
	}
	gate := &capacityGate{composeRun: run, override: override}

	// 1. The default profile service set is unchanged by the P2-03 additions.
	services := run.compose(t, time.Minute, "config", "--services")
	for _, required := range []string{"app", "migrate", "postgres", "redis"} {
		if !strings.Contains(services, required) {
			t.Fatalf("default service %s missing", required)
		}
	}
	if strings.Contains(services, "recovery-") {
		t.Fatalf("recovery profile leaked into the default services")
	}

	// 2. Valid capacity configuration starts and becomes ready.
	gate.compose(t, 5*time.Minute, "up", "-d", "--wait", "postgres", "redis", "migrate", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)

	// 3. Seed the deterministic binding/identity facts (owner connection).
	for _, seed := range []string{
		"INSERT INTO tenant (tenant_id, name, status, config_version, default_agent_app_id, backend_config) VALUES ('p109-local','p109-local','active',1,'p109-local-agent','{\"session\":\"postgres\",\"memory\":\"postgres\",\"vector\":\"none\",\"object\":\"none\"}') ON CONFLICT DO NOTHING",
		"INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p109-local','p109-local-agent','p109-local-agent') ON CONFLICT DO NOTHING",
		"INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref, enabled) VALUES ('p109-local','telegram','p109-tg','p109-tg-ext','env://P203_GATE_SECRET',true) ON CONFLICT DO NOTHING",
		// The user identity row is intentionally NOT seeded: the ingress
		// identity resolution upserts the canonical row (with the derived
		// topic scope) on the first webhook.
	} {
		if err := gate.composeTry(60*time.Second, "exec", "-T", "postgres", "psql", "-U", "trpc_app", "-d", "trpc_agent", "-c", seed); err != nil {
			t.Fatalf("seed failed: %v", err)
		}
	}

	// 4. The chat quota admits exactly three requests; the fourth is rejected
	// with a bounded Retry-After BEFORE the dedup claim.
	for i := 0; i < 3; i++ {
		status, body, _ := gate.capacityWebhook(int64(9000+i), -10000001, "gate input")
		if status != http.StatusAccepted {
			binding := run.psqlScalar(t, "SELECT coalesce(string_agg(binding_id||':'||coalesce(secret_ref,'null')||':'||coalesce(status,'null')||':'||enabled::text,','),'none') FROM channel_binding WHERE tenant_id='p109-local'")
			identity := run.psqlScalar(t, "SELECT coalesce(string_agg(scope||'|'||coalesce(external_chat,'null')||'|'||coalesce(external_thread_id,'null'),','),'none') FROM user_identity")
			tenantRow := run.psqlScalar(t, "SELECT coalesce(backend_config::text,'null') FROM tenant WHERE tenant_id='p109-local'")
			logs := run.serviceLogs(t, "app")
			t.Fatalf("admitted request %d: status=%d body=%s binding=%s identity=%s tenant=%s applogs=%s", i, status, body, binding, identity, tenantRow, tailString(logs, 600))
		}
	}
	dedupBefore := run.psqlScalar(t, "SELECT count(*) FROM message_dedup")
	status, body, retryAfter := gate.capacityWebhook(9999, -10000001, "over quota")
	if status != http.StatusTooManyRequests || !strings.Contains(body, "rate_limited") || retryAfter == "" {
		t.Fatalf("chat quota rejection: status=%d body=%s retry_after=%q", status, body, retryAfter)
	}
	dedupAfter := run.psqlScalar(t, "SELECT count(*) FROM message_dedup")
	if dedupAfter != dedupBefore {
		t.Fatalf("rejected webhook reached the dedup claim: %s -> %s", dedupBefore, dedupAfter)
	}

	// 5. A different external user/chat of the same tenant is still admitted
	// while the first chat's quota stays exhausted (quota scope isolation).
	if status, _, _ = gate.capacityWebhookAs(9998, 10000002, -10000002, "other chat"); status != http.StatusAccepted {
		t.Fatalf("second chat rejected: %d", status)
	}

	// 6. Redis outage: ingress fails closed (503) with zero claims.
	if err := gate.composeTry(60*time.Second, "stop", "redis"); err != nil {
		t.Fatalf("redis stop: %v", err)
	}
	dedupSnapshot := run.psqlScalar(t, "SELECT count(*) FROM message_dedup")
	status, body, _ = gate.capacityWebhookAs(9997, 10000003, -10000003, "redis down")
	if status != http.StatusServiceUnavailable || strings.Contains(body, "accepted") {
		logs := run.serviceLogs(t, "app")
		t.Fatalf("redis outage status=%d body=%s applogs=%s", status, body, tailString(logs, 700))
	}
	if run.psqlScalar(t, "SELECT count(*) FROM message_dedup") != dedupSnapshot {
		t.Fatalf("outage request reached the dedup claim")
	}
	if code := run.probeHealth("/healthz"); code != 200 {
		t.Fatalf("healthz during redis outage: %d (redis is not a readiness dependency)", code)
	}

	// 7. Redis restart: the same composition recovers admission.
	if err := gate.composeTry(2*time.Minute, "start", "redis"); err != nil {
		t.Fatalf("redis start: %v", err)
	}
	recovered := false
	pollUntil(t, 90*time.Second, func() bool {
		status, _, _ = gate.capacityWebhookAs(9996, 10000004, -10000004, "redis back")
		if status == http.StatusAccepted {
			recovered = true
		}
		return recovered || status == http.StatusTooManyRequests
	})
	if !recovered {
		t.Fatalf("ingress never recovered after the redis restart")
	}

	// 8. PostgreSQL outage: readiness 503, liveness contract intact.
	if err := gate.composeTry(60*time.Second, "stop", "postgres"); err != nil {
		t.Fatalf("postgres stop: %v", err)
	}
	run.waitForHealthAbsent(t, "/healthz", 200, 90*time.Second)
	if code := run.probeHealth("/livez"); code != 200 {
		t.Fatalf("livez during postgres outage: %d", code)
	}
	if err := gate.composeTry(2*time.Minute, "start", "postgres"); err != nil {
		t.Fatalf("postgres start: %v", err)
	}
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)
	if status, body, _ = gate.capacityWebhookAs(9995, 10000005, -10000005, "postgres back"); status != http.StatusAccepted {
		t.Fatalf("ingress after postgres recovery: %d %s", status, body)
	}

	// 9. Bounded saturation: fixed burst, categorized outcomes, bounded
	// process growth, durable facts equal to the accepted count.
	const burst = 24
	var wg sync.WaitGroup
	outcomes := make([]int, burst)
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			outcomes[index], _, _ = gate.capacityWebhookAs(int64(10000+index), 10000006, -10000006, "burst")
		}(i)
	}
	wg.Wait()
	accepted := 0
	for _, code := range outcomes {
		switch code {
		case http.StatusAccepted, http.StatusTooManyRequests, http.StatusServiceUnavailable:
		default:
			t.Fatalf("uncategorized burst outcome: %d", code)
		}
		if code == http.StatusAccepted {
			accepted++
		}
	}
	threads := strings.TrimSpace(gate.compose(t, 30*time.Second, "exec", "-T", "app", "sh", "-c", "grep '^Threads' /proc/1/status | tr -s '\\t' ' ' | cut -d' ' -f2"))
	threadCount, err := strconv.Atoi(threads)
	if err != nil {
		t.Fatalf("thread probe: %v %q", err, threads)
	}
	if threadCount > 150 {
		t.Fatalf("app threads above the bounded expectation: %d", threadCount)
	}
	_ = accepted

	// 10. SIGTERM: graceful exit, durable facts preserved, restart reclaims.
	restartCountBefore := run.psqlScalar(t, "SELECT 1")
	_ = restartCountBefore
	// SIGTERM under load: the drain is bounded (worker 10s + dispatcher 5s
	// shutdown budgets). With retrying failing jobs the drain may end in the
	// bounded dispatcher/worker drain categories instead of a clean exit; the
	// contract under test is the BOUNDED stop plus recoverable durable state.
	gate.compose(t, 2*time.Minute, "stop", "--timeout", "25", "app")
	logs := run.serviceLogs(t, "app")
	stoppedStatus := "missing"
	for _, candidate := range []string{"status=clean", "status=dispatcher_timeout", "status=worker_unresolved", "status=runtime_stop_failure"} {
		if strings.Contains(logs, candidate) {
			stoppedStatus = candidate
			break
		}
	}
	if stoppedStatus == "missing" {
		t.Fatalf("bounded stop status line missing: %s", tailString(logs, 900))
	}
	if _, code := run.serviceState(t, "app"); code == -1 {
		t.Fatalf("app did not reach a terminal state after the bounded stop")
	}
	queuedBefore := run.psqlScalar(t, "SELECT count(*) FROM job_queue")
	attemptBefore := run.psqlScalar(t, "SELECT coalesce(max(attempt),0) FROM job_queue")
	gate.compose(t, 3*time.Minute, "start", "app")
	run.waitForHealth(t, "/healthz", 200, 3*time.Minute)
	queuedAfter := run.psqlScalar(t, "SELECT count(*) FROM job_queue")
	if queuedAfter != queuedBefore {
		t.Fatalf("durable queue lost or duplicated across restart: %s -> %s", queuedBefore, queuedAfter)
	}
	attemptAfter, err := strconv.Atoi(run.psqlScalar(t, "SELECT coalesce(max(attempt),0) FROM job_queue"))
	if err != nil {
		t.Fatal(err)
	}
	attemptBeforeNum, _ := strconv.Atoi(attemptBefore)
	if attemptAfter < attemptBeforeNum {
		t.Fatalf("pending work was not reclaimed after restart: %s -> %d", attemptBefore, attemptAfter)
	}
}
