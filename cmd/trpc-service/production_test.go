package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	postgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

const productionEvidenceBlocked = "blocked; TEST_DATABASE_URL is required to close P0-09G-C evidence"

type productionTestFixture struct {
	base              *pgxpool.Pool
	schema            string
	tenantID          string
	agentID           string
	larkBindingID     string
	telegramBindingID string
	larkExternalID    string
	telegramExternal  string
	telegramUpdateID  int64
	larkAppID         string
	ownerID           string
}

func newProductionTestFixture(t *testing.T) *productionTestFixture {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if url == "" {
		t.Fatal(productionEvidenceBlocked)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	base, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: url, MaxConns: 2, MinConns: 1})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	fixture := &productionTestFixture{
		base:              base,
		schema:            "p009gc_" + suffix,
		tenantID:          "p009gc-tenant-" + suffix,
		agentID:           "p009gc-agent-" + suffix,
		larkBindingID:     "p009gc-lark-binding-" + suffix,
		telegramBindingID: "p009gc-telegram-binding-" + suffix,
		larkExternalID:    "p009gc-lark-external-" + suffix,
		telegramExternal:  "p009gc-telegram-external-" + suffix,
		telegramUpdateID:  time.Now().UTC().UnixNano(),
		larkAppID:         "cli_p009gc_" + suffix,
		ownerID:           "p009gc-owner-" + suffix,
	}
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+fixture.schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	t.Setenv("DATABASE_URL", url)
	t.Setenv("DATABASE_SCHEMA", fixture.schema)
	t.Setenv("MIGRATIONS_DIR", filepath.Join(filepath.Dir(file), "../../migrations"))
	t.Setenv("MODEL", "synthetic-model")
	t.Setenv("MODEL_CONFIG_REF", "synthetic-model-config")
	t.Setenv("TOOL_POLICY_REF", "synthetic-tool-policy")
	t.Setenv("ASYNC_OWNER_ID", fixture.ownerID)
	t.Setenv("BOOTSTRAP_TENANT_ID", fixture.tenantID)
	t.Setenv("BOOTSTRAP_TENANT_NAME", "synthetic tenant")
	t.Setenv("BOOTSTRAP_AGENT_APP_ID", fixture.agentID)
	t.Setenv("BOOTSTRAP_AGENT_NAME", "synthetic agent")
	t.Setenv("BOOTSTRAP_AGENT_VERSION", "1")
	t.Setenv("BOOTSTRAP_LARK_ENABLED", "true")
	t.Setenv("BOOTSTRAP_TELEGRAM_ENABLED", "true")
	t.Setenv("BOOTSTRAP_LARK_APP_ID", fixture.larkAppID)
	t.Setenv("BOOTSTRAP_LARK_BINDING_ID", fixture.larkBindingID)
	t.Setenv("BOOTSTRAP_LARK_EXTERNAL_APP_ID", fixture.larkExternalID)
	t.Setenv("BOOTSTRAP_LARK_RECEIVER_ID_TYPE", "chat_id")
	t.Setenv("BOOTSTRAP_TELEGRAM_BINDING_ID", fixture.telegramBindingID)
	t.Setenv("BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID", fixture.telegramExternal)
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_APP_SECRET_REF", "P009GC_LARK_APP_SECRET", productionSyntheticValue("a"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_VERIFY_TOKEN_REF", "P009GC_LARK_VERIFY_TOKEN", productionSyntheticValue("b"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_LARK_ENCRYPT_KEY_REF", "P009GC_LARK_ENCRYPT_KEY", productionSyntheticValue("c")) //gitleaks:allow synthetic test value is generated at runtime
	setProductionSyntheticSecret(t, "BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF", "P009GC_TELEGRAM_BOT_TOKEN", productionSyntheticValue("d"))
	setProductionSyntheticSecret(t, "BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF", "P009GC_TELEGRAM_WEBHOOK_SECRET", productionSyntheticValue("e"))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+fixture.schema+" CASCADE")
		base.Close()
		cancel()
	})
	return fixture
}

func productionSyntheticValue(label string) string {
	return "fixture-" + label + "-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}

func setProductionSyntheticSecret(t *testing.T, refName, envName, value string) {
	t.Helper()
	t.Setenv(refName, "env://"+envName)
	t.Setenv(envName, value)
}

type productionTestResponder struct {
	factory agent.AgentFactory
}

func (r productionTestResponder) Factory() agent.AgentFactory { return r.factory }

type deterministicAgentFactory struct {
	result  string
	started chan agent.AgentInput
	calls   atomic.Int32
}

func (f *deterministicAgentFactory) Build(context.Context, tenant.TenantContext, agent.AgentSpec) (agent.AgentRuntime, error) {
	return deterministicAgentRuntime{factory: f}, nil
}

type deterministicAgentRuntime struct {
	factory *deterministicAgentFactory
}

func (r deterministicAgentRuntime) Run(ctx context.Context, input agent.AgentInput) (agent.AgentResult, error) {
	if err := ctx.Err(); err != nil {
		return agent.AgentResult{}, err
	}
	r.factory.calls.Add(1)
	select {
	case r.factory.started <- input:
	case <-ctx.Done():
		return agent.AgentResult{}, ctx.Err()
	}
	return agent.AgentResult{Text: r.factory.result}, nil
}

func (f *deterministicAgentFactory) Calls() int { return int(f.calls.Load()) }

type recordingProductionSender struct {
	channel string
	calls   chan storage.OutboxMessage
	count   atomic.Int32
}

func (s *recordingProductionSender) Send(ctx context.Context, message storage.OutboxMessage) channels.SenderOutcome {
	if err := ctx.Err(); err != nil {
		return channels.SenderOutcome{Class: channels.OutcomeUnknown, Code: "test_context_canceled"}
	}
	s.count.Add(1)
	select {
	case s.calls <- message:
	default:
	}
	return channels.SenderOutcome{Class: channels.OutcomeDelivered, Code: "test_delivered"}
}

func (s *recordingProductionSender) Calls() int { return int(s.count.Load()) }

type productionRuntimeHooks struct {
	dispatcherEntered chan struct{}
	dispatcherRelease chan struct{}
	releaseOnce       sync.Once
	completion        chan struct{}
	outboxCompleted   chan struct{}
}

func newProductionRuntimeHooks() *productionRuntimeHooks {
	return &productionRuntimeHooks{
		dispatcherEntered: make(chan struct{}),
		dispatcherRelease: make(chan struct{}),
		completion:        make(chan struct{}, 4),
		outboxCompleted:   make(chan struct{}, 4),
	}
}

func (h *productionRuntimeHooks) releaseDispatcher() {
	h.releaseOnce.Do(func() { close(h.dispatcherRelease) })
}

func assembleProductionTestRuntime(t *testing.T, f *productionTestFixture, factory *deterministicAgentFactory, larkSender, telegramSender *recordingProductionSender, hooks *productionRuntimeHooks) (*productionRuntime, *web.Server) {
	t.Helper()
	runtimeValue, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{factory: factory}, productionAssemblyDependencies{
		agentFactory:   factory,
		larkSender:     larkSender,
		telegramSender: telegramSender,
		dispatcherStartHook: func() {
			close(hooks.dispatcherEntered)
			<-hooks.dispatcherRelease
		},
		completionHook:      func() { hooks.completion <- struct{}{} },
		outboxCompletedHook: func() { hooks.outboxCompleted <- struct{}{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	seedProductionRows(t, f, runtimeValue.pool)
	t.Cleanup(func() { hooks.releaseDispatcher() })
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtimeValue.Stop(stopCtx)
	})
	store := platform.NewMemoryStore()
	if err := store.SaveTenant(context.Background(), platform.Tenant{
		ID: f.tenantID, Name: "sync compatibility tenant",
		Agent:   platform.AgentConfig{Name: "sync compatibility agent", Model: "synthetic"},
		Backend: platform.BackendConfig{Session: "memory", Memory: "memory", Vector: "none"},
	}); err != nil {
		t.Fatal(err)
	}
	server := web.NewServer(store, platform.Runner{Store: store, Responder: platform.EchoResponder{}})
	server.Resolver = runtimeValue.resolver
	server.AsyncIngress = runtimeValue.ingress
	server.Readiness = runtimeValue.readiness
	runtimeValue.beginDrainingHook = server.BeginDraining
	return runtimeValue, server
}

func seedProductionRows(t *testing.T, f *productionTestFixture, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `
INSERT INTO tenant (tenant_id, name, status, config_version, default_agent_app_id, backend_config)
VALUES ($1, $2, 'active', 1, $3, '{"session":"postgres","memory":"postgres"}')`, f.tenantID, "synthetic tenant", f.agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO agent_app (tenant_id, agent_app_id, name, status, model_config_ref)
VALUES ($1, $2, $3, 'active', 'synthetic-model-config')`, f.tenantID, f.agentID, "synthetic agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref, enabled)
VALUES ($1, 'lark', $2, $3, 'env://P009GC_LARK_APP_SECRET', true),
       ($1, 'telegram', $4, $5, 'env://P009GC_TELEGRAM_BOT_TOKEN', true)`, f.tenantID, f.larkBindingID, f.larkExternalID, f.telegramBindingID, f.telegramExternal); err != nil {
		t.Fatal(err)
	}
	larkSession := channels.SessionID(f.tenantID, "lark", "ou_"+f.tenantID, "oc_"+f.tenantID)
	telegramSession := channels.SessionIDWithThread(f.tenantID, "telegram", "10000001", "-10000001", "77")
	if _, err := pool.Exec(ctx, `
INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user)
VALUES ($1, $2, $3, 1, 'lark', $4, $5, $6),
       ($1, $7, $3, 1, 'telegram', $8, $9, $10)`, f.tenantID, larkSession, f.agentID, f.larkBindingID, "oc_"+f.tenantID, "ou_"+f.tenantID, telegramSession, f.telegramBindingID, "-10000001", "10000001"); err != nil {
		t.Fatal(err)
	}
}

func startProductionRuntime(t *testing.T, runtimeValue *productionRuntime, hooks *productionRuntimeHooks) <-chan error {
	t.Helper()
	startResult := make(chan error, 1)
	go func() { startResult <- runtimeValue.Start(context.Background()) }()
	select {
	case <-hooks.dispatcherEntered:
	case err := <-startResult:
		t.Fatalf("runtime start ended before dispatcher hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("runtime start did not reach dispatcher hook")
	}
	return startResult
}

func finishProductionStart(t *testing.T, startResult <-chan error, hooks *productionRuntimeHooks) {
	t.Helper()
	hooks.releaseDispatcher()
	select {
	case err := <-startResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime start did not complete")
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitSender(t *testing.T, sender *recordingProductionSender) storage.OutboxMessage {
	t.Helper()
	select {
	case message := <-sender.calls:
		return message
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s sender", sender.channel)
		return storage.OutboxMessage{}
	}
}

func serveProductionWebhook(t *testing.T, server *web.Server, path string, request *http.Request, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request.URL.Path = path
	request.Body = ioNopCloser{Reader: bytes.NewReader(body)}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

type ioNopCloser struct{ *bytes.Reader }

func (ioNopCloser) Close() error { return nil }

func TestProductionPostgresLarkAssembly(t *testing.T) {
	f := newProductionTestFixture(t)
	factory := &deterministicAgentFactory{result: "synthetic lark reply", started: make(chan agent.AgentInput, 4)}
	larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 4)}
	telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 4)}
	hooks := newProductionRuntimeHooks()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	startResult := startProductionRuntime(t, runtimeValue, hooks)

	assertHealthStatus(t, server, http.StatusServiceUnavailable)
	challenge := []byte(`{"type":"url_verification","token":"` + os.Getenv("P009GC_LARK_VERIFY_TOKEN") + `","challenge":"fixture-challenge"}`)
	challengeRequest := httptest.NewRequest(http.MethodPost, "/webhook/lark/"+f.larkExternalID, bytes.NewReader(challenge))
	challengeRecorder := serveProductionWebhook(t, server, "/webhook/lark/"+f.larkExternalID, challengeRequest, challenge)
	if challengeRecorder.Code != http.StatusOK || !strings.Contains(challengeRecorder.Body.String(), "fixture-challenge") || factory.Calls() != 0 || larkSender.Calls() != 0 || telegramSender.Calls() != 0 {
		t.Fatalf("Lark challenge response code=%d runner=%d lark=%d telegram=%d", challengeRecorder.Code, factory.Calls(), larkSender.Calls(), telegramSender.Calls())
	}
	body, timestamp, nonce := larkProductionEvent(t, f, "lark-event-"+f.tenantID, "lark-message-"+f.tenantID)
	request := httptest.NewRequest(http.MethodPost, "/webhook/lark/"+f.larkExternalID, bytes.NewReader(body))
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", larkProductionSignature(timestamp, nonce, os.Getenv("P009GC_LARK_ENCRYPT_KEY"), body))
	recorder := serveProductionWebhook(t, server, "/webhook/lark/"+f.larkExternalID, request, body)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"accepted":true`) {
		t.Fatalf("Lark webhook response code=%d body=%s", recorder.Code, recorder.Body.String())
	}

	input := awaitInput(t, factory.started, "Lark runner")
	if input.Input.ID != "lark-event-"+f.tenantID || input.Input.Content != "synthetic lark input" {
		t.Fatalf("unexpected Lark runner input: %+v", input)
	}
	awaitSignal(t, hooks.completion, "Lark atomic completion")
	jobID, executionID := acceptedIdentity(t, recorder)
	assertPendingCompletionFacts(t, runtimeValue.pool, f.tenantID, jobID, executionID, f.larkBindingID, lark.Channel, "oc_"+f.tenantID, "synthetic lark reply")

	finishProductionStart(t, startResult, hooks)
	message := awaitSender(t, larkSender)
	awaitSignal(t, hooks.outboxCompleted, "Lark outbox completion")
	assertCompletedOutbox(t, runtimeValue.pool, f.tenantID, message.ID)
	if larkSender.Calls() != 1 || telegramSender.Calls() != 0 || factory.Calls() != 1 {
		t.Fatalf("Lark routing calls: lark=%d telegram=%d runner=%d", larkSender.Calls(), telegramSender.Calls(), factory.Calls())
	}
	assertHealthStatus(t, server, http.StatusOK)
	assertLiveStatus(t, server, http.StatusOK)
}

func TestProductionPostgresTelegramAssembly(t *testing.T) {
	f := newProductionTestFixture(t)
	factory := &deterministicAgentFactory{result: "synthetic telegram reply", started: make(chan agent.AgentInput, 4)}
	larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 4)}
	telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 4)}
	hooks := newProductionRuntimeHooks()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	startResult := startProductionRuntime(t, runtimeValue, hooks)

	body := telegramProductionEvent(f.telegramUpdateID, 10000001, -10000001, "synthetic telegram input")
	request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/"+f.telegramExternal, bytes.NewReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", os.Getenv("P009GC_TELEGRAM_WEBHOOK_SECRET"))
	recorder := serveProductionWebhook(t, server, "/webhook/telegram/"+f.telegramExternal, request, body)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"accepted":true`) {
		t.Fatalf("Telegram webhook response code=%d body=%s", recorder.Code, recorder.Body.String())
	}

	input := awaitInput(t, factory.started, "Telegram runner")
	telegramEventID := strconv.FormatInt(f.telegramUpdateID, 10)
	if input.Input.ID != telegramEventID || input.Input.Content != "synthetic telegram input" {
		t.Fatalf("unexpected Telegram runner input: %+v", input)
	}
	awaitSignal(t, hooks.completion, "Telegram atomic completion")
	jobID, executionID := acceptedIdentity(t, recorder)
	assertPendingCompletionFacts(t, runtimeValue.pool, f.tenantID, jobID, executionID, f.telegramBindingID, telegram.Channel, "-10000001", "synthetic telegram reply")

	finishProductionStart(t, startResult, hooks)
	message := awaitSender(t, telegramSender)
	awaitSignal(t, hooks.outboxCompleted, "Telegram outbox completion")
	assertCompletedOutbox(t, runtimeValue.pool, f.tenantID, message.ID)
	if telegramSender.Calls() != 1 || larkSender.Calls() != 0 || factory.Calls() != 1 {
		t.Fatalf("Telegram routing calls: telegram=%d lark=%d runner=%d", telegramSender.Calls(), larkSender.Calls(), factory.Calls())
	}
}

func TestProductionPostgresWebhookFailureWindows(t *testing.T) {
	t.Run("claim success queue submit failure replay", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 2)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 2)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 2)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		installFailureTrigger(t, runtimeValue.pool, "queue_insert_failure", "job_queue", "BEFORE INSERT", "RAISE EXCEPTION 'synthetic queue submit failure'")
		body, timestamp, nonce := larkProductionEvent(t, f, "window-queue-event-"+f.tenantID, "window-queue-message-"+f.tenantID)
		first := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if first.Code == http.StatusAccepted {
			t.Fatalf("queue failure unexpectedly accepted: %d", first.Code)
		}
		dropFailureTrigger(t, runtimeValue.pool, "queue_insert_failure", "job_queue")
		replay := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if replay.Code != http.StatusAccepted {
			t.Fatalf("queue failure replay code=%d", replay.Code)
		}
		awaitInput(t, factory.started, "queue recovery runner")
		awaitSignal(t, hooks.completion, "queue recovery atomic completion")
		assertSingleTenantFacts(t, runtimeValue.pool, f.tenantID)
	})

	t.Run("queue submit success claim complete failure replay", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 2)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 2)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 2)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		installFailureTrigger(t, runtimeValue.pool, "claim_complete_failure", "message_dedup", "BEFORE UPDATE", "IF NEW.status = 'completed' THEN RAISE EXCEPTION 'synthetic claim completion failure'; END IF")
		body, timestamp, nonce := larkProductionEvent(t, f, "window-complete-event-"+f.tenantID, "window-complete-message-"+f.tenantID)
		first := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if first.Code == http.StatusAccepted {
			t.Fatalf("claim completion failure unexpectedly accepted: %d", first.Code)
		}
		dropFailureTrigger(t, runtimeValue.pool, "claim_complete_failure", "message_dedup")
		replay := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if replay.Code != http.StatusAccepted {
			t.Fatalf("claim completion replay code=%d", replay.Code)
		}
		if factory.Calls() == 0 {
			awaitInput(t, factory.started, "claim completion recovery runner")
		}
		awaitSignal(t, hooks.completion, "claim completion recovery atomic completion")
		assertSingleTenantFacts(t, runtimeValue.pool, f.tenantID)
	})

	t.Run("lost HTTP ACK replay", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 2)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 2)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 2)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		body, timestamp, nonce := larkProductionEvent(t, f, "window-ack-event-"+f.tenantID, "window-ack-message-"+f.tenantID)
		first := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if first.Code != http.StatusAccepted {
			t.Fatalf("first ACK code=%d", first.Code)
		}
		second := sendLarkRequest(t, server, f, body, timestamp, nonce)
		if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"duplicate":true`) {
			t.Fatalf("replay code=%d body=%s", second.Code, second.Body.String())
		}
		awaitInput(t, factory.started, "ACK replay runner")
		if factory.Calls() != 1 {
			t.Fatalf("replay runner calls=%d", factory.Calls())
		}
		awaitSignal(t, hooks.completion, "ACK replay atomic completion")
		assertSingleTenantFacts(t, runtimeValue.pool, f.tenantID)
	})

	t.Run("pending claim takeover fences old owner", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, _ := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		coordination, err := postgres.NewCoordinationStore(runtimeValue.pool)
		if err != nil {
			t.Fatal(err)
		}
		tc := resolvedProductionContext(t, runtimeValue, lark.Channel, "ou_"+f.tenantID, "oc_"+f.tenantID, "takeover-message-"+f.tenantID)
		key := storage.DedupKey{TenantID: f.tenantID, Channel: lark.Channel, BindingID: f.larkBindingID, ExternalMessageID: "takeover-event-" + f.tenantID}
		oldClaim, err := coordination.Claim(ctx, tc, key, time.Minute, "old-owner")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtimeValue.pool.Exec(ctx, `UPDATE message_dedup SET expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_message_id=$4`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID); err != nil {
			t.Fatal(err)
		}
		newClaim, err := coordination.Claim(ctx, tc, key, time.Minute, "new-owner")
		if err != nil {
			t.Fatal(err)
		}
		if newClaim.OwnerID != "new-owner" || newClaim.FenceToken <= oldClaim.FenceToken {
			t.Fatalf("takeover claim old=%+v new=%+v", oldClaim, newClaim)
		}
		guard := storage.OperationGuard{Backend: oldClaim.Backend, Epoch: oldClaim.Epoch, OwnerID: oldClaim.OwnerID, FenceToken: oldClaim.FenceToken}
		if err := coordination.Complete(ctx, tc, key, oldClaim.OwnerID, "old-job", guard); !errors.Is(err, storage.ErrFenceRejected) {
			t.Fatalf("old owner completion error=%v", err)
		}
	})

	t.Run("duplicate concurrent webhook has one logical winner", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 2)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 2)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 2)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		body, timestamp, nonce := larkProductionEvent(t, f, "window-concurrent-event-"+f.tenantID, "window-concurrent-message-"+f.tenantID)
		start := make(chan struct{})
		results := make(chan int, 2)
		var group sync.WaitGroup
		for i := 0; i < 2; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				results <- sendLarkRequest(t, server, f, body, timestamp, nonce).Code
			}()
		}
		close(start)
		group.Wait()
		close(results)
		accepted := 0
		for code := range results {
			if code == http.StatusAccepted {
				accepted++
			}
		}
		if accepted != 2 {
			t.Fatalf("concurrent accepted responses=%d", accepted)
		}
		awaitInput(t, factory.started, "concurrent winner runner")
		if factory.Calls() != 1 {
			t.Fatalf("concurrent runner calls=%d", factory.Calls())
		}
		awaitSignal(t, hooks.completion, "concurrent atomic completion")
		assertSingleTenantFacts(t, runtimeValue.pool, f.tenantID)
	})
}

func TestProductionAssemblyFailClosedAndChatCompatibility(t *testing.T) {
	t.Run("required bootstrap fields fail", func(t *testing.T) {
		newProductionTestFixture(t)
		for _, name := range []string{
			"BOOTSTRAP_TENANT_ID", "BOOTSTRAP_AGENT_APP_ID", "BOOTSTRAP_AGENT_VERSION",
			"BOOTSTRAP_LARK_APP_ID", "BOOTSTRAP_LARK_BINDING_ID", "BOOTSTRAP_LARK_EXTERNAL_APP_ID",
			"BOOTSTRAP_LARK_APP_SECRET_REF", "BOOTSTRAP_LARK_VERIFY_TOKEN_REF", "BOOTSTRAP_LARK_ENCRYPT_KEY_REF",
			"BOOTSTRAP_TELEGRAM_BINDING_ID", "BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID",
			"BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF", "BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF",
			"ASYNC_OWNER_ID",
		} {
			t.Run(name, func(t *testing.T) {
				t.Setenv(name, "")
				if _, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{}, productionAssemblyDependencies{}); err == nil {
					t.Fatal("missing bootstrap configuration unexpectedly assembled")
				}
			})
		}
	})

	t.Run("database URL does not fall back to memory", func(t *testing.T) {
		f := newProductionTestFixture(t)
		badURL := "postgres://invalid-host.invalid:5432/does-not-exist"
		t.Setenv("DATABASE_URL", badURL)
		_, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{}, productionAssemblyDependencies{})
		if err == nil || strings.Contains(strings.ToLower(err.Error()), "memory") {
			t.Fatalf("database failure did not fail closed: %v", err)
		}
		_ = f
	})

	t.Run("readiness requires every assembled dependency", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, _ := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		checks := []struct {
			name  string
			clear func(*productionReadiness) func()
		}{
			{name: "migration", clear: func(r *productionReadiness) func() {
				old := r.migration
				r.migration = nil
				return func() { r.migration = old }
			}},
			{name: "pool", clear: func(r *productionReadiness) func() { old := r.pool; r.pool = nil; return func() { r.pool = old } }},
			{name: "queue", clear: func(r *productionReadiness) func() {
				old := r.jobQueue
				r.jobQueue = nil
				return func() { r.jobQueue = old }
			}},
			{name: "worker", clear: func(r *productionReadiness) func() { old := r.worker; r.worker = nil; return func() { r.worker = old } }},
			{name: "dispatcher", clear: func(r *productionReadiness) func() {
				old := r.dispatcher
				r.dispatcher = nil
				return func() { r.dispatcher = old }
			}},
		}
		for _, check := range checks {
			t.Run(check.name, func(t *testing.T) {
				restore := check.clear(runtimeValue.readiness)
				defer restore()
				if err := runtimeValue.readiness.Ready(ctx); err == nil {
					t.Fatal("readiness accepted incomplete production assembly")
				}
			})
		}
	})

	t.Run("health liveness and draining", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		assertHealthStatus(t, server, http.StatusServiceUnavailable)
		assertLiveStatus(t, server, http.StatusOK)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		assertHealthStatus(t, server, http.StatusOK)
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := runtimeValue.Stop(stopCtx); err != nil {
			t.Fatal(err)
		}
		cancel()
		assertHealthStatus(t, server, http.StatusServiceUnavailable)
		assertLiveStatus(t, server, http.StatusOK)
	})

	t.Run("api chat remains synchronous and durable-free", func(t *testing.T) {
		f := newProductionTestFixture(t)
		factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
		larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}
		telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}
		hooks := newProductionRuntimeHooks()
		runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
		startResult := startProductionRuntime(t, runtimeValue, hooks)
		finishProductionStart(t, startResult, hooks)
		tc := resolvedProductionContext(t, runtimeValue, "web", "sync-user", "sync-chat", "sync-message-"+f.tenantID)
		request := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(`{"id":"sync-message-`+f.tenantID+`","user_id":"sync-user","text":"sync input"}`)).WithContext(tenant.WithContext(context.Background(), tc))
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "sync input") {
			t.Fatalf("sync chat response code=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var jobs, results, outboxes int
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtimeValue.pool.QueryRow(ctx, "SELECT count(*) FROM job_queue WHERE tenant_id=$1", f.tenantID).Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if err := runtimeValue.pool.QueryRow(ctx, "SELECT count(*) FROM execution_result WHERE tenant_id=$1", f.tenantID).Scan(&results); err != nil {
			t.Fatal(err)
		}
		if err := runtimeValue.pool.QueryRow(ctx, "SELECT count(*) FROM outbox_message WHERE tenant_id=$1", f.tenantID).Scan(&outboxes); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 || results != 0 || outboxes != 0 || factory.Calls() != 0 {
			t.Fatalf("sync chat touched async facts: jobs=%d results=%d outboxes=%d async runner=%d", jobs, results, outboxes, factory.Calls())
		}
	})
}

func awaitInput(t *testing.T, input <-chan agent.AgentInput, label string) agent.AgentInput {
	t.Helper()
	select {
	case value := <-input:
		return value
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return agent.AgentInput{}
	}
}

func assertHealthStatus(t *testing.T, server *web.Server, expected int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != expected {
		t.Fatalf("health status=%d expected=%d body=%s", recorder.Code, expected, recorder.Body.String())
	}
}

func assertLiveStatus(t *testing.T, server *web.Server, expected int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if recorder.Code != expected {
		t.Fatalf("live status=%d expected=%d body=%s", recorder.Code, expected, recorder.Body.String())
	}
}

func assertPendingCompletionFacts(t *testing.T, pool *pgxpool.Pool, tenantID, jobID, executionID, bindingID, channel, destination, reply string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var queueStatus string
	if err := pool.QueryRow(ctx, "SELECT status FROM job_queue WHERE tenant_id=$1 AND job_id=$2", tenantID, jobID).Scan(&queueStatus); err != nil {
		t.Fatal(err)
	}
	if queueStatus != "acked" {
		t.Fatalf("queue status=%s expected=acked", queueStatus)
	}
	var resultJob, resultExecution, resultTenant, resultSession, owner, resultJSON string
	var fence uint64
	if err := pool.QueryRow(ctx, `SELECT job_id, execution_id, tenant_id, session_id, owner_id, fence_token, result_json::text FROM execution_result WHERE tenant_id=$1 AND job_id=$2 AND execution_id=$3`, tenantID, jobID, executionID).Scan(&resultJob, &resultExecution, &resultTenant, &resultSession, &owner, &fence, &resultJSON); err != nil {
		t.Fatal(err)
	}
	if resultJob != jobID || resultExecution != executionID || resultTenant != tenantID || resultSession == "" || owner == "" || fence == 0 || !strings.Contains(resultJSON, reply) {
		t.Fatalf("execution fact identity/result job=%s execution=%s tenant=%s session=%s owner=%s fence=%d result=%s", resultJob, resultExecution, resultTenant, resultSession, owner, fence, resultJSON)
	}
	var outboxStatus, aggregateID, dedupKey string
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT status, aggregate_id, dedup_key, payload FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2`, tenantID, "reply-"+executionID).Scan(&outboxStatus, &aggregateID, &dedupKey, &payload); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "pending" || aggregateID != executionID || dedupKey != tenantID+"|"+executionID+"|agent.reply" {
		t.Fatalf("outbox status=%s aggregate=%s dedup=%s", outboxStatus, aggregateID, dedupKey)
	}
	decoded, err := channels.DecodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TenantID != tenantID || decoded.ExecutionID != executionID || decoded.BindingID != bindingID || decoded.Channel != channel || decoded.DestinationID != destination || decoded.ReplyText != reply {
		t.Fatalf("outbox payload identity tenant=%s execution=%s binding=%s channel=%s destination=%s reply=%s", decoded.TenantID, decoded.ExecutionID, decoded.BindingID, decoded.Channel, decoded.DestinationID, decoded.ReplyText)
	}
	if channel == telegram.Channel && (decoded.MessageThreadID == nil || *decoded.MessageThreadID != 77) {
		t.Fatalf("Telegram thread identity was not durable")
	}
}

func assertCompletedOutbox(t *testing.T, pool *pgxpool.Pool, tenantID, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2", tenantID, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("outbox status=%s expected=completed", status)
	}
}

func acceptedIdentity(t *testing.T, recorder *httptest.ResponseRecorder) (string, string) {
	t.Helper()
	var accepted struct {
		JobID       string `json:"job_id"`
		ExecutionID string `json:"execution_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.JobID == "" || accepted.ExecutionID == "" {
		t.Fatal("accepted response omitted durable identity")
	}
	return accepted.JobID, accepted.ExecutionID
}

func assertSingleTenantFacts(t *testing.T, pool *pgxpool.Pool, tenantID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var jobs, executions, outboxes int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_queue WHERE tenant_id=$1", tenantID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM execution_result WHERE tenant_id=$1", tenantID).Scan(&executions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_message WHERE tenant_id=$1", tenantID).Scan(&outboxes); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || executions != 1 || outboxes != 1 {
		t.Fatalf("logical facts jobs=%d executions=%d outboxes=%d", jobs, executions, outboxes)
	}
}

func TestProductionObjectConfigurationBoundary(t *testing.T) {
	t.Run("none does not require object credentials", func(t *testing.T) {
		configValue, err := loadProductionObjectConfig(context.Background(), "none")
		config := bootstrapRuntimeConfig{objectBackend: "none", objectConfig: configValue}
		if err != nil || config.objectBackend != "none" || config.objectConfig.Endpoint != "" {
			t.Fatalf("none object backend was not kept disabled")
		}
		store, probe, err := newProductionObjectStore(context.Background(), config)
		if err != nil || store != nil || probe != nil {
			t.Fatalf("disabled object backend constructed a dependency")
		}
	})

	t.Run("enabled backend fails closed without required settings", func(t *testing.T) {
		t.Setenv("BOOTSTRAP_OBJECT_BACKEND", "s3")
		for _, name := range []string{"OBJECT_ENDPOINT", "OBJECT_REGION", "OBJECT_BUCKET", "OBJECT_ACCESS_KEY_REF", "OBJECT_SECRET_KEY_REF"} {
			t.Setenv(name, "")
		}
		if _, err := loadProductionObjectConfig(context.Background(), "s3"); err == nil {
			t.Fatal("enabled object backend accepted incomplete configuration")
		}
	})
}

func TestProductionObjectCompositionAndArtifactReconciliation(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OBJECT_S3_INTEGRATION")) != "1" {
		t.Skip("set OBJECT_S3_INTEGRATION=1 to run the production object gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	f := newProductionTestFixture(t)
	lab := testinfra.NewObjectLab(t, "caelumc")
	lab.Start(ctx)
	lab.CreateBucket(ctx)
	t.Setenv("BOOTSTRAP_OBJECT_BACKEND", "s3")
	t.Setenv("OBJECT_ENDPOINT", lab.Endpoint(ctx))
	t.Setenv("OBJECT_REGION", "us-east-1")
	t.Setenv("OBJECT_BUCKET", lab.Bucket())
	t.Setenv("OBJECT_USE_PATH_STYLE", "true")
	t.Setenv("OBJECT_MAX_BYTES", "64")
	t.Setenv("P009_PROD_OBJECT_ACCESS", lab.AccessKey())
	t.Setenv("P009_PROD_OBJECT_SECRET", lab.SecretKey())
	t.Setenv("OBJECT_ACCESS_KEY_REF", "env://P009_PROD_OBJECT_ACCESS")
	t.Setenv("OBJECT_SECRET_KEY_REF", "env://P009_PROD_OBJECT_SECRET")
	factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
	larkSender := &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}
	telegramSender := &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}
	hooks := newProductionRuntimeHooks()
	runtimeValue, server := assembleProductionTestRuntime(t, f, factory, larkSender, telegramSender, hooks)
	if runtimeValue.objectStore == nil || runtimeValue.artifactRepository == nil || runtimeValue.readiness.objectProbe == nil {
		t.Fatal("production composition did not retain object dependencies")
	}
	startResult := startProductionRuntime(t, runtimeValue, hooks)
	finishProductionStart(t, startResult, hooks)
	assertHealthStatus(t, server, http.StatusOK)

	messageID := "production-object-message"
	tc := resolvedProductionContext(t, runtimeValue, lark.Channel, "ou_"+f.tenantID, "oc_"+f.tenantID, messageID)
	placeholder := string(rune(36))
	eventQuery := "INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, message_id, trace_id, payload) VALUES (" + placeholder + "1," + placeholder + "2," + placeholder + "3,1,'user_message'," + placeholder + "4," + placeholder + "5,'{}')"
	if _, err := runtimeValue.pool.Exec(ctx, eventQuery, tc.TenantID, tc.SessionID, "production-object-event", messageID, messageID); err != nil {
		t.Fatal("could not seed artifact message relation")
	}
	repo := runtimeValue.artifactRepository
	store := runtimeValue.objectStore
	artifactID := "production-object-artifact"
	key, err := storage.CanonicalObjectKey(tc.TenantID, artifactID)
	if err != nil {
		t.Fatal("canonical artifact key was rejected")
	}
	metadata := artifact.Artifact{TenantID: tc.TenantID, ID: artifactID, SessionID: tc.SessionID, MessageID: messageID, ObjectKey: key, MIMEType: "text/plain", SizeBytes: int64(len([]byte("production object bytes"))), Status: artifact.StatusPending}
	if err := repo.Create(ctx, tc, metadata); err != nil {
		t.Fatal("production artifact metadata create failed")
	}
	if _, err := repo.PresignedURL(ctx, tc, artifactID, time.Minute); !errors.Is(err, storage.ErrConflict) {
		t.Fatal("pending artifact exposed a presigned URL")
	}
	body := []byte("production object bytes")
	digest := sha256.Sum256(body)
	info, err := store.Put(ctx, tc, storage.ObjectUpload{ArtifactID: artifactID, MIMEType: "text/plain", ExpectedSize: int64(len(body)), ExpectedSHA256: hex.EncodeToString(digest[:]), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal("production object put failed")
	}
	head, err := store.Head(ctx, tc, artifactID)
	if err != nil || head.SizeBytes != int64(len(body)) || head.MIMEType != "text/plain" || head.SHA256 != info.SHA256 {
		t.Fatal("production object head metadata mismatch")
	}
	reader, getInfo, err := store.Get(ctx, tc, artifactID)
	if err != nil {
		t.Fatal("production object get failed")
	}
	got, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || !bytes.Equal(got, body) || getInfo.SizeBytes != info.SizeBytes || getInfo.SHA256 != info.SHA256 || getInfo.MIMEType != info.MIMEType {
		t.Fatal("production object get bytes metadata mismatch")
	}
	overLimit := storage.ObjectUpload{ArtifactID: "production-object-over-limit", MIMEType: "text/plain", ExpectedSize: 65, ExpectedSHA256: info.SHA256, Body: bytes.NewReader(body)}
	if _, err := store.Put(ctx, tc, overLimit); !errors.Is(err, storage.ErrObjectTooLarge) {
		t.Fatal("production object size limit was not enforced")
	}
	for _, unsafeID := range []string{"", "/absolute", "../escape", `..\escape`} {
		if _, err := store.Head(ctx, tc, unsafeID); !errors.Is(err, storage.ErrObjectInvalid) {
			t.Fatal("production object accepted unsafe artifact id")
		}
	}
	if err := repo.MarkReady(ctx, tc, artifactID, info); err != nil {
		t.Fatal("production artifact ready transition failed")
	}
	ready, err := repo.Get(ctx, tc, artifactID)
	if err != nil || ready.Status != artifact.StatusReady || ready.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatal("production artifact did not persist ready metadata")
	}
	urlValue, err := repo.PresignedURL(ctx, tc, artifactID, time.Minute)
	if err != nil {
		t.Fatal("production metadata presigned URL failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, urlValue, nil)
	if err != nil {
		t.Fatal("production presigned URL request construction failed")
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("production presigned URL access failed")
	}
	urlBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || readErr != nil || !bytes.Equal(urlBody, body) {
		t.Fatal("production presigned URL returned unexpected bytes")
	}
	otherTenant := tc
	otherTenant.TenantID = tc.TenantID + "-other"
	if _, err := repo.Get(ctx, otherTenant, artifactID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("cross-tenant artifact metadata access was accepted")
	}
	if _, err := store.Head(ctx, otherTenant, artifactID); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("cross-tenant object access was accepted")
	}
	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	if _, err := store.Head(canceled, tc, artifactID); !errors.Is(err, context.Canceled) {
		t.Fatal("production object operation ignored cancellation")
	}

	missingID := "production-object-missing"
	missingKey, _ := storage.CanonicalObjectKey(tc.TenantID, missingID)
	missing := artifact.Artifact{TenantID: tc.TenantID, ID: missingID, SessionID: tc.SessionID, MessageID: messageID, ObjectKey: missingKey, MIMEType: "text/plain", SizeBytes: 0, Status: artifact.StatusPending}
	if err := repo.Create(ctx, tc, missing); err != nil {
		t.Fatal("missing-object metadata create failed")
	}
	reconciled, err := repo.Reconcile(ctx, tc, missingID)
	if err != nil || reconciled.Status != artifact.StatusFailed {
		t.Fatal("missing object was not reconciled to failed")
	}

	lab.Stop(ctx)
	probe := runtimeValue.readiness.objectProbe
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	probeErr := probe.Ready(probeCtx)
	probeCancel()
	if probeErr == nil {
		t.Fatal("production readiness accepted stopped object service")
	}
	lab.Restart(ctx)
	if err := probe.Ready(ctx); err != nil {
		t.Fatal("production object readiness did not recover")
	}
	if _, err := store.Head(ctx, tc, artifactID); err != nil {
		t.Fatal("production object head did not recover")
	}
	if err := store.Delete(ctx, tc, artifactID); err != nil {
		t.Fatal("production object delete failed")
	}
	if err := store.Delete(ctx, tc, artifactID); err != nil {
		t.Fatal("production object delete was not idempotent")
	}
	expired, err := repo.Reconcile(ctx, tc, artifactID)
	if err != nil || expired.Status != artifact.StatusExpired {
		t.Fatal("deleted ready object was not reconciled to expired")
	}
	if _, err := repo.PresignedURL(ctx, tc, artifactID, time.Minute); !errors.Is(err, storage.ErrConflict) {
		t.Fatal("expired artifact exposed a presigned URL")
	}
	assertHealthStatus(t, server, http.StatusOK)
}

func resolvedProductionContext(t *testing.T, runtimeValue *productionRuntime, channel, user, chat, messageID string) tenant.TenantContext {
	t.Helper()
	externalAppID := ""
	if channel == lark.Channel {
		externalAppID = requiredEnv("BOOTSTRAP_LARK_EXTERNAL_APP_ID")
	} else if channel == telegram.Channel {
		externalAppID = requiredEnv("BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID")
	}
	if channel == "web" {
		tc := tenant.TenantContext{
			TenantID: requiredEnv("BOOTSTRAP_TENANT_ID"), AgentAppID: requiredEnv("BOOTSTRAP_AGENT_APP_ID"), BindingID: "sync-binding",
			Channel: "web", ExternalUser: user, ExternalChat: chat, SessionID: channels.SessionID(requiredEnv("BOOTSTRAP_TENANT_ID"), channel, user, chat),
			RequestID: messageID, MessageID: messageID, TraceID: messageID, ConfigVersion: 1,
			BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
		}
		if err := tc.Validate(); err != nil {
			t.Fatal(err)
		}
		return tc
	}
	tc, err := runtimeValue.resolver.Resolve(context.Background(), tenant.ResolveRequest{Channel: channel, ExternalAppID: externalAppID, ExternalUser: user, ExternalChat: chat, RequestID: messageID, MessageID: messageID, TraceID: messageID})
	if err != nil {
		t.Fatal(err)
	}
	tc.SessionID = channels.SessionID(tc.TenantID, channel, user, chat)
	return tc
}

func installFailureTrigger(t *testing.T, pool *pgxpool.Pool, name, _, _, _ string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var statement string
	switch name {
	case "queue_insert_failure":
		statement = `CREATE FUNCTION queue_insert_failure_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic queue submit failure'; END; $$; CREATE TRIGGER queue_insert_failure BEFORE INSERT ON job_queue FOR EACH ROW EXECUTE FUNCTION queue_insert_failure_fn()`
	case "claim_complete_failure":
		statement = `CREATE FUNCTION claim_complete_failure_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 'completed' THEN RAISE EXCEPTION 'synthetic claim completion failure'; END IF; RETURN NEW; END; $$; CREATE TRIGGER claim_complete_failure BEFORE UPDATE ON message_dedup FOR EACH ROW EXECUTE FUNCTION claim_complete_failure_fn()`
	default:
		t.Fatalf("unsupported test failure trigger %q", name)
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
}

func dropFailureTrigger(t *testing.T, pool *pgxpool.Pool, name, _ string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var statement string
	switch name {
	case "queue_insert_failure":
		statement = `DROP TRIGGER IF EXISTS queue_insert_failure ON job_queue; DROP FUNCTION IF EXISTS queue_insert_failure_fn()`
	case "claim_complete_failure":
		statement = `DROP TRIGGER IF EXISTS claim_complete_failure ON message_dedup; DROP FUNCTION IF EXISTS claim_complete_failure_fn()`
	default:
		t.Fatalf("unsupported test failure trigger %q", name)
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
}

func larkProductionEvent(t *testing.T, f *productionTestFixture, eventID, messageID string) ([]byte, string, string) {
	t.Helper()
	plain, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{"event_id": eventID, "event_type": "im.message.receive_v1", "app_id": f.larkAppID},
		"event": map[string]any{
			"sender":  map[string]any{"sender_id": map[string]string{"open_id": "ou_" + f.tenantID}},
			"message": map[string]any{"message_id": messageID, "chat_id": "oc_" + f.tenantID, "message_type": "text", "content": `{"text":"synthetic lark input"}`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	iv := bytes.Repeat([]byte{0x09}, aes.BlockSize)
	keyHash := sha256.Sum256([]byte(os.Getenv("P009GC_LARK_ENCRYPT_KEY")))
	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		t.Fatal(err)
	}
	padded := pkcs7Pad(plain, block.BlockSize())
	ciphertext := append(append([]byte{}, iv...), make([]byte, len(padded))...)
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext[aes.BlockSize:], padded)
	encrypted := []byte(`{"encrypt":"` + base64.StdEncoding.EncodeToString(ciphertext) + `"}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	nonce := "synthetic-nonce-" + f.tenantID
	return encrypted, timestamp, nonce
}

func larkProductionSignature(timestamp, nonce, encryptKey string, body []byte) string {
	digest := sha256.Sum256([]byte(timestamp + nonce + encryptKey + string(body)))
	return hex.EncodeToString(digest[:])
}

func pkcs7Pad(value []byte, blockSize int) []byte {
	padding := blockSize - len(value)%blockSize
	return append(append([]byte{}, value...), bytes.Repeat([]byte{byte(padding)}, padding)...)
}

func telegramProductionEvent(updateID int64, userID, chatID int64, text string) []byte {
	threadID := int64(77)
	payload := telegram.Update{UpdateID: &updateID, Message: &telegram.Message{MessageID: 1, MessageThreadID: &threadID, From: &telegram.User{ID: userID}, Chat: telegram.Chat{ID: chatID, Type: "supergroup"}, Text: &text}}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func sendLarkRequest(t *testing.T, server *web.Server, f *productionTestFixture, body []byte, timestamp, nonce string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/webhook/lark/"+f.larkExternalID, bytes.NewReader(body))
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", larkProductionSignature(timestamp, nonce, os.Getenv("P009GC_LARK_ENCRYPT_KEY"), body))
	return serveProductionWebhook(t, server, "/webhook/lark/"+f.larkExternalID, request, body)
}

func TestBootstrapChannelDefaultsAndSecretFailClosed(t *testing.T) {
	t.Setenv("BOOTSTRAP_AGENT_VERSION", "1")
	t.Setenv("BOOTSTRAP_LARK_ENABLED", "")
	t.Setenv("BOOTSTRAP_TELEGRAM_ENABLED", "")
	config, err := loadBootstrapRuntimeConfig(context.Background())
	if err != nil {
		t.Fatalf("default optional channels failed: %v", err)
	}
	if config.larkEnabled || config.telegramEnabled {
		t.Fatalf("optional channel defaults enabled: lark=%v telegram=%v", config.larkEnabled, config.telegramEnabled)
	}

	t.Setenv("BOOTSTRAP_LARK_ENABLED", "true")
	t.Setenv("BOOTSTRAP_LARK_APP_ID", "cli-secret-test")
	t.Setenv("BOOTSTRAP_LARK_BINDING_ID", "binding-secret-test")
	t.Setenv("BOOTSTRAP_LARK_EXTERNAL_APP_ID", "external-secret-test")
	t.Setenv("BOOTSTRAP_LARK_APP_SECRET_REF", "env://P104_MISSING_APP_SECRET")
	t.Setenv("BOOTSTRAP_LARK_VERIFY_TOKEN_REF", "env://P104_MISSING_VERIFY")
	t.Setenv("BOOTSTRAP_LARK_ENCRYPT_KEY_REF", "env://P104_MISSING_ENCRYPT")
	t.Setenv("BOOTSTRAP_LARK_RECEIVER_ID_TYPE", "chat_id")
	if _, err := loadBootstrapRuntimeConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "unavailable") || strings.Contains(err.Error(), "P104_MISSING") {
		t.Fatalf("enabled missing secret was not fail-closed: %v", err)
	}
}
