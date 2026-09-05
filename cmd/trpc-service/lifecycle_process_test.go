package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	postgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const gdProcessOutputLimit = 32 << 10

type gdOutput struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (o *gdOutput) Write(value []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	remaining := o.limit - len(o.data)
	if remaining > 0 {
		if remaining > len(value) {
			remaining = len(value)
		}
		o.data = append(o.data, value[:remaining]...)
	}
	return len(value), nil
}

func (o *gdOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return string(o.data)
}

type gdProcess struct {
	cmd    *exec.Cmd
	stdout gdOutput
	stderr gdOutput
	done   chan struct{}
	addr   string
	mu     sync.Mutex
	err    error
}

func (p *gdProcess) start(t *testing.T) {
	t.Helper()
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start trpc-service binary: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	}()
}

func (p *gdProcess) wait(timeout time.Duration) (error, bool) {
	select {
	case <-p.done:
		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (p *gdProcess) output() string { return p.stdout.String() + "\n" + p.stderr.String() }

func (p *gdProcess) errorValue() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *gdProcess) terminate() error {
	if p.cmd.Process == nil {
		return errors.New("process is not running")
	}
	if runtime.GOOS == "windows" {
		return p.cmd.Process.Signal(os.Interrupt)
	}
	return p.cmd.Process.Signal(syscall.SIGTERM)
}

func (p *gdProcess) kill() error {
	if p.cmd.Process == nil {
		return errors.New("process is not running")
	}
	return p.cmd.Process.Kill()
}

func gdStopAfterEvidence(t *testing.T, p *gdProcess) {
	t.Helper()
	if runtime.GOOS == "windows" {
		if err := p.kill(); err != nil {
			t.Fatalf("stop evidence process: %v", err)
		}
		if _, done := p.wait(8 * time.Second); !done {
			t.Fatal("evidence process did not exit after terminate")
		}
		return
	}
	if err := p.terminate(); err != nil {
		t.Fatalf("terminate evidence process: %v", err)
	}
	if err := gdWaitProcessExit(t, p, 8*time.Second); err != nil {
		t.Fatalf("evidence process exit=%v output=%s", err, p.output())
	}
}

func stopGDProcess(t *testing.T, p *gdProcess) {
	t.Helper()
	if p == nil {
		return
	}
	if _, done := p.wait(0); done {
		return
	}
	if err := p.kill(); err != nil {
		return
	}
	if _, done := p.wait(5 * time.Second); !done {
		t.Fatal("G-D process did not exit after kill")
	}
}

func gdRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func buildGDServiceBinary(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "trpc-service-gd")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/trpc-service")
	cmd.Dir = gdRepoRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build cmd/trpc-service: %v\n%s", err, output)
	}
	return binary
}

func gdFreeHTTPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func gdChildEnv(f *productionTestFixture, addr, modelURL, providerURL, owner string) []string {
	overrides := map[string]string{
		"DATABASE_URL":                os.Getenv("TEST_DATABASE_URL"),
		"DATABASE_RUNTIME_URL":        f.runtimeURL,
		"DATABASE_SCHEMA":             f.schema,
		"MIGRATIONS_DIR":              filepath.Join(gdRepoRootForEnv(), "migrations"),
		"HTTP_ADDR":                   addr,
		"DEFAULT_TENANT":              f.tenantID,
		"DEFAULT_AGENT_APP_ID":        f.agentID,
		"MODEL_PROVIDER":              "runner",
		"MODEL":                       "openai",
		"MODEL_BASE_URL":              modelURL,
		"MODEL_NAME":                  "g-d-local-model",
		"MODEL_API_KEY":               "m",
		"MODEL_CONFIG_VERSION":        "1",
		"ASYNC_OWNER_ID":              owner,
		"G_D_TEST_MODE":               "1",
		"G_D_TEST_PROVIDER_URL":       providerURL,
		"BOOTSTRAP_OBJECT_BACKEND":    "none",
		"G_D_STARTUP_HOLD":            "300ms",
		"STARTUP_TIMEOUT":             "20s",
		"SHUTDOWN_TIMEOUT":            "3s",
		"FORCE_CLOSE_TIMEOUT":         "300ms",
		"WORKER_VISIBILITY_TIMEOUT":   "2s",
		"WORKER_LEASE_TTL":            "2s",
		"WORKER_SHUTDOWN_TIMEOUT":     "2s",
		"WORKER_CLEANUP_TIMEOUT":      "100ms",
		"WORKER_RETRY_DELAY":          "100ms",
		"OUTBOX_LOCK_DURATION":        "2s",
		"DISPATCHER_CLAIM_INTERVAL":   "50ms",
		"DISPATCHER_SHUTDOWN_TIMEOUT": "2s",
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, value := range os.Environ() {
		name, _, ok := strings.Cut(value, "=")
		if ok {
			if strings.HasPrefix(name, "OBJECT_") || name == "BOOTSTRAP_OBJECT_BACKEND" {
				continue
			}
			if _, overridden := overrides[name]; overridden {
				continue
			}
		}
		env = append(env, value)
	}
	for name, value := range overrides {
		env = append(env, name+"="+value)
	}
	return env
}

func gdRepoRootForEnv() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func startGDProcess(t *testing.T, binary string, f *productionTestFixture, addr, modelURL, providerURL, owner string) *gdProcess {
	t.Helper()
	process := &gdProcess{
		cmd:    exec.Command(binary),
		stdout: gdOutput{limit: gdProcessOutputLimit},
		stderr: gdOutput{limit: gdProcessOutputLimit},
		done:   make(chan struct{}),
	}
	process.cmd.Dir = gdRepoRoot(t)
	process.addr = addr
	process.cmd.Env = gdChildEnv(f, addr, modelURL, providerURL, owner)
	process.cmd.Stdout = &process.stdout
	process.cmd.Stderr = &process.stderr
	process.start(t)
	return process
}

func gdWaitReady(t *testing.T, process *gdProcess, addr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 300 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	seenNotReady := false
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("process exited before readiness: err=%v output=%s", process.errorValue(), process.output())
		default:
		}
		response, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			switch response.StatusCode {
			case http.StatusServiceUnavailable:
				seenNotReady = true
			case http.StatusOK:
				if !seenNotReady {
					t.Fatalf("process became ready without an observed pre-ready response")
				}
				return
			}
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-timer.C:
		case <-process.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			t.Fatalf("process exited while waiting for readiness: err=%v output=%s", process.errorValue(), process.output())
		}
	}
	t.Fatalf("process did not become ready; output=%s", process.output())
}

func gdWaitProcessExit(t *testing.T, process *gdProcess, timeout time.Duration) error {
	t.Helper()
	err, done := process.wait(timeout)
	if !done {
		t.Fatalf("process did not exit within %s; output=%s", timeout, process.output())
	}
	return err
}

func gdAssertSafeOutput(t *testing.T, output string, sensitive ...string) {
	t.Helper()
	for _, value := range sensitive {
		if value != "" && strings.Contains(output, value) {
			t.Fatalf("process output contains sensitive value %q: %s", value, output)
		}
	}
	if strings.Contains(output, "postgres://") || strings.Contains(output, "postgresql://") || strings.Contains(output, "Authorization") || strings.Contains(output, "Bearer ") {
		t.Fatalf("process output contains connection or authorization material: %s", output)
	}
}

func gdWaitCounter(t *testing.T, counter *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= want {
			return
		}
		timer := time.NewTimer(20 * time.Millisecond)
		<-timer.C
	}
	t.Fatalf("counter=%d, want at least %d", counter.Load(), want)
}

type gdModelServer struct {
	server   *httptest.Server
	calls    atomic.Int32
	started  chan struct{}
	release  chan struct{}
	block    atomic.Bool
	once     sync.Once
	requests atomic.Int32
	lastPath atomic.Value
}

func newGDModelServer(block bool) *gdModelServer {
	model := &gdModelServer{started: make(chan struct{}, 8), release: make(chan struct{})}
	model.block.Store(block)
	model.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		model.requests.Add(1)
		model.lastPath.Store(r.URL.Path)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		model.calls.Add(1)
		select {
		case model.started <- struct{}{}:
		default:
		}
		if model.block.Load() {
			select {
			case <-model.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"g-d-model","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"g-d reply"},"finish_reason":"stop"}]}`)
	}))
	return model
}

func (m *gdModelServer) Summary() string {
	path := ""
	if value := m.lastPath.Load(); value != nil {
		path = value.(string)
	}
	return fmt.Sprintf("requests=%d matching=%d last_path=%s", m.requests.Load(), m.calls.Load(), path)
}

func (m *gdModelServer) Release() { m.once.Do(func() { close(m.release) }) }

func (m *gdModelServer) Close() { m.Release(); m.server.Close() }

type gdProviderServer struct {
	server      *httptest.Server
	calls       atomic.Int32
	sendStarted chan struct{}
	release     chan struct{}
	blockSend   atomic.Bool
	releaseOnce sync.Once
}

func newGDProviderServer(blockSend bool) *gdProviderServer {
	provider := &gdProviderServer{sendStarted: make(chan struct{}, 8), release: make(chan struct{})}
	provider.blockSend.Store(blockSend)
	provider.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "tenant_access_token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"code":0,"tenant_access_token":"opaque","expire":3600}`)
		case strings.Contains(r.URL.Path, "messages") || strings.Contains(r.URL.Path, "sendMessage"):
			provider.calls.Add(1)
			select {
			case provider.sendStarted <- struct{}{}:
			default:
			}
			if provider.blockSend.Load() {
				<-provider.release
			}
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.URL.Path, "sendMessage") {
				_, _ = fmt.Fprint(w, `{"ok":true,"result":{"message_id":1}}`)
			} else {
				_, _ = fmt.Fprint(w, `{"code":0,"data":{"message_id":"g-d-message"}}`)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return provider
}

func (p *gdProviderServer) Release() { p.releaseOnce.Do(func() { close(p.release) }) }

func (p *gdProviderServer) Close() { p.Release(); p.server.Close() }

const gdOutageWebhookDeadline = 20 * time.Second

type gdWebhookResult struct {
	status           int
	completeResponse bool
	stage            gdRecoveryStage
	category         string
	elapsed          time.Duration
}

func gdWebhookErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "outage_webhook_deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "outage_webhook_canceled"
	}
	return "outage_webhook_transport"
}

func gdPostLarkWebhook(t *testing.T, addr string, f *productionTestFixture, body []byte, timestamp, nonce string) gdWebhookResult {
	t.Helper()
	started := time.Now()
	result := gdWebhookResult{stage: gdStageOutageWebhook}
	ctx, cancel := context.WithTimeout(context.Background(), gdOutageWebhookDeadline)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/webhook/lark/"+f.larkExternalID, bytes.NewReader(body))
	if err != nil {
		result.category = "outage_webhook_request"
		result.elapsed = time.Since(started)
		return result
	}
	request.Header.Set("X-Lark-Request-Timestamp", timestamp)
	request.Header.Set("X-Lark-Request-Nonce", nonce)
	request.Header.Set("X-Lark-Signature", larkProductionSignature(timestamp, nonce, os.Getenv("P009GC_LARK_ENCRYPT_KEY"), body))
	response, err := (&http.Client{Timeout: gdOutageWebhookDeadline}).Do(request)
	result.elapsed = time.Since(started)
	if err != nil {
		result.category = gdWebhookErrorCategory(err)
		return result
	}
	result.status = response.StatusCode
	_, copyErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if copyErr != nil || closeErr != nil {
		if copyErr != nil {
			result.category = gdWebhookErrorCategory(copyErr)
		} else {
			result.category = "outage_webhook_transport"
		}
		return result
	}
	result.completeResponse = true
	if result.status == http.StatusServiceUnavailable {
		result.category = "service_unavailable"
	} else {
		result.category = "unexpected_status"
	}
	return result
}

func gdSchemaTable(schema, table string) string { return pgx.Identifier{schema, table}.Sanitize() }

func gdIngressJobIdentity(key storage.DedupKey) (string, string) {
	identity := strings.Join([]string{key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-job:"+identity)).String(),
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("trpc-agent-execution:"+identity)).String()
}

func openGDSchemaPool(t *testing.T, f *productionTestFixture) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: os.Getenv("TEST_DATABASE_URL"), SearchPath: f.schema, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type gdQueueState struct {
	status        string
	deliveryID    string
	deliveryCount int
	leasedUntil   time.Time
}

func gdReadQueueState(ctx context.Context, pool *pgxpool.Pool, f *productionTestFixture, jobID string) (gdQueueState, error) {
	var state gdQueueState
	err := pool.QueryRow(ctx, "SELECT status, COALESCE(delivery_id, ''), delivery_count, COALESCE(leased_until, 'epoch'::timestamptz) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&state.status, &state.deliveryID, &state.deliveryCount, &state.leasedUntil)
	return state, err
}

func gdWaitFor(t *testing.T, timeout time.Duration, check func(context.Context) (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	backoff := 25 * time.Millisecond
	var lastErr error
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining > 500*time.Millisecond {
			remaining = 500 * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		ok, err := check(ctx)
		cancel()
		if err == nil && ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		if !gdWaitRecoveryBackoff(deadline, backoff) {
			break
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	t.Fatalf("condition did not converge within %s: %v", timeout, lastErr)
}

func gdWaitQueueInFlight(t *testing.T, pool *pgxpool.Pool, f *productionTestFixture, jobID string) gdQueueState {
	t.Helper()
	var state gdQueueState
	gdWaitFor(t, 15*time.Second, func(ctx context.Context) (bool, error) {
		var err error
		state, err = gdReadQueueState(ctx, pool, f, jobID)
		return err == nil && state.status == "in_flight" && state.deliveryID != "", err
	})
	return state
}

func gdWaitDeliveryExpired(t *testing.T, pool *pgxpool.Pool, f *productionTestFixture, jobID string) {
	t.Helper()
	gdWaitFor(t, 10*time.Second, func(ctx context.Context) (bool, error) {
		// A queued/released job has no active lease; it is ready for Receive to reclaim.
		var queueExpired, leaseExpired bool
		err := pool.QueryRow(ctx, "SELECT COALESCE(leased_until <= clock_timestamp(), true) FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&queueExpired)
		if err != nil {
			return false, err
		}
		// The worker may release the session lease after the database returns; absence means no active session lease remains.
		err = pool.QueryRow(ctx, "SELECT COALESCE((SELECT leased_until <= clock_timestamp() FROM "+gdSchemaTable(f.schema, "session_lease")+" WHERE tenant_id=$1), true)", f.tenantID).Scan(&leaseExpired)
		return queueExpired && leaseExpired, err
	})
}

func gdWaitQueueCompletion(t *testing.T, pool *pgxpool.Pool, f *productionTestFixture, jobID, executionID string) {
	t.Helper()
	gdWaitFor(t, 15*time.Second, func(ctx context.Context) (bool, error) {
		var queueStatus, outboxStatus string
		var executions, outboxes int
		err := pool.QueryRow(ctx, "SELECT status FROM "+gdSchemaTable(f.schema, "job_queue")+" WHERE tenant_id=$1 AND job_id=$2", f.tenantID, jobID).Scan(&queueStatus)
		if err != nil {
			return false, err
		}
		err = pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "execution_result")+" WHERE tenant_id=$1 AND execution_id=$2", f.tenantID, executionID).Scan(&executions)
		if err != nil {
			return false, err
		}
		err = pool.QueryRow(ctx, "SELECT status FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, "reply-"+executionID).Scan(&outboxStatus)
		if err != nil {
			return false, err
		}
		err = pool.QueryRow(ctx, "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1", f.tenantID).Scan(&outboxes)
		return queueStatus == "acked" && executions == 1 && outboxes == 1 && outboxStatus == "completed", err
	})
}

func gdOutboxTestContext(f *productionTestFixture) tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: f.tenantID, AgentAppID: f.agentID, BindingID: f.larkBindingID, Channel: lark.Channel,
		ExternalUser: "ou_" + f.tenantID, ExternalChat: "oc_" + f.tenantID,
		SessionID: channels.SessionID(f.tenantID, lark.Channel, "ou_"+f.tenantID, "oc_"+f.tenantID),
		RequestID: "g-d-outbox-request", MessageID: "g-d-outbox-message", TraceID: "g-d-outbox-trace", ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"},
	}
}

func gdOutboxTestMessage(t *testing.T, f *productionTestFixture) (tenant.TenantContext, storage.OutboxMessage) {
	t.Helper()
	tc := gdOutboxTestContext(f)
	payload, err := channels.EncodeReplyOutboxPayload(channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: f.tenantID, SessionID: tc.SessionID, JobID: "g-d-outbox-job", ExecutionID: "g-d-outbox-execution",
		RequestID: tc.RequestID, MessageID: tc.MessageID, TraceID: tc.TraceID, BindingID: f.larkBindingID,
		Channel: lark.Channel, DestinationType: channels.DestinationTypeChat, DestinationID: tc.ExternalChat,
		ReplyText: "g-d outbox reply", SenderRoutingVersion: channels.SenderRoutingVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tc, storage.OutboxMessage{
		TenantID: f.tenantID, ID: "reply-g-d-outbox-execution", Kind: channels.ReplyOutboxKind,
		AggregateID: "g-d-outbox-execution", DedupKey: f.tenantID + "|g-d-outbox-execution|agent.reply", Payload: payload,
	}
}

func TestGDLifecycleOSProcessSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not support delivering SIGTERM/SIGINT to an exec child")
	}
	f := newProductionTestFixture(t)
	model := newGDModelServer(false)
	defer model.Close()
	provider := newGDProviderServer(false)
	defer provider.Close()
	binary := buildGDServiceBinary(t)
	addr := gdFreeHTTPAddr(t)
	process := startGDProcess(t, binary, f, addr, model.server.URL, provider.server.URL, f.ownerID)
	defer stopGDProcess(t, process)
	gdWaitReady(t, process, addr, 30*time.Second)

	client := &http.Client{Timeout: 500 * time.Millisecond}
	response, err := client.Get("http://" + addr + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("livez status=%d", response.StatusCode)
	}
	signalAt := time.Now()
	if err := process.terminate(); err != nil {
		t.Fatalf("send termination signal: %v", err)
	}
	if err := gdWaitProcessExit(t, process, 8*time.Second); err != nil {
		t.Fatalf("clean SIGTERM exit=%v output=%s", err, process.output())
	}
	if elapsed := time.Since(signalAt); elapsed > 3*time.Second {
		t.Fatalf("clean SIGTERM exceeded shutdown deadline: %s", elapsed)
	}
	if process.cmd.ProcessState == nil || process.cmd.ProcessState.ExitCode() != processExitOK {
		exitCode := -1
		if process.cmd.ProcessState != nil {
			exitCode = process.cmd.ProcessState.ExitCode()
		}
		t.Fatalf("clean SIGTERM exit code=%d output=%s", exitCode, process.output())
	}
	if !strings.Contains(process.output(), "service stopped status=clean") {
		t.Fatalf("SIGTERM did not report clean coordinator status: %s", process.output())
	}
	response, err = client.Get("http://" + addr + "/livez")
	if err == nil {
		response.Body.Close()
		t.Fatalf("listener remained available after process exit: status=%d", response.StatusCode)
	}
	gdAssertSafeOutput(t, process.output(), os.Getenv("TEST_DATABASE_URL"), os.Getenv("P009GC_LARK_APP_SECRET"), os.Getenv("P009GC_TELEGRAM_BOT_TOKEN"))
}

func TestGDLifecycleOSProcessStartupAndListenFailures(t *testing.T) {
	binary := buildGDServiceBinary(t)
	t.Run("listen failure", func(t *testing.T) {
		f := newProductionTestFixture(t)
		model := newGDModelServer(false)
		defer model.Close()
		provider := newGDProviderServer(false)
		defer provider.Close()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		defer listener.Close()
		process := startGDProcess(t, binary, f, addr, model.server.URL, provider.server.URL, f.ownerID)
		err = gdWaitProcessExit(t, process, 8*time.Second)
		if err == nil {
			t.Fatalf("listen failure exited successfully; output=%s", process.output())
		}
		gdAssertSafeOutput(t, process.output(), os.Getenv("TEST_DATABASE_URL"), "g-d-model-secret")
	})

	t.Run("startup dependency failure", func(t *testing.T) {
		f := newProductionTestFixture(t)
		model := newGDModelServer(false)
		defer model.Close()
		provider := newGDProviderServer(false)
		defer provider.Close()
		process := &gdProcess{
			cmd:    exec.Command(binary),
			stdout: gdOutput{limit: gdProcessOutputLimit},
			stderr: gdOutput{limit: gdProcessOutputLimit},
			done:   make(chan struct{}),
		}
		process.cmd.Dir = gdRepoRoot(t)
		env := gdChildEnv(f, gdFreeHTTPAddr(t), model.server.URL, provider.server.URL, f.ownerID)
		for i := range env {
			if strings.HasPrefix(env[i], "DATABASE_URL=") {
				env[i] = "DATABASE_URL=postgres://g-d-invalid.invalid:5432/g-d-invalid"
			}
		}
		process.cmd.Env = env
		process.cmd.Stdout = &process.stdout
		process.cmd.Stderr = &process.stderr
		process.start(t)
		err := gdWaitProcessExit(t, process, 10*time.Second)
		if err == nil {
			t.Fatalf("startup dependency failure exited successfully; output=%s", process.output())
		}
		gdAssertSafeOutput(t, process.output(), "g-d-invalid.invalid", os.Getenv("P009GC_LARK_APP_SECRET"))
	})
}

func TestGDSIGKILLReclaimsQueueDelivery(t *testing.T) {
	f := newProductionTestFixture(t)
	model := newGDModelServer(true)
	defer model.Close()
	provider := newGDProviderServer(false)
	defer provider.Close()
	binary := buildGDServiceBinary(t)
	pool := openGDSchemaPool(t, f)
	addrA := gdFreeHTTPAddr(t)
	processA := startGDProcess(t, binary, f, addrA, model.server.URL, provider.server.URL, f.ownerID)
	defer stopGDProcess(t, processA)
	gdWaitReady(t, processA, addrA, 30*time.Second)
	seedProductionRows(t, f, pool)

	eventID := "g-d-crash-event-" + f.tenantID
	body, timestamp, nonce := larkProductionEvent(t, f, eventID, "g-d-crash-message-"+f.tenantID)
	webhook := gdPostLarkWebhook(t, addrA, f, body, timestamp, nonce)
	if !webhook.completeResponse || webhook.status != http.StatusAccepted {
		t.Fatalf("crash webhook status=%d complete_response=%t category=%s elapsed=%s", webhook.status, webhook.completeResponse, webhook.category, webhook.elapsed)
	}
	key := storage.DedupKey{TenantID: f.tenantID, Channel: lark.Channel, BindingID: f.larkBindingID, ExternalMessageID: eventID}
	jobID, executionID := gdIngressJobIdentity(key)
	gdWaitCounter(t, &model.calls, 1, 15*time.Second)
	oldState := gdWaitQueueInFlight(t, pool, f, jobID)
	oldDeliveryID := oldState.deliveryID

	if err := processA.kill(); err != nil {
		t.Fatalf("SIGKILL process A: %v", err)
	}
	if err := gdWaitProcessExit(t, processA, 10*time.Second); err == nil {
		t.Fatal("SIGKILL process A exited successfully")
	}
	gdWaitDeliveryExpired(t, pool, f, jobID)

	addrB := gdFreeHTTPAddr(t)
	processB := startGDProcess(t, binary, f, addrB, model.server.URL, provider.server.URL, f.ownerID+"-b")
	defer stopGDProcess(t, processB)
	gdWaitReady(t, processB, addrB, 30*time.Second)
	gdWaitCounter(t, &model.calls, 2, 15*time.Second)
	model.Release()
	gdWaitQueueCompletion(t, pool, f, jobID, executionID)
	finalState, err := gdReadQueueState(context.Background(), pool, f, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if finalState.deliveryCount < 2 || finalState.deliveryID != "" || oldDeliveryID == "" {
		t.Fatalf("queue reclaim state=%+v old_delivery=%s", finalState, oldDeliveryID)
	}
	queueStore, err := queue.NewPostgresQueue(pool, queue.PostgresQueueConfig{PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	oldDelivery := queue.Delivery{DeliveryID: oldDeliveryID, Job: queue.AgentJob{JobID: jobID, Tenant: queue.TenantContextDTO{TenantID: f.tenantID}}}
	if err := queueStore.Ack(context.Background(), oldDelivery); !errors.Is(err, queue.ErrDeliveryFinished) && !errors.Is(err, queue.ErrDeliveryExpired) {
		t.Fatalf("stale delivery ack=%v", err)
	}
	gdStopAfterEvidence(t, processB)
	gdAssertSafeOutput(t, processA.output()+processB.output(), os.Getenv("TEST_DATABASE_URL"), os.Getenv("P009GC_LARK_APP_SECRET"), os.Getenv("P009GC_TELEGRAM_BOT_TOKEN"))
}

func TestGDOutboxLockReclaimAfterSIGKILL(t *testing.T) {
	f := newProductionTestFixture(t)
	model := newGDModelServer(false)
	defer model.Close()
	provider := newGDProviderServer(true)
	defer provider.Close()
	binary := buildGDServiceBinary(t)
	pool := openGDSchemaPool(t, f)
	tc, message := gdOutboxTestMessage(t, f)

	addrA := gdFreeHTTPAddr(t)
	processA := startGDProcess(t, binary, f, addrA, model.server.URL, provider.server.URL, f.ownerID)
	defer stopGDProcess(t, processA)
	gdWaitReady(t, processA, addrA, 30*time.Second)
	seedProductionRows(t, f, pool)
	repository, err := postgres.NewOutboxRepository(pool, postgres.OutboxRepositoryConfig{LockDuration: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Enqueue(context.Background(), tc, message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.sendStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("process A did not claim the outbox")
	}
	gdWaitFor(t, 5*time.Second, func(ctx context.Context) (bool, error) {
		var status, owner string
		var lockedUntil time.Time
		err := pool.QueryRow(ctx, "SELECT status, COALESCE(locked_by, ''), locked_until FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, message.ID).Scan(&status, &owner, &lockedUntil)
		return status == "processing" && owner == f.ownerID && !lockedUntil.IsZero(), err
	})
	if err := processA.kill(); err != nil {
		t.Fatalf("SIGKILL outbox process A: %v", err)
	}
	if err := gdWaitProcessExit(t, processA, 10*time.Second); err == nil {
		t.Fatal("outbox process A exited successfully after SIGKILL")
	}
	gdWaitFor(t, 10*time.Second, func(ctx context.Context) (bool, error) {
		var expired bool
		err := pool.QueryRow(ctx, "SELECT locked_until <= clock_timestamp() FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, message.ID).Scan(&expired)
		return expired, err
	})
	if err := repository.MarkCompleted(context.Background(), tc, f.ownerID, message.ID); !errors.Is(err, storage.ErrOutboxLockExpired) && !errors.Is(err, storage.ErrOutboxLockLost) && !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale outbox owner mutation=%v", err)
	}
	provider.Release()

	addrB := gdFreeHTTPAddr(t)
	processB := startGDProcess(t, binary, f, addrB, model.server.URL, provider.server.URL, f.ownerID+"-b")
	defer stopGDProcess(t, processB)
	gdWaitReady(t, processB, addrB, 30*time.Second)
	gdWaitFor(t, 15*time.Second, func(ctx context.Context) (bool, error) {
		var status string
		err := pool.QueryRow(ctx, "SELECT status FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, message.ID).Scan(&status)
		return status == "completed", err
	})
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+gdSchemaTable(f.schema, "outbox_message")+" WHERE tenant_id=$1 AND outbox_id=$2", f.tenantID, message.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("outbox logical count=%d", count)
	}
	gdStopAfterEvidence(t, processB)
	gdAssertSafeOutput(t, processA.output()+processB.output(), os.Getenv("TEST_DATABASE_URL"), os.Getenv("P009GC_LARK_APP_SECRET"), os.Getenv("P009GC_TELEGRAM_BOT_TOKEN"))
}
