//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/migrations"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelcatalog"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// TestProcessRecovery crosses the actual Consumer -> authenticated Worker HTTP
// boundary. Each case owns a fresh database; only the external model is a local
// OpenAI protocol fixture. No Delivery process or live IM account is involved.
// The PostgreSQL test role must have CREATEDB. Child executables are race-built
// from the current production commands. A build-time Go overlay injects only
// the local provider HTTP client; no production environment bypass is enabled.
func TestProcessRecovery(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" || os.Getenv("TEST_REDIS_URL") == "" {
		t.Fatal("TEST_DATABASE_URL and TEST_REDIS_URL are required")
	}
	binaries := processRecoveryBuild(t)
	for _, scenario := range []string{"claim_before_worker_dispatch", "committed_receipt_before_consumer_completion"} {
		t.Run(scenario, func(t *testing.T) { runProcessRecovery(t, binaries, scenario) })
	}
}

const processRecoveryMasterKey = "process-recovery-test-master-key-32-bytes"
const processRecoveryProfiles = `[{"id":"process-postgres","backend":"postgres","connectionEnv":"DATABASE_URL","allowInsecure":true}]`

func runProcessRecovery(t *testing.T, binaries map[string]string, scenario string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, dsn := processRecoveryDatabase(t)
	redisOptions, err := redisv8.ParseURL(os.Getenv("TEST_REDIS_URL"))
	if err != nil {
		t.Fatal("invalid TEST_REDIS_URL")
	}
	rdb := redisv8.NewClient(redisOptions)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal("process recovery Redis unavailable")
	}

	// This established protocol fixture checks the exact allowed memory_add
	// tool and requires its durable result in the second model request. Any
	// third provider request fails, including after the Worker is restarted.
	provider := &wecomE2EProvider{errors: make(chan string, 16), stop: make(chan struct{})}
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "unexpected model path", 400)
			return
		}
		provider.serveModel(w, r)
	}))
	t.Cleanup(modelServer.Close)

	repo, err := tenant.NewSQLRepository("postgres", dsn)
	if err != nil {
		t.Fatal("open process recovery tenant repository")
	}
	validator, err := storage.LoadBackendProfileValidator(processRecoveryProfiles)
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenant.NewService(repo, processRecoveryMasterKey, tenant.WithStorageConfigValidator(
		func(_ context.Context, id string, cfg tenant.StorageConfig) error {
			return validator.ValidateTenantStorage(id, cfg)
		}))
	t.Cleanup(func() { _ = tenants.Close() })
	tn, err := tenants.CreateTenant(tenant.ContextWithAuditActor(ctx, "process-recovery"), "Process recovery", tenant.TenantConfig{
		Agents:     []tenant.AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt-4o-mini", MaxLLMCalls: 2, Tools: []string{memory.AddToolName}}},
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: wecomE2EModelKey, MaxTokens: 256}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{memory.AddToolName}},
		Storage:    tenant.StorageConfig{SessionBackend: "postgres", SessionProfile: "process-postgres", MemoryBackend: "postgres", MemoryProfile: "process-postgres"},
	})
	if err != nil {
		t.Fatalf("create process tenant: %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		// UUID tenant IDs delimit all budget/session-lock keys owned by this
		// case. Never flush Redis or delete another run's service nonces.
		iter := rdb.Scan(cleanup, 0, "*"+tn.ID+"*", 100).Iterator()
		for iter.Next(cleanup) {
			if err := rdb.Del(cleanup, iter.Val()).Err(); err != nil {
				t.Error("clean process tenant Redis key")
			}
		}
		if err := iter.Err(); err != nil {
			t.Error("scan process tenant Redis keys")
		}
	})
	processRecoveryDeploy(t, ctx, db, tn)

	profiles, err := storage.LoadBackendProfiles(processRecoveryProfiles, func(name string) (string, bool) {
		if name == "DATABASE_URL" {
			return dsn, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal("load process storage profiles")
	}
	observer := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles})
	t.Cleanup(func() { _ = observer.Close() })
	workerPort := processRecoveryPort(t)
	workerURL := "http://127.0.0.1:" + workerPort
	gate := &processRecoveryGate{target: workerURL, afterCommit: scenario == "committed_receipt_before_consumer_completion", reached: make(chan struct{}), stop: make(chan struct{}), failed: make(chan int, 1), nonces: make(map[string]bool)}
	gate.transport = &http.Transport{Proxy: nil}
	t.Cleanup(gate.transport.CloseIdleConnections)
	gateServer := httptest.NewServer(gate)
	t.Cleanup(func() {
		close(gate.stop)
		gateServer.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		gate.mu.Lock()
		defer gate.mu.Unlock()
		for key := range gate.nonces {
			if err := rdb.Del(cleanup, key).Err(); err != nil {
				t.Error("clean process-owned service nonce")
			}
		}
	})
	baseEnv := processRecoveryEnvironment(map[string]string{
		"DATABASE_URL": dsn, "DATABASE_ALLOW_INSECURE": "true", "REDIS_URL": os.Getenv("TEST_REDIS_URL"),
		"MASTER_KEY": processRecoveryMasterKey, "MASTER_KEY_RING": "", "ACTIVE_MASTER_KEY_ID": "", "MASTER_KEY_REF": "", "MASTER_KEY_RING_REF": "",
		"SERVICE_AUTH_SECRET": "process-recovery-service-auth-secret-32-bytes", "STORAGE_BACKEND_PROFILES": processRecoveryProfiles, "DATA_PLANE_PROFILES": "[]",
		"TRPC_OPENAI_BASE_URL": modelServer.URL + "/v1/", "OPENAI_BASE_URL": "", "EXECUTION_TIMEOUT": "5s", "WORKER_EXECUTION_TIMEOUT": "5s", "PROCESS_TIMEOUT": "40s", "LEASE_DURATION": "45s",
		"EXECUTION_LEASE_TTL": "1m", "EXECUTION_HEARTBEAT_INTERVAL": "5s", "WORKER_ENDPOINT": gateServer.URL, "WORKER_TRANSPORT_MODE": "development",
		"CONCURRENCY": "1", "POLL_INTERVAL": "20ms", "EXPIRY_REAP_INTERVAL": "1s", "FAIR_QUEUE_ENABLED": "false", "SHUTDOWN_TIMEOUT": "75s",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "", "OTEL_TRACES_EXPORTER": "none", "MCP_PROFILES": "[]",
		// Unexpected external HTTP/CONNECT requests are rejected by the local
		// fixture. Database clients do not use the HTTP proxy environment.
		"HTTP_PROXY": modelServer.URL, "HTTPS_PROXY": modelServer.URL, "ALL_PROXY": modelServer.URL, "NO_PROXY": "127.0.0.1,localhost",
	})
	agentProcess := processRecoveryStart(t, binaries["worker"], baseEnv, workerPort, "worker-first")
	processRecoveryHealthy(t, ctx, workerURL, agentProcess)

	store := reliable.NewPostgresStore(db)
	inbound := &channel.InboundMessage{TenantID: tn.ID, ChannelType: "telegram", ChannelAccountID: "process-account", ExternalUserID: wecomE2EUser, ConversationID: "process-conversation", MessageID: "process-message", Content: wecomE2EPrompt}
	identity, err := channel.BuildSessionIdentity(inbound)
	if err != nil {
		t.Fatal(err)
	}
	inbound.SessionOwnerID = identity.SessionOwnerID
	payload, err := json.Marshal(inbound)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	msg := &reliable.InboxMessage{TenantID: tn.ID, ChannelType: inbound.ChannelType, ChannelAccountID: inbound.ChannelAccountID, AgentApp: "support", ExternalMessageID: inbound.MessageID, ConversationID: inbound.ConversationID, UserID: inbound.ExternalUserID, SessionID: identity.SessionID, SessionOwnerID: identity.SessionOwnerID, RoutingVersion: 1, Payload: payload, PayloadHash: hex.EncodeToString(digest[:])}
	if inserted, err := store.EnqueueInbox(ctx, msg); err != nil || !inserted {
		t.Fatalf("durable receipt inserted=%v err=%v", inserted, err)
	}
	if duplicate, err := store.EnqueueInbox(ctx, msg); err != nil || duplicate {
		t.Fatalf("receipt dedup inserted=%v err=%v", duplicate, err)
	}
	consumerPort := processRecoveryPort(t)
	first := processRecoveryStart(t, binaries["consumer"], baseEnv, consumerPort, "consumer-first")
	processRecoveryHealthy(t, ctx, "http://127.0.0.1:"+consumerPort, first)
	select {
	case <-gate.reached:
	case status := <-gate.failed:
		t.Fatalf("Worker rejected the first process request: HTTP %d", status)
	case message := <-provider.errors:
		t.Fatal(message)
	case <-ctx.Done():
		t.Fatal("fault gate was not reached")
	}
	var old reliable.Lease
	var state string
	if err := db.QueryRowContext(ctx, `SELECT status,lease_owner,lease_version,lease_until FROM inbox_messages WHERE id=$1`, msg.ID).Scan(&state, &old.Owner, &old.Fence, &old.Until); err != nil {
		t.Fatal(err)
	}
	if state != "PROCESSING" || old.Fence != 1 {
		t.Fatalf("fault boundary inbox=%s fence=%d", state, old.Fence)
	}
	processRecoveryCount(t, db, `SELECT count(*) FROM outbox_messages`, 0)
	if gate.afterCommit {
		processRecoveryCount(t, db, `SELECT count(*) FROM execution_records WHERE status='SUCCEEDED'`, 1)
		processRecoveryCount(t, db, `SELECT count(*) FROM invocation_results r JOIN execution_records e ON e.id=r.execution_id AND e.tenant_id=r.tenant_id WHERE e.status='SUCCEEDED'`, 1)
		processRecoveryEffects(t, ctx, observer, tn, identity, inbound)
	} else {
		processRecoveryCount(t, db, `SELECT count(*) FROM execution_records`, 0)
		if provider.modelCalls.Load() != 0 {
			t.Fatal("model ran before dispatch gate")
		}
	}
	first.kill(t)
	if gate.afterCommit {
		agentProcess.kill(t)
		agentProcess = processRecoveryStart(t, binaries["worker"], baseEnv, workerPort, "worker-restarted")
		processRecoveryHealthy(t, ctx, workerURL, agentProcess)
	}
	// Wait for actual database time to expire the lease, without modifying any
	// queue clock/status. A zombie consumer must be unable to complete under it.
	processRecoveryEventually(t, ctx, "original Inbox lease expiry", func() bool {
		var expired bool
		return db.QueryRowContext(ctx, `SELECT now()>lease_until FROM inbox_messages WHERE id=$1`, msg.ID).Scan(&expired) == nil && expired
	})
	if _, err := store.CompleteInbox(ctx, msg.ID, old, reliable.OutboxReply{ContentType: "text", Content: "stale consumer reply"}); !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("expired completion error=%v, want stale lease", err)
	}
	processRecoveryCount(t, db, `SELECT count(*) FROM outbox_messages`, 0)
	secondPort := processRecoveryPort(t)
	second := processRecoveryStart(t, binaries["consumer"], baseEnv, secondPort, "consumer-restarted")
	processRecoveryHealthy(t, ctx, "http://127.0.0.1:"+secondPort, second)
	processRecoveryEventually(t, ctx, "recovered Inbox completion", func() bool {
		select {
		case message := <-provider.errors:
			t.Fatal(message)
		default:
		}
		var completed bool
		return db.QueryRowContext(ctx, `SELECT status='COMPLETED' AND lease_version=2 AND attempt_count=2 FROM inbox_messages WHERE id=$1`, msg.ID).Scan(&completed) == nil && completed
	})
	second.kill(t)
	agentProcess.kill(t)
	if _, err := store.CompleteInbox(ctx, msg.ID, old, reliable.OutboxReply{ContentType: "text", Content: "late stale reply"}); !errors.Is(err, reliable.ErrStaleLease) {
		t.Fatalf("old fence after recovery error=%v, want stale lease", err)
	}
	processRecoveryCount(t, db, `SELECT count(*) FROM inbox_messages`, 1)
	processRecoveryCount(t, db, `SELECT count(*) FROM execution_records`, 1)
	processRecoveryCount(t, db, `SELECT count(*) FROM execution_records WHERE status='SUCCEEDED' AND attempt_number=1`, 1)
	processRecoveryCount(t, db, `SELECT count(*) FROM invocation_results`, 1)
	processRecoveryCount(t, db, `SELECT count(*) FROM outbox_messages WHERE status='REPLY_PENDING' AND content='Your receipt preference has been recorded.'`, 1)
	processRecoveryCount(t, db, `SELECT count(*) FROM outbox_messages`, 1)
	if provider.modelCalls.Load() != 2 {
		t.Fatalf("model calls=%d, want exactly 2", provider.modelCalls.Load())
	}
	processRecoveryEffects(t, ctx, observer, tn, identity, inbound)
	t.Logf("real process recovery verified: scenario=%s inbox=1 claims=2 fences=1->2 executions=1 receipts=1 outbox=1 model_calls=2 memory_add=1; no IM delivery", scenario)
}

func processRecoveryDeploy(t *testing.T, ctx context.Context, db *sql.DB, tn *tenant.Tenant) {
	t.Helper()
	snapshot := controlplane.VersionSnapshot{Agent: tn.Agents[0], Model: tn.Models[0]}
	snapshot.Model.APIKey = ""
	service := controlplane.NewService(db, func(_ context.Context, id string, value *controlplane.VersionSnapshot) error {
		if id != tn.ID {
			return errors.New("unexpected fixture tenant")
		}
		profile, ok := modelcatalog.Resolve(value.Model.Provider, value.Model.ModelName)
		if !ok {
			return errors.New("fixture model absent from operator catalog")
		}
		value.ModelCatalogRevision, value.ModelContextWindow = profile.Revision, profile.ContextWindow
		value.RuntimeCapabilityFingerprint = worker.NewRuntimeAgentRegistry().Fingerprint()
		return tenant.ValidatePinnedAgentModelBudget(value.Agent, value.Model, tn.Budget, profile.Revision, profile.ContextWindow)
	})
	if _, err := service.CreateApp(ctx, tn.ID, "support", "Process recovery fixture", "process-recovery"); err != nil {
		t.Fatal(err)
	}
	version, err := service.CreateVersion(ctx, tn.ID, "support", "process-recovery", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.PublishVersion(ctx, tn.ID, version.ID, "process-recovery"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deploy(ctx, tn.ID, "support", version.ID, "", 0, "process-recovery"); err != nil {
		t.Fatal(err)
	}
}

func processRecoveryEffects(t *testing.T, ctx context.Context, observer *storage.MultiTenantStorageAdapterImpl, tn *tenant.Tenant, identity channel.SessionIdentity, inbound *channel.InboundMessage) {
	t.Helper()
	sess, err := observer.GetSession(ctx, tn, session.Key{AppName: "support", UserID: identity.SessionOwnerID, SessionID: identity.SessionID})
	if err != nil {
		t.Fatalf("read process transcript: %v", err)
	}
	toolEvents := 0
	for _, event := range sess.Events {
		if event.Response != nil {
			for _, choice := range event.Response.Choices {
				if choice.Message.Role == model.RoleTool {
					toolEvents++
				}
			}
		}
	}
	if toolEvents != 1 {
		t.Fatalf("persisted memory tool events=%d, want 1", toolEvents)
	}
	appName, err := storage.TenantScopedAppName(tn, "support")
	if err != nil {
		t.Fatal(err)
	}
	actor, err := channel.MemoryActorID(tn.ID, inbound.ChannelType, inbound.ChannelAccountID, inbound.ExternalUserID)
	if err != nil {
		t.Fatal(err)
	}
	memories, err := observer.MemoryService(ctx, tn)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := memories.ReadMemories(ctx, memory.UserKey{AppName: appName, UserID: actor}, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != wecomE2EMemory {
		t.Fatalf("persisted memories=%d err=%v, want exactly one expected memory", len(entries), err)
	}
}

type processRecoveryGate struct {
	target      string
	afterCommit bool
	reached     chan struct{}
	stop        chan struct{}
	failed      chan int
	requests    atomic.Int64
	transport   *http.Transport
	mu          sync.Mutex
	nonces      map[string]bool
}

func (g *processRecoveryGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	process := r.URL.Path == worker.ExecutionContractProcessPath
	first := process && g.requests.Add(1) == 1
	if process {
		g.mu.Lock()
		g.nonces["service-auth:nonce:"+r.Header.Get("X-Service-Name")+":"+r.Header.Get("X-Service-Nonce")] = true
		g.mu.Unlock()
	}
	if first && !g.afterCommit {
		// Reading the body lets net/http monitor connection closure while the
		// handler holds the response; otherwise cleanup could await unread data.
		_, _ = io.Copy(io.Discard, r.Body)
		close(g.reached)
		select {
		case <-r.Context().Done():
		case <-g.stop:
		}
		return
	}
	req := r.Clone(r.Context())
	target, _ := url.Parse(g.target)
	req.URL.Scheme, req.URL.Host, req.Host, req.RequestURI = target.Scheme, target.Host, target.Host, ""
	response, err := g.transport.RoundTrip(req)
	if err != nil {
		http.Error(w, "process gate upstream unavailable", 502)
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		http.Error(w, "process gate upstream incomplete", 502)
		return
	}
	if first && g.afterCommit && response.StatusCode == http.StatusOK {
		close(g.reached)
		select {
		case <-r.Context().Done():
		case <-g.stop:
		}
		return
	}
	if first && g.afterCommit {
		g.failed <- response.StatusCode
	}
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(body)
}

func processRecoveryDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		t.Fatal("process recovery requires a PostgreSQL URL")
	}
	admin, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal("open database creation connection")
	}
	t.Cleanup(func() { _ = admin.Close() })
	name := "process_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+pq.QuoteIdentifier(name)); err != nil {
		t.Fatal("create dedicated process database (TEST_DATABASE_URL role needs CREATEDB)")
	}
	parsed.Path, parsed.RawPath = "/"+name, ""
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal("open dedicated process database")
	}
	t.Cleanup(func() {
		_ = db.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, `DROP DATABASE `+pq.QuoteIdentifier(name)); err != nil {
			t.Errorf("drop owned process database %s: %v", name, err)
		}
	})
	if err := migrations.NewRunner(db).Up(ctx); err != nil {
		t.Fatal(err)
	}
	return db, parsed.String()
}

func processRecoveryBuild(t *testing.T) map[string]string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	overlay := processRecoveryModelOverlay(t, root, directory)
	result := make(map[string]string)
	for _, name := range []string{"worker", "consumer"} {
		path := filepath.Join(directory, name)
		if runtime.GOOS == "windows" {
			path += ".exe"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		args := []string{"build", "-race", "-buildvcs=false", "-p=1"}
		if name == "worker" {
			args = append(args, "-overlay="+overlay)
		}
		args = append(args, "-o", path, "./cmd/"+name)
		cmd := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), args...)
		cmd.Dir = root
		cmd.WaitDelay = 5 * time.Second
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build actual %s process: %v\n%s", name, err, output)
		}
		result[name] = path
	}
	return result
}

// processRecoveryModelOverlay changes only the existing private model-client
// construction seam in this test executable. The factory, actual SDK, Runner,
// auth, durable execution and storage code remain the current production code.
// The fixture client rejects every URL other than its explicit loopback origin
// and completion path, and does not use proxies or follow redirects.
func processRecoveryModelOverlay(t *testing.T, root, directory string) string {
	t.Helper()
	original := filepath.Join(root, "pkg", "worker", "model_factory.go")
	source, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	const constructor = "return &ModelFactory{}"
	const imports = "\"os\""
	if strings.Count(text, constructor) != 1 || strings.Count(text, imports) != 1 {
		t.Fatal("model factory test seam changed; update the explicit overlay")
	}
	text = strings.Replace(text, constructor, "return &ModelFactory{endpointClient: newProcessRecoveryFixtureClient}", 1)
	text = strings.Replace(text, imports, imports+"\n recoveryhttp \"net/http\"\n recoveryurl \"net/url\"\n recoverytime \"time\"", 1)
	text += `
type processRecoveryFixtureClient struct { client *recoveryhttp.Client; origin string }
func newProcessRecoveryFixtureClient(raw string) (openaiopt.HTTPClient,error) {
 u,err:=recoveryurl.Parse(raw)
 if err!=nil || u.Scheme!="http" || u.Hostname()!="127.0.0.1" || u.Port()=="" || u.User!=nil || u.RawQuery!="" || u.Fragment!="" || (u.Path!="/v1" && u.Path!="/v1/") { return nil,fmt.Errorf("invalid loopback model fixture") }
 return &processRecoveryFixtureClient{origin:u.Host,client:&recoveryhttp.Client{
  Transport:&recoveryhttp.Transport{Proxy:nil},Timeout:10*recoverytime.Second,
  CheckRedirect:func(*recoveryhttp.Request,[]*recoveryhttp.Request)error{return recoveryhttp.ErrUseLastResponse},
 }},nil
}
func(c *processRecoveryFixtureClient) Do(req *recoveryhttp.Request)(*recoveryhttp.Response,error){
 if req==nil || req.URL.Scheme!="http" || req.URL.Host!=c.origin || req.URL.Path!="/v1/chat/completions" || req.URL.RawQuery!="" || req.URL.User!=nil { return nil,fmt.Errorf("request escaped loopback model fixture") }
 return c.client.Do(req)
}
`
	formatted, err := format.Source([]byte(text))
	if err != nil {
		t.Fatal("invalid model fixture overlay source")
	}
	replacement := filepath.Join(directory, "model_factory.go")
	if err := os.WriteFile(replacement, formatted, 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(map[string]any{"Replace": map[string]string{original: replacement}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "model-fixture-overlay.json")
	if err := os.WriteFile(path, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

type processRecoveryChild struct {
	cmd     *exec.Cmd
	done    chan struct{}
	logPath string
	mu      sync.Mutex
	killed  bool
	err     error
}

func processRecoveryStart(t *testing.T, path string, environment []string, port, label string) *processRecoveryChild {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), label+".log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path)
	cmd.Env = append(append([]string(nil), environment...), "PORT="+port, "CONSUMER_ID="+label)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatalf("start %s: %v", label, err)
	}
	child := &processRecoveryChild{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() { child.err = cmd.Wait(); _ = log.Close(); close(child.done) }()
	t.Cleanup(func() {
		child.kill(t)
		output, _ := os.ReadFile(logPath)
		if strings.Contains(string(output), "DATA RACE") {
			t.Errorf("race detected in %s\n%s", label, output)
		}
		if t.Failed() {
			t.Logf("%s process log:\n%s", label, output)
		}
	})
	return child
}

func (p *processRecoveryChild) kill(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.killed {
		return
	}
	select {
	case <-p.done:
		t.Errorf("process exited before intended crash: %v", p.err)
		p.killed = true
		return
	default:
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Errorf("kill owned process: %v", err)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Error("owned process did not exit after kill")
	}
	p.killed = true
}

func processRecoveryHealthy(t *testing.T, ctx context.Context, base string, child *processRecoveryChild) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	processRecoveryEventually(t, ctx, "process health", func() bool {
		select {
		case <-child.done:
			t.Fatalf("process exited during startup: %v", child.err)
		default:
		}
		r, err := client.Get(base + "/health")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
		return r.StatusCode == http.StatusOK
	})
}

func processRecoveryEventually(t *testing.T, ctx context.Context, label string, check func() bool) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func processRecoveryCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil || count != want {
		t.Fatalf("%s: count=%d want=%d err=%v", query, count, want, err)
	}
}

func processRecoveryPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	_ = listener.Close()
	return port
}

func processRecoveryEnvironment(overrides map[string]string) []string {
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if _, overridden := overrides[strings.ToUpper(name)]; overridden || strings.EqualFold(name, "PORT") || strings.EqualFold(name, "CONSUMER_ID") {
			continue
		}
		result = append(result, item)
	}
	for name, value := range overrides {
		result = append(result, name+"="+value)
	}
	return result
}
