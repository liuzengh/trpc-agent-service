package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type phase55RecoveryExecutor struct {
	backend      *storage.SQLBackend
	controlDir   string
	mu           sync.Mutex
	agentCalls   int
	persistCalls int
}

func (*phase55RecoveryExecutor) Ready(context.Context) error { return nil }
func (*phase55RecoveryExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, fmt.Errorf("legacy execution is not expected")
}
func (e *phase55RecoveryExecutor) PersistenceRoute(_ context.Context, task message.ExecutionTask) (persistence.Route, error) {
	return persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: e.backend.Fingerprint()}, nil
}
func (e *phase55RecoveryExecutor) ExecuteFenced(_ context.Context, task message.ExecutionTask, fence sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.mu.Lock()
	e.agentCalls++
	e.mu.Unlock()
	if e.controlDir != "" {
		if err := os.WriteFile(filepath.Join(e.controlDir, "request-stop"), []byte("stop"), 0o600); err != nil {
			return message.OutboundMessage{}, sessionfence.TurnCommit{}, err
		}
		if err := waitPhase55File(filepath.Join(e.controlDir, "stopped"), 30*time.Second); err != nil {
			return message.OutboundMessage{}, sessionfence.TurnCommit{}, err
		}
	}
	current := event.New("phase55-recovery-event", "user")
	current.Response = &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "recover"}}}}
	return message.OutboundMessage{Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "recovered"}, sessionfence.TurnCommit{
		SessionCoord: fence.SessionCoord, SessionSeq: fence.SessionSeq, AppName: tenant.AppName(task.TenantID, task.AgentAppID),
		UserID: task.RunnerUserID, SessionID: task.SessionID, Events: []event.Event{*current}, FinalState: session.StateMap{"result": []byte(`"recovered"`)},
	}, nil
}
func (e *phase55RecoveryExecutor) Persist(ctx context.Context, envelope persistence.Envelope) error {
	e.mu.Lock()
	e.persistCalls++
	e.mu.Unlock()
	if err := e.backend.Ready(ctx); err != nil {
		return err
	}
	return e.backend.Committer().Commit(ctx, envelope)
}

type phase55ModelConcurrency struct {
	mu             sync.Mutex
	activeSerial   int
	serialOverlap  bool
	activeTotal    int
	maxActiveTotal int
}

func (c *phase55ModelConcurrency) serveHTTP(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var body struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	content := ""
	for index := len(body.Messages) - 1; index >= 0; index-- {
		if body.Messages[index].Role == "user" {
			content = body.Messages[index].Content
			break
		}
	}
	c.mu.Lock()
	if strings.HasPrefix(content, "serial-") {
		if c.activeSerial != 0 {
			c.serialOverlap = true
		}
		c.activeSerial++
	}
	c.activeTotal++
	if c.activeTotal > c.maxActiveTotal {
		c.maxActiveTotal = c.activeTotal
	}
	c.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	c.mu.Lock()
	if strings.HasPrefix(content, "serial-") {
		c.activeSerial--
	}
	c.activeTotal--
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "phase55", "object": "chat.completion", "created": 1, "model": "mock",
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "reply-" + content}, "finish_reason": "stop"}},
	})
}

func TestPhase55RedisPostgresMySQLTwoWorkersSmoke(t *testing.T) {
	redisURL := strings.TrimSpace(os.Getenv("PHASE55_REDIS_SMOKE_URL"))
	postgresDSN := strings.TrimSpace(os.Getenv("PHASE55_POSTGRES_DSN"))
	mysqlDSN := strings.TrimSpace(os.Getenv("PHASE55_MYSQL_DSN"))
	if redisURL == "" || postgresDSN == "" || mysqlDSN == "" {
		t.Skip("PHASE55_REDIS_SMOKE_URL, PHASE55_POSTGRES_DSN and PHASE55_MYSQL_DSN are required")
	}
	var err error
	redisURL, err = config.NormalizeRedisURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}

	modelConcurrency := &phase55ModelConcurrency{}
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelConcurrency.serveHTTP(t, w, r)
	}))
	defer modelServer.Close()

	stamp := fmt.Sprintf("%x", time.Now().UnixNano())
	messagingPrefix := "phase55-e2e:" + stamp
	postgresPrefix := "p" + stamp
	mysqlPrefix := "m" + stamp
	catalog := phase55SmokeCatalog(modelServer.URL+"/v1", messagingPrefix, postgresPrefix, mysqlPrefix)
	credentials := config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL": "model-key", "env:REDIS": redisURL, "env:POSTGRES": postgresDSN, "env:MYSQL": mysqlDSN,
	})
	msgConfig := config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: messagingPrefix,
		LeaseDuration: 2 * time.Second, HeartbeatInterval: 250 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: 10 * time.Second,
		SessionFencing: "strong", SessionLockDuration: 2 * time.Second,
		SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 50 * time.Millisecond,
		MaxTurnEvents: 64, MaxTurnBytes: 256 << 10, ShutdownTimeout: 3 * time.Second,
		PersistenceTimeout: 3 * time.Second, PersistenceMaxAttempts: 5,
		PersistenceInitialBackoff: 20 * time.Millisecond, PersistenceMaxBackoff: 100 * time.Millisecond,
		PersistencePayloadMaxBytes: 512 << 10,
	}

	newRuntime := func() *executor.Runtime {
		cfg, configErr := config.NewCatalogConfig(catalog, []byte("01234567890123456789012345678901"), credentials)
		if configErr != nil {
			t.Fatal(configErr)
		}
		cfg.Messaging = &msgConfig
		runtime, runtimeErr := executor.New(cfg)
		if runtimeErr != nil {
			t.Fatal(runtimeErr)
		}
		return runtime
	}
	runtimeA, runtimeB := newRuntime(), newRuntime()
	defer runtimeA.Close()
	defer runtimeB.Close()

	storeGateway, err := messaging.NewStore(msgConfig)
	if err != nil {
		t.Fatal(err)
	}
	storeA, err := messaging.NewStore(msgConfig)
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := messaging.NewStore(msgConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer storeGateway.Close()
	defer storeA.Close()
	defer storeB.Close()
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	gatewayService, err := gateway.New(router, storeGateway, "phase55-gateway")
	if err != nil {
		t.Fatal(err)
	}
	workerA, err := New(storeA, runtimeA, "phase55-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := New(storeB, runtimeB, "phase55-worker-b")
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 3)
	go func() { done <- gatewayService.Run(runCtx) }()
	go func() { done <- workerA.Run(runCtx) }()
	go func() { done <- workerB.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		for i := 0; i < 3; i++ {
			<-done
		}
		cleanupPhase55Redis(t, redisURL, messagingPrefix)
	})
	waitPhase55Ready(t, gatewayService, workerA, workerB)

	inbounds := []message.InboundMessage{
		phase55Inbound("demo-redis", "redis-shared", "shared"),
		phase55Inbound("demo-postgres", "serial-a", "shared"),
		phase55Inbound("demo-postgres", "serial-b", "shared"),
		phase55Inbound("demo-mysql", "mysql-shared", "shared"),
		phase55Inbound("demo-postgres", "parallel-a", "postgres-parallel"),
		phase55Inbound("demo-mysql", "parallel-b", "mysql-parallel"),
	}
	expectedTasks := make([]message.ExecutionTask, len(inbounds))
	for index, inbound := range inbounds {
		expectedTasks[index], err = router.Resolve(context.Background(), inbound)
		if err != nil {
			t.Fatal(err)
		}
	}
	type response struct {
		index int
		reply message.OutboundMessage
		err   error
	}
	responses := make(chan response, len(inbounds))
	for index, inbound := range inbounds {
		go func(index int, inbound message.InboundMessage) {
			reply, handleErr := gatewayService.Handle(context.Background(), inbound)
			responses <- response{index: index, reply: reply, err: handleErr}
		}(index, inbound)
	}
	for range inbounds {
		current := <-responses
		if current.err != nil || current.reply.Text != "reply-"+inbounds[current.index].Text {
			t.Fatalf("gateway response %d = (%#v,%v)", current.index, current.reply, current.err)
		}
	}
	modelConcurrency.mu.Lock()
	serialOverlap, maxActive := modelConcurrency.serialOverlap, modelConcurrency.maxActiveTotal
	modelConcurrency.mu.Unlock()
	if serialOverlap {
		t.Fatal("same PostgreSQL Session ran concurrently")
	}
	if maxActive < 2 {
		t.Fatalf("different Sessions never ran in parallel; max active=%d", maxActive)
	}

	assertPhase55SQLSession(t, catalog.StorageProfiles[1], postgresDSN, expectedTasks[1], 2)
	assertPhase55SQLSession(t, catalog.StorageProfiles[2], mysqlDSN, expectedTasks[3], 1)
	redisSession, err := sessionfence.New(redisURL, messagingPrefix, messagingPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer redisSession.Close()
	redisTask := expectedTasks[0]
	stored, err := redisSession.GetSession(context.Background(), session.Key{AppName: tenant.AppName(redisTask.TenantID, redisTask.AgentAppID), UserID: redisTask.RunnerUserID, SessionID: redisTask.SessionID})
	if err != nil || stored == nil || len(stored.GetEvents()) == 0 {
		t.Fatalf("Redis tenant Session = (%#v,%v)", stored, err)
	}
}

func TestPhase55MySQLStopResumeSmoke(t *testing.T) {
	redisURL := strings.TrimSpace(os.Getenv("PHASE55_REDIS_SMOKE_URL"))
	mysqlDSN := strings.TrimSpace(os.Getenv("PHASE55_MYSQL_DSN"))
	controlDir := strings.TrimSpace(os.Getenv("PHASE55_MYSQL_CONTROL_DIR"))
	if redisURL == "" || mysqlDSN == "" || controlDir == "" {
		t.Skip("PHASE55_REDIS_SMOKE_URL, PHASE55_MYSQL_DSN and PHASE55_MYSQL_CONTROL_DIR are required")
	}
	var err error
	redisURL, err = config.NormalizeRedisURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	stamp := fmt.Sprintf("%x", time.Now().UnixNano())
	prefix := "phase55-recovery:" + stamp
	profile := tenant.StorageProfile{TenantID: "tenant-a", ID: "mysql", Kind: tenant.StorageKindMySQL, CredentialRef: "env:MYSQL", TablePrefix: "r" + stamp}
	backend, err := storage.NewSQLBackend(profile, mysqlDSN, sessionfence.Limits{MaxTurnEvents: 32, MaxTurnBytes: 128 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix,
		LeaseDuration: 2 * time.Second, HeartbeatInterval: 250 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: 5 * time.Second,
		SessionFencing: "strong", SessionLockDuration: 2 * time.Second,
		SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 50 * time.Millisecond,
		MaxTurnEvents: 32, MaxTurnBytes: 128 << 10, ShutdownTimeout: 3 * time.Second,
		PersistenceTimeout: time.Second, PersistenceMaxAttempts: 10,
		PersistenceInitialBackoff: 500 * time.Millisecond, PersistenceMaxBackoff: 2 * time.Second,
		PersistencePayloadMaxBytes: 256 << 10,
	}
	storeA, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer storeA.Close()
	defer storeB.Close()
	exec := &phase55RecoveryExecutor{backend: backend, controlDir: controlDir}
	workerA, err := New(storeA, exec, "phase55-recovery-a")
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := New(storeB, exec, "phase55-recovery-b")
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { done <- workerA.Run(runCtx) }()
	go func() { done <- workerB.Run(runCtx) }()
	defer func() {
		cancel()
		<-done
		<-done
		cleanupPhase55Redis(t, redisURL, prefix)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if workerA.Ready(context.Background()) == nil && workerB.Ready(context.Background()) == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	task := phase4SmokeTask("phase55-mysql-recovery", "phase55-mysql-recovery-message", "phase55-mysql-recovery-session")
	if _, _, err := storeA.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		snapshot, snapshotErr := storeA.Snapshot(context.Background(), task.InboxID())
		if snapshotErr == nil && snapshot.State == messaging.StatePersistRetryWait {
			if snapshot.Attempt != task.Attempt || snapshot.PersistAttempt != 2 || snapshot.RawEnvelope == "" {
				t.Fatalf("MySQL outage persistence snapshot = %#v", snapshot)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL outage did not enter persistence retry: (%#v,%v)", snapshot, snapshotErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := storeA.ReadReply(context.Background(), "phase55-before-mysql", time.Millisecond); err == nil {
		t.Fatal("MySQL outage emitted a reply before SQL commit")
	}
	if os.Getenv("PHASE55_REQUIRE_ISOLATION_CHECK") == "1" {
		if err := os.WriteFile(filepath.Join(controlDir, "request-isolation-check"), []byte("check"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := waitPhase55File(filepath.Join(controlDir, "isolation-checked"), 60*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(controlDir, "request-start"), []byte("start"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitPhase55File(filepath.Join(controlDir, "started"), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		snapshot, snapshotErr := storeA.Snapshot(context.Background(), task.InboxID())
		if snapshotErr == nil && snapshot.State == messaging.StateSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL recovery did not finalize: (%#v,%v)", snapshot, snapshotErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	exec.mu.Lock()
	agentCalls, persistCalls := exec.agentCalls, exec.persistCalls
	exec.mu.Unlock()
	if agentCalls != 1 || persistCalls < 2 {
		t.Fatalf("MySQL recovery calls agent=%d persist=%d", agentCalls, persistCalls)
	}
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatalf("MySQL backend did not recover: %v", err)
	}
	verifierProfile := profile
	verifierProfile.SkipDBInit = true
	verifier, err := storage.NewSQLBackend(verifierProfile, mysqlDSN, sessionfence.Limits{MaxTurnEvents: 32, MaxTurnBytes: 128 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()
	if err := verifier.Ready(context.Background()); err != nil {
		t.Fatalf("fresh MySQL backend did not observe recovered database: %v", err)
	}
	stored, err := verifier.Session().GetSession(context.Background(), session.Key{AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID})
	if err != nil || stored == nil || len(stored.GetEvents()) != 1 {
		t.Fatalf("MySQL recovered Session = (%#v,%v)", stored, err)
	}
}

func waitPhase55File(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", filepath.Base(path))
}

func phase55SmokeCatalog(modelURL, redisPrefix, postgresPrefix, mysqlPrefix string) tenant.Catalog {
	model := tenant.ModelConfig{Name: "mock", BaseURL: modelURL, CredentialRef: "env:MODEL", RequestTimeout: 5 * time.Second, MaxOutputTokens: 128}
	return tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: "tenant-redis", Enabled: true}, {ID: "tenant-postgres", Enabled: true}, {ID: "tenant-mysql", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{
			{TenantID: "tenant-redis", ID: "redis", Kind: tenant.StorageKindRedis, CredentialRef: "env:REDIS", KeyPrefix: redisPrefix},
			{TenantID: "tenant-postgres", ID: "postgres", Kind: tenant.StorageKindPostgres, CredentialRef: "env:POSTGRES", TablePrefix: postgresPrefix, Schema: "public"},
			{TenantID: "tenant-mysql", ID: "mysql", Kind: tenant.StorageKindMySQL, CredentialRef: "env:MYSQL", TablePrefix: mysqlPrefix},
		},
		AgentApps: []tenant.AgentApp{
			{TenantID: "tenant-redis", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
			{TenantID: "tenant-postgres", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
			{TenantID: "tenant-mysql", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
		},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-redis", AgentAppID: "assistant", Version: "v1", StorageProfileID: "redis", Instruction: "test", Model: model},
			{TenantID: "tenant-postgres", AgentAppID: "assistant", Version: "v1", StorageProfileID: "postgres", Instruction: "test", Model: model},
			{TenantID: "tenant-mysql", AgentAppID: "assistant", Version: "v1", StorageProfileID: "mysql", Instruction: "test", Model: model},
		},
		ChannelBindings: []tenant.ChannelBinding{
			{ID: "demo-redis", Channel: "demo", ExternalAccountID: "demo-redis", TenantID: "tenant-redis", AgentAppID: "assistant", Enabled: true},
			{ID: "demo-postgres", Channel: "demo", ExternalAccountID: "demo-postgres", TenantID: "tenant-postgres", AgentAppID: "assistant", Enabled: true},
			{ID: "demo-mysql", Channel: "demo", ExternalAccountID: "demo-mysql", TenantID: "tenant-mysql", AgentAppID: "assistant", Enabled: true},
		},
	}
}

func phase55Inbound(binding, text, conversation string) message.InboundMessage {
	return message.InboundMessage{
		Channel: "demo", BindingID: binding, MessageID: binding + "-" + text,
		ExternalUserID: "same-user", ConversationID: conversation, Text: text,
		RequestID: binding + "-" + text + "-request", TraceID: binding + "-" + text + "-trace", ReceivedAt: time.Now().UTC(),
	}
}

func waitPhase55Ready(t *testing.T, gatewayService *gateway.Service, workers ...*Worker) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := gatewayService.Ready(context.Background()) == nil
		for _, current := range workers {
			ready = ready && current.Ready(context.Background()) == nil
		}
		if ready {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("Phase 5.5 Gateway/Workers did not become ready")
}

func assertPhase55SQLSession(t *testing.T, profile tenant.StorageProfile, dsn string, task message.ExecutionTask, minEvents int) {
	t.Helper()
	profile.SkipDBInit = true
	backend, err := storage.NewSQLBackend(profile, dsn, sessionfence.Limits{MaxTurnEvents: 64, MaxTurnBytes: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := backend.Session().GetSession(context.Background(), session.Key{AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID})
	if err != nil || stored == nil || len(stored.GetEvents()) < minEvents {
		t.Fatalf("%s tenant Session = (%#v,%v)", profile.Kind, stored, err)
	}
}

func cleanupPhase55Redis(t *testing.T, redisURL, prefix string) {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return
	}
	client := redis.NewClient(options)
	defer client.Close()
	var cursor uint64
	for {
		keys, next, scanErr := client.Scan(context.Background(), cursor, prefix+":reliable-v1:*", 100).Result()
		if scanErr == nil && len(keys) > 0 {
			_ = client.Del(context.Background(), keys...).Err()
		}
		if scanErr != nil || next == 0 {
			return
		}
		cursor = next
	}
}
