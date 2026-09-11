package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
)

type connectorTestSecretResolver struct {
	values map[string]string
	err    error
}

func (r connectorTestSecretResolver) Resolve(_ context.Context, reference string) (string, error) {
	if r.err != nil {
		return "", r.err
	}
	value, ok := r.values[reference]
	if !ok {
		return "", errors.New("test secret not found")
	}
	return value, nil
}

type connectorTestSender struct {
	replies []channels.OutboundMessage
}

func (s *connectorTestSender) Send(_ context.Context, _ channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	s.replies = append(s.replies, message)
	return channels.SendReceipt{ExternalMessageID: "reply-1"}, nil
}

type connectorProgressSender struct {
	connectorTestSender
	updates []string
	updateC chan string
}

func (s *connectorProgressSender) StartProgress(context.Context, channels.ReplyTarget) (channels.SendReceipt, error) {
	return channels.SendReceipt{ExternalMessageID: "progress-1"}, nil
}

func (s *connectorProgressSender) UpdateProgress(_ context.Context, _ channels.ReplyTarget, _ string, content string) error {
	s.updates = append(s.updates, content)
	if s.updateC != nil {
		select {
		case s.updateC <- content:
		default:
		}
	}
	return nil
}

type connectorTestArtifactProvider struct {
	service agentartifact.Service
	err     error
}

func (p connectorTestArtifactProvider) ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error) {
	return p.service, p.err
}

func newConnectorTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func publishConnectorTestConfig(t *testing.T, repository *tenant.MemoryRepository, tenantID, appCode, status string, bindings ...config.ChannelBinding) tenant.Snapshot {
	t.Helper()
	snapshot, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: tenantID, AppCode: appCode, Status: status, ConfigVersion: 1, Channels: bindings,
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	return snapshot
}

func newCompleteConnectorTestManager(t *testing.T) (*channelConnectorManager, *redis.Client) {
	t.Helper()
	_, client := newConnectorTestRedis(t)
	return newCompleteConnectorTestManagerWithRedis(t, client), client
}

func newCompleteConnectorTestManagerWithRedis(t *testing.T, client redis.UniversalClient) *channelConnectorManager {
	t.Helper()
	cache, err := feishu.NewRedisCache(client)
	if err != nil {
		t.Fatalf("feishu.NewRedisCache() error = %v", err)
	}
	approvals, err := governance.NewRedisApprovalBroker(client, governance.NewMemoryApprovalStore(), time.Minute)
	if err != nil {
		t.Fatalf("NewRedisApprovalBroker() error = %v", err)
	}
	progress, err := messaging.NewRedisIMProgressHub(client)
	if err != nil {
		t.Fatalf("NewRedisIMProgressHub() error = %v", err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("connector-test", []byte("connector-test-manifest-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatalf("NewExecutionManifestCodec() error = %v", err)
	}
	manager, err := newChannelConnectorManager(
		tenant.NewMemoryRepository(),
		&testProducer{},
		connectorTestSecretResolver{values: map[string]string{}},
		&http.Client{Timeout: time.Second},
		cache,
		client,
		storage.NewMemoryStateStore(),
		identity.NewMemoryIdentityStore(),
		approvals,
		progress,
		connectorTestArtifactProvider{service: artifactinmemory.NewService()},
		&connectorTestSender{},
		manifests,
	)
	if err != nil {
		t.Fatalf("newChannelConnectorManager() error = %v", err)
	}
	return manager
}

func TestNewChannelConnectorManagerValidatesDependencies(t *testing.T) {
	t.Parallel()
	if _, err := newChannelConnectorManager(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("newChannelConnectorManager(incomplete) error = nil")
	}
	manager, _ := newCompleteConnectorTestManager(t)
	if manager.ingress == nil || manager.controls == nil || manager.owner == "" {
		t.Fatalf("newChannelConnectorManager() = %+v", manager)
	}
}

func TestChannelConnectorManagerRunReturnsForCancelledContext(t *testing.T) {
	t.Parallel()
	manager, _ := newCompleteConnectorTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.Run(ctx)
	if manager.IsLeader() {
		t.Fatal("cancelled manager unexpectedly became leader")
	}
}

func TestChannelConnectorManagerRunAcquiresAndReleasesLeaderLease(t *testing.T) {
	manager, client := newCompleteConnectorTestManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	manager.Run(ctx)
	if manager.IsLeader() {
		t.Fatal("manager still reports leader after Run returned")
	}
	if _, err := client.Get(context.Background(), channelConnectorLeaseKey).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("leader lease after Run = %v", err)
	}
}

func TestChannelConnectorManagerRunWaitsWhenLeaseIsOwnedElsewhere(t *testing.T) {
	manager, client := newCompleteConnectorTestManager(t)
	if err := client.Set(context.Background(), channelConnectorLeaseKey, "other-channel", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	manager.Run(ctx)
	if manager.IsLeader() {
		t.Fatal("manager acquired a lease owned by another channel node")
	}
}

func TestChannelConnectorManagerStandbyTakesLeadershipAfterLeaderStops(t *testing.T) {
	_, client := newConnectorTestRedis(t)
	leader := newCompleteConnectorTestManagerWithRedis(t, client)
	standby := newCompleteConnectorTestManagerWithRedis(t, client)

	leaderCtx, stopLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		leader.Run(leaderCtx)
	}()
	awaitConnectorLeadership(t, leader, true)

	standbyCtx, stopStandby := context.WithCancel(context.Background())
	standbyDone := make(chan struct{})
	go func() {
		defer close(standbyDone)
		standby.Run(standbyCtx)
	}()
	t.Cleanup(func() {
		stopLeader()
		stopStandby()
		<-leaderDone
		<-standbyDone
	})

	time.Sleep(10 * time.Millisecond)
	if standby.IsLeader() {
		t.Fatal("standby became leader while the original leader still held the lease")
	}

	stopLeader()
	<-leaderDone
	awaitConnectorLeadership(t, standby, true)
	stopStandby()
	<-standbyDone
	if standby.IsLeader() {
		t.Fatal("standby still reports leader after shutdown")
	}
}

func awaitConnectorLeadership(t *testing.T, manager *channelConnectorManager, want bool) {
	t.Helper()
	deadline := time.Now().Add(channelConnectorAcquireRetry + time.Second)
	for time.Now().Before(deadline) {
		if manager.IsLeader() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("connector leadership = %v, want %v", manager.IsLeader(), want)
}

func TestChannelConnectorManagerLeaderLifecycleReturnsOnCancellation(t *testing.T) {
	t.Parallel()
	manager, client := newCompleteConnectorTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.runLeader(ctx)
	if manager.IsLeader() {
		t.Fatal("runLeader() did not clear leader state")
	}
	if _, err := client.Get(context.Background(), channelConnectorLeaseKey).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("leader lease after shutdown error = %v", err)
	}
}

func TestChannelConnectorManagerUpdatesProgressWhenSenderSupportsIt(t *testing.T) {
	t.Parallel()
	sender := &connectorProgressSender{}
	manager := &channelConnectorManager{webSender: sender}
	err := manager.updateIMProgress(context.Background(), messaging.IMProgressEvent{
		TenantID: "support", AppCode: "assistant", ConfigVersion: 1,
		Channel: channels.Web, BindingID: "", RequestID: "request-1", ConversationID: "conversation-1",
		ProgressMessageID: "progress-1", Content: "正在查询订单",
	})
	if err != nil {
		t.Fatalf("updateIMProgress() error = %v", err)
	}
	if len(sender.updates) != 1 || sender.updates[0] != "正在查询订单" {
		t.Fatalf("progress updates = %+v", sender.updates)
	}

	plain := &connectorTestSender{}
	manager.webSender = plain
	if err := manager.updateIMProgress(context.Background(), messaging.IMProgressEvent{Channel: channels.Web}); err != nil {
		t.Fatalf("updateIMProgress(non-progress sender) error = %v", err)
	}
}

func TestChannelConnectorManagerRunsProgressSubscription(t *testing.T) {
	_, client := newConnectorTestRedis(t)
	hub, err := messaging.NewRedisIMProgressHub(client)
	if err != nil {
		t.Fatal(err)
	}
	repository := tenant.NewMemoryRepository()
	snapshot := publishConnectorTestConfig(t, repository, "support", "assistant", config.AgentActive,
		config.ChannelBinding{Type: config.ChannelFeishu, BindingID: "support-chat"},
	)
	sender := &connectorProgressSender{updateC: make(chan string, 1)}
	manager := &channelConnectorManager{
		repository: repository, progress: hub,
		feishuSenders: map[string]channels.Sender{"support-chat": sender},
		wecomSenders:  map[string]channels.Sender{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.runProgressUpdates(ctx)
	}()

	// Give the Redis subscription a bounded moment to become active. The
	// subscription itself is the behavior under test; no external service is used.
	time.Sleep(20 * time.Millisecond)
	event := messaging.IMProgressEvent{
		TenantID: "support", AppCode: "assistant", ConfigVersion: snapshot.Config.ConfigVersion,
		Channel: channels.Feishu, BindingID: "support-chat", ConversationID: "customer-chat",
		ConversationScope: channels.ConversationDirect, ProgressMessageID: "progress-1",
		RequestID: "request-1", Content: "正在查询订单",
	}
	if err := hub.PublishProgress(context.Background(), event); err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case got := <-sender.updateC:
		if got != event.Content {
			t.Fatalf("progress update = %q, want %q", got, event.Content)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("progress update was not delivered")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runProgressUpdates() did not stop after cancellation")
	}
}

func TestChannelConnectorManagerLeaderState(t *testing.T) {
	t.Parallel()
	var nilManager *channelConnectorManager
	if nilManager.IsLeader() {
		t.Fatal("nil manager must not report leader state")
	}
	manager := &channelConnectorManager{}
	if manager.IsLeader() {
		t.Fatal("new manager unexpectedly reports leader state")
	}
	manager.setLeader(true)
	if !manager.IsLeader() {
		t.Fatal("manager did not record leader state")
	}
	manager.setLeader(false)
	if manager.IsLeader() {
		t.Fatal("manager did not clear leader state")
	}
}

func TestChannelStatusKeyIsStableAndBindingScoped(t *testing.T) {
	t.Parallel()
	first := channelStatusKey(channels.Telegram, "support-main")
	if first == "" || first != channelStatusKey(channels.Telegram, "support-main") {
		t.Fatalf("channelStatusKey() is not stable: %q", first)
	}
	if first == channelStatusKey(channels.Telegram, "support-secondary") {
		t.Fatal("different bindings must have different status keys")
	}
	if first == channelStatusKey(channels.Feishu, "support-main") {
		t.Fatal("different channels must have different status keys")
	}
}

func TestTelegramOffsetStoreIsMonotonicAndCredentialScoped(t *testing.T) {
	t.Parallel()
	_, client := newConnectorTestRedis(t)
	key := telegramOffsetKey("support-main", "token-a")
	if key == telegramOffsetKey("support-main", "token-b") || key == telegramOffsetKey("support-secondary", "token-a") {
		t.Fatal("Telegram offset key must change with binding or credential")
	}
	store := telegramRedisOffsetStore{client: client, key: key}
	if got, err := store.Load(context.Background()); err != nil || got != 0 {
		t.Fatalf("Load(empty) = %d, %v", got, err)
	}
	if err := store.Save(context.Background(), 103); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), 99); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(context.Background()); err != nil || got != 103 {
		t.Fatalf("Load(after monotonic saves) = %d, %v; want 103", got, err)
	}
}

func TestChannelConnectorManagerPersistsAndListsBindingStatus(t *testing.T) {
	t.Parallel()
	server, client := newConnectorTestRedis(t)
	repository := tenant.NewMemoryRepository()
	publishConnectorTestConfig(t, repository, "support", "assistant", config.AgentActive,
		config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "support-main"},
		config.ChannelBinding{Type: config.ChannelFeishu, BindingID: "support-chat"},
	)
	manager := &channelConnectorManager{repository: repository, redis: client, owner: "gateway-test"}

	changedAt := time.Now().UTC().Truncate(time.Millisecond)
	manager.persistStatus(context.Background(), channels.BindingStatus{
		Channel: channels.Telegram, BindingID: "support-main", State: channels.ChannelStateConnected,
		Owner: "gateway-test", LastChangedAt: changedAt,
	})
	statuses, err := manager.ListBindingStatuses(context.Background(), "support", "assistant")
	if err != nil {
		t.Fatalf("ListBindingStatuses() error = %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("ListBindingStatuses() length = %d, want 2", len(statuses))
	}
	byBinding := map[string]channels.BindingStatus{}
	for _, status := range statuses {
		byBinding[status.BindingID] = status
	}
	if got := byBinding["support-main"]; got.State != channels.ChannelStateConnected || got.Owner != "gateway-test" {
		t.Fatalf("persisted status = %+v", got)
	}
	if got := byBinding["support-chat"]; got.State != channels.ChannelStateOffline {
		t.Fatalf("missing runtime status = %+v, want offline", got)
	}

	server.Set(channelStatusKey(channels.Feishu, "support-chat"), "not-json")
	if _, err := manager.ListBindingStatuses(context.Background(), "support", "assistant"); err == nil || !strings.Contains(err.Error(), "decode channel runtime status") {
		t.Fatalf("ListBindingStatuses(invalid JSON) error = %v", err)
	}
	server.Del(channelStatusKey(channels.Feishu, "support-chat"))
	server.SetError("forced redis error")
	if _, err := manager.ListBindingStatuses(context.Background(), "support", "assistant"); err == nil || !strings.Contains(err.Error(), "read channel runtime status") {
		t.Fatalf("ListBindingStatuses(redis error) error = %v", err)
	}
}

func TestChannelConnectorManagerListsInactiveBindingsOffline(t *testing.T) {
	t.Parallel()
	_, client := newConnectorTestRedis(t)
	repository := tenant.NewMemoryRepository()
	publishConnectorTestConfig(t, repository, "support", "assistant", config.AgentDisabled,
		config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "support-main"},
	)
	manager := &channelConnectorManager{repository: repository, redis: client}
	statuses, err := manager.ListBindingStatuses(context.Background(), "support", "assistant")
	if err != nil || len(statuses) != 1 || statuses[0].State != channels.ChannelStateOffline {
		t.Fatalf("ListBindingStatuses(inactive) = %+v, %v", statuses, err)
	}
	statuses, err = manager.ListBindingStatuses(context.Background(), "support", "other-app")
	if err != nil || len(statuses) != 0 {
		t.Fatalf("ListBindingStatuses(other app) = %+v, %v", statuses, err)
	}
}

func TestChannelConnectorManagerResolveSender(t *testing.T) {
	t.Parallel()
	repository := tenant.NewMemoryRepository()
	snapshot := publishConnectorTestConfig(t, repository, "support", "assistant", config.AgentActive,
		config.ChannelBinding{Type: config.ChannelFeishu, BindingID: "support-feishu"},
		config.ChannelBinding{Type: config.ChannelWeCom, BindingID: "support-wecom"},
	)
	webSender := &connectorTestSender{}
	feishuSender := &connectorTestSender{}
	wecomSender := &connectorTestSender{}
	manager := &channelConnectorManager{
		repository: repository, webSender: webSender,
		feishuSenders: map[string]channels.Sender{"support-feishu": feishuSender},
		wecomSenders:  map[string]channels.Sender{"support-wecom": wecomSender},
	}

	got, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Web})
	if err != nil || got != webSender {
		t.Fatalf("ResolveSender(web) = %T, %v", got, err)
	}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Feishu}); err == nil {
		t.Fatal("ResolveSender(empty binding) error = nil")
	}
	got, err = manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Feishu, BindingID: "support-feishu"})
	if err != nil || got != feishuSender {
		t.Fatalf("ResolveSender(feishu) = %T, %v", got, err)
	}
	got, err = manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.WeCom, BindingID: "support-wecom"})
	if err != nil || got != wecomSender {
		t.Fatalf("ResolveSender(wecom) = %T, %v", got, err)
	}
	manager.feishuSenders = map[string]channels.Sender{}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Feishu, BindingID: "support-feishu"}); err == nil {
		t.Fatal("ResolveSender(disconnected feishu) error = nil")
	}
	manager.wecomSenders = map[string]channels.Sender{}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.WeCom, BindingID: "support-wecom"}); err == nil {
		t.Fatal("ResolveSender(disconnected wecom) error = nil")
	}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Telegram, BindingID: "missing"}); !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("ResolveSender(missing binding) error = %v", err)
	}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", snapshot.Config.ConfigVersion, channels.BindingKey{Channel: channels.Channel("unknown"), BindingID: "unknown"}); !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("ResolveSender(unknown binding) error = %v", err)
	}
	if _, err := manager.ResolveSender(context.Background(), "support", "assistant", 999, channels.BindingKey{Channel: channels.Feishu, BindingID: "support-feishu"}); err == nil {
		t.Fatal("ResolveSender(missing version) error = nil")
	}
}

func TestChannelConnectorManagerMaterializesOutboundArtifacts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	snapshot := publishConnectorTestConfig(t, repository, "support", "assistant", config.AgentActive)
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: snapshot.Config.AppName(), UserID: "customer-1", SessionID: "session-1"}
	version, err := service.SaveArtifact(ctx, info, "user:answer.txt", &agentartifact.Artifact{Data: []byte("support answer"), MimeType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	manager := &channelConnectorManager{repository: repository, artifacts: connectorTestArtifactProvider{service: service}}

	files, cleanup, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", snapshot.Config.ConfigVersion, []messaging.OutboundArtifactRef{{
		Filename: "user:answer.txt", Version: version, UserID: "customer-1", SessionID: "session-1", Name: "../answer.txt",
	}})
	if err != nil {
		t.Fatalf("MaterializeOutboundArtifacts() error = %v", err)
	}
	if len(files) != 1 || files[0].Name != "answer.txt" {
		t.Fatalf("materialized files = %+v", files)
	}
	data, err := os.ReadFile(files[0].Path)
	if err != nil || string(data) != "support answer" {
		t.Fatalf("materialized data = %q, %v", data, err)
	}
	directory := filepath.Dir(files[0].Path)
	cleanup()
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact directory still exists after cleanup: %v", err)
	}

	empty, emptyCleanup, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", snapshot.Config.ConfigVersion, nil)
	if err != nil || len(empty) != 0 || emptyCleanup == nil {
		t.Fatalf("empty materialization = %+v, cleanup=%v, err=%v", empty, emptyCleanup != nil, err)
	}
	emptyCleanup()
	if _, _, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", 999, []messaging.OutboundArtifactRef{{Filename: "x", UserID: "u", SessionID: "s"}}); err == nil {
		t.Fatal("missing config version error = nil")
	}
	manager.artifacts = connectorTestArtifactProvider{err: errors.New("artifact backend unavailable")}
	if _, _, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", snapshot.Config.ConfigVersion, []messaging.OutboundArtifactRef{{Filename: "x", UserID: "u", SessionID: "s"}}); err == nil {
		t.Fatal("artifact provider error = nil")
	}
	manager.artifacts = connectorTestArtifactProvider{service: service}
	invalid := []messaging.OutboundArtifactRef{{Filename: "", Version: 0, UserID: "u", SessionID: "s"}}
	if _, _, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", snapshot.Config.ConfigVersion, invalid); err == nil {
		t.Fatal("invalid artifact reference error = nil")
	}
	missing := []messaging.OutboundArtifactRef{{Filename: "user:missing.txt", Version: 0, UserID: "customer-1", SessionID: "session-1"}}
	if _, _, err := manager.MaterializeOutboundArtifacts(ctx, "support", "assistant", snapshot.Config.ConfigVersion, missing); err == nil {
		t.Fatal("missing artifact error = nil")
	}
}

func TestChannelConnectorManagerProcessStatusLifecycle(t *testing.T) {
	t.Parallel()
	_, client := newConnectorTestRedis(t)
	key := channels.BindingKey{Channel: channels.Feishu, BindingID: "support-chat"}
	done := make(chan struct{})
	close(done)
	cancelled := false
	process := &connectorProcess{
		fingerprint: "fingerprint", done: done, state: channels.ChannelStateConnecting,
		changedAt: time.Now().UTC(), cancel: func() { cancelled = true },
	}
	manager := &channelConnectorManager{
		redis: client, owner: "gateway-test", processes: map[channels.BindingKey]*connectorProcess{key: process},
		feishuSenders: map[string]channels.Sender{key.BindingID: &connectorTestSender{}},
		wecomSenders:  map[string]channels.Sender{},
	}
	manager.updateProcessStatus(key, channels.ChannelStateConnected, "")
	if process.state != channels.ChannelStateConnected {
		t.Fatalf("process state = %q", process.state)
	}
	manager.refreshProcessStatus(context.Background(), key, process)
	manager.updateProcessStatus(channels.BindingKey{Channel: channels.Feishu, BindingID: "missing"}, channels.ChannelStateConnected, "")
	manager.refreshProcessStatus(context.Background(), key, &connectorProcess{})

	manager.stopConnector(key, process)
	if !cancelled {
		t.Fatal("stopConnector() did not cancel process")
	}
	if _, ok := manager.processes[key]; ok {
		t.Fatal("stopped process still registered")
	}
	if _, ok := manager.feishuSenders[key.BindingID]; ok {
		t.Fatal("stopped sender still registered")
	}
	manager.stopConnector(key, nil)

	secondDone := make(chan struct{})
	close(secondDone)
	secondKey := channels.BindingKey{Channel: channels.WeCom, BindingID: "support-direct"}
	manager.processes[secondKey] = &connectorProcess{done: secondDone, cancel: func() {}, state: channels.ChannelStateConnected, changedAt: time.Now().UTC()}
	manager.wecomSenders[secondKey.BindingID] = &connectorTestSender{}
	manager.stopAllConnectors()
	if len(manager.processes) != 0 || len(manager.wecomSenders) != 0 {
		t.Fatalf("stopAllConnectors() left state: processes=%d senders=%d", len(manager.processes), len(manager.wecomSenders))
	}
}

func TestChannelConnectorManagerReconcileHandlesSafeControlPlaneCases(t *testing.T) {
	t.Parallel()
	_, client := newConnectorTestRedis(t)
	repository := tenant.NewMemoryRepository()
	publishConnectorTestConfig(t, repository, "support", "inactive", config.AgentDisabled,
		config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "inactive-bot", CredentialRef: "env:BOT"},
	)
	manager := &channelConnectorManager{
		repository: repository, redis: client, owner: "gateway-test",
		secrets:   connectorTestSecretResolver{values: map[string]string{}},
		processes: map[channels.BindingKey]*connectorProcess{}, wecomSenders: map[string]channels.Sender{}, feishuSenders: map[string]channels.Sender{},
	}
	if err := manager.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile(safe cases) error = %v", err)
	}

	publishConnectorTestConfig(t, repository, "support", "active", config.AgentActive,
		config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "support-main", CredentialRef: "env:BOT"},
	)
	manager.secrets = connectorTestSecretResolver{err: errors.New("credential unavailable")}
	if err := manager.reconcile(context.Background()); err == nil || !strings.Contains(err.Error(), "resolve connector credential") {
		t.Fatalf("reconcile(secret error) = %v", err)
	}

	done := make(chan struct{})
	close(done)
	staleKey := channels.BindingKey{Channel: channels.Feishu, BindingID: "stale"}
	manager.processes[staleKey] = &connectorProcess{done: done, cancel: func() {}, state: channels.ChannelStateConnected, changedAt: time.Now().UTC()}
	if err := manager.reconcile(context.Background()); err == nil {
		t.Fatal("reconcile should still report active binding credential error")
	}
	if _, ok := manager.processes[staleKey]; ok {
		t.Fatal("reconcile did not stop stale connector")
	}
}

func TestChannelConnectorManagerRejectsUnsupportedConnectorStart(t *testing.T) {
	t.Parallel()
	manager := &channelConnectorManager{}
	err := manager.startConnector(context.Background(), channels.BindingKey{Channel: channels.Channel("unsupported"), BindingID: "support"}, config.ChannelBinding{}, "fingerprint")
	if err == nil || !strings.Contains(err.Error(), "unsupported connector channel") {
		t.Fatalf("startConnector() error = %v", err)
	}
}

func TestChannelConnectorManagerStartsAndStopsSupportedConnectors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	t.Cleanup(server.Close)
	_, client := newConnectorTestRedis(t)
	cache, err := feishu.NewRedisCache(client)
	if err != nil {
		t.Fatal(err)
	}
	manager := &channelConnectorManager{
		secrets: connectorTestSecretResolver{values: map[string]string{
			"env:TG": `{"bot_token":"12345:test-token","api_base_url":"` + server.URL + `"}`,
			"env:WX": `{"bot_id":"support-bot","secret":"test-secret","endpoint":"ws://127.0.0.1:1/ws","origin":"http://127.0.0.1"}`,
			"env:FS": `{"app_id":"support-app","app_secret":"test-secret","api_base_url":"http://127.0.0.1:1"}`,
		}},
		httpClient:    &http.Client{Timeout: 50 * time.Millisecond},
		feishuCache:   cache,
		redis:         client,
		owner:         "gateway-test",
		processes:     map[channels.BindingKey]*connectorProcess{},
		wecomSenders:  map[string]channels.Sender{},
		feishuSenders: map[string]channels.Sender{},
	}

	tests := []struct {
		name       string
		key        channels.BindingKey
		credential string
	}{
		{"telegram", channels.BindingKey{Channel: channels.Telegram, BindingID: "support-telegram"}, "env:TG"},
		{"wecom", channels.BindingKey{Channel: channels.WeCom, BindingID: "support-wecom"}, "env:WX"},
		{"feishu", channels.BindingKey{Channel: channels.Feishu, BindingID: "support-feishu"}, "env:FS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := manager.startConnector(ctx, tt.key, config.ChannelBinding{
				Type: string(tt.key.Channel), BindingID: tt.key.BindingID, CredentialRef: tt.credential,
			}, "fingerprint-"+tt.name); err != nil {
				t.Fatalf("startConnector(%s) error = %v", tt.name, err)
			}
			process := manager.processes[tt.key]
			if process == nil || process.channel == nil {
				t.Fatalf("startConnector(%s) did not register process", tt.name)
			}
			manager.stopConnector(tt.key, process)
			if _, ok := manager.processes[tt.key]; ok {
				t.Fatalf("stopConnector(%s) left process registered", tt.name)
			}
		})
	}
}

func TestChannelConnectorManagerStartRejectsInvalidCredentials(t *testing.T) {
	t.Parallel()
	_, client := newConnectorTestRedis(t)
	cache, err := feishu.NewRedisCache(client)
	if err != nil {
		t.Fatal(err)
	}
	manager := &channelConnectorManager{
		secrets: connectorTestSecretResolver{values: map[string]string{
			"env:INVALID": `{}`,
		}},
		httpClient: &http.Client{Timeout: time.Second}, feishuCache: cache, redis: client,
		processes: map[channels.BindingKey]*connectorProcess{}, wecomSenders: map[string]channels.Sender{}, feishuSenders: map[string]channels.Sender{},
	}
	for _, channel := range []channels.Channel{channels.Telegram, channels.WeCom, channels.Feishu} {
		channel := channel
		t.Run(string(channel), func(t *testing.T) {
			t.Parallel()
			key := channels.BindingKey{Channel: channel, BindingID: "support-" + string(channel)}
			if err := manager.startConnector(context.Background(), key, config.ChannelBinding{Type: string(channel), BindingID: key.BindingID, CredentialRef: "env:INVALID"}, "fingerprint"); err == nil {
				t.Fatalf("startConnector(%s, invalid credential) error = nil", channel)
			}
		})
	}
}

func TestChannelConnectorManagerHelperGuards(t *testing.T) {
	t.Parallel()
	manager := &channelConnectorManager{}
	if err := manager.publishInboundReliably(context.Background(), "support", channels.InboundMessage{}); err == nil {
		t.Fatal("publishInboundReliably() without ingress error = nil")
	}
	if _, err := manager.handlePlatformControl(context.Background(), tenant.Snapshot{}, "support", channels.InboundMessage{}); err == nil {
		t.Fatal("handlePlatformControl() without controls error = nil")
	}
	if err := manager.reconcileApprovals(context.Background()); err == nil {
		t.Fatal("reconcileApprovals() without controls error = nil")
	}
	manager.persistStatus(context.Background(), channels.BindingStatus{BindingID: "ignored"})
	(&channelConnectorManager{}).persistStatus(context.Background(), channels.BindingStatus{BindingID: "ignored"})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if waitContext(cancelled, time.Hour) {
		t.Fatal("waitContext(cancelled) = true")
	}
	if !waitContext(context.Background(), time.Millisecond) {
		t.Fatal("waitContext(timer) = false")
	}
}
