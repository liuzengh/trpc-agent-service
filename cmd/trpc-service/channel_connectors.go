package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecombot"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

const (
	channelConnectorLeaseKey     = "trpc-agent:channel-connectors:leader"
	channelConnectorLeaseTTL     = 15 * time.Second
	channelConnectorRenewEvery   = 5 * time.Second
	channelConnectorAcquireRetry = time.Second
	channelConnectorReconcile    = 5 * time.Second
	approvalReconcile            = time.Second
	channelStatusTTL             = 20 * time.Second
)

var renewConnectorLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var releaseConnectorLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

var advanceTelegramOffsetScript = redis.NewScript(`
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local incoming = tonumber(ARGV[1])
if incoming > current then
  redis.call("SET", KEYS[1], ARGV[1])
  return incoming
end
return current
`)

type telegramRedisOffsetStore struct {
	client redis.UniversalClient
	key    string
}

func (s telegramRedisOffsetStore) Load(ctx context.Context) (int64, error) {
	if s.client == nil || strings.TrimSpace(s.key) == "" {
		return 0, errors.New("telegram offset store is not configured")
	}
	offset, err := s.client.Get(ctx, s.key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read Telegram offset: %w", err)
	}
	if offset < 0 {
		return 0, fmt.Errorf("read Telegram offset: invalid negative value %d", offset)
	}
	return offset, nil
}

func (s telegramRedisOffsetStore) Save(ctx context.Context, offset int64) error {
	if s.client == nil || strings.TrimSpace(s.key) == "" {
		return errors.New("telegram offset store is not configured")
	}
	if offset <= 0 {
		return nil
	}
	if _, err := advanceTelegramOffsetScript.Run(ctx, s.client, []string{s.key}, offset).Int64(); err != nil {
		return fmt.Errorf("persist Telegram offset: %w", err)
	}
	return nil
}

type connectorProcess struct {
	fingerprint string
	channel     channels.OpenClawChannel
	cancel      context.CancelFunc
	stop        func(context.Context) error
	done        chan struct{}
	state       string
	changedAt   time.Time
	lastError   string
}

type channelConnectorManager struct {
	repository  tenant.Repository
	secrets     credential.SecretResolver
	httpClient  *http.Client
	feishuCache *feishu.RedisCache
	redis       redis.UniversalClient
	controls    *channelControlHandler
	ingress     *channelIngress
	progress    *messaging.RedisIMProgressHub
	artifacts   channelArtifactProvider
	webSender   channels.Sender
	owner       string
	leader      bool

	mu            sync.RWMutex
	processes     map[channels.BindingKey]*connectorProcess
	wecomSenders  map[string]channels.Sender
	feishuSenders map[string]channels.Sender
}

type channelArtifactProvider interface {
	ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error)
}

func newChannelConnectorManager(
	repository tenant.Repository,
	producer messaging.Producer,
	secrets credential.SecretResolver,
	httpClient *http.Client,
	feishuCache *feishu.RedisCache,
	redisClient redis.UniversalClient,
	state storage.StateStore,
	identities messaging.ChannelIdentityResolver,
	approvals governance.ApprovalBroker,
	progress *messaging.RedisIMProgressHub,
	artifacts channelArtifactProvider,
	webSender channels.Sender,
	manifests *messaging.ExecutionManifestCodec,
) (*channelConnectorManager, error) {
	if repository == nil || producer == nil || secrets == nil || httpClient == nil || feishuCache == nil || redisClient == nil || state == nil || identities == nil || approvals == nil || progress == nil || artifacts == nil || webSender == nil || manifests == nil {
		return nil, fmt.Errorf("channel connector dependencies are incomplete")
	}
	owner := uuid.NewString()
	manager := &channelConnectorManager{
		repository: repository, secrets: secrets, httpClient: httpClient,
		feishuCache: feishuCache, redis: redisClient, progress: progress, artifacts: artifacts, webSender: webSender, owner: owner,
		processes:    make(map[channels.BindingKey]*connectorProcess),
		wecomSenders: make(map[string]channels.Sender), feishuSenders: make(map[string]channels.Sender),
	}
	controls, err := newChannelControlHandler(approvals, manager.ResolveSender)
	if err != nil {
		return nil, err
	}
	manager.controls = controls
	ingress, err := newChannelIngress(repository, producer, state, identities, controls, artifacts, manifests, manager.ResolveSender)
	if err != nil {
		return nil, err
	}
	manager.ingress = ingress
	return manager, nil
}

// Run elects one Channel node as the owner of long-lived IM connections. This is a
// control-plane lease only: Kafka remains the sole execution scheduler. On
// lease loss every connector is cancelled before another Channel node takes over.
func (m *channelConnectorManager) Run(ctx context.Context) {
	for ctx.Err() == nil {
		acquired, err := m.redis.SetNX(ctx, channelConnectorLeaseKey, m.owner, channelConnectorLeaseTTL).Result()
		if err != nil {
			logBackgroundError("channel connector lease", err)
			if !waitContext(ctx, time.Second) {
				return
			}
			continue
		}
		if !acquired {
			if !waitContext(ctx, channelConnectorAcquireRetry) {
				return
			}
			continue
		}
		m.runLeader(ctx)
	}
}

func (m *channelConnectorManager) runLeader(ctx context.Context) {
	leaderCtx, cancel := context.WithCancel(ctx)
	m.setLeader(true)
	defer func() {
		// Leadership owns the complete connector lifecycle. Stop all work that
		// may still use the external binding before making the Redis lease
		// available to another Channel node.
		cancel()
		m.stopAllConnectors()
		m.setLeader(false)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer releaseCancel()
		_, _ = releaseConnectorLeaseScript.Run(releaseCtx, m.redis, []string{channelConnectorLeaseKey}, m.owner).Result()
	}()
	safego.Go("channel progress updates", func() { m.runProgressUpdates(leaderCtx) })

	if err := m.reconcile(leaderCtx); err != nil {
		logBackgroundError("channel connector reconcile", err)
	}
	if err := m.reconcileApprovals(leaderCtx); err != nil {
		logBackgroundError("approval notification reconcile", err)
	}
	renewTicker := time.NewTicker(channelConnectorRenewEvery)
	reconcileTicker := time.NewTicker(channelConnectorReconcile)
	approvalTicker := time.NewTicker(approvalReconcile)
	defer renewTicker.Stop()
	defer reconcileTicker.Stop()
	defer approvalTicker.Stop()

	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-renewTicker.C:
			result, err := renewConnectorLeaseScript.Run(
				leaderCtx, m.redis, []string{channelConnectorLeaseKey}, m.owner, channelConnectorLeaseTTL.Milliseconds(),
			).Int64()
			if err != nil || result != 1 {
				if err == nil {
					err = errors.New("channel connector lease ownership lost")
				}
				logBackgroundError("channel connector lease renewal", err)
				return
			}
		case <-reconcileTicker.C:
			if err := m.reconcile(leaderCtx); err != nil {
				logBackgroundError("channel connector reconcile", err)
			}
		case <-approvalTicker.C:
			if err := m.reconcileApprovals(leaderCtx); err != nil {
				logBackgroundError("approval notification reconcile", err)
			}
		}
	}
}

func (m *channelConnectorManager) runProgressUpdates(ctx context.Context) {
	events, closeSubscription, err := m.progress.Subscribe(ctx)
	if err != nil {
		logBackgroundError("subscribe IM progress", err)
		return
	}
	defer closeSubscription()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	latest := make(map[string]messaging.IMProgressEvent)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			key := event.TenantID + "\x00" + string(event.Channel) + "\x00" + event.BindingID + "\x00" + event.RequestID
			latest[key] = event
		case <-ticker.C:
			for key, event := range latest {
				delete(latest, key)
				if err := m.updateIMProgress(ctx, event); err != nil {
					slog.Warn("update IM progress", "channel", event.Channel, "binding_id", event.BindingID, "error", err)
				}
			}
		}
	}
}

func (m *channelConnectorManager) updateIMProgress(ctx context.Context, event messaging.IMProgressEvent) error {
	sender, err := m.ResolveSender(ctx, event.TenantID, event.AppCode, event.ConfigVersion, channels.BindingKey{Channel: event.Channel, BindingID: event.BindingID})
	if err != nil {
		return err
	}
	progress, ok := sender.(channels.ProgressSender)
	if !ok {
		return nil
	}
	return progress.UpdateProgress(ctx, channels.ReplyTarget{
		TenantID: event.TenantID, Channel: event.Channel, BindingID: event.BindingID,
		ConversationID: event.ConversationID, ConversationScope: event.ConversationScope,
		ProviderReplyToken: event.ProviderReplyToken,
	}, event.ProgressMessageID, event.Content)
}

func (m *channelConnectorManager) reconcileApprovals(ctx context.Context) error {
	if m.controls == nil {
		return errors.New("channel control handler is unavailable")
	}
	return m.controls.reconcileApprovals(ctx)
}

func (m *channelConnectorManager) setLeader(value bool) {
	m.mu.Lock()
	m.leader = value
	m.mu.Unlock()
}

func (m *channelConnectorManager) IsLeader() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leader
}

func (m *channelConnectorManager) reconcile(ctx context.Context) error {
	snapshots, err := m.repository.ListApplications(ctx, "")
	if err != nil {
		return fmt.Errorf("list applications: %w", err)
	}
	desired := make(map[channels.BindingKey]config.ChannelBinding)
	for _, snapshot := range snapshots {
		if snapshot.Config.Status != config.AgentActive {
			continue
		}
		for _, binding := range snapshot.Config.Channels {
			channel := channels.Channel(binding.Type)
			if channel != channels.Telegram && channel != channels.WeCom && channel != channels.Feishu {
				continue
			}
			desired[channels.BindingKey{Channel: channel, BindingID: binding.BindingID}] = binding
		}
	}

	var firstErr error
	for key, binding := range desired {
		secretValue, err := m.secrets.Resolve(ctx, binding.CredentialRef)
		if err != nil {
			m.persistStatus(ctx, channels.BindingStatus{Channel: key.Channel, BindingID: key.BindingID, State: channels.ChannelStateError, Owner: m.owner, LastChangedAt: time.Now().UTC(), LastError: "凭据不可用"})
			if firstErr == nil {
				firstErr = fmt.Errorf("resolve connector credential for %s/%s: %w", key.Channel, key.BindingID, err)
			}
			continue
		}
		fingerprintBytes := sha256.Sum256([]byte(string(key.Channel) + "\x00" + binding.BindingID + "\x00" + binding.CredentialRef + "\x00" + secretValue))
		fingerprint := fmt.Sprintf("%x", fingerprintBytes[:])
		m.mu.RLock()
		current := m.processes[key]
		m.mu.RUnlock()
		if current != nil && current.fingerprint == fingerprint {
			select {
			case <-current.done:
				m.stopConnector(key, current)
				current = nil
			default:
				m.refreshProcessStatus(ctx, key, current)
				continue
			}
		}
		if current != nil {
			m.stopConnector(key, current)
		}
		if err := m.startConnector(ctx, key, binding, fingerprint); err != nil {
			m.persistStatus(ctx, channels.BindingStatus{Channel: key.Channel, BindingID: key.BindingID, State: channels.ChannelStateError, Owner: m.owner, LastChangedAt: time.Now().UTC(), LastError: "连接启动失败"})
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}

	m.mu.RLock()
	stale := make(map[channels.BindingKey]*connectorProcess)
	for key, process := range m.processes {
		if _, keep := desired[key]; !keep {
			stale[key] = process
		}
	}
	m.mu.RUnlock()
	for key, process := range stale {
		m.stopConnector(key, process)
	}
	return firstErr
}

func (m *channelConnectorManager) startConnector(ctx context.Context, key channels.BindingKey, binding config.ChannelBinding, fingerprint string) error {
	connectorCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	process := &connectorProcess{
		fingerprint: fingerprint, cancel: cancel, done: done,
		state: channels.ChannelStateConnecting, changedAt: time.Now().UTC(),
	}

	switch key.Channel {
	case channels.Telegram:
		value, err := decodeTelegramCredential(ctx, m.secrets, binding)
		if err != nil {
			cancel()
			return err
		}
		poller, err := telegram.NewPoller(telegram.PollerConfig{
			BotToken: value.BotToken, BaseURL: fallback(value.APIBaseURL, defaultTelegramBaseURL),
			// Long polls idle up to 20s server-side; the shared client's
			// request_timeout cap would abort every idle getUpdates cycle.
			HTTPClient:   &http.Client{},
			MaxFileBytes: storage.MaxArtifactBytes,
			OffsetStore: telegramRedisOffsetStore{
				client: m.redis,
				key:    telegramOffsetKey(key.BindingID, value.BotToken),
			},
			OnReady: func() { m.updateProcessStatus(key, channels.ChannelStateConnected, "") },
			OnError: func(error) {
				m.updateProcessStatus(key, channels.ChannelStateConnecting, "连接中断，正在重连")
			},
		})
		if err != nil {
			cancel()
			return err
		}
		m.mu.Lock()
		m.processes[key] = process
		m.mu.Unlock()
		bound, err := channels.NewBoundChannel(string(key.Channel)+"/"+key.BindingID, func(runCtx context.Context) error {
			return poller.Run(runCtx, func(messageCtx context.Context, message channels.InboundMessage) error {
				return m.publishInboundReliably(messageCtx, key.BindingID, message)
			})
		})
		if err != nil {
			cancel()
			return err
		}
		process.channel = bound
		m.refreshProcessStatus(ctx, key, process)
		m.runOpenClawChannel(connectorCtx, key, process, "Telegram")
	case channels.WeCom:
		value, err := decodeWeComCredential(ctx, m.secrets, binding)
		if err != nil {
			cancel()
			return err
		}
		client, err := wecombot.NewClient(wecombot.Config{
			BotID: value.BotID, Secret: value.Secret, Endpoint: value.Endpoint, Origin: value.Origin,
			HTTPClient: m.httpClient, MaxFileBytes: storage.MaxArtifactBytes,
			OnReady: func() { m.updateProcessStatus(key, channels.ChannelStateConnected, "") },
			OnDisconnected: func(error) {
				m.updateProcessStatus(key, channels.ChannelStateConnecting, "连接中断，正在重连")
			},
		})
		if err != nil {
			cancel()
			return err
		}
		sender, err := wecombot.NewSender(client)
		if err != nil {
			cancel()
			return err
		}
		process.stop = func(context.Context) error { return client.Close() }
		m.mu.Lock()
		m.processes[key] = process
		m.wecomSenders[key.BindingID] = sender
		m.mu.Unlock()
		bound, err := channels.NewBoundChannel(string(key.Channel)+"/"+key.BindingID, func(runCtx context.Context) error {
			return client.Run(runCtx, func(messageCtx context.Context, message channels.InboundMessage) error {
				return m.publishInboundReliably(messageCtx, key.BindingID, message)
			})
		})
		if err != nil {
			cancel()
			return err
		}
		process.channel = bound
		m.refreshProcessStatus(ctx, key, process)
		m.runOpenClawChannel(connectorCtx, key, process, "WeCom smart bot")
	case channels.Feishu:
		value, err := decodeFeishuCredential(ctx, m.secrets, binding)
		if err != nil {
			cancel()
			return err
		}
		connector, err := feishu.NewConnector(feishu.ConnectorConfig{
			AppID: value.AppID, AppSecret: value.AppSecret, BaseURL: value.APIBaseURL,
			HTTPClient: m.httpClient, TokenCache: m.feishuCache, MaxFileBytes: storage.MaxArtifactBytes,
			OnReady: func() { m.updateProcessStatus(key, channels.ChannelStateConnected, "") },
			OnError: func(error) {
				m.updateProcessStatus(key, channels.ChannelStateConnecting, "连接中断，正在重连")
			},
			OnReconnecting: func() { m.updateProcessStatus(key, channels.ChannelStateConnecting, "连接中断，正在重连") },
			OnReconnected:  func() { m.updateProcessStatus(key, channels.ChannelStateConnected, "") },
			OnDisconnected: func() { m.updateProcessStatus(key, channels.ChannelStateConnecting, "连接已断开，等待重连") },
		})
		if err != nil {
			cancel()
			return err
		}
		sender, err := connector.Sender()
		if err != nil {
			cancel()
			return err
		}
		process.stop = connector.Stop
		m.mu.Lock()
		m.processes[key] = process
		m.feishuSenders[key.BindingID] = sender
		m.mu.Unlock()
		bound, err := channels.NewBoundChannel(string(key.Channel)+"/"+key.BindingID, func(runCtx context.Context) error {
			return connector.Run(runCtx, func(messageCtx context.Context, message channels.InboundMessage) error {
				return m.publishInboundReliably(messageCtx, key.BindingID, message)
			})
		})
		if err != nil {
			cancel()
			return err
		}
		process.channel = bound
		m.refreshProcessStatus(ctx, key, process)
		m.runOpenClawChannel(connectorCtx, key, process, "Feishu")
	default:
		cancel()
		return fmt.Errorf("unsupported connector channel %q", key.Channel)
	}
	return nil
}

func (m *channelConnectorManager) runOpenClawChannel(ctx context.Context, key channels.BindingKey, process *connectorProcess, label string) {
	go func() {
		defer close(process.done)
		var runErr error
		panicErr := safego.Run(label+" connector", func() { runErr = process.channel.Run(ctx) })
		if panicErr != nil {
			runErr = panicErr
		}
		if runErr != nil && ctx.Err() == nil {
			m.updateProcessStatus(key, channels.ChannelStateError, "连接已停止")
			slog.Error(label+" connector stopped", "binding_id", key.BindingID, "error", runErr)
		}
	}()
}

func (m *channelConnectorManager) publishInboundReliably(ctx context.Context, bindingID string, inbound channels.InboundMessage) error {
	if m.ingress == nil {
		return errors.New("channel ingress is unavailable")
	}
	return m.ingress.publishReliably(ctx, bindingID, inbound)
}

func (m *channelConnectorManager) stopConnector(key channels.BindingKey, process *connectorProcess) {
	if process == nil {
		return
	}
	process.cancel()
	if process.stop != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = process.stop(stopCtx)
		cancel()
	}
	select {
	case <-process.done:
	case <-time.After(2 * time.Second):
	}
	m.mu.Lock()
	if m.processes[key] == process {
		delete(m.processes, key)
		switch key.Channel {
		case channels.WeCom:
			delete(m.wecomSenders, key.BindingID)
		case channels.Feishu:
			delete(m.feishuSenders, key.BindingID)
		}
	}
	m.mu.Unlock()
	m.persistStatus(context.Background(), channels.BindingStatus{
		Channel: key.Channel, BindingID: key.BindingID, State: channels.ChannelStateOffline,
		LastChangedAt: time.Now().UTC(),
	})
}

func (m *channelConnectorManager) stopAllConnectors() {
	m.mu.RLock()
	current := make(map[channels.BindingKey]*connectorProcess, len(m.processes))
	for key, process := range m.processes {
		current[key] = process
	}
	m.mu.RUnlock()
	for key, process := range current {
		m.stopConnector(key, process)
	}
}

func (m *channelConnectorManager) handlePlatformControl(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) (bool, error) {
	if m.controls == nil {
		return false, errors.New("channel control handler is unavailable")
	}
	return m.controls.handle(ctx, snapshot, bindingID, inbound)
}

func (m *channelConnectorManager) ResolveSender(ctx context.Context, tenantID, appCode string, configVersion uint64, key channels.BindingKey) (channels.Sender, error) {
	if key.Channel == channels.Web {
		return m.webSender, nil
	}
	if strings.TrimSpace(key.BindingID) == "" {
		return nil, fmt.Errorf("channel sender binding ID is required")
	}
	snapshot, err := m.repository.GetVersion(ctx, tenantID, appCode, configVersion)
	if err != nil {
		return nil, err
	}
	var resolvedBinding config.ChannelBinding
	found := false
	for _, candidate := range snapshot.Config.Channels {
		if candidate.Type == string(key.Channel) && candidate.BindingID == key.BindingID {
			resolvedBinding = candidate
			found = true
			break
		}
	}
	if !found {
		return nil, tenant.ErrNotFound
	}
	switch key.Channel {
	case channels.Telegram:
		value, err := decodeTelegramCredential(ctx, m.secrets, resolvedBinding)
		if err != nil {
			return nil, err
		}
		return telegram.NewSender(value.BotToken, fallback(value.APIBaseURL, defaultTelegramBaseURL), m.httpClient)
	case channels.Feishu:
		m.mu.RLock()
		sender := m.feishuSenders[key.BindingID]
		m.mu.RUnlock()
		if sender == nil {
			return nil, fmt.Errorf("Feishu connector %q is not connected on the active Gateway", key.BindingID)
		}
		return sender, nil
	case channels.WeCom:
		m.mu.RLock()
		sender := m.wecomSenders[key.BindingID]
		m.mu.RUnlock()
		if sender == nil {
			return nil, fmt.Errorf("WeCom connector %q is not connected on the active Gateway", key.BindingID)
		}
		return sender, nil
	default:
		return nil, fmt.Errorf("unsupported channel sender %q", key.Channel)
	}
}

func (m *channelConnectorManager) MaterializeOutboundArtifacts(ctx context.Context, tenantID, appCode string, configVersion uint64, refs []messaging.OutboundArtifactRef) ([]channels.OutboundFile, func(), error) {
	if len(refs) == 0 {
		return nil, func() {}, nil
	}
	snapshot, err := m.repository.GetVersion(ctx, tenantID, appCode, configVersion)
	if err != nil {
		return nil, nil, err
	}
	service, err := m.artifacts.ArtifactService(ctx, snapshot.Config)
	if err != nil {
		return nil, nil, err
	}
	directory, err := os.MkdirTemp("", "trpc-im-artifacts-")
	if err != nil {
		return nil, nil, fmt.Errorf("create outbound artifact directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	files := make([]channels.OutboundFile, 0, len(refs))
	for index, ref := range refs {
		if strings.TrimSpace(ref.UserID) == "" || strings.TrimSpace(ref.SessionID) == "" || strings.TrimSpace(ref.Filename) == "" || ref.Version < 0 {
			cleanup()
			return nil, nil, fmt.Errorf("invalid outbound artifact reference")
		}
		version := ref.Version
		artifact, err := service.LoadArtifact(ctx, agentartifact.SessionInfo{
			AppName: snapshot.Config.AppName(), UserID: ref.UserID, SessionID: ref.SessionID,
		}, ref.Filename, &version)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("load outbound artifact %q: %w", ref.Filename, err)
		}
		if artifact == nil {
			cleanup()
			return nil, nil, fmt.Errorf("outbound artifact %q version %d not found", ref.Filename, ref.Version)
		}
		name := strings.TrimSpace(ref.Name)
		if name == "" {
			name = strings.TrimPrefix(ref.Filename, "user:")
		}
		name = filepath.Base(name)
		if name == "" || name == "." {
			name = fmt.Sprintf("artifact-%d", index+1)
		}
		path := filepath.Join(directory, fmt.Sprintf("%02d-%s", index, name))
		if err := os.WriteFile(path, artifact.Data, 0o600); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("materialize outbound artifact %q: %w", ref.Filename, err)
		}
		files = append(files, channels.OutboundFile{Path: path, Name: name})
	}
	return files, cleanup, nil
}

func (m *channelConnectorManager) updateProcessStatus(key channels.BindingKey, state, lastError string) {
	m.mu.Lock()
	process := m.processes[key]
	if process == nil {
		m.mu.Unlock()
		return
	}
	if process.state != state || process.lastError != lastError {
		process.state = state
		process.lastError = lastError
		process.changedAt = time.Now().UTC()
	}
	status := channels.BindingStatus{
		Channel: key.Channel, BindingID: key.BindingID, State: process.state,
		Owner: m.owner, LastChangedAt: process.changedAt, LastError: process.lastError,
	}
	m.mu.Unlock()
	m.persistStatus(context.Background(), status)
}

func (m *channelConnectorManager) refreshProcessStatus(ctx context.Context, key channels.BindingKey, process *connectorProcess) {
	m.mu.RLock()
	if m.processes[key] != process {
		m.mu.RUnlock()
		return
	}
	status := channels.BindingStatus{
		Channel: key.Channel, BindingID: key.BindingID, State: process.state,
		Owner: m.owner, LastChangedAt: process.changedAt, LastError: process.lastError,
	}
	m.mu.RUnlock()
	m.persistStatus(ctx, status)
}

func (m *channelConnectorManager) persistStatus(ctx context.Context, status channels.BindingStatus) {
	if m == nil || m.redis == nil || strings.TrimSpace(status.BindingID) == "" {
		return
	}
	payload, err := json.Marshal(status)
	if err != nil {
		return
	}
	statusCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = m.redis.Set(statusCtx, channelStatusKey(status.Channel, status.BindingID), payload, channelStatusTTL).Err()
}

func channelStatusKey(channel channels.Channel, bindingID string) string {
	digest := sha256.Sum256([]byte(string(channel) + "\x00" + bindingID))
	return fmt.Sprintf("trpc-agent:channel-status:%x", digest[:12])
}

func telegramOffsetKey(bindingID, botToken string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(bindingID) + "\x00" + strings.TrimSpace(botToken)))
	return fmt.Sprintf("trpc-agent:telegram-offset:%x", digest[:16])
}

func (m *channelConnectorManager) ListBindingStatuses(ctx context.Context, tenantID, appCode string) ([]channels.BindingStatus, error) {
	snapshots, err := m.repository.ListApplications(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	statuses := make([]channels.BindingStatus, 0)
	for _, snapshot := range snapshots {
		if appCode != "" && snapshot.Config.AppCode != appCode {
			continue
		}
		for _, binding := range snapshot.Config.Channels {
			channel := channels.Channel(binding.Type)
			status := channels.BindingStatus{Channel: channel, BindingID: binding.BindingID}
			if snapshot.Config.Status != config.AgentActive {
				status.State = channels.ChannelStateOffline
				statuses = append(statuses, status)
				continue
			}
			payload, getErr := m.redis.Get(ctx, channelStatusKey(channel, binding.BindingID)).Bytes()
			if getErr != nil {
				if errors.Is(getErr, redis.Nil) {
					status.State = channels.ChannelStateOffline
					statuses = append(statuses, status)
					continue
				}
				return nil, fmt.Errorf("read channel runtime status: %w", getErr)
			}
			if err := json.Unmarshal(payload, &status); err != nil {
				return nil, fmt.Errorf("decode channel runtime status: %w", err)
			}
			statuses = append(statuses, status)
		}
	}
	return statuses, nil
}

var _ channels.BindingStatusLister = (*channelConnectorManager)(nil)

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
