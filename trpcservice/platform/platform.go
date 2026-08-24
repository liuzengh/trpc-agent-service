// Package platform composes gateways, durable outboxes, workers and admin APIs.
package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/agent"
	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/DocJlm/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/governance"
	platformmetrics "github.com/DocJlm/trpc-agent-service/trpcservice/metrics"
	"github.com/DocJlm/trpc-agent-service/trpcservice/queue"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	platformtelemetry "github.com/DocJlm/trpc-agent-service/trpcservice/telemetry"
	"github.com/DocJlm/trpc-agent-service/trpcservice/web"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

type Role string

const (
	RoleAll     Role = "all"
	RoleGateway Role = "gateway"
	RoleWorker  Role = "worker"
	RoleAdmin   Role = "admin"
)

func ParseRole(value string) (Role, error) {
	role := Role(strings.ToLower(value))
	switch role {
	case RoleAll, RoleGateway, RoleWorker, RoleAdmin:
		return role, nil
	default:
		return "", fmt.Errorf("unsupported role %q", value)
	}
}

type Options struct {
	Repository      store.Repository
	RuntimeResolver store.RuntimeProfileResolver
	Governance      governance.Controller
	Queue           queue.Queue
	Locker          queue.Locker
	Engine          agent.Engine
	Secrets         secrets.Provider
	Logger          *slog.Logger
}

type Platform struct {
	cfg             config.Config
	repo            store.Repository
	resolver        store.RuntimeProfileResolver
	governance      governance.Controller
	governanceOwned bool
	queue           queue.Queue
	locker          queue.Locker
	engine          agent.Engine
	secrets         secrets.Provider
	logger          *slog.Logger

	registry *prometheus.Registry
	metrics  *platformmetrics.Registry
	workerID string

	mu       sync.RWMutex
	role     Role
	adapters map[string]channels.ChannelAdapter
}

func New(ctx context.Context, cfg config.Config, options Options) (*Platform, error) {
	p := &Platform{
		cfg: cfg, workerID: hostnameID(), adapters: make(map[string]channels.ChannelAdapter),
		registry: prometheus.NewRegistry(),
	}
	p.metrics = platformmetrics.New(p.registry)
	p.logger = options.Logger
	if p.logger == nil {
		p.logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	p.secrets = options.Secrets
	if p.secrets == nil {
		p.secrets = secrets.FileEnvProvider{}
	}

	var err error
	p.repo = options.Repository
	if p.repo == nil {
		if cfg.Database.PostgresDSN != "" {
			p.repo, err = store.NewPostgresRepository(ctx, cfg.Database.PostgresDSN)
			if err != nil {
				return nil, err
			}
		} else {
			p.repo = store.NewMemoryRepository()
		}
	}
	if err := p.repo.Migrate(ctx); err != nil {
		p.repo.Close()
		return nil, err
	}
	if err := p.repo.SeedTenants(ctx, cfg.Tenants); err != nil {
		p.repo.Close()
		return nil, err
	}
	p.resolver = options.RuntimeResolver
	if p.resolver == nil {
		p.resolver = store.NewCachedRuntimeResolver(p.repo, 30*time.Second)
	}

	p.queue = options.Queue
	p.locker = options.Locker
	if p.queue == nil {
		if cfg.Database.RedisAddr != "" {
			redisQueue, queueErr := queue.NewRedisQueue(
				ctx, cfg.Database.RedisAddr, cfg.Database.QueueName, cfg.Database.ConsumerGroup,
			)
			if queueErr != nil {
				p.repo.Close()
				return nil, queueErr
			}
			p.queue = redisQueue
			if p.locker == nil {
				p.locker = redisQueue.Locker()
			}
		} else {
			p.queue = queue.NewMemoryQueue(256)
		}
	}
	if p.locker == nil {
		p.locker = queue.NewMemoryLocker()
	}
	p.engine = options.Engine
	if p.engine == nil {
		p.governance = options.Governance
		if p.governance == nil {
			p.governance, err = governance.New(cfg.Database.RedisAddr)
			if err != nil {
				p.Close()
				return nil, err
			}
			p.governanceOwned = true
		}
		p.engine = agent.NewTRPCEngine(
			p.secrets,
			agent.WithRedisURL(cfg.Database.RedisAddr),
			agent.WithPostgresDSN(cfg.Database.PostgresDSN),
			agent.WithGovernance(p.governance),
		)
	}
	if err := p.buildAdapters(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Platform) buildAdapters() error {
	for _, tenantProfile := range p.cfg.Tenants {
		if !tenantProfile.Enabled {
			continue
		}
		for _, binding := range tenantProfile.Channels {
			if !binding.Enabled {
				continue
			}
			var adapter channels.ChannelAdapter
			switch strings.ToLower(binding.Type) {
			case wecom.ChannelName:
				adapter = wecom.New(tenantProfile.ID, binding.ID, binding.CredentialRef, p.secrets, p.acceptInbound)
			case feishu.ChannelName:
				adapter = feishu.New(tenantProfile.ID, binding.ID, binding.CredentialRef, p.secrets, p.acceptInbound)
			default:
				return fmt.Errorf("tenant %s has unsupported channel %q", tenantProfile.ID, binding.Type)
			}
			key := adapterKey(tenantProfile.ID, binding.ID)
			if _, exists := p.adapters[key]; exists {
				return fmt.Errorf("duplicate adapter %s", key)
			}
			p.adapters[key] = adapter
		}
	}
	return nil
}

func (p *Platform) Run(ctx context.Context, role Role) error {
	if _, err := ParseRole(string(role)); err != nil {
		return err
	}
	p.mu.Lock()
	p.role = role
	p.mu.Unlock()

	group, groupCtx := errgroup.WithContext(ctx)
	if role == RoleAll || role == RoleAdmin {
		admin := web.New(p.cfg, p.repo, p.registry, p.readiness)
		group.Go(func() error { return admin.Run(groupCtx) })
	}

	if role == RoleAll || role == RoleGateway {
		group.Go(func() error { return p.dispatchRelay(groupCtx) })
		group.Go(func() error { return p.replyRelay(groupCtx) })
		for key, item := range p.adapters {
			adapterKey := key
			adapter := item
			group.Go(func() error { return p.runAdapter(groupCtx, adapterKey, adapter) })
		}
	}
	if role == RoleAll || role == RoleWorker {
		group.Go(func() error { return p.resolver.Run(groupCtx) })
		group.Go(func() error { return p.worker(groupCtx) })
	}
	return group.Wait()
}

func (p *Platform) acceptInbound(ctx context.Context, message channels.InboundEnvelope) error {
	ctx = platformtelemetry.ContextWithTraceID(ctx, message.TraceID)
	ctx, span := otel.Tracer("trpc-agent-service/channel").Start(ctx, "channel.accept_inbound")
	defer span.End()
	span.SetAttributes(
		attribute.String("tenant.id", message.TenantID),
		attribute.String("messaging.system", message.Channel),
		attribute.String("messaging.message.id", message.ExternalMessageID),
	)
	accepted, err := p.repo.AcceptInbound(ctx, message)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if accepted {
		p.metrics.Inbound.WithLabelValues(message.TenantID, message.Channel).Inc()
		p.logger.InfoContext(ctx, "inbound accepted",
			"tenant", message.TenantID, "channel", message.Channel,
			"message_id", message.ExternalMessageID, "trace_id", message.TraceID)
	} else {
		p.metrics.Duplicate.WithLabelValues(message.TenantID, message.Channel).Inc()
	}
	return nil
}

func (p *Platform) dispatchRelay(ctx context.Context) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := p.relayDispatchBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.ErrorContext(ctx, "dispatch relay", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Platform) relayDispatchBatch(ctx context.Context) error {
	tasks, err := p.repo.ClaimDispatch(ctx, p.workerID+":relay", 32, 15*time.Second)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		taskCtx := platformtelemetry.ContextWithTraceID(ctx, task.Message.TraceID)
		taskCtx, span := otel.Tracer("trpc-agent-service/outbox").Start(taskCtx, "outbox.dispatch")
		if err := p.queue.Publish(taskCtx, task); err != nil {
			span.RecordError(err)
			span.End()
			_ = p.repo.RetryDispatch(taskCtx, task.ID, err)
			continue
		}
		if err := p.repo.CompleteDispatch(taskCtx, task.ID); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}
		span.End()
	}
	return nil
}

func (p *Platform) worker(ctx context.Context) error {
	consumer := p.workerID + ":worker"
	consecutiveReceiveErrors := 0
	for {
		delivery, err := p.queue.Receive(ctx, consumer, 2*time.Second)
		if errors.Is(err, context.DeadlineExceeded) {
			consecutiveReceiveErrors = 0
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			consecutiveReceiveErrors++
			backoff := time.Duration(min(consecutiveReceiveErrors, 5)) * 250 * time.Millisecond
			p.logger.WarnContext(ctx, "receive dispatch failed; retrying",
				"error", err, "backoff", backoff, "consumer", consumer)
			if !wait(ctx, backoff) {
				return nil
			}
			continue
		}
		consecutiveReceiveErrors = 0
		p.processDelivery(ctx, delivery)
	}
}

func (p *Platform) processDelivery(ctx context.Context, delivery queue.Delivery) {
	message := delivery.Task.Message
	ctx = platformtelemetry.ContextWithTraceID(ctx, message.TraceID)
	ctx, span := otel.Tracer("trpc-agent-service/worker").Start(ctx, "worker.process_message")
	defer span.End()
	span.SetAttributes(
		attribute.String("tenant.id", message.TenantID),
		attribute.String("messaging.system", message.Channel),
		attribute.String("session.id", message.SessionID()),
	)
	baseProfile, ok := p.cfg.TenantByID(message.TenantID)
	if !ok || !baseProfile.Enabled {
		_ = p.queue.Dead(ctx, delivery, errors.New("tenant is missing or disabled"))
		return
	}
	runtimeProfile, err := p.resolver.Resolve(ctx, message.TenantID)
	if err != nil {
		span.RecordError(err)
		p.retryOrDead(ctx, delivery, err)
		return
	}
	tenantProfile := runtimeProfile.Apply(baseProfile)
	replyID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(delivery.Task.ID+"|reply")).String()
	exists, err := p.repo.ReplyExists(ctx, replyID)
	if err == nil && exists {
		_ = p.queue.Ack(ctx, delivery)
		return
	}
	lockCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	lease, contended, err := acquireLeaseWithWait(
		lockCtx, p.locker, "session:"+message.TenantID+":"+message.SessionID(),
		60*time.Second, 100*time.Millisecond,
	)
	cancel()
	if contended {
		p.metrics.LeaseContention.WithLabelValues("session").Inc()
	}
	if err != nil {
		p.retryOrDead(ctx, delivery, err)
		return
	}
	defer lease.Release(context.Background())
	// A second delivery may have waited for the first delivery's session lease.
	// Re-check the deterministic reply after acquiring the lease so a retry can
	// never execute the model twice for the same inbox task.
	exists, err = p.repo.ReplyExists(ctx, replyID)
	if err != nil {
		span.RecordError(err)
		p.retryOrDead(ctx, delivery, err)
		return
	}
	if exists {
		_ = p.queue.Ack(ctx, delivery)
		return
	}

	started := time.Now()
	runCtx, runSpan := otel.Tracer("trpc-agent-service/runner").Start(ctx, "runner.run")
	result, runErr := p.engine.Run(runCtx, agent.Request{
		Tenant: tenantProfile, UserID: message.ExternalUserID,
		SessionID: message.SessionID(), Content: message.Content, TraceID: message.TraceID,
	})
	if runErr != nil {
		runSpan.RecordError(runErr)
	}
	runSpan.End()
	latency := time.Since(started)
	if runErr != nil {
		span.RecordError(runErr)
		p.metrics.AgentRuns.WithLabelValues(message.TenantID, "error").Inc()
		decision := "agent_error"
		if reason := governanceReason(runErr); reason != "" {
			decision = "governance_rejected"
			p.metrics.GovernanceReject.WithLabelValues(message.TenantID, reason).Inc()
		}
		_ = p.repo.AppendAudit(ctx, store.AuditLog{
			TenantID: message.TenantID, Channel: message.Channel, UserID: message.ExternalUserID,
			SessionID: message.SessionID(), AgentName: tenantProfile.Agent.ID,
			Decision: decision, Latency: latency, ErrorType: classifyError(runErr),
			TraceID: message.TraceID,
		})
		p.retryOrDead(ctx, delivery, runErr)
		return
	}
	p.metrics.AgentRuns.WithLabelValues(message.TenantID, "success").Inc()
	p.metrics.AgentLatency.WithLabelValues(message.TenantID).Observe(latency.Seconds())
	p.metrics.TokenUsage.WithLabelValues(message.TenantID, "input").Add(float64(result.PromptTokens))
	p.metrics.TokenUsage.WithLabelValues(message.TenantID, "output").Add(float64(result.CompletionTokens))
	p.metrics.CostUSD.WithLabelValues(message.TenantID).Add(result.CostUSD)
	out := channels.OutboundEnvelope{
		ID: replyID, TenantID: message.TenantID, BindingID: message.BindingID,
		Channel: message.Channel, ExternalConversationID: message.ExternalConversationID,
		ConversationType: message.ConversationType, ReplyToMessageID: message.ReplyToken,
		ReplyToken: message.ReplyToken, Content: result.Content, Final: true, TraceID: message.TraceID,
	}
	commitCtx, commitSpan := otel.Tracer("trpc-agent-service/session").Start(ctx, "session.commit_result")
	if err := p.repo.CommitAgentResult(commitCtx, message, out); err != nil {
		commitSpan.RecordError(err)
		commitSpan.End()
		span.RecordError(err)
		p.retryOrDead(ctx, delivery, err)
		return
	}
	commitSpan.End()
	for _, toolName := range result.ToolCalls {
		_ = p.repo.AppendAudit(ctx, store.AuditLog{
			TenantID: message.TenantID, Channel: message.Channel, UserID: message.ExternalUserID,
			SessionID: message.SessionID(), AgentName: tenantProfile.Agent.ID, ToolName: toolName,
			Decision: "tool_call", Latency: latency, TraceID: message.TraceID,
		})
	}
	_ = p.repo.AppendAudit(ctx, store.AuditLog{
		TenantID: message.TenantID, Channel: message.Channel, UserID: message.ExternalUserID,
		SessionID: message.SessionID(), AgentName: tenantProfile.Agent.ID,
		Decision: "reply_queued", Latency: latency, Cost: result.CostUSD, TraceID: message.TraceID,
	})
	if err := p.queue.Ack(ctx, delivery); err != nil {
		p.logger.ErrorContext(ctx, "ack completed dispatch", "error", err, "trace_id", message.TraceID)
	}
}

func (p *Platform) retryOrDead(ctx context.Context, delivery queue.Delivery, cause error) {
	if delivery.Task.Attempts >= 7 {
		_ = p.queue.Dead(ctx, delivery, cause)
		return
	}
	_ = p.queue.Retry(ctx, delivery)
}

func acquireLeaseWithWait(ctx context.Context, locker queue.Locker, key string, ttl, retryInterval time.Duration) (queue.Lease, bool, error) {
	contended := false
	for {
		lease, err := locker.Acquire(ctx, key, ttl)
		if err == nil {
			return lease, contended, nil
		}
		if !errors.Is(err, queue.ErrLeaseBusy) {
			return nil, contended, err
		}
		contended = true
		if !wait(ctx, retryInterval) {
			return nil, contended, ctx.Err()
		}
	}
}

func (p *Platform) replyRelay(ctx context.Context) error {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		tasks, err := p.repo.ClaimReplies(ctx, p.workerID+":gateway", 16, 30*time.Second)
		if err != nil && ctx.Err() == nil {
			p.logger.ErrorContext(ctx, "claim replies", "error", err)
		}
		for _, task := range tasks {
			taskCtx := platformtelemetry.ContextWithTraceID(ctx, task.Message.TraceID)
			taskCtx, span := otel.Tracer("trpc-agent-service/gateway").Start(taskCtx, "channel.deliver_reply")
			span.SetAttributes(
				attribute.String("tenant.id", task.Message.TenantID),
				attribute.String("messaging.system", task.Message.Channel),
			)
			adapter := p.adapters[adapterKey(task.Message.TenantID, task.Message.BindingID)]
			if adapter == nil {
				err = errors.New("channel adapter is not active on this gateway")
			} else {
				err = adapter.Send(taskCtx, task.Message)
			}
			if err != nil {
				span.RecordError(err)
				span.End()
				p.metrics.Reply.WithLabelValues(task.Message.TenantID, task.Message.Channel, "error").Inc()
				_ = p.repo.RetryReply(ctx, task.ID, err)
				continue
			}
			p.metrics.Reply.WithLabelValues(task.Message.TenantID, task.Message.Channel, "success").Inc()
			_ = p.repo.CompleteReply(ctx, task.ID)
			span.End()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Platform) runAdapter(ctx context.Context, adapterKey string, adapter channels.ChannelAdapter) error {
	key := "channel:" + adapterKey
	for ctx.Err() == nil {
		lease, err := p.locker.Acquire(ctx, key, 45*time.Second)
		if err != nil {
			if !errors.Is(err, queue.ErrLeaseBusy) && ctx.Err() == nil {
				p.logger.ErrorContext(ctx, "channel lease", "binding", adapter.ID(), "error", err)
			}
			p.metrics.LeaseContention.WithLabelValues("channel").Inc()
			if !wait(ctx, 5*time.Second) {
				return nil
			}
			continue
		}
		adapterCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-adapterCtx.Done():
					return
				case <-ticker.C:
					if err := lease.Renew(adapterCtx, 45*time.Second); err != nil {
						cancel()
						return
					}
				}
			}
		}()
		runErr := adapter.Run(adapterCtx)
		cancel()
		<-renewDone
		_ = lease.Release(context.Background())
		if ctx.Err() != nil {
			return nil
		}
		if runErr != nil {
			p.logger.ErrorContext(ctx, "channel stopped", "binding", adapter.ID(), "error", runErr)
		}
		if !wait(ctx, 3*time.Second) {
			return nil
		}
	}
	return nil
}

func (p *Platform) readiness() web.ReadyStatus {
	p.mu.RLock()
	role := p.role
	p.mu.RUnlock()
	result := web.ReadyStatus{Ready: true, Role: string(role), Channels: make(map[string]channels.ChannelHealth)}
	if role == RoleGateway || role == RoleAll {
		for key, adapter := range p.adapters {
			health := adapter.Health()
			result.Channels[key] = health
			p.metrics.ChannelReady.WithLabelValues(tenantFromKey(key), channelFromKey(key, p.cfg), adapter.ID()).Set(boolFloat(health.Ready))
			if !health.Ready {
				result.Ready = false
			}
		}
	}
	return result
}

func (p *Platform) Close() {
	if p.engine != nil {
		_ = p.engine.Close()
	}
	if p.governanceOwned && p.governance != nil {
		_ = p.governance.Close()
	}
	if p.queue != nil {
		_ = p.queue.Close()
	}
	if p.repo != nil {
		p.repo.Close()
	}
}

func adapterKey(tenantID, bindingID string) string { return tenantID + "|" + bindingID }

func tenantFromKey(key string) string {
	tenantID, _, _ := strings.Cut(key, "|")
	return tenantID
}

func channelFromKey(key string, cfg config.Config) string {
	tenantID, bindingID, _ := strings.Cut(key, "|")
	profile, ok := cfg.TenantByID(tenantID)
	if !ok {
		return "unknown"
	}
	for _, binding := range profile.Channels {
		if binding.ID == bindingID {
			return binding.Type
		}
	}
	return "unknown"
}

func hostnameID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "node"
	}
	return hostname + "-" + uuid.NewString()[:8]
}

func classifyError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "401"), strings.Contains(text, "credential"), strings.Contains(text, "api key"):
		return "authentication"
	case strings.Contains(text, "429"), strings.Contains(text, "rate"):
		return "rate_limit"
	default:
		return "agent"
	}
}

func governanceReason(err error) string {
	switch {
	case errors.Is(err, governance.ErrConcurrencyLimit):
		return "concurrency"
	case errors.Is(err, governance.ErrInputTokenLimit):
		return "input_tokens"
	case errors.Is(err, governance.ErrDailyCostLimit):
		return "daily_cost"
	case errors.Is(err, governance.ErrToolDenied):
		return "tool_denied"
	default:
		text := strings.ToLower(err.Error())
		for _, reason := range []string{"concurrency", "input token", "daily cost", "tool denied"} {
			if strings.Contains(text, reason) {
				return strings.ReplaceAll(reason, " ", "_")
			}
		}
		return ""
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
