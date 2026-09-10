package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

var (
	ErrDuplicateMessage     = errors.New("duplicate message")
	ErrMessageInProgress    = errors.New("message processing is already in progress")
	ErrExecutionClaimLost   = errors.New("execution claim ownership lost")
	ErrSessionLeaseLost     = errors.New("session execution lease ownership lost")
	ErrAgentExecutionFailed = errors.New("agent execution failed")
	ErrAgentProducedNoReply = errors.New("agent did not produce a reply")
)

const finalizationTimeout = 2 * time.Second

type runnerProvider interface {
	Acquire(context.Context, config.TenantConfig) (runner.Runner, func(), error)
}

type artifactProvider interface {
	ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error)
}

type modelInputValidator interface {
	ValidateInputs(config.ModelConfig, []config.ModelInputKind) error
}

type documentInputExtractor interface {
	SupportsDocument(string) bool
	ExtractDocument(context.Context, string, []byte) (string, error)
}

type replyDeltaPublisher interface {
	PublishDelta(context.Context, string, string, string, string) error
}

type imReplyDeltaPublisher interface {
	PublishProgress(context.Context, messaging.IMProgressEvent) error
}

type runtimeStateStore interface {
	storage.SessionManager
	storage.SessionExecutionLeaser
	RecordExecution(context.Context, storage.ExecutionRecord) (storage.OutboxEvent, error)
	RecordExecutionTrace(context.Context, storage.ExecutionTraceRecord) error
}

// InvocationFactory creates governance data for one inbound message. It is an
// explicit Runtime option so a cached Runner cannot retain data from a prior call.
type InvocationFactory func(context.Context, tenant.Snapshot, string, channels.InboundMessage, string) (governance.Invocation, error)

type RuntimeOption func(*Runtime) error

type tenantRoleResolver interface {
	RoleFor(context.Context, string, string) (identity.Role, error)
}

// WithTenantRateLimiter makes the tenant request bucket effective before
// running a fresh, durably claimed invocation.
func WithTenantRateLimiter(limiter governance.TenantRateLimiter) RuntimeOption {
	return func(runtime *Runtime) error {
		if limiter == nil {
			return fmt.Errorf("tenant rate limiter is required")
		}
		runtime.rateLimiter = limiter
		return nil
	}
}

// WithTenantRoleResolver resolves an authenticated enterprise actor to its
// current tenant role rather than relying on a placeholder runtime role.
func WithTenantRoleResolver(resolver tenantRoleResolver) RuntimeOption {
	return func(runtime *Runtime) error {
		if resolver == nil {
			return fmt.Errorf("tenant role resolver is required")
		}
		runtime.roleResolver = resolver
		return nil
	}
}

// WithModelCostCalculator adds platform-owned pricing to usage telemetry.
func WithModelCostCalculator(calculator ModelCostCalculator) RuntimeOption {
	return func(runtime *Runtime) error {
		if calculator == nil {
			return fmt.Errorf("model cost calculator is required")
		}
		runtime.costCalculator = calculator
		return nil
	}
}

func WithUsageGovernor(governor governance.UsageGovernor) RuntimeOption {
	return func(runtime *Runtime) error {
		if governor == nil {
			return fmt.Errorf("model usage governor is required")
		}
		runtime.usageGovernor = governor
		return nil
	}
}

// WithInvocationFactory injects the factory used to govern tool calls for each
// successfully acquired message lease.
func WithInvocationFactory(factory InvocationFactory) RuntimeOption {
	return func(runtime *Runtime) error {
		if factory == nil {
			return fmt.Errorf("governance invocation factory is required")
		}
		runtime.invocationFactory = factory
		return nil
	}
}

// WithObserver records redacted execution telemetry for each Runtime call.
func WithObserver(observer metrics.Observer) RuntimeOption {
	return func(runtime *Runtime) error {
		if observer == nil {
			return fmt.Errorf("execution observer is required")
		}
		runtime.observer = observer
		return nil
	}
}

// WithExecutionDedup injects the durable idempotency backstop. When set, a
// message whose execution is completed or owned by another node is rejected as
// duplicate even if the Redis lease was lost, closing the exactly-once gap.
func WithExecutionDedup(store storage.ExecutionDedupStore) RuntimeOption {
	return func(runtime *Runtime) error {
		if store == nil {
			return fmt.Errorf("execution dedup store is required")
		}
		runtime.executionDedup = store
		return nil
	}
}

// WithReplyDeltaPublisher enables best-effort token publication for browser
// requests. Final replies still use the durable Outbox path.
func WithReplyDeltaPublisher(publisher replyDeltaPublisher) RuntimeOption {
	return func(runtime *Runtime) error {
		if publisher == nil {
			return fmt.Errorf("reply delta publisher is required")
		}
		runtime.deltaPublisher = publisher
		return nil
	}
}

// WithIMReplyDeltaPublisher enables best-effort provider progress updates for
// IM requests. It never participates in final delivery; PostgreSQL Outbox is
// still the only durable reply path.
func WithIMReplyDeltaPublisher(publisher imReplyDeltaPublisher) RuntimeOption {
	return func(runtime *Runtime) error {
		if publisher == nil {
			return fmt.Errorf("IM reply delta publisher is required")
		}
		runtime.imDeltaPublisher = publisher
		return nil
	}
}

// WithArtifactProvider lets Web file inputs reuse the tenant's configured
// framework artifact backend. Runtime resolves the staged bytes only at the
// worker, keeping raw uploads out of Kafka.
func WithArtifactProvider(provider artifactProvider) RuntimeOption {
	return func(runtime *Runtime) error {
		if provider == nil {
			return fmt.Errorf("artifact provider is required")
		}
		runtime.artifacts = provider
		return nil
	}
}

// WithModelInputValidator makes multimodal support an explicit model-catalog
// contract instead of inferring it from an OpenAI-compatible protocol.
func WithModelInputValidator(validator modelInputValidator) RuntimeOption {
	return func(runtime *Runtime) error {
		if validator == nil {
			return fmt.Errorf("model input validator is required")
		}
		runtime.modelInputs = validator
		return nil
	}
}

// WithDocumentInputExtractor lets chat attachments reuse the platform's
// configured document extraction path before invoking the model.
func WithDocumentInputExtractor(extractor documentInputExtractor) RuntimeOption {
	return func(runtime *Runtime) error {
		if extractor == nil {
			return fmt.Errorf("document input extractor is required")
		}
		runtime.documentInputs = extractor
		return nil
	}
}

// Runtime is the tenant-aware asynchronous execution path used by Kafka Workers.
type Runtime struct {
	configurations    tenant.Repository
	runners           runnerProvider
	idempotency       storage.IdempotencyStore
	processingTTL     time.Duration
	completedTTL      time.Duration
	invocationFactory InvocationFactory
	observer          metrics.Observer
	executionDedup    storage.ExecutionDedupStore
	stateStore        runtimeStateStore
	deltaPublisher    replyDeltaPublisher
	imDeltaPublisher  imReplyDeltaPublisher
	sessionManager    storage.SessionManager
	artifacts         artifactProvider
	modelInputs       modelInputValidator
	documentInputs    documentInputExtractor
	rateLimiter       governance.TenantRateLimiter
	roleResolver      tenantRoleResolver
	costCalculator    ModelCostCalculator
	usageGovernor     governance.UsageGovernor
}

type snapshotContextKey struct{}
type resolvedSessionContextKey struct{}

// WithConfigurationSnapshot pins a validated control-plane snapshot to one
// asynchronous invocation. It is used after Kafka has durably captured the
// ingress version, so a later publish cannot change the in-flight execution.
func WithConfigurationSnapshot(ctx context.Context, snapshot tenant.Snapshot) context.Context {
	return context.WithValue(ctx, snapshotContextKey{}, snapshot)
}

func snapshotFromContext(ctx context.Context) (tenant.Snapshot, bool) {
	if ctx == nil {
		return tenant.Snapshot{}, false
	}
	snapshot, ok := ctx.Value(snapshotContextKey{}).(tenant.Snapshot)
	if !ok || snapshot.Config.TenantID == "" || snapshot.Config.AppCode == "" || snapshot.Config.ConfigVersion == 0 {
		return tenant.Snapshot{}, false
	}
	return snapshot, true
}

// WithResolvedSessionKey pins the canonical Session selected at Gateway
// ingress. Workers must not independently re-resolve an external identity.
func WithResolvedSessionKey(ctx context.Context, sessionKey string) context.Context {
	return context.WithValue(ctx, resolvedSessionContextKey{}, strings.TrimSpace(sessionKey))
}

func resolvedSessionKeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(resolvedSessionContextKey{}).(string)
	return strings.TrimSpace(value)
}

type Result struct {
	TenantID      string
	AppCode       string
	ConfigVersion uint64
	SessionKey    string
}

// NewRuntime constructs a runtime from explicit, testable dependencies.
func NewRuntime(configurations tenant.Repository, runners runnerProvider, idempotency storage.IdempotencyStore, stateStore runtimeStateStore, processingTTL time.Duration, completedTTL time.Duration, options ...RuntimeOption) (*Runtime, error) {
	if configurations == nil {
		return nil, fmt.Errorf("tenant configuration repository is required")
	}
	if runners == nil {
		return nil, fmt.Errorf("tenant runner provider is required")
	}
	if idempotency == nil {
		return nil, fmt.Errorf("idempotency store is required")
	}
	if stateStore == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if processingTTL <= 0 || completedTTL <= 0 {
		return nil, fmt.Errorf("idempotency TTLs must be positive")
	}
	runtime := &Runtime{
		configurations: configurations, runners: runners, idempotency: idempotency,
		stateStore: stateStore, sessionManager: stateStore,
		processingTTL: processingTTL, completedTTL: completedTTL,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("runtime option is required")
		}
		if err := option(runtime); err != nil {
			return nil, err
		}
	}
	return runtime, nil
}

// Handle resolves the external binding to a fixed tenant configuration snapshot,
// performs an idempotent Agent invocation, and commits the reply to durable state.
func (r *Runtime) Handle(ctx context.Context, externalBindingID string, inbound channels.InboundMessage) (result Result, returnErr error) {
	if err := inbound.Validate(); err != nil {
		return Result{}, fmt.Errorf("validate inbound message: %w", err)
	}
	if inbound.Channel == channels.Web && strings.TrimSpace(inbound.WebOwnerID) == "" {
		return Result{}, fmt.Errorf("validate inbound message: web owner ID is required")
	}
	if strings.TrimSpace(externalBindingID) == "" {
		return Result{}, fmt.Errorf("external binding ID is required")
	}
	snapshot, err := r.resolveSnapshot(ctx, inbound.Channel, externalBindingID)
	if err != nil {
		return Result{}, fmt.Errorf("resolve tenant channel binding: %w", err)
	}
	if r.observer != nil {
		var finish func(error)
		ctx, finish = r.observer.StartExecution(ctx, metrics.ExecutionAttributes{
			TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode, Channel: string(inbound.Channel),
			ProviderRequestID: inbound.ProviderRequestID, ConfigVersion: snapshot.Config.ConfigVersion,
		})
		defer func() { finish(returnErr) }()
	}
	ctx, inbound, sessionKey, err := r.prepareInboundExecution(ctx, snapshot, externalBindingID, inbound)
	if err != nil {
		return Result{}, err
	}
	traceID := traceIDFor(ctx, inbound.MessageID)
	ctx, leases, err := r.acquireExecutionLeases(ctx, snapshot, externalBindingID, sessionKey, inbound, traceID)
	if err != nil {
		return Result{}, err
	}
	defer leases.Close()
	if r.rateLimiter != nil {
		if err := r.rateLimiter.Take(ctx, snapshot.Config.TenantID, snapshot.Config.AppCode, snapshot.Config.Governance.RequestsPerMinute); err != nil {
			return Result{}, fmt.Errorf("enforce tenant request rate limit: %w", err)
		}
	}
	if requiredInputs := requiredModelInputKinds(inbound.Files, r.documentInputs); len(requiredInputs) > 0 {
		if r.modelInputs == nil {
			return Result{}, errors.New("model input validator is required for non-text input")
		}
		if err := r.modelInputs.ValidateInputs(snapshot.Config.Model, requiredInputs); err != nil {
			return Result{}, fmt.Errorf("validate model input capabilities: %w", err)
		}
	}
	tenantRunner, releaseRunner, err := r.runners.Acquire(ctx, snapshot.Config)
	if err != nil {
		return Result{}, fmt.Errorf("get tenant runner: %w", err)
	}
	defer releaseRunner()
	userMessage, err := r.buildUserMessage(ctx, snapshot.Config, sessionKey, inbound)
	if err != nil {
		return Result{}, err
	}
	runContext := ctx
	if r.invocationFactory != nil {
		invocation, err := r.invocationFactory(ctx, snapshot, externalBindingID, inbound, sessionKey)
		if err != nil {
			return Result{}, fmt.Errorf("build governance invocation: %w", err)
		}
		if err := invocation.Validate(); err != nil {
			return Result{}, fmt.Errorf("validate governance invocation: %w", err)
		}
		if invocation.Units != nil {
			if err := invocation.Units.Consume(1); err != nil {
				return Result{}, fmt.Errorf("reserve model budget: %w", err)
			}
		}
		runContext = governance.WithInvocation(runContext, invocation)
	}
	savedArtifacts := platformtool.NewSavedArtifactRecorder()
	runContext = platformtool.WithSavedArtifactRecorder(runContext, savedArtifacts)
	usagePolicy := snapshot.Config.Governance
	governUsage := usagePolicy.MaxConcurrentRuns > 0 || usagePolicy.TokenBudgetPerHour > 0
	var (
		usageReservation governance.UsageReservation
		usageHeartbeat   *usageReservationHeartbeat
		usageSettled     bool
	)
	if governUsage {
		if r.usageGovernor == nil {
			return Result{}, errors.New("model usage governor is required by tenant policy")
		}
		usageReservation, err = r.usageGovernor.Reserve(runContext, governance.UsageReservationRequest{
			TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode,
			Channel: string(inbound.Channel), BindingID: externalBindingID, MessageID: inbound.MessageID, TraceID: traceID,
			MaxConcurrentRuns: usagePolicy.MaxConcurrentRuns,
			TokenBudget:       usagePolicy.TokenBudgetPerHour, ReservedTokens: usagePolicy.TokenReservation,
			LeaseTTL: r.processingTTL,
		})
		if err != nil {
			return Result{}, fmt.Errorf("reserve model usage: %w", err)
		}
		usageHeartbeat = startUsageReservationHeartbeat(runContext, r.usageGovernor, usageReservation, r.processingTTL)
		runContext = usageHeartbeat.context
		defer func() {
			if usageHeartbeat != nil {
				usageHeartbeat.Stop()
			}
			if usageSettled {
				return
			}
			finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
			defer cancel()
			_ = r.usageGovernor.SettleUnknown(finalizeCtx, usageReservation)
		}()
	}
	events, err := tenantRunner.Run(
		runContext,
		inbound.SubjectID,
		sessionKey,
		userMessage,
		agent.WithRequestID(inbound.MessageID),
		agent.WithExecutionTraceEnabled(true),
		agent.WithLatencyDiagnostics(true),
	)
	if err != nil {
		if usageHeartbeat != nil {
			usageHeartbeat.Stop()
			usageHeartbeat = nil
		}
		if governUsage {
			finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
			settleErr := r.usageGovernor.SettleUnknown(finalizeCtx, usageReservation)
			cancel()
			if settleErr != nil {
				return Result{}, errors.Join(fmt.Errorf("run tenant agent: %w", err), fmt.Errorf("settle unknown model usage: %w", settleErr))
			}
			usageSettled = true
		}
		return Result{}, fmt.Errorf("run tenant agent: %w", err)
	}
	var imProgress strings.Builder
	outcome := collectRun(runContext, events, func(content string) {
		if inbound.Channel == channels.Web && r.deltaPublisher != nil {
			// Stream loss must not fail a governed model run: the committed
			// Outbox reply is the durable fallback and will finish the SSE.
			_ = r.deltaPublisher.PublishDelta(ctx, snapshot.Config.TenantID, inbound.WebOwnerID, inbound.MessageID, content)
			return
		}
		if inbound.Channel != channels.Web && r.imDeltaPublisher != nil && strings.TrimSpace(inbound.ProgressMessageID) != "" {
			imProgress.WriteString(content)
			current := strings.TrimSpace(imProgress.String())
			if current == "" {
				return
			}
			_ = r.imDeltaPublisher.PublishProgress(ctx, messaging.IMProgressEvent{
				TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode, ConfigVersion: snapshot.Config.ConfigVersion,
				Channel: inbound.Channel, BindingID: externalBindingID, ConversationID: inbound.ConversationID,
				ConversationScope: inbound.ConversationScope, ProviderReplyToken: inbound.ProviderReplyToken,
				ProgressMessageID: inbound.ProgressMessageID, RequestID: inbound.MessageID, Content: current,
			})
		}
	})
	if outcome.err != nil {
		if usageHeartbeat != nil {
			usageHeartbeat.Stop()
			usageHeartbeat = nil
		}
		if governUsage {
			finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
			settleErr := r.usageGovernor.SettleUnknown(finalizeCtx, usageReservation)
			cancel()
			if settleErr != nil {
				return Result{}, errors.Join(outcome.err, fmt.Errorf("settle unknown model usage: %w", settleErr))
			}
			usageSettled = true
		}
		if err := leases.Check(); err != nil {
			return Result{}, err
		}
		if projected := projectExecutionTrace(outcome.trace); projected != nil {
			finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
			traceErr := r.stateStore.RecordExecutionTrace(finalizeCtx, storage.ExecutionTraceRecord{
				TenantID:  snapshot.Config.TenantID,
				AppCode:   snapshot.Config.AppCode,
				Channel:   string(inbound.Channel),
				BindingID: externalBindingID,
				MessageID: inbound.MessageID,
				TraceID:   traceID,
				Trace:     *projected,
			})
			cancel()
			if traceErr != nil {
				return Result{}, fmt.Errorf("record failed execution trace: %w", traceErr)
			}
		}
		return Result{}, outcome.err
	}
	reply, usage := outcome.reply, outcome.usage
	costMicros := int64(0)
	pricedUsage := pricedTokenUsage(usage)
	if usage.known && r.costCalculator != nil {
		costMicros = r.costCalculator.CostMicros(snapshot.Config.Model, pricedUsage)
	}
	if usageHeartbeat != nil {
		usageHeartbeat.Stop()
		if heartbeatErr := usageHeartbeat.Err(); heartbeatErr != nil {
			finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
			settleErr := r.usageGovernor.SettleUnknown(finalizeCtx, usageReservation)
			cancel()
			usageHeartbeat = nil
			if settleErr == nil {
				usageSettled = true
			}
			return Result{}, errors.Join(fmt.Errorf("model usage reservation lease lost: %w", heartbeatErr), settleErr)
		}
		usageHeartbeat = nil
	}
	if governUsage {
		finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
		if usage.known {
			err = r.usageGovernor.SettleKnown(finalizeCtx, usageReservation, governance.SettledUsage{
				PromptTokens: usage.promptTokens, CompletionTokens: usage.completionTokens,
				TotalTokens: usage.totalTokens, CostMicros: costMicros,
			})
		} else {
			err = r.usageGovernor.SettleUnknown(finalizeCtx, usageReservation)
		}
		cancel()
		if err != nil {
			return Result{}, fmt.Errorf("settle model usage: %w", err)
		}
		usageSettled = true
	}
	if usage.known {
		if usageObserver, ok := r.observer.(metrics.ModelUsageObserver); ok {
			usageObserver.RecordModelUsage(ctx, metrics.ModelUsageAttributes{
				TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode,
				ProviderID: snapshot.Config.Model.ProviderID, ModelName: snapshot.Config.Model.Name,
				PromptTokens: usage.promptTokens, CachedPromptTokens: pricedUsage.CachedPromptTokens,
				CompletionTokens: usage.completionTokens, TotalTokens: usage.totalTokens,
				CostMicros: costMicros,
			})
		}
	}
	if err := leases.Check(); err != nil {
		return Result{}, err
	}
	outboundArtifacts := make([]messaging.OutboundArtifactRef, 0)
	for _, saved := range savedArtifacts.Snapshot() {
		name := strings.TrimSpace(saved.Name)
		if name == "" {
			name = strings.TrimPrefix(saved.Filename, "user:")
		}
		outboundArtifacts = append(outboundArtifacts, messaging.OutboundArtifactRef{
			Filename: saved.Filename, Version: saved.Version, Name: name, MimeType: saved.MimeType,
			UserID: inbound.SubjectID, SessionID: sessionKey,
		})
	}
	if _, err := r.recordExecution(ctx, snapshot, externalBindingID, sessionKey, inbound, reply, outboundArtifacts, usage, outcome.trace, leases.FencingToken()); err != nil {
		return Result{}, err
	}
	if err := leases.Complete(); err != nil {
		return Result{}, fmt.Errorf("complete message lease after reply: %w", err)
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	if err := r.deleteInboundFiles(finalizeCtx, snapshot.Config, sessionKey, inbound); err != nil {
		slog.Warn("clean completed inbound files", "tenant_id", snapshot.Config.TenantID, "app_code", snapshot.Config.AppCode, "error", err)
	}
	return Result{TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode, ConfigVersion: snapshot.Config.ConfigVersion, SessionKey: sessionKey}, nil
}

func (r *Runtime) resolveSnapshot(ctx context.Context, channel channels.Channel, externalBindingID string) (tenant.Snapshot, error) {
	if snapshot, ok := snapshotFromContext(ctx); ok {
		if channel.Supported() {
			return snapshot, nil
		}
		return tenant.Snapshot{}, fmt.Errorf("unsupported channel %q", channel)
	}
	return r.configurations.ResolveBinding(ctx, channel, externalBindingID)
}
