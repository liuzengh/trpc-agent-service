// Package executor implements the single-process multi-tenant Agent runtime.
package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	openaioption "github.com/openai/openai-go/option"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformmessage "github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	ErrUnknownBinding           = errors.New("unknown binding")
	ErrConfigurationUnavailable = errors.New("runtime configuration unavailable")
	ErrDependencyUnavailable    = errors.New("runtime dependency unavailable")
	ErrRunnerDraining           = errors.New("runner is draining")
	ErrAgentTimeout             = errors.New("agent request timed out")
	ErrAgentFailed              = errors.New("agent request failed")
	ErrEmptyAgentResponse       = errors.New("agent returned an empty response")
	ErrWorkerLost               = errors.New("worker lost while processing task")
	ErrTurnTooLarge             = errors.New("session turn too large")
)

type Request = platformmessage.InboundMessage
type Reply = platformmessage.OutboundMessage

type Runtime struct {
	identitySecret []byte
	repository     tenant.Repository
	router         *routing.Router
	backends       *storage.BackendProvider
	registry       *agent.RunnerRegistry
	catalogTenants []tenant.Tenant
	controlRepo    control.Repository
	policyCache    *governance.PolicyCache
	policyEnforcer *governance.PolicyEnforcer
	confirmations  *governance.ConfirmationManager
	auditSink      *metrics.AsyncAuditSink
	policyWarmMu   sync.Mutex
	policyWarmed   bool

	lifecycle sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

func New(cfg config.Config) (*Runtime, error) {
	catalog, credentials, err := cfg.RuntimeCatalog()
	if err != nil {
		return nil, err
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		return nil, fmt.Errorf("create tenant repository: %w", err)
	}
	fencingMode := config.DefaultSessionFencing
	if cfg.Messaging != nil && cfg.Messaging.SessionFencing != "" {
		fencingMode = cfg.Messaging.SessionFencing
	}
	messagingPrefix := ""
	messagingURL := ""
	if cfg.Messaging != nil {
		messagingPrefix = cfg.Messaging.KeyPrefix
		messagingURL = cfg.Messaging.RedisURL
	}
	backends, err := storage.NewBackendProvider(repository, credentials, fencingMode, messagingPrefix, messagingURL)
	if err != nil {
		return nil, err
	}
	if cfg.Messaging != nil {
		backends.SetTurnLimits(sessionfence.Limits{MaxTurnEvents: cfg.Messaging.MaxTurnEvents, MaxTurnBytes: cfg.Messaging.MaxTurnBytes})
	}
	registry, err := agent.NewRunnerRegistry(
		repository,
		backends,
		credentials,
		agent.DefaultCacheConfig(),
		func(ctx context.Context, key agent.CacheKey, configVersion tenant.ConfigVersion, apiKey string, sessions session.Service, memories memory.Service) (frameworkrunner.Runner, error) {
			if cfg.ControlPlane == nil {
				return newRunner(ctx, key, configVersion, apiKey, sessions, memories)
			}
			return newGovernedRunner(ctx, key, configVersion, apiKey, sessions, memories)
		},
	)
	if err != nil {
		_ = backends.Close()
		return nil, err
	}
	router, err := routing.NewWithDigestV2(repository, cfg.IdentitySecret, cfg.ControlPlane != nil && cfg.ControlPlane.TraceDigestV2Enabled)
	if err != nil {
		_ = registry.Close()
		_ = backends.Close()
		return nil, err
	}
	runtime := &Runtime{
		identitySecret: append([]byte(nil), cfg.IdentitySecret...),
		repository:     repository,
		router:         router,
		backends:       backends,
		registry:       registry,
		catalogTenants: append([]tenant.Tenant(nil), catalog.Tenants...),
	}
	if cfg.ControlPlane != nil {
		rawControl, err := control.NewRedisRepository(*cfg.ControlPlane)
		if err != nil {
			_ = registry.Close()
			_ = backends.Close()
			return nil, err
		}
		defaultTools := platformtool.NewRegistry().NonDangerousNames()
		controlRepo := control.NewInitializingRepository(rawControl, catalog.Tenants, defaultTools)
		cache, err := governance.NewPolicyCache(controlRepo, cfg.ControlPlane.PolicyCacheTTL)
		if err != nil {
			_ = rawControl.Close()
			_ = registry.Close()
			_ = backends.Close()
			return nil, err
		}
		confirmations, err := governance.NewConfirmationManager(controlRepo, 5*time.Minute, cfg.IdentitySecret)
		if err != nil {
			_ = rawControl.Close()
			_ = registry.Close()
			_ = backends.Close()
			return nil, err
		}
		runtime.controlRepo = controlRepo
		runtime.policyCache = cache
		runtime.auditSink = metrics.NewAuditSink(controlRepo, 256)
		runtime.policyEnforcer = governance.NewPolicyEnforcer(cache, cfg.IdentitySecret, controlRepo)
		runtime.policyEnforcer.SetAuditSink(runtime.auditSink)
		runtime.confirmations = confirmations
	}
	return runtime, nil
}

func newRunner(
	_ context.Context,
	key agent.CacheKey,
	configVersion tenant.ConfigVersion,
	apiKey string,
	sessions session.Service,
	memories memory.Service,
) (frameworkrunner.Runner, error) {
	if key.TenantID != configVersion.TenantID || key.AgentAppID != configVersion.AgentAppID || key.ConfigVersion != configVersion.Version {
		return nil, errors.New("runner configuration does not match cache key")
	}
	return frameworkrunner.NewRunner(
		tenant.AppName(key.TenantID, key.AgentAppID),
		buildAgent(configVersion, apiKey),
		frameworkrunner.WithSessionService(sessions),
		frameworkrunner.WithMemoryService(memories),
	), nil
}

func newGovernedRunner(
	ctx context.Context,
	key agent.CacheKey,
	configVersion tenant.ConfigVersion,
	apiKey string,
	sessions session.Service,
	memories memory.Service,
) (frameworkrunner.Runner, error) {
	if key.TenantID != configVersion.TenantID || key.AgentAppID != configVersion.AgentAppID || key.ConfigVersion != configVersion.Version {
		return nil, errors.New("runner configuration does not match cache key")
	}
	return frameworkrunner.NewRunner(
		tenant.AppName(key.TenantID, key.AgentAppID),
		buildGovernedAgent(configVersion, apiKey),
		frameworkrunner.WithSessionService(sessions),
		frameworkrunner.WithMemoryService(memories),
	), nil
}

func (r *Runtime) Ready(ctx context.Context) error {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := r.registry.Ready(ctx); err != nil {
		return fmt.Errorf("runtime not ready: %w", err)
	}
	if r.policyCache != nil {
		if err := r.controlRepo.Ready(ctx); err != nil {
			return fmt.Errorf("control plane not ready: %w", err)
		}
		if err := r.ensurePoliciesWarmed(ctx); err != nil {
			return fmt.Errorf("policy cache not ready: %w", err)
		}
	}
	return nil
}

func (r *Runtime) ensurePoliciesWarmed(ctx context.Context) error {
	r.policyWarmMu.Lock()
	defer r.policyWarmMu.Unlock()
	if r.policyWarmed {
		return nil
	}
	ids := make([]string, 0, len(r.repositoryCatalogTenants()))
	for _, item := range r.repositoryCatalogTenants() {
		if item.Enabled {
			ids = append(ids, item.ID)
		}
	}
	if err := r.policyCache.Warm(ctx, ids); err != nil {
		return err
	}
	r.policyWarmed = true
	return nil
}

func (r *Runtime) repositoryCatalogTenants() []tenant.Tenant {
	return append([]tenant.Tenant(nil), r.catalogTenants...)
}

// AuthorizeTask is called by the assigned Worker after node admission and
// before the Session lease is acquired.
func (r *Runtime) AuthorizeTask(ctx context.Context, task platformmessage.ExecutionTask) error {
	if r.policyEnforcer == nil {
		return nil
	}
	return r.policyEnforcer.ReauthorizeTask(ctx, task)
}

func (r *Runtime) policyForTask(ctx context.Context, task platformmessage.ExecutionTask) (governance.PolicySnapshot, error) {
	if r.policyCache == nil {
		return governance.PolicySnapshot{}, nil
	}
	snapshot, err := r.policyCache.Snapshot(ctx, task.TenantID)
	if err != nil {
		return governance.PolicySnapshot{}, err
	}
	if err := governance.AuthorizeActor(snapshot.Policy, r.identitySecret, task.ActorUserID); err != nil {
		return governance.PolicySnapshot{}, err
	}
	return snapshot, nil
}

func (r *Runtime) backendForBinding(ctx context.Context, channel, bindingID string) (storage.Backend, error) {
	binding, err := r.repository.ResolveBinding(ctx, channel, bindingID)
	if err != nil {
		return nil, err
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AgentAppID)
	if err != nil {
		return nil, err
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, binding.TenantID, binding.AgentAppID, app.ActiveConfigVersion)
	if err != nil {
		return nil, err
	}
	profile, err := r.repository.GetStorageProfile(ctx, binding.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return nil, err
	}
	return r.backends.BackendFor(ctx, profile)
}

// PersistenceRoute resolves the immutable storage identity selected by the
// exact task config. Worker locks this route in Messaging Redis before running
// the Agent so a later config edit cannot move an Agent App between backends.
func (r *Runtime) PersistenceRoute(ctx context.Context, task platformmessage.ExecutionTask) (persistence.Route, error) {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := task.Validate(); err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil || binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return persistence.Route{}, ErrUnknownBinding
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	profile, err := r.repository.GetStorageProfile(ctx, task.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	fingerprint, err := r.backends.BackendIdentityFor(ctx, profile)
	if err != nil {
		return persistence.Route{}, fmt.Errorf("%w: selected storage identity unavailable", ErrConfigurationUnavailable)
	}
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: fingerprint}
	if err := route.Validate(); err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	return route, nil
}

// Persist commits an already staged SQL turn without invoking the Agent.
func (r *Runtime) Persist(ctx context.Context, envelope persistence.Envelope) error {
	ctx, span := telemetry.Start(ctx, "persistence.persist")
	defer span.End()
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := envelope.Validate(); err != nil {
		return err
	}
	profile, err := r.repository.GetStorageProfile(ctx, envelope.TenantID, envelope.StorageProfileID)
	if err != nil || profile.Kind != envelope.BackendKind {
		return ErrConfigurationUnavailable
	}
	backend, err := r.backends.BackendFor(ctx, profile)
	if err != nil {
		if errors.Is(err, persistence.ErrSchemaIncompatible) {
			return persistence.ErrSchemaIncompatible
		}
		return persistence.ErrBackendUnavailable
	}
	if backend.Fingerprint() != envelope.BackendFingerprint {
		return persistence.ErrFingerprintConflict
	}
	persistent, ok := backend.(storage.PersistentBackend)
	if !ok || persistent.Committer() == nil {
		return persistence.ErrBackendUnavailable
	}
	return persistent.Committer().Commit(ctx, envelope)
}

func (r *Runtime) Handle(ctx context.Context, req Request) (Reply, error) {
	ctx, span := telemetry.Start(ctx, "runner.execute")
	defer span.End()
	router := r.router
	if router == nil {
		var err error
		router, err = routing.New(r.repository, r.identitySecret)
		if err != nil {
			return Reply{}, fmt.Errorf("%w: router unavailable", ErrConfigurationUnavailable)
		}
	}
	task, err := router.Resolve(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrUnknownBinding):
			return Reply{}, ErrUnknownBinding
		case errors.Is(err, routing.ErrConfigurationUnavailable):
			return Reply{}, ErrConfigurationUnavailable
		default:
			return Reply{}, err
		}
	}
	return r.Execute(ctx, task)
}

// Execute runs an immutable task created by the trusted Router. It validates
// the binding and exact config version again before acquiring a Runner.
func (r *Runtime) Execute(ctx context.Context, task platformmessage.ExecutionTask) (Reply, error) {
	ctx, span := telemetry.Start(ctx, "runner.execute")
	defer span.End()
	// Runtime.Close takes the write lock, so active Runner and backend work is
	// drained before the registry and borrowed services are closed.
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := task.Validate(); err != nil {
		return Reply{}, fmt.Errorf("%w: invalid execution task", ErrConfigurationUnavailable)
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return Reply{}, ErrUnknownBinding
		}
		return Reply{}, fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	if binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return Reply{}, fmt.Errorf("%w: task binding mismatch", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetTenant(ctx, binding.TenantID); err != nil {
		return Reply{}, fmt.Errorf("%w: tenant lookup failed", ErrConfigurationUnavailable)
	}
	app, err := r.repository.GetAgentApp(ctx, task.TenantID, task.AgentAppID)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: agent app lookup failed", ErrConfigurationUnavailable)
	}
	if app.TenantID != task.TenantID || app.ID != task.AgentAppID {
		return Reply{}, fmt.Errorf("%w: task agent app mismatch", ErrConfigurationUnavailable)
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: active config lookup failed", ErrConfigurationUnavailable)
	}
	policySnapshot, err := r.policyForTask(ctx, task)
	if err != nil {
		return Reply{}, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, configVersion.Model.RequestTimeout)
	defer cancel()
	requestCtx = governance.WithPolicy(requestCtx, policySnapshot)
	requestCtx = governance.WithTaskAudit(requestCtx, r.auditSink, task, r.identitySecret)
	requestCtx, capturedGovernanceError := governance.WithExecutionErrorCapture(requestCtx)
	if r.confirmations != nil {
		requestCtx = governance.WithConfirmationManager(requestCtx, r.confirmations, governance.ActorHash(r.identitySecret, task.ActorUserID), task.SessionID)
	}
	if nonce, ok := governance.ParseConfirmationCommand(task.Text); ok && r.confirmations != nil {
		if _, confirmErr := r.confirmations.Approve(requestCtx, task.TenantID, governance.ActorHash(r.identitySecret, task.ActorUserID), task.SessionID, nonce); confirmErr != nil {
			return Reply{}, confirmErr
		}
		return Reply{Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID, TraceID: task.TraceID, TraceParent: task.TraceParent, DigestVersion: task.DigestVersion, SessionID: task.SessionID, Text: "confirmation accepted"}, nil
	}
	key := agent.CacheKey{
		TenantID: task.TenantID, AgentAppID: task.AgentAppID, ConfigVersion: task.ConfigVersion,
	}
	lease, err := r.registry.Acquire(requestCtx, key)
	if err != nil {
		switch {
		case errors.Is(err, agent.ErrRunnerDrain):
			return Reply{}, ErrRunnerDraining
		case errors.Is(err, context.DeadlineExceeded):
			return Reply{}, ErrAgentTimeout
		case errors.Is(err, agent.ErrRegistryConfiguration):
			return Reply{}, fmt.Errorf("%w: runner configuration unavailable", ErrConfigurationUnavailable)
		case errors.Is(err, agent.ErrRegistryDependency), errors.Is(err, agent.ErrCacheClosed), errors.Is(err, agent.ErrCacheFull):
			return Reply{}, fmt.Errorf("%w: runner dependency unavailable", ErrDependencyUnavailable)
		default:
			return Reply{}, fmt.Errorf("acquire runner: %w", err)
		}
	}
	defer lease.Release()

	modelStarted := time.Now()
	storageCtx, sessionSpan := telemetry.Start(requestCtx, "session.read")
	storageCtx, memorySpan := telemetry.Start(storageCtx, "memory.read")
	events, err := lease.Runner.Run(
		storageCtx,
		task.RunnerUserID,
		task.SessionID,
		model.Message{Role: model.RoleUser, Content: task.Text},
		frameworkagent.WithRequestID(task.RequestID),
	)
	if err != nil {
		memorySpan.End()
		sessionSpan.End()
		telemetry.RecordModel(requestCtx, time.Since(modelStarted).Seconds())
		if captured := capturedGovernanceError(); captured != nil {
			return Reply{}, captured
		}
		if contextDeadlineExceeded(requestCtx) {
			return Reply{}, ErrAgentTimeout
		}
		if requestCtx.Err() == nil {
			if checkErr := lease.Backend.Check(requestCtx); checkErr != nil {
				return Reply{}, fmt.Errorf("%w: selected storage unavailable", ErrDependencyUnavailable)
			}
		}
		return Reply{}, ErrAgentFailed
	}
	text, eventErr := collectText(requestCtx, events)
	memorySpan.End()
	sessionSpan.End()
	telemetry.RecordModel(requestCtx, time.Since(modelStarted).Seconds())
	_, sessionWriteSpan := telemetry.Start(requestCtx, "session.write")
	sessionWriteSpan.End()
	_, memoryWriteSpan := telemetry.Start(requestCtx, "memory.write")
	memoryWriteSpan.End()
	if captured := capturedGovernanceError(); captured != nil {
		return Reply{}, captured
	}
	if eventErr != nil {
		if contextDeadlineExceeded(requestCtx) {
			return Reply{}, ErrAgentTimeout
		}
		if requestCtx.Err() == nil {
			if checkErr := lease.Backend.Check(requestCtx); checkErr != nil {
				return Reply{}, fmt.Errorf("%w: selected storage unavailable", ErrDependencyUnavailable)
			}
		}
		return Reply{}, eventErr
	}
	return Reply{
		Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID,
		TraceID: task.TraceID, TraceParent: task.TraceParent, DigestVersion: task.DigestVersion, SessionID: task.SessionID, Text: text,
	}, nil
}

// ExecuteFenced runs a turn against the platform-owned fenced Session
// namespace. Runner writes are staged locally and committed only after the
// runner has returned successfully.
func (r *Runtime) ExecuteFenced(ctx context.Context, task platformmessage.ExecutionTask, fence sessionfence.Fence) (Reply, sessionfence.TurnCommit, error) {
	ctx, span := telemetry.Start(ctx, "runner.execute")
	defer span.End()
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := task.Validate(); err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil || binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return Reply{}, sessionfence.TurnCommit{}, ErrUnknownBinding
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	policySnapshot, policyErr := r.policyForTask(ctx, task)
	if policyErr != nil {
		return Reply{}, sessionfence.TurnCommit{}, policyErr
	}
	requestCtx, cancel := context.WithTimeout(ctx, configVersion.Model.RequestTimeout)
	defer cancel()
	profile, err := r.repository.GetStorageProfile(ctx, task.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	backend, err := r.backends.BackendFor(requestCtx, profile)
	if err != nil {
		if profile.Kind.IsSQL() {
			if errors.Is(err, persistence.ErrSchemaIncompatible) {
				return Reply{}, sessionfence.TurnCommit{}, persistence.ErrSchemaIncompatible
			}
			return Reply{}, sessionfence.TurnCommit{}, persistence.ErrBackendUnavailable
		}
		return Reply{}, sessionfence.TurnCommit{}, ErrDependencyUnavailable
	}
	svc, ok := backend.Session().(sessionfence.StagingSession)
	if !ok {
		return Reply{}, sessionfence.TurnCommit{}, errors.New("strong session fencing backend is unavailable")
	}
	key := agent.CacheKey{TenantID: task.TenantID, AgentAppID: task.AgentAppID, ConfigVersion: task.ConfigVersion}
	lease, err := r.registry.Acquire(requestCtx, key)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	defer lease.Release()
	if fence.UserCoord == "" {
		fence.UserCoord = sessionfence.UserCoordFor(task.TenantID, task.ChannelBindingID, task.RunnerUserID)
	}
	turn := svc.StartTurn(session.Key{AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID}, fence)
	if nonce, ok := governance.ParseConfirmationCommand(task.Text); ok && r.confirmations != nil {
		actorHash := governance.ActorHash(r.identitySecret, task.ActorUserID)
		confirmation, confirmErr := r.confirmations.Approve(ctx, task.TenantID, actorHash, task.SessionID, nonce)
		if confirmErr != nil {
			svc.Discard(turn)
			return Reply{}, sessionfence.TurnCommit{}, confirmErr
		}
		// Approval is deliberately separate from consumption. The next matching
		// dangerous tool call consumes the one-time record atomically.
		_ = confirmation
		commit, prepareErr := svc.Prepare(turn)
		if prepareErr != nil {
			svc.Discard(turn)
			return Reply{}, sessionfence.TurnCommit{}, prepareErr
		}
		commit.TraceParent = task.TraceParent
		commit.DigestVersion = task.DigestVersion
		return Reply{Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID, TraceID: task.TraceID, TraceParent: task.TraceParent, DigestVersion: task.DigestVersion, SessionID: task.SessionID, Text: "confirmation accepted"}, commit, nil
	}
	requestCtx, requestCancel := context.WithCancel(requestCtx)
	defer requestCancel()
	runCtx := sessionfence.WithTurn(requestCtx, turn)
	runCtx = governance.WithPolicy(runCtx, policySnapshot)
	runCtx = governance.WithTaskAudit(runCtx, r.auditSink, task, r.identitySecret)
	runCtx, capturedGovernanceError := governance.WithExecutionErrorCapture(runCtx)
	if r.confirmations != nil {
		runCtx = governance.WithConfirmationManager(runCtx, r.confirmations, governance.ActorHash(r.identitySecret, task.ActorUserID), task.SessionID)
	}
	modelStarted := time.Now()
	storageCtx, sessionSpan := telemetry.Start(runCtx, "session.read")
	storageCtx, memorySpan := telemetry.Start(storageCtx, "memory.read")
	events, err := lease.Runner.Run(storageCtx, task.RunnerUserID, task.SessionID, model.Message{Role: model.RoleUser, Content: task.Text}, frameworkagent.WithRequestID(task.RequestID))
	if err != nil {
		memorySpan.End()
		sessionSpan.End()
		telemetry.RecordModel(runCtx, time.Since(modelStarted).Seconds())
		svc.Discard(turn)
		if captured := capturedGovernanceError(); captured != nil {
			return Reply{}, sessionfence.TurnCommit{}, captured
		}
		if contextDeadlineExceeded(requestCtx) {
			return Reply{}, sessionfence.TurnCommit{}, ErrAgentTimeout
		}
		if errors.Is(err, persistence.ErrPostgresSummaryDisabled) {
			return Reply{}, sessionfence.TurnCommit{}, persistence.ErrPostgresSummaryDisabled
		}
		return Reply{}, sessionfence.TurnCommit{}, ErrAgentFailed
	}
	text, err := collectText(runCtx, events)
	memorySpan.End()
	sessionSpan.End()
	telemetry.RecordModel(runCtx, time.Since(modelStarted).Seconds())
	_, sessionWriteSpan := telemetry.Start(runCtx, "session.write")
	sessionWriteSpan.End()
	_, memoryWriteSpan := telemetry.Start(runCtx, "memory.write")
	memoryWriteSpan.End()
	if captured := capturedGovernanceError(); captured != nil {
		err = captured
	}
	if svcTurnTooLarge(turn) {
		svc.Discard(turn)
		return Reply{}, sessionfence.TurnCommit{}, ErrTurnTooLarge
	}
	if err != nil {
		svc.Discard(turn)
		if contextDeadlineExceeded(requestCtx) {
			return Reply{}, sessionfence.TurnCommit{}, ErrAgentTimeout
		}
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	commit, err := svc.Prepare(turn)
	if err != nil {
		svc.Discard(turn)
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	commit.TraceParent = task.TraceParent
	commit.DigestVersion = task.DigestVersion
	return Reply{Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID, TraceID: task.TraceID, TraceParent: task.TraceParent, DigestVersion: task.DigestVersion, SessionID: task.SessionID, Text: text}, commit, nil
}

func svcTurnTooLarge(turn *sessionfence.Turn) bool {
	return turn != nil && turn.IsOverLimit()
}

func collectText(ctx context.Context, events <-chan *event.Event) (string, error) {
	var builder strings.Builder
	var eventErr error
	for current := range events {
		if current == nil {
			continue
		}
		if current.IsError() && current.Response != nil && current.Response.Error != nil {
			eventErr = ErrAgentFailed
			continue
		}
		if current.Response == nil || current.IsToolCallResponse() || current.IsToolResultResponse() {
			continue
		}
		if current.Response.Usage != nil {
			telemetry.RecordTokens(ctx, current.Response.Usage.PromptTokens, current.Response.Usage.CompletionTokens, current.Response.Usage.TotalTokens)
		}
		for _, choice := range current.Response.Choices {
			part := choice.Message.Content
			if current.Response.IsPartial || current.Response.Object == model.ObjectTypeChatCompletionChunk {
				part = choice.Delta.Content
			}
			builder.WriteString(part)
		}
	}
	if eventErr != nil {
		return "", eventErr
	}
	text := strings.TrimSpace(builder.String())
	if text == "" {
		return "", ErrEmptyAgentResponse
	}
	return text, nil
}

func contextDeadlineExceeded(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

func (r *Runtime) Close() error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.closeOnce.Do(func() {
		// Runners borrow Session/Memory services from the provider. Close all
		// runners before closing the provider-owned services.
		r.closeErr = errors.Join(r.registry.Close(), r.backends.Close())
		if r.auditSink != nil {
			r.closeErr = errors.Join(r.closeErr, r.auditSink.Close())
		}
		if r.controlRepo != nil {
			r.closeErr = errors.Join(r.closeErr, r.controlRepo.Close())
		}
	})
	return r.closeErr
}

func newOpenAIModel(configVersion tenant.ConfigVersion, apiKey string) *openai.Model {
	return openai.New(
		configVersion.Model.Name,
		openai.WithBaseURL(configVersion.Model.BaseURL),
		openai.WithAPIKey(apiKey),
		openai.WithOpenAIOptions(openaioption.WithMaxRetries(0)),
	)
}

func buildAgent(configVersion tenant.ConfigVersion, apiKey string) frameworkagent.Agent {
	maxTokens := configVersion.Model.MaxOutputTokens
	temperature := 0.2
	return llmagent.New(
		configVersion.AgentAppID,
		llmagent.WithModel(newOpenAIModel(configVersion, apiKey)),
		llmagent.WithInstruction(configVersion.Instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{
			MaxTokens: &maxTokens, Temperature: &temperature, Stream: false,
		}),
	)
}

func buildGovernedAgent(configVersion tenant.ConfigVersion, apiKey string) frameworkagent.Agent {
	maxTokens := configVersion.Model.MaxOutputTokens
	temperature := 0.2
	registry := platformtool.NewRegistry()
	allowed := make(map[string]struct{})
	for _, name := range registry.Names() {
		allowed[name] = struct{}{}
	}
	callbacks := frameworktool.NewCallbacks()
	callbacks.RegisterBeforeTool(func(ctx context.Context, args *frameworktool.BeforeToolArgs) (*frameworktool.BeforeToolResult, error) {
		nonce, err := governance.AuthorizeToolCall(ctx, args.ToolName, args.Arguments)
		if err != nil {
			return nil, err
		}
		if nonce != "" {
			return &frameworktool.BeforeToolResult{CustomResult: fmt.Sprintf("confirmation required: confirm %s", nonce)}, nil
		}
		return nil, nil
	})
	return llmagent.New(
		configVersion.AgentAppID,
		llmagent.WithModel(newOpenAIModel(configVersion, apiKey)),
		llmagent.WithInstruction(configVersion.Instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{
			MaxTokens: &maxTokens, Temperature: &temperature, Stream: false,
		}),
		llmagent.WithTools(registry.Tools(allowed)),
		llmagent.WithToolCallbacks(callbacks),
	)
}
