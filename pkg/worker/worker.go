//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// Worker processes requests for a tenant.
type Worker struct {
	tenant              *tenant.Tenant
	storage             storage.StorageAdapter
	modelFactory        *ModelFactory
	runner              runner.Runner
	sessionService      session.Service
	approvalStore       governance.ApprovalStore
	releaseStorage      func()
	releaseDataPlane    func() error
	budgetTracker       budgetController
	sessionLocks        *storage.SessionLockManager
	collector           *telemetry.Collector
	appName             string
	logicalAppName      string
	agentName           string
	modelName           string
	confirmationEnabled bool
	versionID           string
	agentAppID          string
	deploymentID        string
	summaryEnabled      bool
	strictScope         bool
	// executionTimeout is an operator-owned upper bound for one production
	// invocation. It is deliberately not tenant supplied: a tenant must not be
	// able to extend a Worker slot, Session lease, or model/tool run forever.
	// A zero value is retained only for compatibility with in-process callers;
	// NewProductionWorkerWithOptionsContext always supplies a finite value.
	executionTimeout time.Duration
	closeOnce        sync.Once
	closeErr         error
}

var (
	// ErrSharedStorageRequired is returned when a production worker would
	// otherwise be able to construct a Runner without tenant-routed durable
	// Session and Memory services. The default constructor remains permissive
	// for in-process unit composition; production callers opt in explicitly.
	ErrSharedStorageRequired = errors.New("shared tenant storage is required")
	// ErrDistributedSessionCoordinationRequired indicates that a production
	// worker cannot serialize a full session invocation across replicas.
	ErrDistributedSessionCoordinationRequired = errors.New("distributed session coordination is required")
	// ErrAtomicFencingRequired indicates that a production worker was composed
	// with a lease-only storage adapter.
	ErrAtomicFencingRequired = errors.New("backend-native session fencing is required")
	// ErrTenantNotActive prevents a strict production Worker from serving a
	// suspended, deleted, or otherwise non-active control-plane snapshot.
	ErrTenantNotActive = errors.New("tenant is not active")
	// ErrApprovalResumeUnsafe means the durable approval and Session transcript
	// do not prove a single safe resume target. The caller must reconcile rather
	// than replay a potentially side-effecting tool call.
	ErrApprovalResumeUnsafe = errors.New("approval retry requires operator reconciliation")
	// ErrApprovalRecoveryStoreRequired prevents a confirmation-enabled
	// production Worker from accepting a store that cannot drive the durable
	// challenge, grant, and one-time resume workflow across replicas.
	ErrApprovalRecoveryStoreRequired = errors.New("approval recovery store is required")
	// ErrApprovalIdentityRequired prevents a confirmation-enabled invocation
	// from binding a grant to the reusable Session ID. Every approval challenge
	// must be tied to the durable Inbox idempotency identity.
	ErrApprovalIdentityRequired = errors.New("approval flow requires an explicit idempotency key")
	// ErrExecutionTimedOut means an execution deadline elapsed. It covers the
	// Worker-owned upper bound and an earlier outer execution deadline after
	// Runner.Run may have started model or Tool work. The caller must treat that
	// outcome as potentially side-effecting until the durable execution record
	// is reconciled; it is never safe to replay solely because a model or tool
	// context was cancelled.
	ErrExecutionTimedOut = errors.New("worker execution deadline exceeded")
	// ErrExecutionPreflightTimedOut means the deadline elapsed before the
	// Worker called Runner.Run. Budget admission, session coordination and
	// approval inspection can be retried because no model or Tool side effect
	// could have started on this attempt.
	ErrExecutionPreflightTimedOut = errors.New("worker execution preflight deadline exceeded")
	// ErrExecutionPreflight marks an error returned before Runner.Run was
	// entered. No model or Tool invocation can have started at this boundary, so
	// a durable execution record may be retried after the transient dependency
	// or admission failure is repaired. The HTTP and local queue adapters use
	// this sentinel instead of guessing from an error string.
	ErrExecutionPreflight = errors.New("worker execution preflight failed")
	// ErrExecutionPreflightPermanent marks a deterministic request/policy
	// rejection that happened before Runner.Run. It still carries the
	// ErrExecutionPreflight marker for observability, but must be dead-lettered
	// rather than retried indefinitely.
	ErrExecutionPreflightPermanent = errors.New("worker execution preflight rejected")
	// ErrInvalidExecutionTimeout rejects an unsafe operator configuration before
	// the Worker begins serving durable invocations.
	ErrInvalidExecutionTimeout = errors.New("worker execution timeout is invalid")
	// ErrSummaryRuntimeRequired prevents a production Worker from scheduling
	// durable Summary jobs whose checkpoints cannot be made visible to Runner.
	ErrSummaryRuntimeRequired = errors.New("durable summary runtime is required")
	// ErrDataPlaneRuntimeRequired prevents an Agent version from declaring a
	// framework-managed Knowledge capability or Artifact backend that the
	// Worker cannot construct from operator-owned profiles.
	ErrDataPlaneRuntimeRequired = errors.New("tenant runtime data plane is required")
)

const (
	// DefaultExecutionTimeout bounds a production invocation when an operator
	// did not set EXECUTION_TIMEOUT explicitly.
	DefaultExecutionTimeout = 90 * time.Second
	// ResponseCompletionGrace bounds Worker-side result encoding, execution
	// finalization and response delivery after the Runner execution deadline.
	// Consumer validates this advertised protocol budget before claiming work.
	ResponseCompletionGrace = 30 * time.Second
	// ConsumerPersistenceGrace leaves time for the Consumer to durably commit
	// Inbox and Outbox state after the Worker response window closes.
	ConsumerPersistenceGrace = 5 * time.Second
	minExecutionTimeout      = time.Second
	maxExecutionTimeout      = 15 * time.Minute
)

// approvalRecoveryStore is the complete capability set required when a
// strict production Worker enables confirmation-gated tools. The Admin API
// needs safe challenge inspection, the queue retry needs an atomic grant
// consumer, and the Worker needs a read-only resume inspector before it can
// suppress an already-persisted user message. Requiring all three at startup
// avoids a delayed fallback that would rerun the model instead of safely
// resuming the durable tool call.
type approvalRecoveryStore interface {
	governance.ApprovalStore
	governance.ApprovalInspector
	governance.ApprovalGrantConsumer
	governance.ApprovalGrantConsumerForChallenge
	governance.ApprovalResumeInspector
	governance.ApprovalGrantInspector
	governance.ApprovalResumeStateInspector
}

type budgetController interface {
	CheckBudget(context.Context) error
	AcquireSessionSlot(context.Context) (governance.SessionSlot, error)
	ReserveTokenBudget(context.Context) (governance.TokenReservation, error)
	DispatchTokenBudget(context.Context, governance.TokenReservation) error
	SettleTokenBudget(context.Context, governance.TokenReservation, int64) error
	ReleaseTokenBudget(context.Context, governance.TokenReservation) error
}

// Options binds Worker construction to a resolved immutable Agent version.
// AppName is a logical tenant-local name; Worker always converts it to the
// canonical tenant-scoped backend identity. Nil Agent/Model values retain the
// tenant's legacy first-agent behavior.
type Options struct {
	Agent                *tenant.AgentConfig
	Model                *tenant.ModelConfig
	ModelCatalogRevision string
	ModelContextWindow   int
	AppName              string
	VersionID            string
	// AgentAppID is the immutable control-plane identity. AppName remains the
	// tenant-local logical name used to derive the physical Session namespace.
	AgentAppID   string
	DeploymentID string
	Collector    *telemetry.Collector
	ToolResolver ToolResolver
	// SecretResolver resolves operator-owned model credential references. A
	// resolver is required when a model uses APIKeyRef; legacy encrypted APIKey
	// values continue to work for compatibility.
	SecretResolver tenant.SecretResolver
	// ApprovalStore is the tenant-scoped durable approval capability. A nil
	// store intentionally keeps confirmation-required tools fail-closed.
	ApprovalStore governance.ApprovalStore
	// RuntimeFactories is operator-owned. A nil registry uses the bundled LLM,
	// chain, graph, parallel and bounded-cycle implementations. Replacements
	// remain explicit and never fall back to another runtime type.
	RuntimeFactories *RuntimeAgentRegistry
	// RuntimeCapabilityFingerprint binds a version snapshot to the runtime
	// capability set that was present during Admin admission. Empty retains
	// compatibility for historical snapshots created before this field existed;
	// strict production admission still requires a declared capability for every
	// non-built-in runtime.
	RuntimeCapabilityFingerprint string
	// SummaryCheckpoints is the fenced, tenant-scoped read side used to hydrate
	// tRPC Session.Summaries before Runner constructs model history.
	SummaryCheckpoints summarycoord.CheckpointReader
	// DataPlaneResolver acquires tenant/app-scoped Knowledge and Artifact
	// services from operator profiles. It is Worker-only because the resolved
	// catalog contains Qdrant/S3/embedding credentials.
	DataPlaneResolver runtimeplane.Resolver
	// RequireSummaryRuntime is enabled by the production constructor. It keeps
	// deployment misconfiguration from silently generating checkpoints that no
	// Agent replica consumes.
	RequireSummaryRuntime bool
	// RequireSharedStorage enables the production composition contract. When
	// set, the worker requires a tenant-routed ServiceLeaseAdapter and Redis
	// session coordination before creating a Runner. It does not claim that a
	// Redis lease is a database write-fencing token; the underlying Session and
	// Memory backends must provide their own consistency guarantees.
	RequireSharedStorage bool
	// RequireAtomicFencing requires the Session/Memory adapter to expose a
	// backend-native fence. This is stronger than the Redis invocation lease;
	// production composition should enable it whenever execution generations
	// are used.
	RequireAtomicFencing bool
	// ExecutionTimeout bounds one production invocation from admission through
	// Runner completion. It is ignored by compatibility constructors when zero;
	// NewProductionWorkerWithOptionsContext defaults zero to
	// DefaultExecutionTimeout. Context cancellation is cooperative, so custom
	// tools that can ignore context still require process/container isolation.
	ExecutionTimeout time.Duration
}

// ValidateExecutionTimeout verifies an operator-owned execution limit. A
// multi-hour value can exhaust bounded worker and tenant concurrency slots;
// a sub-second value cannot reliably complete durable cleanup after a cancelled
// model or tool invocation.
func ValidateExecutionTimeout(value time.Duration) error {
	if value < minExecutionTimeout || value > maxExecutionTimeout {
		return fmt.Errorf(
			"%w: must be between %s and %s",
			ErrInvalidExecutionTimeout,
			minExecutionTimeout,
			maxExecutionTimeout,
		)
	}
	return nil
}

// ToolResolver maps tenant-selected names to operator-approved tools. It is a
// separate interface so MCP/business catalogs can be injected without letting
// tenant JSON instantiate arbitrary code.
type ToolResolver interface {
	Resolve(names []string) ([]tool.Tool, error)
}

// ContextToolResolver lets lazy remote catalogs (for example MCP profiles)
// inherit Worker construction cancellation and deadlines. Legacy in-process
// catalogs continue to use ToolResolver.
type ContextToolResolver interface {
	ResolveContext(context.Context, []string) ([]tool.Tool, error)
}

// NewWorker creates a worker for a tenant.
// If redisClient is provided, a BudgetTracker is created for token/cost enforcement.
func NewWorker(t *tenant.Tenant, storageAdapter storage.StorageAdapter, redisClient *redis.Client) (*Worker, error) {
	return NewWorkerWithOptions(t, storageAdapter, redisClient, Options{})
}

// NewWorkerWithOptions creates a Worker for an exact control-plane snapshot.
func NewWorkerWithOptions(t *tenant.Tenant, storageAdapter storage.StorageAdapter, redisClient *redis.Client, options Options) (*Worker, error) {
	return NewWorkerWithOptionsContext(context.Background(), t, storageAdapter, redisClient, options)
}

// NewProductionWorkerWithOptionsContext is the explicit production
// composition entry point. Unlike the compatibility constructors used by
// local/unit callers, it cannot silently fall back to process-local
// Session/Memory services when a caller forgets the production option.
func NewProductionWorkerWithOptionsContext(
	ctx context.Context,
	t *tenant.Tenant,
	storageAdapter storage.StorageAdapter,
	redisClient *redis.Client,
	options Options,
) (*Worker, error) {
	options.RequireSharedStorage = true
	options.RequireAtomicFencing = true
	options.RequireSummaryRuntime = true
	return NewWorkerWithOptionsContext(ctx, t, storageAdapter, redisClient, options)
}

// NewWorkerWithOptionsContext creates a Worker while preserving cancellation
// and deadlines for backend initialization. Production cache factories use
// this form so a shutting-down node cannot strand a new Redis/PostgreSQL
// connection attempt behind context.Background.
func NewWorkerWithOptionsContext(ctx context.Context, t *tenant.Tenant, storageAdapter storage.StorageAdapter, redisClient *redis.Client, options Options) (*Worker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil {
		return nil, fmt.Errorf("tenant is required")
	}
	// Runtime objects must own an immutable control-plane snapshot. Without
	// this clone, a caller updating a cached Tenant can change governance,
	// credentials, or storage routing while a Runner is executing.
	snapshot, err := tenant.Clone(t)
	if err != nil {
		return nil, fmt.Errorf("clone tenant snapshot: %w", err)
	}
	t = snapshot
	if isNilInterface(storageAdapter) {
		storageAdapter = nil
	}
	if isNilInterface(options.ToolResolver) {
		options.ToolResolver = nil
	}
	if options.RequireSharedStorage {
		if err := tenant.ValidateDistributedStorage(t.Storage); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSharedStorageRequired, err)
		}
		if storageAdapter == nil {
			return nil, ErrSharedStorageRequired
		}
		if _, ok := storageAdapter.(storage.ServiceLeaseAdapter); !ok {
			return nil, fmt.Errorf("%w: storage adapter must implement ServiceLeaseAdapter", ErrSharedStorageRequired)
		}
		if redisClient == nil {
			return nil, ErrDistributedSessionCoordinationRequired
		}
		if options.RequireAtomicFencing {
			fenced, ok := storageAdapter.(storage.AtomicFenceAdapter)
			if !ok || !fenced.AtomicWriteFenceEnabled() {
				return nil, ErrAtomicFencingRequired
			}
		}
	}
	if options.RequireSummaryRuntime && isNilInterface(options.SummaryCheckpoints) {
		return nil, ErrSummaryRuntimeRequired
	}
	strictComposition := options.RequireSharedStorage || options.RequireAtomicFencing
	if options.ExecutionTimeout == 0 && strictComposition {
		options.ExecutionTimeout = DefaultExecutionTimeout
	}
	if options.ExecutionTimeout != 0 {
		if err := ValidateExecutionTimeout(options.ExecutionTimeout); err != nil {
			return nil, err
		}
	}
	if strictComposition {
		if t.Status != tenant.TenantStatusActive {
			return nil, fmt.Errorf("%w: tenant status is %q", ErrTenantNotActive, t.Status)
		}
		if options.AppName == "" || options.AgentAppID == "" || options.VersionID == "" || options.DeploymentID == "" {
			return nil, fmt.Errorf("strict worker requires app, app id, version and deployment identities")
		}
	}
	// Fail closed for records created before strict TenantConfig validation was
	// introduced. An empty or blacklist policy must never become an implicit
	// grant of every operator-registered tool.
	if t.ToolPolicy.Mode != "whitelist" {
		return nil, fmt.Errorf("tenant tool policy must use explicit whitelist mode")
	}
	if redisClient == nil && requiresBudgetBackend(t.Budget) {
		return nil, fmt.Errorf("redis is required for configured tenant budget enforcement")
	}
	modelFactory := NewModelFactory()

	// Build agent configuration.
	if options.Agent == nil && len(t.Agents) == 0 {
		return nil, fmt.Errorf("no agents configured for tenant")
	}

	var agentConfig tenant.AgentConfig
	if options.Agent != nil {
		agentConfig = cloneAgentConfig(*options.Agent)
	} else {
		agentConfig = t.Agents[0]
	}
	if strictComposition {
		if err := validateWorkerAppIdentity(options.AppName, agentConfig.Name); err != nil {
			return nil, err
		}
	}
	// Reject runtimes that are not installed before resolving credentials or
	// initializing model/storage dependencies. Admin admission performs the
	// same check, but Worker is also a production boundary for queue and
	// non-HTTP callers and must fail closed independently.
	validateRuntime := options.RuntimeFactories.ValidateRuntimeType
	if strictComposition {
		validateRuntime = options.RuntimeFactories.ValidateRuntimeTypeStrict
	}
	if err := validateRuntime(agentConfig.Type); err != nil {
		return nil, fmt.Errorf("agent runtime is not admitted: %w", err)
	}
	if !options.RuntimeFactories.MatchesFingerprint(agentConfig.Type, options.RuntimeCapabilityFingerprint) {
		return nil, fmt.Errorf("%w: published=%s installed=%s", ErrRuntimeCapabilityMismatch,
			options.RuntimeCapabilityFingerprint, options.RuntimeFactories.Fingerprint())
	}
	confirmationEnabled := hasConfiguredConfirmation(agentConfig, t.ToolPolicy)
	staticToolNames, knowledgeRequested := splitRuntimeToolNames(agentConfig.Tools)
	artifactRequested := t.Storage.ArtifactBackend != "" || t.Storage.ArtifactProfile != ""
	if (knowledgeRequested || artifactRequested) && isNilInterface(options.DataPlaneResolver) {
		return nil, fmt.Errorf("%w: configured capability has no resolver", ErrDataPlaneRuntimeRequired)
	}
	if strictComposition && confirmationEnabled {
		if isNilInterface(options.ApprovalStore) {
			return nil, fmt.Errorf("%w: confirmation-enabled production worker has no approval store", ErrApprovalRecoveryStoreRequired)
		}
		if _, ok := options.ApprovalStore.(approvalRecoveryStore); !ok {
			return nil, fmt.Errorf("%w: confirmation-enabled production store must support challenge inspection, granted consumption, and resume inspection", ErrApprovalRecoveryStoreRequired)
		}
	}
	modelConfig := options.Model
	if modelConfig != nil {
		clonedModel := cloneModelConfig(*modelConfig)
		modelConfig = &clonedModel
	}
	if modelConfig == nil || modelConfig.ModelName == "" {
		modelConfig = findModelConfig(t, agentConfig.DefaultModel)
	}
	if modelConfig == nil {
		return nil, fmt.Errorf("model %q is not configured for agent %q", agentConfig.DefaultModel, agentConfig.Name)
	}
	versioned := options.Agent != nil || options.Model != nil || options.VersionID != ""
	if strictComposition {
		// Production workers must independently enforce the immutable operator
		// model catalog. The Admin publish gate is not sufficient protection
		// against a stale, imported, or directly-mutated version row, and a
		// zero-budget tenant would otherwise bypass catalog validation here.
		if err := tenant.ValidateProductionModelCatalog(agentConfig, *modelConfig); err != nil {
			return nil, fmt.Errorf("validate production model catalog: %w", err)
		}
	}
	if versioned {
		if err := tenant.ValidatePinnedAgentModelBudget(
			agentConfig,
			*modelConfig,
			t.Budget,
			options.ModelCatalogRevision,
			options.ModelContextWindow,
		); err != nil {
			return nil, fmt.Errorf("validate immutable agent version: %w", err)
		}
	} else if err := tenant.ValidateAgentModelBudget(agentConfig, *modelConfig, t.Budget); err != nil {
		return nil, fmt.Errorf("validate tenant agent configuration: %w", err)
	}
	if modelConfig.APIKey == "" {
		resolved, resolveErr := resolveModelCredential(ctx, modelConfig, t, options.SecretResolver)
		if resolveErr != nil {
			return nil, resolveErr
		}
		modelConfig = resolved
	}

	// Create model
	mdl, err := modelFactory.CreateModel(modelConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create model: %w", err)
	}

	appName := options.AppName
	if appName == "" {
		appName = agentConfig.Name
	}
	logicalAppName := appName
	appName, err = storage.TenantScopedAppName(t, appName)
	if err != nil {
		return nil, fmt.Errorf("scope Runner app identity: %w", err)
	}

	var (
		sessionService session.Service
		memoryService  memory.Service
	)
	var sharedServiceOptions []runner.Option
	var releaseStorage func()
	// releaseOnError owns the backend lease until the Worker is fully
	// constructed.  Runner/tool validation below can still fail after the
	// lease is acquired; transferring ownership only at the successful return
	// prevents failed cache builds from pinning a backend forever.
	var releaseOnError func()
	if storageAdapter != nil {
		if leased, ok := storageAdapter.(storage.ServiceLeaseAdapter); ok {
			sessionService, memoryService, releaseStorage, err = leased.AcquireServices(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("initialize shared storage services: %w", err)
			}
			if isNilInterface(sessionService) || isNilInterface(memoryService) {
				if releaseStorage != nil {
					releaseStorage()
					releaseStorage = nil
				}
				return nil, fmt.Errorf("%w: adapter returned a typed-nil Session or Memory service", ErrSharedStorageRequired)
			}
			releaseOnError = releaseStorage
		} else {
			sessionService, err = storageAdapter.SessionService(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("initialize shared session service: %w", err)
			}
			memoryService, err = storageAdapter.MemoryService(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("initialize shared memory service: %w", err)
			}
			if isNilInterface(sessionService) || isNilInterface(memoryService) {
				return nil, fmt.Errorf("%w: adapter returned a typed-nil Session or Memory service", ErrSharedStorageRequired)
			}
		}
		if options.RequireSharedStorage && (sessionService == nil || memoryService == nil) {
			if releaseOnError != nil {
				releaseOnError()
				releaseOnError = nil
			}
			return nil, fmt.Errorf("%w: adapter returned a nil Session or Memory service", ErrSharedStorageRequired)
		}
		if !isNilInterface(options.SummaryCheckpoints) {
			sessionService, err = summarycoord.NewCheckpointSessionService(
				sessionService, options.SummaryCheckpoints, t.ID, options.AgentAppID, appName,
			)
			if err != nil {
				if releaseOnError != nil {
					releaseOnError()
					releaseOnError = nil
				}
				return nil, fmt.Errorf("initialize durable Summary read plane: %w", err)
			}
		}
		// Preserve a resumable pending tool call when governance pauses a run for
		// operator approval. The wrapper is otherwise a transparent session
		// service and keeps all backend-specific storage semantics intact.
		sessionService = newApprovalPauseSessionService(sessionService)
		defer func() {
			if releaseOnError != nil {
				releaseOnError()
			}
		}()
		sharedServiceOptions = append(
			sharedServiceOptions,
			runner.WithSessionService(sessionService),
		)
	}
	if memoryService != nil {
		// Runner creates Session objects with SessionOwnerID. The wrapper keeps
		// that stable session identity while mapping personal Memory operations
		// to the authenticated actor supplied by Process.
		memoryService, err = newActorScopedMemoryService(memoryService, appName)
		if err != nil {
			return nil, fmt.Errorf("scope memory service: %w", err)
		}
		sharedServiceOptions = append(sharedServiceOptions, runner.WithMemoryService(memoryService))
	}

	var dataPlane runtimeplane.Lease
	var releaseDataPlaneOnError func() error
	if knowledgeRequested || artifactRequested {
		agentAppID := options.AgentAppID
		if agentAppID == "" {
			agentAppID = logicalAppName
		}
		dataPlane, err = options.DataPlaneResolver.Acquire(ctx, runtimeplane.Request{
			Tenant: t, AgentAppID: agentAppID,
			NeedKnowledge: knowledgeRequested, NeedArtifact: artifactRequested,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDataPlaneRuntimeRequired, err)
		}
		if dataPlane.Release == nil || (knowledgeRequested && isNilInterface(dataPlane.Knowledge)) ||
			(artifactRequested && isNilInterface(dataPlane.Artifact)) {
			if dataPlane.Release != nil {
				_ = dataPlane.Release()
			}
			return nil, fmt.Errorf("%w: resolver returned incomplete services", ErrDataPlaneRuntimeRequired)
		}
		releaseDataPlaneOnError = dataPlane.Release
		defer func() {
			if releaseDataPlaneOnError != nil {
				_ = releaseDataPlaneOnError()
			}
		}()
	}

	baseAgentOptions := []llmagent.Option{
		llmagent.WithModel(mdl),
		llmagent.WithGenerationConfig(BuildGenerationConfig(modelConfig)),
	}
	if !isNilInterface(options.SummaryCheckpoints) {
		// Platform checkpoints summarize the whole logical session and are stored
		// under SummaryFilterKeyAllContents (the empty filter key). The framework's
		// default prefix mode searches the current agent branch instead, which would
		// silently keep the summarized history and omit this checkpoint. Align the
		// Runner request with the platform's full-session summary contract.
		baseAgentOptions = append(baseAgentOptions,
			llmagent.WithAddSessionSummary(true),
			llmagent.WithMessageBranchFilterMode(llmagent.BranchFilterModeAll),
		)
	}
	if memoryService != nil {
		// Bound native recall. A positive limit avoids unbounded prompt growth;
		// writes remain explicit governed memory tool calls or framework jobs.
		baseAgentOptions = append(baseAgentOptions, llmagent.WithPreloadMemory(10))
	}
	var resolvedRuntimeTools []tool.Tool
	if len(agentConfig.Tools) > 0 {
		for _, name := range agentConfig.Tools {
			if !t.ToolPolicy.IsAllowed(name) {
				return nil, fmt.Errorf("agent %q configures tool %q outside the tenant policy", agentConfig.Name, name)
			}
		}
		resolvedTools, err := resolveConfiguredTools(ctx, staticToolNames, options.ToolResolver, memoryService)
		if err != nil {
			return nil, fmt.Errorf("resolve tools for agent %q: %w", agentConfig.Name, err)
		}
		// Keep the resolved set available to a non-LLM factory as well. The
		// default factory consumes the same options; custom factories can use
		// the explicit dependency field without re-resolving names.
		resolvedRuntimeTools = resolvedTools
	}
	buildLeafOptions := func(leaf RuntimeLLMAgentBuildSpec) ([]llmagent.Option, error) {
		leafOptions := append([]llmagent.Option(nil), baseAgentOptions...)
		leafOptions = append(leafOptions, llmagent.WithMaxLLMCalls(leaf.MaxLLMCalls))
		staticNames, needsKnowledge := splitRuntimeToolNames(leaf.ToolNames)
		if needsKnowledge {
			if isNilInterface(dataPlane.Knowledge) {
				return nil, fmt.Errorf("runtime node %q requires an unavailable Knowledge service", leaf.Name)
			}
			leafOptions = append(leafOptions, llmagent.WithKnowledge(dataPlane.Knowledge))
		}
		leafTools, err := selectResolvedRuntimeTools(staticNames, resolvedRuntimeTools)
		if err != nil {
			return nil, fmt.Errorf("runtime node %q: %w", leaf.Name, err)
		}
		if len(leafTools) > 0 {
			if leaf.MaxLLMCalls < 2 {
				return nil, fmt.Errorf("runtime node %q needs at least two LLM calls when tools are enabled", leaf.Name)
			}
			leafOptions = append(leafOptions,
				llmagent.WithMaxToolIterations(leaf.MaxLLMCalls-1),
				llmagent.WithTools(leafTools),
			)
		}
		if leaf.SystemPrompt != "" {
			leafOptions = append(leafOptions, llmagent.WithInstruction(leaf.SystemPrompt))
		}
		return leafOptions, nil
	}
	rootLeaf := RuntimeLLMAgentBuildSpec{
		Name:         agentConfig.Name,
		SystemPrompt: agentConfig.SystemPrompt,
		MaxLLMCalls:  agentConfig.EffectiveMaxLLMCalls(),
		ToolNames:    append([]string(nil), agentConfig.Tools...),
	}
	agentOptions, err := buildLeafOptions(rootLeaf)
	if err != nil {
		return nil, err
	}
	leafBuilder := RuntimeLLMAgentBuilderFunc(func(_ context.Context, leaf RuntimeLLMAgentBuildSpec) (agent.Agent, error) {
		leafOptions, err := buildLeafOptions(leaf)
		if err != nil {
			return nil, err
		}
		return llmagent.New(leaf.Name, leafOptions...), nil
	})

	collector := options.Collector
	if collector == nil {
		collector = telemetry.NewCollector()
	}

	// Create the exact immutable Agent configuration selected by the control
	// plane. Prompt, generation limits and tool capabilities all enter runtime.
	ag, err := buildRuntimeAgent(ctx, RuntimeAgentBuildSpec{
		Tenant:      t,
		Agent:       agentConfig,
		Model:       *modelConfig,
		ModelObject: mdl,
		Tools:       resolvedRuntimeTools,
	}, RuntimeAgentDependencies{
		MemoryService:   memoryService,
		Knowledge:       dataPlane.Knowledge,
		Artifact:        dataPlane.Artifact,
		Collector:       collector,
		LLMOptions:      agentOptions,
		LLMAgentBuilder: leafBuilder,
	}, options.RuntimeFactories)
	if err != nil {
		return nil, err
	}
	governancePlugin := governance.NewPluginWithApprovalStore(
		governance.NewGovernanceFilter(t), "tenant-governance", options.ApprovalStore, collector,
	)
	runnerOptions := []runner.Option{runner.WithPlugins(governancePlugin)}
	runnerOptions = append(runnerOptions, sharedServiceOptions...)
	if !isNilInterface(dataPlane.Artifact) {
		runnerOptions = append(runnerOptions, runner.WithArtifactService(dataPlane.Artifact))
	}

	// Runner receives the same tenant-selected shared Session and Memory
	// services as the StorageAdapter, so any replica can continue the session
	// and observe memory after the backend transaction commits.
	r := runner.NewRunner(appName, ag, runnerOptions...)

	// Create budget tracker if Redis is available
	var budgetTracker *governance.BudgetTracker
	var sessionLocks *storage.SessionLockManager
	if redisClient != nil {
		budgetTracker = governance.NewBudgetTracker(redisClient, t)
		sessionLocks = storage.NewSessionLockManager(redisClient)
	}

	value := &Worker{
		tenant:              t,
		storage:             storageAdapter,
		modelFactory:        modelFactory,
		runner:              r,
		sessionService:      sessionService,
		approvalStore:       options.ApprovalStore,
		budgetTracker:       budgetTracker,
		sessionLocks:        sessionLocks,
		collector:           collector,
		appName:             appName,
		logicalAppName:      logicalAppName,
		agentName:           agentConfig.Name,
		modelName:           modelConfig.ModelName,
		confirmationEnabled: confirmationEnabled,
		versionID:           options.VersionID,
		agentAppID:          options.AgentAppID,
		deploymentID:        options.DeploymentID,
		summaryEnabled:      !isNilInterface(options.SummaryCheckpoints),
		strictScope:         strictComposition,
		executionTimeout:    options.ExecutionTimeout,
		releaseStorage:      releaseStorage,
		releaseDataPlane:    dataPlane.Release,
	}
	// The Worker now owns the lease and will release it from Close.  Clear the
	// construction guard after the value is complete so every earlier error
	// path remains covered.
	releaseOnError = nil
	releaseDataPlaneOnError = nil
	return value, nil
}

func splitRuntimeToolNames(names []string) ([]string, bool) {
	static := make([]string, 0, len(names))
	knowledge := false
	for _, name := range names {
		if platformtool.IsManagedKnowledgeTool(name) {
			knowledge = true
			continue
		}
		static = append(static, name)
	}
	return static, knowledge
}

func selectResolvedRuntimeTools(names []string, resolved []tool.Tool) ([]tool.Tool, error) {
	if len(names) == 0 {
		return nil, nil
	}
	byName := make(map[string]tool.Tool, len(resolved))
	for _, candidate := range resolved {
		if isNilInterface(candidate) || candidate.Declaration() == nil {
			continue
		}
		byName[candidate.Declaration().Name] = candidate
	}
	selected := make([]tool.Tool, 0, len(names))
	for _, name := range names {
		candidate, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("resolved tool %q is unavailable", name)
		}
		selected = append(selected, candidate)
	}
	return selected, nil
}

func validateWorkerAppIdentity(appName, agentName string) error {
	if appName == "" || agentName == "" || appName != agentName {
		return fmt.Errorf("strict worker app %q does not match snapshot agent %q", appName, agentName)
	}
	return nil
}

func requiresBudgetBackend(config tenant.BudgetConfig) bool {
	return config.MaxTokensPerDay != 0 || config.MaxTokensPerRequest != 0 ||
		config.MaxCostPerDay != 0 || config.MaxConcurrentSessions != 0 ||
		len(config.AlertThresholds) != 0
}

func hasConfiguredConfirmation(agent tenant.AgentConfig, policy tenant.ToolPolicy) bool {
	for _, toolName := range agent.Tools {
		if policy.RequiresConfirmation(toolName) {
			return true
		}
	}
	return false
}

func cloneAgentConfig(source tenant.AgentConfig) tenant.AgentConfig {
	cloned := source
	cloned.Tools = append([]string(nil), source.Tools...)
	if source.Metadata != nil {
		cloned.Metadata = make(map[string]string, len(source.Metadata))
		for key, value := range source.Metadata {
			cloned.Metadata[key] = value
		}
	}
	if source.Runtime != nil {
		runtimeCopy := *source.Runtime
		runtimeCopy.Nodes = append([]tenant.AgentRuntimeNode(nil), source.Runtime.Nodes...)
		for i := range runtimeCopy.Nodes {
			runtimeCopy.Nodes[i].Tools = append([]string(nil), source.Runtime.Nodes[i].Tools...)
		}
		runtimeCopy.Edges = append([]tenant.AgentRuntimeEdge(nil), source.Runtime.Edges...)
		cloned.Runtime = &runtimeCopy
	}
	return cloned
}

func cloneModelConfig(source tenant.ModelConfig) tenant.ModelConfig {
	cloned := source
	if source.Metadata != nil {
		cloned.Metadata = make(map[string]string, len(source.Metadata))
		for key, value := range source.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

// SetCollector overrides the telemetry collector. Used by tests to capture
// audit output; production callers rely on the default from NewWorker.
func (w *Worker) SetCollector(c *telemetry.Collector) {
	w.collector = c
}

// collectorStart returns the request start time used for latency measurement.
func (w *Worker) collectorStart() time.Time {
	if w.collector != nil {
		return w.collector.StartTimer()
	}
	return time.Now()
}

// audit records an outcome and returns any persistence failure. Error paths
// retain their original execution classification; success must acknowledge a
// durable audit record before it can be returned to the consumer.
func (w *Worker) audit(ctx context.Context, req *Request, decision string, start time.Time, errorType string, tokens int) error {
	if w.collector == nil {
		return nil
	}
	entry := &telemetry.AuditLog{
		TenantID:    w.tenant.ID,
		ChannelType: req.ChannelType,
		UserID:      req.UserID,
		SessionID:   req.SessionID,
		AgentName:   w.agentName,
		Decision:    decision,
		LatencyMS:   int(time.Since(start).Milliseconds()),
		ErrorType:   errorType,
		TokenCount:  tokens,
		TraceID:     traceIDFromRequest(req),
	}
	auditErr := w.collector.LogAudit(ctx, entry)
	if auditErr != nil {
		log.Printf("audit write failed for tenant %s: error=%s", w.tenant.ID, telemetry.StableErrorCode(auditErr))
	}
	w.collector.RecordRequestDuration(w.tenant.ID, w.agentName, start)
	if auditErr != nil {
		w.collector.RecordError(w.tenant.ID, req.ChannelType, "audit_write_failed")
	} else if decision == "allowed" {
		w.collector.RecordSuccess(w.tenant.ID, req.ChannelType)
	} else {
		w.collector.RecordError(w.tenant.ID, req.ChannelType, errorType)
	}
	if tokens > 0 {
		w.collector.RecordAccountedTokens(w.tenant.ID, w.modelName, tokens)
	}
	return auditErr
}

func traceIDFromRequest(req *Request) string {
	if req == nil || req.Metadata == nil {
		return ""
	}
	traceParent, _ := req.Metadata["traceparent"].(string)
	// W3C traceparent: version-traceid-parentid-flags. Invalid caller input
	// must never become a misleading audit correlation identifier.
	if validTraceParent(traceParent) {
		return traceParent[3:35]
	}
	return ""
}

func normalizeRequestSessionOwner(req *Request) error {
	if req == nil {
		return fmt.Errorf("session identity is required")
	}
	owner, err := channel.NormalizeSessionOwner(&channel.InboundMessage{
		TenantID:         req.TenantID,
		ChannelType:      req.ChannelType,
		ChannelAccountID: req.ChannelAccountID,
		ExternalUserID:   req.UserID,
		ConversationID:   req.ConversationID,
		IsGroupChat:      req.IsGroupChat,
	}, req.SessionID)
	if err != nil {
		return fmt.Errorf("derive session owner: %w", err)
	}
	if strings.HasPrefix(req.SessionID, "sess_") {
		identity, identityErr := channel.BuildSessionIdentity(&channel.InboundMessage{
			TenantID:         req.TenantID,
			ChannelType:      req.ChannelType,
			ChannelAccountID: req.ChannelAccountID,
			ExternalUserID:   req.UserID,
			ConversationID:   req.ConversationID,
			IsGroupChat:      req.IsGroupChat,
		})
		if identityErr == nil {
			if req.SessionID != identity.SessionID {
				return fmt.Errorf("session ID does not match canonical conversation")
			}
			owner = identity.SessionOwnerID
		} else {
			return fmt.Errorf("validate canonical session identity: %w", identityErr)
		}
	}
	if req.SessionOwnerID != "" && req.SessionOwnerID != owner {
		return fmt.Errorf("session owner does not match actor and conversation")
	}
	req.SessionOwnerID = owner
	return nil
}

// NormalizeRequestSessionOwner validates and fills the Runner Session owner
// at trusted HTTP or queue boundaries. It is exported so callers that create
// an execution fence before Process can carry the same canonical identity.
func NormalizeRequestSessionOwner(req *Request) error {
	return normalizeRequestSessionOwner(req)
}

// Request represents a request to process.
type Request struct {
	// ExecutionContract is populated by the authenticated HTTP client and is
	// required only on the versioned Consumer-to-Worker endpoint. Because it is
	// part of this JSON body, service authentication binds it to the HMAC.
	ExecutionContract *ExecutionContract `json:"executionContract,omitempty"`
	// Gateway fields
	TenantID       string `json:"tenantId"`
	ChannelType    string `json:"channelType"`
	ConversationID string `json:"conversationId"`
	MessageID      string `json:"messageId"`
	IsGroupChat    bool   `json:"isGroupChat"`
	AgentApp       string `json:"agentApp,omitempty"`
	AgentVersion   string `json:"agentVersion,omitempty"`
	DeploymentID   string `json:"deploymentId,omitempty"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	PayloadHash    string `json:"payloadHash,omitempty"`
	// ApprovalToken is a one-time opaque capability issued by the tenant's
	// approval service. It is never persisted in Session, audit, or model input.
	ApprovalToken string `json:"approvalToken,omitempty"`
	// ApprovalResumeChallengeID is an internal admission fence populated only
	// by the trusted Worker HTTP boundary after an atomic approval inspection.
	// It is excluded from JSON so external callers cannot submit this marker.
	ApprovalResumeChallengeID string `json:"-"`

	// Core fields
	// UserID is the external actor who sent the message. SessionOwnerID is the
	// Runner user key: it equals UserID for direct messages and is shared by all
	// members of a group conversation.
	ChannelAccountID string                 `json:"channelAccountId,omitempty"`
	UserID           string                 `json:"userId"`
	SessionOwnerID   string                 `json:"sessionOwnerId,omitempty"`
	SessionID        string                 `json:"sessionId"`
	Content          string                 `json:"content"`
	Attachments      []channel.Attachment   `json:"attachments,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// Response represents the response from processing.
type Response struct {
	ContentType string                 `json:"contentType"`
	SessionID   string                 `json:"sessionId"`
	Content     string                 `json:"content"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	// Summary is a durable scheduling receipt, not generated content. The
	// Consumer commits it atomically with Inbox completion and the Outbox reply.
	Summary *summarycoord.EnqueueRequest `json:"summary,omitempty"`
}

// Process processes a request.
func (w *Worker) Process(ctx context.Context, req *Request) (response *Response, err error) {
	if w == nil || w.tenant == nil {
		return nil, fmt.Errorf("worker is not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("worker request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Keep the Worker lifecycle as the parent of every runtime operation. The
	// deferred end runs after later cleanup/timeout defers have finalized the
	// named return error, so the span cannot incorrectly report success when a
	// lease release or deadline boundary fails the invocation.
	ctx, workerSpan := telemetry.StartOperation(ctx, telemetry.OperationWorkerProcess)
	defer func() { telemetry.EndOperation(workerSpan, err) }()
	// Deriving the owner must not mutate a caller-owned request. Queue retries
	// and concurrent callers may retain the original value.
	requestCopy := *req
	if req.Metadata != nil {
		requestCopy.Metadata = make(map[string]interface{}, len(req.Metadata))
		for key, value := range req.Metadata {
			requestCopy.Metadata[key] = value
		}
	}
	if req.Attachments != nil {
		requestCopy.Attachments = append([]channel.Attachment(nil), req.Attachments...)
	}
	req = &requestCopy
	if err := w.validateRequestScope(req); err != nil {
		return nil, err
	}
	if w.strictScope && w.tenant.Status != tenant.TenantStatusActive {
		return nil, fmt.Errorf("%w: tenant status is %q", ErrTenantNotActive, w.tenant.Status)
	}
	if err := validateProcessRequest(req, w.strictScope); err != nil {
		return nil, err
	}
	if err := normalizeRequestSessionOwner(req); err != nil {
		return nil, err
	}
	t := w.tenant
	start := w.collectorStart()
	executionMayHaveStarted := false
	validatedApprovalPause := false
	// Every return before the Runner boundary must retain an explicit execution
	// phase. Without this guard a transient budget/lease/backend error is
	// recorded as retry-unsafe, causing the next Inbox delivery to be blocked
	// as if a model or Tool had already run. Permanent policy errors are marked
	// separately at their return sites so queue adapters can dead-letter them.
	defer func() {
		if err == nil {
			return
		}
		if executionMayHaveStarted {
			if !validatedApprovalPause {
				err = errors.Join(ErrWorkerExecutionOutcomeUnknown, err)
			}
			return
		}
		if errors.Is(err, ErrExecutionTimedOut) ||
			errors.Is(err, ErrExecutionPreflightTimedOut) ||
			errors.Is(err, ErrApprovalResumeUnsafe) {
			return
		}
		if errors.Is(err, ErrExecutionPreflight) {
			return
		}
		err = wrapExecutionPreflight(err)
	}()
	if w.executionTimeout > 0 {
		// Keep the deadline on the request context, before acquiring any
		// distributed admission slot or Session lease. This bounds the entire
		// Worker-owned execution path even when tenant token budgets are disabled.
		var cancelExecution context.CancelFunc
		ctx, cancelExecution = context.WithTimeout(ctx, w.executionTimeout)
		defer cancelExecution()
		// Some dependencies return the context error directly. The boundary is
		// explicit: before Runner.Run the attempt is safe to retry; after it,
		// model or Tool work may have started and requires reconciliation.
		defer func() {
			if err == nil || errors.Is(err, ErrExecutionTimedOut) ||
				errors.Is(err, ErrExecutionPreflightTimedOut) ||
				!errors.Is(err, context.DeadlineExceeded) {
				return
			}
			response = nil
			if executionMayHaveStarted {
				if timeoutErr := executionTimeoutError(ctx); timeoutErr != nil {
					err = errors.Join(err, timeoutErr)
				}
				return
			}
			if timeoutErr := executionPreflightTimeoutError(ctx); timeoutErr != nil {
				err = errors.Join(err, timeoutErr)
			}
		}()
	}
	if w.confirmationEnabled && req.IdempotencyKey == "" {
		w.audit(ctx, req, "denied", start, "approval_identity_required", 0)
		return nil, permanentExecutionPreflightError(ErrApprovalIdentityRequired)
	}

	// Content policy is applied before memory retrieval or model invocation so
	// blocked input is not persisted in shared Session/Memory backends or sent
	// to an external model provider. Warn/log rules remain observable without
	// silently changing the user request.
	contentFilter := governance.NewContentFilter(t.Governance.ContentFilters)
	if action, matched := contentFilter.FilterContent(req.Content); matched {
		switch action {
		case "block":
			w.audit(ctx, req, "denied", start, "content_policy_block", 0)
			return nil, permanentExecutionPreflightError(errors.New("request blocked by tenant content policy"))
		case "warn", "log":
			log.Printf("tenant %s content policy action=%s", t.ID, action)
		default:
			w.audit(ctx, req, "denied", start, "invalid_content_policy_action", 0)
			return nil, permanentExecutionPreflightError(fmt.Errorf("unsupported content policy action %q", action))
		}
	}

	// Check budget before processing
	if w.budgetTracker != nil {
		if err := w.budgetTracker.CheckBudget(ctx); err != nil {
			w.audit(ctx, req, "denied", start, "budget_exceeded", 0)
			return nil, fmt.Errorf("budget check failed: %w", err)
		}
	}
	var concurrencySlot governance.SessionSlot
	concurrencySlotReleased := false
	if w.budgetTracker != nil {
		concurrencySlot, err = w.budgetTracker.AcquireSessionSlot(ctx)
		if err != nil {
			w.audit(ctx, req, "denied", start, "concurrency_limit", 0)
			return nil, fmt.Errorf("tenant concurrency limit: %w", err)
		}
		defer func() {
			if concurrencySlot == nil || concurrencySlotReleased {
				return
			}
			releaseCtx, cancelRelease := detachedPersistenceContext(ctx)
			releaseErr := concurrencySlot.Release(releaseCtx)
			cancelRelease()
			concurrencySlotReleased = true
			if releaseErr != nil {
				errorType := "concurrency_slot_release_failed"
				if errors.Is(releaseErr, governance.ErrSessionSlotLost) {
					errorType = "concurrency_slot_lost"
				}
				w.audit(ctx, req, "error", start, errorType, 0)
				response = nil
				err = errors.Join(err, fmt.Errorf("release tenant concurrency slot: %w", releaseErr))
			}
		}()
	}
	if req.UserID == "" || req.SessionID == "" {
		w.audit(ctx, req, "denied", start, "invalid_session_scope", 0)
		return nil, permanentExecutionPreflightError(errors.New("user ID and session ID are required"))
	}
	if w.sessionLocks == nil {
		w.audit(ctx, req, "denied", start, "session_coordination_unavailable", 0)
		return nil, fmt.Errorf("%w: worker has no session lock manager", ErrDistributedSessionCoordinationRequired)
	}
	memoryCtx, identityErr := contextWithMemoryActor(ctx, t.ID, req, w.strictScope)
	if identityErr != nil {
		return nil, permanentExecutionPreflightError(identityErr)
	}
	lease, err := w.sessionLocks.AcquireLease(ctx, sessionLeaseKey(t.ID, w.appName, req.SessionOwnerID, req.SessionID), storage.DefaultLockTTL)
	if err != nil {
		w.audit(ctx, req, "error", start, "session_lease_unavailable", 0)
		return nil, fmt.Errorf("acquire session invocation lease: %w", err)
	}
	leaseReleased := false
	runCtx, cancelRun := context.WithCancel(memoryCtx)
	runCtx = storage.ContextWithSessionLease(runCtx, session.Key{AppName: w.appName, UserID: req.SessionOwnerID, SessionID: req.SessionID}, lease)
	runCtx, fenceState := fence.WithState(runCtx)
	approvalState := governance.NewApprovalState()
	runCtx = governance.ContextWithApprovalState(runCtx, approvalState)
	runCtx = governance.ContextWithInvocationAudit(runCtx, governance.InvocationAuditContext{
		ChannelType:    req.ChannelType,
		UserID:         req.UserID,
		SessionOwnerID: req.SessionOwnerID,
		SessionID:      req.SessionID,
		AgentName:      w.agentName,
		TraceID:        traceIDFromRequest(req),
		InvocationID:   invocationIdentity(req),
		ApprovalToken:  req.ApprovalToken,
	})
	// Runner's Session identity is intentionally the group owner for group
	// chats. Carry the authenticated actor separately so the scoped Memory
	// service can keep personal reads/writes isolated from shared transcript
	// state on every replica.
	defer cancelRun()
	var concurrencySlotDone <-chan struct{}
	if concurrencySlot != nil {
		concurrencySlotDone = concurrencySlot.Done()
	}
	go func() {
		select {
		case <-lease.Done():
			cancelRun()
		case <-concurrencySlotDone:
			cancelRun()
		case <-runCtx.Done():
		}
	}()
	defer func() {
		if leaseReleased {
			return
		}
		releaseCtx, cancelRelease := detachedPersistenceContext(ctx)
		defer cancelRelease()
		if err := lease.Release(releaseCtx); err != nil {
			log.Printf("release session invocation lease tenant=%s: error=%s", t.ID, telemetry.StableErrorCode(err))
		}
	}()

	// Runner performs native memory recall through the injected memory.Service.
	// Avoid a second manual search/prompt injection, which would duplicate
	// context and bypass the framework's memory policy.
	message, err := buildUserMessage(req)
	if err != nil {
		w.audit(ctx, req, "denied", start, "invalid_attachment", 0)
		return nil, permanentExecutionPreflightError(err)
	}
	resumeApproval, err := w.shouldResumeApproval(runCtx, req)
	if err != nil {
		var required *governance.ApprovalRequiredError
		if errors.As(err, &required) {
			w.audit(ctx, req, "approval_required", start, "tool_confirmation_required", 0)
		} else {
			w.audit(ctx, req, "error", start, "approval_resume_unsafe", 0)
		}
		return nil, err
	}
	if resumeApproval {
		// The state is consumed by the governance plugin at the actual tool hook;
		// retaining the challenge ID here prevents a post-admission race from
		// consuming a replacement grant for the same invocation.
		approvalState.SetResumeChallengeID(req.ApprovalResumeChallengeID)
	}
	runOptions := []agent.RunOption{agent.WithRequestID(invocationIdentity(req))}
	if resumeApproval {
		// The original user message and assistant tool call already form the
		// persisted transcript. An empty current turn prevents a duplicate user
		// event; WithResume executes only the pending tool call before the next
		// model cycle.
		message = model.Message{}
		runOptions = append(runOptions, agent.WithResume(true))
	}

	var tokenReservation governance.TokenReservation
	budgetDispatched := false
	budgetFinalized := true
	if w.budgetTracker != nil {
		tokenReservation, err = w.budgetTracker.ReserveTokenBudget(runCtx)
		if err != nil {
			w.audit(ctx, req, "denied", start, "token_budget_reservation_failed", 0)
			return nil, fmt.Errorf("reserve token budget: %w", err)
		}
		budgetFinalized = tokenReservation.ID == ""
	}
	// This fallback protects future return paths added after a reservation. A
	// model that may have started is charged the full reservation; only a
	// pre-start failure can release it. The explicit success path below settles
	// before returning a response.
	defer func() {
		if budgetFinalized || w.budgetTracker == nil {
			return
		}
		finalizeCtx, cancelFinalize := detachedPersistenceContext(ctx)
		defer cancelFinalize()
		var finalizeErr error
		if budgetDispatched {
			finalizeErr = w.budgetTracker.SettleTokenBudget(finalizeCtx, tokenReservation, tokenReservation.Reserved)
		} else {
			finalizeErr = w.budgetTracker.ReleaseTokenBudget(finalizeCtx, tokenReservation)
		}
		if finalizeErr != nil {
			response = nil
			err = errors.Join(err, fmt.Errorf("finalize token budget: %w", finalizeErr))
		}
	}()

	modelRunCtx := runCtx
	if deadline, ok := tokenReservation.ExecutionDeadline(); ok {
		// ExpiresAt is expressed in Redis's wall clock and is intentionally not
		// compared with this process's wall clock. The local monotonic deadline
		// bounds model work conservatively; Redis remains the authorization
		// authority when DispatchTokenBudget atomically checks the lease.
		settlementDeadline := deadline.Add(-5 * time.Second)
		if !settlementDeadline.After(time.Now()) {
			w.audit(ctx, req, "denied", start, "token_budget_reservation_expired", 0)
			return nil, fmt.Errorf("token budget reservation has no executable lease window")
		}
		var cancelBudgetRun context.CancelFunc
		modelRunCtx, cancelBudgetRun = context.WithDeadline(runCtx, settlementDeadline)
		defer cancelBudgetRun()
	}
	if t.Governance.AuditLevel == "detailed" && w.collector != nil {
		// The additional admission event contains identity and timing only. It
		// precedes budget dispatch and Runner, so a failed write is safely
		// retryable and the still-undispatched reservation can be released.
		if auditErr := w.collector.LogAudit(ctx, &telemetry.AuditLog{
			TenantID: t.ID, ChannelType: req.ChannelType, UserID: req.UserID,
			SessionID: req.SessionID, AgentName: w.agentName,
			Decision: "execution_admitted", LatencyMS: int(time.Since(start).Milliseconds()),
			TraceID: traceIDFromRequest(req),
		}); auditErr != nil {
			return nil, fmt.Errorf("persist execution admission audit: %w", auditErr)
		}
	}
	if tokenReservation.ID != "" {
		if err := w.budgetTracker.DispatchTokenBudget(modelRunCtx, tokenReservation); err != nil {
			w.audit(ctx, req, "error", start, "token_budget_dispatch_failed", 0)
			return nil, fmt.Errorf("dispatch token budget: %w", err)
		}
		budgetDispatched = true
	}
	if preflightTimeoutErr := executionPreflightTimeoutError(modelRunCtx); preflightTimeoutErr != nil {
		// A custom Adapter may ignore context cancellation and still return nil.
		// Check once at the exact side-effect boundary so an already-expired
		// admission can never enter Runner and later be mislabeled retry-safe.
		w.audit(ctx, req, "error", start, "execution_preflight_timeout", 0)
		return nil, preflightTimeoutErr
	}
	if err := modelRunCtx.Err(); err != nil {
		w.audit(ctx, req, "error", start, "execution_preflight_cancelled", 0)
		return nil, err
	}

	// Run the agent using runner. From this point a model or Tool can have an
	// external side effect even if Run returns an error before yielding events.
	// The Runner span intentionally ends before response masking, fencing, and
	// accounting so it measures only framework execution and stream collection.
	executionMayHaveStarted = true
	runnerCtx, runnerSpan := telemetry.StartOperation(modelRunCtx, telemetry.OperationRunnerRun)
	events, err := w.runner.Run(runnerCtx, req.SessionOwnerID, req.SessionID, message, runOptions...)
	if err != nil {
		telemetry.EndOperation(runnerSpan, err)
		if timeoutErr := executionTimeoutError(modelRunCtx); timeoutErr != nil {
			w.audit(ctx, req, "error", start, "execution_timeout", 0)
			return nil, timeoutErr
		}
		// A plugin may raise the approval challenge while Runner is starting
		// the event stream and return that error synchronously. Preserve the
		// typed challenge instead of collapsing it into a generic run failure.
		if challenge, ok := approvalState.Challenge(); ok {
			validatedApprovalPause = true
			w.audit(ctx, req, "approval_required", start, "tool_confirmation_required", 0)
			return nil, &governance.ApprovalRequiredError{Challenge: challenge}
		}
		w.audit(ctx, req, "error", start, "agent_run_failed", 0)
		return nil, fmt.Errorf("failed to run agent: %w", err)
	}

	// A response prefix is not a successful invocation. Runner can emit useful
	// partial output before a terminal provider/tool error, and a cancelled
	// stream can close without a completion event. Only a clean runner
	// completion is allowed to reach the durable result path.
	runnerResponse, collectErr := collectRunnerResponseContext(runnerCtx, events)
	telemetry.EndOperation(runnerSpan, collectErr)
	if collectErr != nil {
		if timeoutErr := executionTimeoutError(modelRunCtx); timeoutErr != nil {
			w.audit(ctx, req, "error", start, "execution_timeout", 0)
			return nil, timeoutErr
		}
		if challenge, ok := approvalState.Challenge(); ok {
			validatedApprovalPause = true
			w.audit(ctx, req, "approval_required", start, "tool_confirmation_required", 0)
			return nil, &governance.ApprovalRequiredError{Challenge: challenge}
		}
		w.audit(ctx, req, "error", start, "agent_run_incomplete", 0)
		return nil, collectErr
	}
	if challenge, ok := approvalState.Challenge(); ok {
		validatedApprovalPause = true
		w.audit(ctx, req, "approval_required", start, "tool_confirmation_required", 0)
		return nil, &governance.ApprovalRequiredError{Challenge: challenge}
	}
	if timeoutErr := executionTimeoutError(modelRunCtx); timeoutErr != nil {
		// A Runner may emit completion at the same time its context expires. A
		// timeout is never a successful invocation: accepting that response would
		// make retry safety depend on scheduler timing.
		w.audit(ctx, req, "error", start, "execution_timeout", 0)
		return nil, timeoutErr
	}
	if fenceErr := fenceState.Error(); fenceErr != nil {
		w.audit(ctx, req, "error", start, "session_fence_lost", 0)
		return nil, fmt.Errorf("session fence validation failed: %w", fenceErr)
	}
	responseContent := runnerResponse.Content
	// Tool output is masked by the Runner plugin. The final model response must
	// be masked independently because it can contain data learned from memory,
	// model context, or a non-tool response.
	governanceFilter := governance.NewGovernanceFilter(t)
	maskedResponse, maskErr := governanceFilter.AfterToolInvocation(runCtx, "model_response", responseContent, nil)
	if maskErr != nil {
		w.audit(ctx, req, "error", start, "response_masking_failed", 0)
		return nil, fmt.Errorf("mask model response: %w", maskErr)
	}
	maskedText, ok := maskedResponse.(string)
	if !ok {
		w.audit(ctx, req, "error", start, "invalid_masked_response", 0)
		return nil, fmt.Errorf("masked model response has unexpected type %T", maskedResponse)
	}
	responseContent = maskedText
	if timeoutErr := executionTimeoutError(modelRunCtx); timeoutErr != nil {
		// Masking and accounting must not convert a deadline boundary into a
		// success. The response could otherwise be durably delivered after an
		// unbounded Tool or provider call has become an uncertain side effect.
		w.audit(ctx, req, "error", start, "execution_timeout", 0)
		return nil, timeoutErr
	}
	if err := lease.Err(); err != nil {
		w.audit(ctx, req, "error", start, "session_lease_lost", 0)
		return nil, fmt.Errorf("session invocation lease lost: %w", err)
	}
	if concurrencySlot != nil {
		if err := concurrencySlot.Err(); err != nil {
			w.audit(ctx, req, "error", start, "concurrency_slot_lost", 0)
			return nil, fmt.Errorf("tenant concurrency slot lost: %w", err)
		}
	}
	// Session event persistence has completed while this invocation still owns
	// the distributed Session lease. Capture the exact committed prefix. A
	// bounded read failure degrades only to deferred target resolution; it must
	// never turn an already side-effecting model run into an automatic replay.
	summarySchedule := w.buildSummarySchedule(runCtx, req)
	releaseCtx, cancelRelease := detachedPersistenceContext(ctx)
	releaseErr := lease.Release(releaseCtx)
	cancelRelease()
	leaseReleased = true
	if releaseErr != nil {
		w.audit(ctx, req, "error", start, "session_lease_release_failed", 0)
		return nil, fmt.Errorf("release session invocation lease: %w", releaseErr)
	}

	accountedTokens := runnerResponse.TotalTokens
	if tokenReservation.ID != "" {
		if !runnerResponse.UsageReliable {
			accountedTokens = tokenReservation.Reserved
		}
		persistCtx, cancelPersist := detachedPersistenceContext(ctx)
		settlementErr := w.budgetTracker.SettleTokenBudget(persistCtx, tokenReservation, accountedTokens)
		cancelPersist()
		if settlementErr == nil || errors.Is(settlementErr, governance.ErrBudgetReservationExceeded) ||
			errors.Is(settlementErr, governance.ErrBudgetExceeded) ||
			errors.Is(settlementErr, governance.ErrBudgetSettlementConflict) {
			budgetFinalized = true
		}
		if settlementErr != nil {
			w.audit(ctx, req, "error", start, "token_budget_settlement_failed", int(accountedTokens))
			return nil, fmt.Errorf("settle token budget: %w", settlementErr)
		}
	}
	if concurrencySlot != nil {
		releaseCtx, cancelRelease := detachedPersistenceContext(ctx)
		releaseErr := concurrencySlot.Release(releaseCtx)
		cancelRelease()
		concurrencySlotReleased = true
		if releaseErr != nil {
			w.audit(ctx, req, "error", start, "concurrency_slot_release_failed", int(accountedTokens))
			return nil, fmt.Errorf("release tenant concurrency slot: %w", releaseErr)
		}
	}

	if auditErr := w.audit(ctx, req, "allowed", start, "", int(accountedTokens)); auditErr != nil {
		// Runner may already have produced external side effects. The deferred
		// execution classifier makes this an uncertain outcome, never a safe
		// retry that would invoke the model or tools a second time.
		return nil, fmt.Errorf("persist execution outcome audit: %w", auditErr)
	}

	return &Response{
		ContentType: "text",
		SessionID:   req.SessionID,
		Content:     responseContent,
		Metadata: map[string]interface{}{
			"agent_app":        w.appName,
			"agent_version":    w.versionID,
			"deployment_id":    w.deploymentID,
			"session_owner_id": req.SessionOwnerID,
		},
		Summary: summarySchedule,
	}, nil
}

const summaryTargetReadTimeout = 2 * time.Second

func (w *Worker) buildSummarySchedule(ctx context.Context, req *Request) *summarycoord.EnqueueRequest {
	if w == nil || !w.summaryEnabled || w.tenant == nil || req == nil || w.agentAppID == "" || w.versionID == "" ||
		req.SessionOwnerID == "" || req.SessionID == "" {
		return nil
	}
	request := &summarycoord.EnqueueRequest{
		Key: summarycoord.Key{
			TenantID:       w.tenant.ID,
			AgentAppID:     w.agentAppID,
			SessionOwnerID: req.SessionOwnerID,
			SessionID:      req.SessionID,
		},
		AgentVersionID: w.versionID,
	}
	request.SessionIncarnationID = storage.SessionIncarnationFromContext(ctx)
	if w.sessionService == nil {
		return request
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), summaryTargetReadTimeout)
	defer cancel()
	value, err := w.sessionService.GetSession(readCtx, session.Key{
		AppName: w.appName, UserID: req.SessionOwnerID, SessionID: req.SessionID,
	})
	if err != nil || value == nil || value.AppName != w.appName || value.UserID != req.SessionOwnerID || value.ID != req.SessionID {
		if w.strictScope && request.SessionIncarnationID == "" {
			return nil
		}
		return request
	}
	incarnation, incarnationErr := storage.SessionIncarnationID(value)
	if incarnationErr != nil || (request.SessionIncarnationID != "" && request.SessionIncarnationID != incarnation) {
		return nil
	}
	request.SessionIncarnationID = incarnation
	if w.strictScope && incarnation == "" {
		return nil
	}
	if count := value.GetEventCount(); count > 0 && count != 1000 {
		request.TargetEventSequence = int64(count)
	}
	return request
}

// validateRequestScope prevents a Worker built for one tenant/version from
// acting as a confused deputy for a request carrying another identity. Legacy
// in-process callers may omit routing fields; production composition sets
// strictScope and rejects omissions as well.
func (w *Worker) validateRequestScope(req *Request) error {
	if req == nil || w == nil || w.tenant == nil {
		return fmt.Errorf("worker request scope is not configured")
	}
	if req.TenantID != "" && req.TenantID != w.tenant.ID {
		return fmt.Errorf("request tenant does not match worker tenant")
	}
	if req.AgentApp != "" && req.AgentApp != w.logicalAppName {
		return fmt.Errorf("request agent app does not match worker deployment")
	}
	if req.AgentVersion != "" && w.versionID != "" && req.AgentVersion != w.versionID {
		return fmt.Errorf("request agent version does not match worker deployment")
	}
	if req.DeploymentID != "" && w.deploymentID != "" && req.DeploymentID != w.deploymentID {
		return fmt.Errorf("request deployment does not match worker deployment")
	}
	if !w.strictScope {
		return nil
	}
	if req.TenantID == "" || req.AgentApp == "" || req.AgentVersion == "" || req.DeploymentID == "" {
		return fmt.Errorf("production request must include tenant, agent app, version and deployment identity")
	}
	return nil
}

// Close closes the worker and releases resources.
func (w *Worker) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		if w.runner != nil {
			w.closeErr = w.runner.Close()
		}
		if w.releaseDataPlane != nil {
			w.closeErr = errors.Join(w.closeErr, w.releaseDataPlane())
			w.releaseDataPlane = nil
		}
		if w.releaseStorage != nil {
			w.releaseStorage()
			w.releaseStorage = nil
		}
	})
	return w.closeErr
}

func findModelConfig(t *tenant.Tenant, modelName string) *tenant.ModelConfig {
	for i := range t.Models {
		if t.Models[i].ModelName == modelName {
			return &t.Models[i]
		}
	}
	return nil
}

func detachedPersistenceContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
}

func executionTimeoutError(ctx context.Context) error {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrExecutionTimedOut, context.DeadlineExceeded)
	}
	return nil
}

func executionPreflightTimeoutError(ctx context.Context) error {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrExecutionPreflightTimedOut, context.DeadlineExceeded)
	}
	return nil
}

func wrapExecutionPreflight(err error) error {
	if err == nil || errors.Is(err, ErrExecutionPreflight) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrExecutionPreflight, err)
}

func permanentExecutionPreflightError(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrExecutionPreflightPermanent, wrapExecutionPreflight(err))
}

func sessionLeaseKey(tenantID, appName, userID, sessionID string) string {
	return storage.SessionInvocationLeaseKey(tenantID, session.Key{AppName: appName, UserID: userID, SessionID: sessionID})
}

// SessionLeaseKey returns the opaque distributed-lock identity shared by Agent
// execution and deferred Summary target resolution. Raw tenant/user/session
// identifiers never appear in Redis keys.
func SessionLeaseKey(tenantID, appName, userID, sessionID string) string {
	return sessionLeaseKey(tenantID, appName, userID, sessionID)
}

func buildUserMessage(req *Request) (model.Message, error) {
	if req == nil {
		return model.Message{}, fmt.Errorf("worker request is required")
	}
	message := model.Message{Role: model.RoleUser, Content: req.Content}
	for _, attachment := range req.Attachments {
		attachmentType := strings.ToLower(strings.TrimSpace(attachment.Type))
		switch attachmentType {
		case "image":
			message.AddImageURL(attachment.URL, "")
		case "audio":
			message.AddAudioURL(attachment.URL, attachment.MimeType)
		case "video":
			message.AddVideoURL(attachment.URL, attachment.MimeType)
		case "file":
			message.AddFileURL(attachment.Name, attachment.URL, attachment.MimeType)
		default:
			return model.Message{}, fmt.Errorf("unsupported attachment type %q", attachment.Type)
		}
	}
	return message, nil
}

func validateAttachmentURL(rawURL string, strict bool) error {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("attachment URL is invalid")
	}
	// Worker passes the URL to the model provider and does not fetch it itself,
	// so this is only a static admission guard. Reject literal non-routable
	// targets nevertheless; a future server-side downloader must add DNS
	// pinning, redirect checks and an operator-owned egress allowlist.
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return fmt.Errorf("attachment URL host is not allowed")
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		addr = addr.Unmap()
		if addr.Zone() != "" || addr.IsLoopback() || addr.IsPrivate() ||
			addr.IsLinkLocalUnicast() || addr.IsUnspecified() || addr.IsMulticast() {
			return fmt.Errorf("attachment URL host is not allowed")
		}
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "https" && (!(!strict && scheme == "http")) {
		return fmt.Errorf("attachment URL must use HTTPS")
	}
	return nil
}

func invocationIdentity(req *Request) string {
	if req == nil {
		return ""
	}
	if req.IdempotencyKey != "" {
		return req.IdempotencyKey
	}
	if req.MessageID != "" {
		return req.MessageID
	}
	return req.SessionID
}

// approvalInvocationIdentity is deliberately narrower than the Runner request
// ID. A Session ID is stable across distinct user turns, so it cannot prove
// that a later message is a retry of the approval-gated invocation. Durable
// automatic recovery therefore requires the Inbox idempotency key. Confirmation
// enabled Workers reject calls without one before model or tool work starts;
// this keeps explicit approval tokens bound to the same durable identity too.
func approvalInvocationIdentity(req *Request) string {
	if req == nil {
		return ""
	}
	return req.IdempotencyKey
}

// shouldResumeApproval proves that a queue retry belongs to exactly one
// persisted pending tool call before it suppresses the incoming user message.
// Without all three facts (active challenge, stable request identity, and an
// exact pending tool call), replaying the Runner would be unsafe.
func (w *Worker) shouldResumeApproval(ctx context.Context, req *Request) (bool, error) {
	if w == nil || req == nil || w.approvalStore == nil || w.sessionService == nil {
		return false, nil
	}
	invocationID := approvalInvocationIdentity(req)
	if invocationID == "" {
		return false, nil
	}
	scope := governance.ApprovalInvocationScope{
		TenantID:       req.TenantID,
		UserID:         req.UserID,
		SessionOwnerID: req.SessionOwnerID,
		SessionID:      req.SessionID,
		InvocationID:   invocationID,
	}
	inspector, ok := w.approvalStore.(governance.ApprovalResumeStateInspector)
	if !ok || isNilInterface(inspector) {
		// A marker supplied by the HTTP admission gate must never be trusted when
		// the store cannot prove grant state atomically. Strict production workers
		// reject this capability set at construction; compatibility workers fail
		// closed rather than falling back to a request-scope-only read.
		if req.ApprovalResumeChallengeID != "" {
			return false, ErrApprovalResumeUnsafe
		}
		if legacy, legacyOK := w.approvalStore.(governance.ApprovalResumeInspector); legacyOK && !isNilInterface(legacy) {
			if _, legacyErr := legacy.FindActiveApproval(ctx, scope); legacyErr == nil {
				return false, ErrApprovalResumeUnsafe
			} else if !errors.Is(legacyErr, governance.ErrApprovalNotFound) {
				return false, fmt.Errorf("inspect pending approval: %w", legacyErr)
			}
		}
		return false, nil
	}
	state, err := inspector.InspectApprovalResume(ctx, scope)
	if errors.Is(err, governance.ErrApprovalNotFound) {
		// A trusted admission fence may have observed a granted challenge that
		// another worker consumed before this Worker acquired its session lease.
		// Never reinterpret that retry as a fresh user turn: doing so could run a
		// model and create a replacement dangerous-tool challenge.
		if req.ApprovalResumeChallengeID != "" {
			return false, ErrApprovalResumeUnsafe
		}
		// Even without the HTTP marker, a durable transcript containing a
		// pending tool call for this invocation proves that the request is an
		// approval retry. If its challenge disappeared, require reconciliation
		// instead of silently appending a duplicate user message.
		sess, sessionErr := w.sessionService.GetSession(ctx, session.Key{
			AppName: w.appName, UserID: req.SessionOwnerID, SessionID: req.SessionID,
		})
		if sessionErr != nil {
			return false, fmt.Errorf("load approval retry session: %w", sessionErr)
		}
		if pendingApprovalInvocationPresent(sess, invocationID) {
			return false, ErrApprovalResumeUnsafe
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect pending approval: %w", err)
	}
	challenge := state.Challenge
	if err := governance.ValidateApprovalChallenge(challenge); err != nil {
		return false, fmt.Errorf("validate pending approval: %w", err)
	}
	if req.ApprovalResumeChallengeID != "" && challenge.ChallengeID != req.ApprovalResumeChallengeID {
		return false, ErrApprovalResumeUnsafe
	}
	sess, err := w.sessionService.GetSession(ctx, session.Key{
		AppName: w.appName, UserID: req.SessionOwnerID, SessionID: req.SessionID,
	})
	if err != nil {
		return false, fmt.Errorf("load pending approval session: %w", err)
	}
	matched, err := pendingApprovalMatchesSession(sess, challenge)
	if err != nil {
		return false, fmt.Errorf("inspect pending approval session: %w", err)
	}
	if !matched {
		return false, ErrApprovalResumeUnsafe
	}
	if !state.Granted {
		// The transcript is a valid pending tool call, but the operator has not
		// granted this exact challenge yet. Return the typed challenge directly so
		// callers can wait without appending a duplicate user turn or re-running
		// the model.
		return false, &governance.ApprovalRequiredError{Challenge: challenge}
	}
	// Carry the exact row identity into the governance plugin. Consumption is
	// deferred until the tool hook so a crash cannot lose an unexecuted grant.
	req.ApprovalResumeChallengeID = challenge.ChallengeID
	return true, nil
}

func pendingApprovalInvocationPresent(sess *session.Session, invocationID string) bool {
	if sess == nil || invocationID == "" {
		return false
	}
	sess.EventMu.RLock()
	defer sess.EventMu.RUnlock()
	if len(sess.Events) == 0 {
		return false
	}
	last := sess.Events[len(sess.Events)-1]
	if last.RequestID != invocationID || last.Response == nil || last.IsPartial ||
		!last.IsValidContent() || !last.Response.IsToolCallResponse() || len(last.Response.Choices) != 1 {
		return false
	}
	return len(last.Response.Choices[0].Message.ToolCalls) > 0
}

func pendingApprovalMatchesSession(sess *session.Session, challenge governance.ApprovalChallenge) (bool, error) {
	if sess == nil {
		return false, nil
	}
	sess.EventMu.RLock()
	defer sess.EventMu.RUnlock()
	if len(sess.Events) == 0 {
		return false, nil
	}
	last := sess.Events[len(sess.Events)-1]
	if last.RequestID != challenge.Request.InvocationID || last.Response == nil || last.IsPartial ||
		!last.IsValidContent() || !last.Response.IsToolCallResponse() || len(last.Response.Choices) != 1 {
		return false, nil
	}
	calls := last.Response.Choices[0].Message.ToolCalls
	if len(calls) != 1 {
		return false, nil
	}
	call := calls[0]
	if call.Function.Name != challenge.Request.ToolName {
		return false, nil
	}
	argsHash, err := governance.CanonicalArgsHash(call.Function.Arguments)
	if err != nil {
		return false, err
	}
	return argsHash == challenge.Request.ArgsHash, nil
}

func findModelCredential(t *tenant.Tenant, provider, modelName string) string {
	for i := range t.Models {
		if t.Models[i].Provider == provider && t.Models[i].ModelName == modelName {
			return t.Models[i].APIKey
		}
	}
	return ""
}

func resolveModelCredential(ctx context.Context, config *tenant.ModelConfig, t *tenant.Tenant, resolver tenant.SecretResolver) (*tenant.ModelConfig, error) {
	if config == nil {
		return nil, fmt.Errorf("model configuration is nil")
	}
	if config.APIKey != "" {
		return config, nil
	}
	if config.APIKeyRef != "" {
		if resolver == nil {
			return nil, fmt.Errorf("model credential resolver is required")
		}
		var credential []byte
		var err error
		if t != nil {
			scoped, ok := resolver.(tenant.TenantSecretResolver)
			if !ok {
				return nil, fmt.Errorf("tenant-scoped model credential authorization is required")
			}
			credential, err = scoped.ResolveForTenant(ctx, t.ID, config.Provider, config.ModelName, "model", tenant.SecretRef(config.APIKeyRef))
		} else {
			credential, err = resolver.Resolve(ctx, tenant.SecretRef(config.APIKeyRef))
		}
		if err != nil {
			return nil, fmt.Errorf("model credential resolution failed: %s", telemetry.StableErrorCode(err))
		}
		if len(credential) == 0 {
			return nil, fmt.Errorf("model credential resolution failed: unavailable")
		}
		copy := *config
		copy.APIKey = string(credential)
		for i := range credential {
			credential[i] = 0
		}
		return &copy, nil
	}
	credential := findModelCredential(t, config.Provider, config.ModelName)
	if credential == "" {
		return nil, fmt.Errorf("no credential configured for model %s/%s", config.Provider, config.ModelName)
	}
	copy := *config
	copy.APIKey = credential
	return &copy, nil
}

func resolveConfiguredTools(ctx context.Context, names []string, platform ToolResolver, memoryService memory.Service) ([]tool.Tool, error) {
	memoryTools := make(map[string]tool.Tool)
	if memoryService != nil {
		for _, value := range memoryService.Tools() {
			declaration, ok := safeToolDeclaration(value)
			if !ok {
				return nil, fmt.Errorf("memory service returned a tool without a declaration name")
			}
			name := declaration.Name
			if _, duplicate := memoryTools[name]; duplicate {
				return nil, fmt.Errorf("memory service returned duplicate tool %q", name)
			}
			memoryTools[name] = value
		}
	}

	resolved := make([]tool.Tool, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("tool %q is configured more than once", name)
		}
		seen[name] = struct{}{}
		if value, ok := memoryTools[name]; ok {
			resolved = append(resolved, value)
			continue
		}
		if platform == nil {
			return nil, fmt.Errorf("tool %q is not available from the tenant memory service and no platform resolver is installed", name)
		}
		var values []tool.Tool
		var err error
		if contextual, ok := platform.(ContextToolResolver); ok {
			values, err = contextual.ResolveContext(ctx, []string{name})
		} else {
			values, err = platform.Resolve([]string{name})
		}
		if err != nil {
			return nil, err
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("platform resolver returned an invalid result for tool %q", name)
		}
		if _, ok := safeToolDeclaration(values[0]); !ok {
			return nil, fmt.Errorf("platform resolver returned a tool without a valid declaration for tool %q", name)
		}
		resolved = append(resolved, values[0])
	}
	return resolved, nil
}

func isNilTool(value tool.Tool) bool {
	return isNilInterface(value)
}

func safeToolDeclaration(value tool.Tool) (declaration *tool.Declaration, ok bool) {
	if isNilTool(value) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			declaration, ok = nil, false
		}
	}()
	declaration = value.Declaration()
	return declaration, declaration != nil && declaration.Name != ""
}

func isNilInterface(value interface{}) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

type collectedRunnerResponse struct {
	Content       string
	TotalTokens   int64
	UsageReliable bool
}

// maxRunnerResponseBytes bounds the amount of model output retained in a
// single durable response. Without this limit a provider can exhaust Worker
// memory before the result reaches Outbox, even though the HTTP client bounds
// the eventual wire response.
const maxRunnerResponseBytes = 1 << 20

func collectRunnerResponse(events <-chan *event.Event) (collectedRunnerResponse, error) {
	return collectRunnerResponseContext(context.Background(), events)
}

// collectRunnerResponseContext consumes a Runner stream without allowing a
// non-cooperative producer to hold the Worker request after its execution
// deadline. On ordinary stream failures it still drains the channel so a
// cooperative unbuffered producer can exit cleanly.
func collectRunnerResponseContext(ctx context.Context, events <-chan *event.Event) (collectedRunnerResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if events == nil {
		return collectedRunnerResponse{}, fmt.Errorf("agent run returned a nil event stream")
	}
	var result collectedRunnerResponse
	completed := false
	usageByResponse := make(map[string]int64)
	sawUsage := false
	sawPositiveUsage := false
	usageReliable := true
	var firstErr error
	responseBytes := 0
	for {
		var evt *event.Event
		var open bool
		select {
		case <-ctx.Done():
			return collectedRunnerResponse{}, ctx.Err()
		case evt, open = <-events:
			if !open {
				goto complete
			}
		}
		if evt == nil {
			continue
		}
		if firstErr != nil {
			// Runner owns the producer channel. Continue receiving after a
			// terminal event so an unbuffered producer cannot leak a goroutine,
			// but never accept data emitted after the first invalid event.
			continue
		}
		if evt.IsTerminalError() {
			if evt.Error != nil {
				// Provider messages can contain request URLs, prompts or other
				// sensitive material. Preserve only the stable error class here;
				// detailed diagnostics belong in a separately protected provider
				// telemetry sink, never in the worker response/error chain.
				firstErr = fmt.Errorf("agent run terminated with %s", evt.Error.Type)
			} else {
				firstErr = fmt.Errorf("agent run terminated with an unspecified error")
			}
			continue
		}
		if evt.IsRunnerCompletion() {
			completed = true
			continue
		}
		if evt.Response != nil && evt.Response.Usage != nil {
			tokens := evt.Response.Usage.TotalTokens
			if tokens < 0 {
				firstErr = fmt.Errorf("agent returned invalid negative token usage")
				continue
			}
			sawUsage = true
			if tokens > 0 {
				sawPositiveUsage = true
			}
			if evt.Response.ID == "" {
				usageReliable = false
			} else if value := int64(tokens); value > usageByResponse[evt.Response.ID] {
				usageByResponse[evt.Response.ID] = value
			}
		}
		// Extract content from event choices
		if evt.Response != nil && len(evt.Response.Choices) > 0 {
			choice := evt.Response.Choices[0]
			// Check delta first (streaming), then message
			if choice.Delta.Content != "" {
				if len(choice.Delta.Content) > maxRunnerResponseBytes-responseBytes {
					firstErr = fmt.Errorf("agent response exceeds %d bytes", maxRunnerResponseBytes)
					continue
				}
				result.Content += choice.Delta.Content
				responseBytes += len(choice.Delta.Content)
			} else if choice.Message.Content != "" {
				if len(choice.Message.Content) > maxRunnerResponseBytes {
					firstErr = fmt.Errorf("agent response exceeds %d bytes", maxRunnerResponseBytes)
					continue
				}
				result.Content = choice.Message.Content
				responseBytes = len(choice.Message.Content)
			}
		}
	}

complete:
	if firstErr != nil {
		return collectedRunnerResponse{}, firstErr
	}
	if !completed {
		return collectedRunnerResponse{}, fmt.Errorf("agent run ended without runner completion")
	}
	if !sawUsage || !sawPositiveUsage || !usageReliable {
		return result, nil
	}
	for _, tokens := range usageByResponse {
		if tokens > int64(^uint64(0)>>1)-result.TotalTokens {
			return collectedRunnerResponse{}, fmt.Errorf("agent token usage overflow")
		}
		result.TotalTokens += tokens
	}
	result.UsageReliable = true
	return result, nil
}
