package summaryruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	frameworksummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

const (
	defaultMaxSummaryWords = 256
	defaultMaxOutputTokens = 512
	defaultMinEvents       = 20
	maxSummaryWords        = 4096
	maxSummaryOutputTokens = 8192
)

type TenantReader interface {
	GetTenant(context.Context, string) (*tenant.Tenant, error)
}

type VersionReader interface {
	LoadVersion(context.Context, string, string, string) (*controlplane.ResolvedVersion, error)
}

type ServiceAcquirer interface {
	AcquireServices(context.Context, *tenant.Tenant) (session.Service, memory.Service, func(), error)
}

type ModelBuilder func(
	context.Context,
	tenant.ModelConfig,
	*tenant.Tenant,
	tenant.SecretResolver,
) (model.Model, error)

type CheckpointReader interface {
	Get(context.Context, summarycoord.Key) (summarycoord.Checkpoint, bool, error)
}

type RuntimeOptions struct {
	Tenants         TenantReader
	Versions        VersionReader
	Services        ServiceAcquirer
	Redis           *redis.Client
	SecretResolver  tenant.SecretResolver
	ModelBuilder    ModelBuilder
	Checkpoints     CheckpointReader
	MaxSummaryWords int
	MaxOutputTokens int
	MinEvents       int64
	SessionLockTTL  time.Duration
}

// Runtime reconstructs one immutable summary execution from a durable Job. It
// implements both summary.Generator and summary.TargetResolver.
type Runtime struct {
	tenants         TenantReader
	versions        VersionReader
	services        ServiceAcquirer
	redis           *redis.Client
	secretResolver  tenant.SecretResolver
	modelBuilder    ModelBuilder
	checkpoints     CheckpointReader
	maxSummaryWords int
	maxOutputTokens int
	minEvents       int64
	lockTTL         time.Duration
}

func New(options RuntimeOptions) (*Runtime, error) {
	if nilValue(options.Tenants) || nilValue(options.Versions) || nilValue(options.Services) ||
		options.Redis == nil || nilValue(options.Checkpoints) {
		return nil, fmt.Errorf("summary runtime requires tenant, version, storage, Redis and checkpoint dependencies")
	}
	if options.MaxSummaryWords == 0 {
		options.MaxSummaryWords = defaultMaxSummaryWords
	}
	if options.MaxOutputTokens == 0 {
		options.MaxOutputTokens = defaultMaxOutputTokens
	}
	if options.MinEvents == 0 {
		options.MinEvents = defaultMinEvents
	}
	if options.MinEvents < 1 || options.MinEvents > 100000 {
		return nil, fmt.Errorf("summary event threshold must be between 1 and 100000")
	}
	if options.MaxSummaryWords < 1 || options.MaxSummaryWords > maxSummaryWords {
		return nil, fmt.Errorf("summary word limit must be between 1 and %d", maxSummaryWords)
	}
	if options.MaxOutputTokens < 1 || options.MaxOutputTokens > maxSummaryOutputTokens {
		return nil, fmt.Errorf("summary output token limit must be between 1 and %d", maxSummaryOutputTokens)
	}
	if options.SessionLockTTL == 0 {
		options.SessionLockTTL = storage.DefaultLockTTL
	}
	if options.SessionLockTTL < time.Second || options.SessionLockTTL > 10*time.Minute {
		return nil, fmt.Errorf("summary Session lock TTL is outside the safe range")
	}
	if options.ModelBuilder == nil {
		options.ModelBuilder = worker.NewModelForTenant
	}
	return &Runtime{
		tenants: options.Tenants, versions: options.Versions, services: options.Services,
		redis: options.Redis, secretResolver: options.SecretResolver, modelBuilder: options.ModelBuilder,
		checkpoints:     options.Checkpoints,
		maxSummaryWords: options.MaxSummaryWords, maxOutputTokens: options.MaxOutputTokens,
		minEvents: options.MinEvents,
		lockTTL:   options.SessionLockTTL,
	}, nil
}

type loadedSession struct {
	tenant   *tenant.Tenant
	version  *controlplane.ResolvedVersion
	sessions session.Service
	appName  string
	release  func()
}

func (r *Runtime) load(ctx context.Context, job summarycoord.Job) (*loadedSession, error) {
	if r == nil {
		return nil, summarycoord.ErrGeneratorUnavailable
	}
	if err := job.Validate(); err != nil {
		return nil, err
	}
	if job.SessionIncarnationID == "" {
		return nil, summarycoord.ErrTranscriptIncomplete
	}
	tenantValue, err := r.tenants.GetTenant(ctx, job.TenantID)
	if err != nil {
		return nil, fmt.Errorf("load summary tenant: %w", err)
	}
	if tenantValue == nil || tenantValue.ID != job.TenantID || tenantValue.Status != tenant.TenantStatusActive {
		return nil, fmt.Errorf("load summary tenant: active tenant is unavailable")
	}
	if err := tenant.ValidateDistributedStorage(tenantValue.Storage); err != nil {
		return nil, fmt.Errorf("load summary tenant storage: %w", err)
	}
	version, err := r.versions.LoadVersion(ctx, job.TenantID, job.AgentAppID, job.AgentVersionID)
	if err != nil {
		return nil, fmt.Errorf("load summary version: %w", err)
	}
	if version == nil || version.TenantID != job.TenantID || version.AgentAppID != job.AgentAppID ||
		version.VersionID != job.AgentVersionID || version.VersionNumber <= 0 {
		return nil, fmt.Errorf("load summary version: pinned identity mismatch")
	}
	if err := version.Snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("load summary version: %w", err)
	}
	sessions, _, release, err := r.services.AcquireServices(ctx, tenantValue)
	if err != nil {
		return nil, fmt.Errorf("acquire summary Session backend: %w", err)
	}
	if nilValue(sessions) {
		if release != nil {
			release()
		}
		return nil, fmt.Errorf("acquire summary Session backend: unavailable")
	}
	if release == nil {
		release = func() {}
	}
	appName, err := storage.TenantScopedAppName(tenantValue, version.AgentAppName)
	if err != nil {
		release()
		return nil, fmt.Errorf("scope summary Session app: %w", err)
	}
	return &loadedSession{
		tenant: tenantValue, version: version, sessions: sessions, appName: appName, release: release,
	}, nil
}

func (r *Runtime) Generate(ctx context.Context, job summarycoord.Job) (summarycoord.Candidate, error) {
	if job.TargetEventSequence <= 0 {
		return summarycoord.Candidate{}, summarycoord.ErrTranscriptIncomplete
	}
	loaded, err := r.load(ctx, job)
	if err != nil {
		return summarycoord.Candidate{}, err
	}
	defer loaded.release()
	checkpoint, found, err := r.checkpoints.Get(ctx, job.Key)
	if err != nil {
		return summarycoord.Candidate{}, fmt.Errorf("read summary checkpoint policy: %w", err)
	}
	baseline := int64(0)
	if found {
		baseline = checkpoint.EventSequence
	}
	if job.TargetEventSequence <= baseline || job.TargetEventSequence-baseline < r.minEvents {
		return summarycoord.Candidate{}, summarycoord.ErrSummaryNotDue
	}
	base, err := r.modelBuilder(ctx, loaded.version.Snapshot.Model, loaded.tenant, r.secretResolver)
	if err != nil {
		return summarycoord.Candidate{}, fmt.Errorf("build summary model: %w", err)
	}
	configured, err := newConfiguredSummaryModel(base, loaded.version.Snapshot.Model, r.maxOutputTokens)
	if err != nil {
		return summarycoord.Candidate{}, err
	}
	budgeted, err := NewBudgetedModel(configured, governance.NewBudgetTracker(r.redis, loaded.tenant))
	if err != nil {
		return summarycoord.Candidate{}, err
	}
	reader, err := summarycoord.NewTRPCSessionTranscriptReader(loaded.sessions, func(key summarycoord.Key) (string, error) {
		if key != job.Key {
			return "", summarycoord.ErrTranscriptIncomplete
		}
		return loaded.appName, nil
	})
	if err != nil {
		return summarycoord.Candidate{}, err
	}
	summarizer := frameworksummary.NewSummarizer(
		budgeted,
		frameworksummary.WithName("tenant-session-summary"),
		frameworksummary.WithMaxSummaryWords(r.maxSummaryWords),
	)
	generator, err := summarycoord.NewProductionGenerator(reader, summarizer)
	if err != nil {
		return summarycoord.Candidate{}, err
	}
	return generator.Generate(ctx, job)
}

func (r *Runtime) ResolveTarget(ctx context.Context, job summarycoord.Job) (sequence int64, err error) {
	if job.TargetEventSequence != 0 && job.TargetResolutionLeaseVersion == 0 {
		return 0, summarycoord.ErrTranscriptIncomplete
	}
	loaded, err := r.load(ctx, job)
	if err != nil {
		return 0, err
	}
	defer loaded.release()
	manager := storage.NewSessionLockManager(r.redis)
	lease, err := manager.AcquireLease(ctx, worker.SessionLeaseKey(
		job.TenantID, loaded.appName, job.SessionOwnerID, job.SessionID,
	), r.lockTTL)
	if err != nil {
		return 0, fmt.Errorf("acquire summary Session read lease: %w", err)
	}
	resolver, err := summarycoord.NewTRPCSessionTargetResolver(loaded.sessions, func(key summarycoord.Key) (string, error) {
		if key != job.Key {
			return "", summarycoord.ErrTranscriptIncomplete
		}
		return loaded.appName, nil
	})
	if err == nil {
		sequence, err = resolver.ResolveTarget(ctx, job)
	}
	if leaseErr := lease.Err(); err == nil && leaseErr != nil {
		err = leaseErr
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	releaseErr := lease.Release(releaseCtx)
	cancel()
	if err == nil && releaseErr != nil {
		err = releaseErr
	}
	return sequence, err
}

type configuredSummaryModel struct {
	base      model.Model
	config    tenant.ModelConfig
	maxTokens int
}

func newConfiguredSummaryModel(base model.Model, config tenant.ModelConfig, operatorMax int) (model.Model, error) {
	if nilValue(base) {
		return nil, fmt.Errorf("summary model is unavailable")
	}
	maxTokens := operatorMax
	if config.MaxTokens > 0 && config.MaxTokens < maxTokens {
		maxTokens = config.MaxTokens
	}
	return &configuredSummaryModel{base: base, config: config, maxTokens: maxTokens}, nil
}

func (m *configuredSummaryModel) Info() model.Info { return m.base.Info() }

func (m *configuredSummaryModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if request == nil {
		return m.base.GenerateContent(ctx, nil)
	}
	copy := *request
	generation := request.GenerationConfig
	maxTokens := m.maxTokens
	generation.MaxTokens = &maxTokens
	if m.config.Temperature > 0 {
		temperature := m.config.Temperature
		generation.Temperature = &temperature
	}
	copy.GenerationConfig = generation
	return m.base.GenerateContent(ctx, &copy)
}
