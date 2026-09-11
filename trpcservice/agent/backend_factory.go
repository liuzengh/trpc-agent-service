package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	platformbackend "github.com/cyl6/trpc-agent-service/trpcservice/backend"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/dbbinding"
	"github.com/cyl6/trpc-agent-service/trpcservice/memoryvisibility"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"

	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgeopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorymem0 "trpc.group/trpc-go/trpc-agent-go/memory/mem0"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
	summarypkg "trpc.group/trpc-go/trpc-agent-go/session/summary"
	s3storage "trpc.group/trpc-go/trpc-agent-go/storage/s3"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// backendBundle owns constructed resources until they are transferred to Runtime.
// Construction failure closes everything acquired so far in reverse order.
// Framework SPIs remain the boundary; strict transactions are an extra capability.
type backendBundle struct {
	session          session.Service
	turn             sessionturn.TransactionalService
	databaseIdentity string
	memory           *runtimeMemoryBackend
	artifact         artifact.Service
	knowledge        knowledge.Knowledge
	closers          []io.Closer
}

func buildBackends(ctx context.Context, tenant config.TenantConfig, namespace string, summarizer summarypkg.SessionSummarizer, visibility memoryvisibility.Store) (_ *backendBundle, err error) {
	b := &backendBundle{}
	defer func() {
		if err == nil {
			return
		}
		for i := len(b.closers) - 1; i >= 0; i-- {
			_ = b.closers[i].Close()
		}
		if b.session != nil {
			_ = b.session.Close()
		}
	}()
	b.session, err = buildSessionService(ctx, tenant, namespace, summarizer)
	if err != nil {
		return nil, err
	}
	b.turn, _ = b.session.(sessionturn.TransactionalService)
	if provider, ok := b.session.(dbbinding.Provider); ok {
		b.databaseIdentity = provider.DatabaseIdentity()
	}
	if tenant.Data.Session.Type == "sql" && (b.turn == nil || b.databaseIdentity == "") {
		return nil, errors.New("create sql session service: strict turn or database identity capability is unavailable")
	}
	b.memory, err = buildMemoryBackend(tenant, namespace, visibility)
	if err != nil {
		return nil, err
	}
	b.closers = append(b.closers, b.memory)
	var closer io.Closer
	b.artifact, closer, err = buildArtifactService(ctx, tenant, namespace)
	if err != nil {
		return nil, err
	}
	if closer != nil {
		b.closers = append(b.closers, closer)
	}
	b.knowledge, closer, err = buildKnowledgeService(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if closer != nil {
		b.closers = append(b.closers, closer)
	}
	return b, nil
}

type runtimeMemoryBackend struct {
	Service  memory.Service
	Reader   memory.Reader
	Ingestor session.Ingestor
	Tools    []tool.Tool
	closer   io.Closer
}

func (b *runtimeMemoryBackend) Close() error {
	if b == nil || b.closer == nil {
		return nil
	}
	return b.closer.Close()
}

func buildMemoryBackend(tenant config.TenantConfig, appNamespace string, visibility ...memoryvisibility.Store) (*runtimeMemoryBackend, error) {
	var visibilityStore memoryvisibility.Store
	if len(visibility) > 0 {
		visibilityStore = visibility[0]
	}
	if tenant.Data.Memory.Type != "external" {
		service, err := buildMemoryService(tenant, appNamespace)
		if err != nil {
			return nil, err
		}
		service = newVisibleMemoryService(service, visibilityStore, tenant.TenantID, appNamespace, tenant.Data.Memory.Type)
		backend := &runtimeMemoryBackend{Service: service, Reader: service, closer: service}
		if service != nil {
			backend.Tools = service.Tools()
		}
		return backend, nil
	}

	cfg := tenant.Data.Memory
	options := []memorymem0.ServiceOpt{memorymem0.WithHost(cfg.Endpoint)}
	if cfg.APIKeyEnv != "" {
		apiKey, err := config.Secret(cfg.APIKeyEnv)
		if err != nil {
			return nil, errors.New("create external memory service: api key unavailable")
		}
		options = append(options, memorymem0.WithAPIKey(apiKey))
	}
	if cfg.Mode == "self_hosted" {
		options = append(options, memorymem0.WithSelfHostedOSS())
	}
	if cfg.Async != nil {
		options = append(options, memorymem0.WithAsyncMode(*cfg.Async))
	}
	options = append(options, memorymem0.WithLoadToolEnabled(toolAllowed(tenant.Tools, memory.LoadToolName)))
	service, err := memorymem0.NewService(options...)
	if err != nil {
		return nil, errors.New("create external memory service: invalid configuration")
	}
	tools := make([]tool.Tool, 0, len(service.Tools()))
	for _, candidate := range service.Tools() {
		if candidate != nil && candidate.Declaration() != nil && toolAllowed(tenant.Tools, candidate.Declaration().Name) {
			tools = append(tools, candidate)
		}
	}
	return &runtimeMemoryBackend{Reader: service, Ingestor: service, Tools: tools, closer: service}, nil
}

func buildMemoryService(tenant config.TenantConfig, appNamespace string) (memory.Service, error) {
	memoryToolNames := []string{
		memory.AddToolName, memory.UpdateToolName, memory.DeleteToolName,
		memory.ClearToolName, memory.SearchToolName, memory.LoadToolName,
	}
	switch tenant.Data.Memory.Type {
	case "disabled":
		return nil, nil
	case "inmemory":
		options := make([]memoryinmemory.ServiceOpt, 0, len(memoryToolNames))
		for _, name := range memoryToolNames {
			options = append(options, memoryinmemory.WithToolEnabled(name, toolAllowed(tenant.Tools, name)))
		}
		return memoryinmemory.NewMemoryService(options...), nil
	case "redis":
		rawURL, err := config.Secret(tenant.Data.Memory.DSNEnv)
		if err != nil {
			return nil, err
		}
		prefix := tenant.Data.Memory.Namespace
		if prefix == "" {
			prefix = appNamespace
		}
		options := []memoryredis.ServiceOpt{
			memoryredis.WithRedisClientURL(rawURL),
			memoryredis.WithKeyPrefix(prefix),
		}
		for _, name := range memoryToolNames {
			options = append(options, memoryredis.WithToolEnabled(name, toolAllowed(tenant.Tools, name)))
		}
		service, err := memoryredis.NewService(options...)
		if err != nil {
			return nil, errors.New("create redis memory service: invalid or unavailable backend")
		}
		return service, nil
	default:
		return nil, fmt.Errorf("unsupported runnable memory backend %q", tenant.Data.Memory.Type)
	}
}

func toolAllowed(policy config.ToolPolicy, name string) bool {
	for _, denied := range policy.Deny {
		if denied == name {
			return false
		}
	}
	for _, allowed := range policy.Allow {
		if allowed == name {
			return true
		}
	}
	return false
}

func buildArtifactService(ctx context.Context, tenant config.TenantConfig, appNamespace string) (artifact.Service, io.Closer, error) {
	cfg := tenant.Data.Artifact
	switch cfg.Type {
	case "", "inmemory":
		service, err := platformbackend.NewScopedArtifact(artifactinmemory.NewService(), appNamespace)
		return service, nil, err
	case "object":
		options := []s3storage.ClientBuilderOpt{s3storage.WithBucket(cfg.Bucket)}
		if cfg.Endpoint != "" {
			options = append(options, s3storage.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Region != "" {
			options = append(options, s3storage.WithRegion(cfg.Region))
		}
		if cfg.PathStyle {
			options = append(options, s3storage.WithPathStyle(true))
		}
		if cfg.AccessKeyEnv != "" {
			accessKey, err := config.Secret(cfg.AccessKeyEnv)
			if err != nil {
				return nil, nil, errors.New("create object artifact service: access key unavailable")
			}
			secretKey, err := config.Secret(cfg.SecretKeyEnv)
			if err != nil {
				return nil, nil, errors.New("create object artifact service: secret key unavailable")
			}
			options = append(options, s3storage.WithCredentials(accessKey, secretKey))
		}
		if cfg.SessionTokenEnv != "" {
			token, err := config.Secret(cfg.SessionTokenEnv)
			if err != nil {
				return nil, nil, errors.New("create object artifact service: session token unavailable")
			}
			options = append(options, s3storage.WithSessionToken(token))
		}
		blobs, err := s3storage.NewClient(ctx, options...)
		if err != nil {
			return nil, nil, errors.New("create object artifact service: invalid or unavailable backend")
		}
		dsn, err := config.Secret(cfg.DSNEnv)
		if err != nil {
			_ = blobs.Close()
			return nil, nil, errors.New("create object artifact metadata: runtime DSN unavailable")
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			_ = blobs.Close()
			return nil, nil, errors.New("create object artifact metadata: invalid backend")
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			_ = blobs.Close()
			return nil, nil, errors.New("create object artifact metadata: unavailable backend")
		}
		service, err := platformbackend.NewPostgresObjectArtifact(
			ctx, pool, blobs, tenant.TenantID, appNamespace, time.Minute,
		)
		if err != nil {
			pool.Close()
			_ = blobs.Close()
			return nil, nil, errors.New("create object artifact metadata: schema verification failed")
		}
		scoped, err := platformbackend.NewScopedArtifact(service, appNamespace)
		if err != nil {
			_ = service.Close()
			return nil, nil, err
		}
		return scoped, service, nil
	default:
		return nil, nil, fmt.Errorf("unsupported runnable artifact backend %q", cfg.Type)
	}
}

func buildKnowledgeService(
	ctx context.Context,
	tenant config.TenantConfig,
) (knowledge.Knowledge, io.Closer, error) {
	cfg := tenant.Data.Knowledge
	if cfg.Type == "" || cfg.Type == "disabled" {
		return nil, nil, nil
	}
	if cfg.Type != "vector" || cfg.Provider != "pgvector" {
		return nil, nil, fmt.Errorf("unsupported runnable knowledge backend %q/%q", cfg.Type, cfg.Provider)
	}
	dsn, err := config.Secret(cfg.DSNEnv)
	if err != nil {
		return nil, nil, errors.New("create vector knowledge service: database secret unavailable")
	}
	apiKey, err := config.Secret(cfg.APIKeyEnv)
	if err != nil {
		return nil, nil, errors.New("create vector knowledge service: embedding api key unavailable")
	}
	embedderOptions := []knowledgeopenai.Option{
		knowledgeopenai.WithModel(cfg.EmbeddingModel),
		knowledgeopenai.WithDimensions(cfg.EmbeddingDimension),
		knowledgeopenai.WithAPIKey(apiKey),
	}
	if cfg.EmbeddingBaseURL != "" {
		embedderOptions = append(embedderOptions, knowledgeopenai.WithBaseURL(cfg.EmbeddingBaseURL))
	}
	service, err := platformbackend.NewPostgresKnowledge(ctx, platformbackend.PostgresKnowledgeOptions{
		DSN: dsn, Table: cfg.Namespace, TenantID: tenant.TenantID,
		AppName: tenant.App.Name, Dimension: cfg.EmbeddingDimension,
		Embedder: knowledgeopenai.New(embedderOptions...),
	})
	if err != nil {
		return nil, nil, errors.New("create vector knowledge service: invalid, unmigrated or unavailable backend")
	}
	return service, service, nil
}

func buildSessionService(ctx context.Context, tenant config.TenantConfig, appNamespace string, summarizer summarypkg.SessionSummarizer) (session.Service, error) {
	switch tenant.Data.Session.Type {
	case "inmemory":
		return sessioninmemory.NewSessionService(sessioninmemory.WithSummarizer(summarizer)), nil
	case "redis":
		rawURL, err := config.Secret(tenant.Data.Session.DSNEnv)
		if err != nil {
			return nil, err
		}
		prefix := tenant.Data.Session.Namespace
		if prefix == "" {
			prefix = appNamespace
		}
		service, err := sessionredis.NewService(
			sessionredis.WithRedisClientURL(rawURL),
			sessionredis.WithKeyPrefix(prefix),
			sessionredis.WithEnableTracing(true),
			sessionredis.WithSummarizer(summarizer),
		)
		if err != nil {
			return nil, errors.New("create redis session service: invalid or unavailable backend")
		}
		return service, nil
	case "sql":
		envName := tenant.Data.Session.DSNEnv
		dsn, err := config.Secret(envName)
		if err != nil {
			return nil, fmt.Errorf("create sql session service from %s: secret unavailable", safeEnvReference(envName))
		}
		service, err := sessionturn.OpenSessionServiceForTenant(ctx, dsn, tenant.TenantID)
		if err != nil {
			// Backend errors can include the parsed connection string. Keep the
			// externally visible error stable and limited to the safe env label.
			return nil, fmt.Errorf("create sql session service from %s: invalid or unavailable backend", safeEnvReference(envName))
		}
		if summarizer != nil {
			if err := service.ConfigureSummarizer(summarizer); err != nil {
				_ = service.Close()
				return nil, err
			}
		}
		return service, nil
	default:
		return nil, fmt.Errorf("unsupported runnable session backend %q", tenant.Data.Session.Type)
	}
}

func safeEnvReference(name string) string {
	if name == "" {
		return "dsn_env <unset>"
	}
	for i, r := range name {
		letter := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
		if letter || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return "dsn_env <invalid>"
	}
	return fmt.Sprintf("dsn_env %q", name)
}
