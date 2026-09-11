package assembly

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorychromadb "trpc.group/trpc-go/trpc-agent-go/memory/chromadb"
	"trpc.group/trpc-go/trpc-agent-go/memory/extractor"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory/mem0"
	memorymysql "trpc.group/trpc-go/trpc-agent-go/memory/mysql"
	memorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	memorytencentdb "trpc.group/trpc-go/trpc-agent-go/memory/tencentdb"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	preloadMemoryBudget = 4
	knowledgeSearchTool = "knowledge_search"
	currentTimeTool     = "environment_context_current_time"

	MemoryDriverPostgres  = "postgres"
	MemoryDriverInMemory  = "inmemory"
	MemoryDriverRedis     = "redis"
	MemoryDriverMySQL     = "mysql"
	MemoryDriverMem0      = "mem0"
	MemoryDriverChromaDB  = "chromadb"
	MemoryDriverTencentDB = "tencentdb"
)

const selectiveMemoryPrompt = `You manage durable user memory for an enterprise assistant.
Only persist information that is highly likely to improve future conversations with this SAME user.

Remember only:
- explicit, stable preferences (language, response style, units, recurring choices);
- durable constraints or requirements the user expects to keep applying;
- stable background facts that materially affect future assistance;
- ongoing long-lived context only when the user clearly indicates it will matter again.

Do NOT remember:
- one-off requests, current task details, greetings, temporary states, routine events, or ordinary chat;
- every activity, purchase, meeting, place, quote, or incidental detail;
- assistant-generated assumptions or inferred preferences that the user did not clearly express;
- secrets, credentials, access tokens, private keys, or sensitive values;
- facts about unrelated third parties unless the relationship itself is clearly relevant long-term.

Use memory_add only when a durable item is new and high-confidence. Use memory_update when an existing durable preference changed. Use memory_delete when the user explicitly asks to forget it. Avoid duplicates.
Prefer memory_kind="fact". Only create an episode when the user explicitly asks the assistant to remember a dated event for future use.
Write concise memory in the user's language. If there is nothing worth retaining for future conversations, make no memory tool call.`

// GovernedToolNames returns the fail-closed tool allow-list: platform runtime
// tools plus framework memory, artifact, knowledge, and time tools.
func GovernedToolNames(platformTools ...string) []string {
	return append(append([]string{}, platformTools...),
		memory.SearchToolName,
		platformtool.SaveArtifactToolName,
		knowledgeSearchTool,
		currentTimeTool,
	)
}

func newTenantMemoryExtractor(configuredModel model.Model) extractor.MemoryExtractor {
	return extractor.NewExtractor(
		configuredModel,
		extractor.WithPrompt(selectiveMemoryPrompt),
		extractor.WithChecker(extractor.CheckMessageThreshold(4)),
	)
}

// MemoryBackend represents both framework memory integration contracts.
// PostgreSQL/InMemory implement memory.Service; Mem0 is ingest-first and is
// attached through runner.WithSessionIngestor plus its framework tools.
type MemoryBackend struct {
	Service  memory.Service
	Ingestor session.Ingestor
	Tools    []agenttool.Tool
	release  func() error
}

type groupSafeMemoryService struct{ memory.Service }

func (s groupSafeMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if isGroupMemorySubject(sess) {
		return nil
	}
	return s.Service.EnqueueAutoMemoryJob(ctx, sess)
}

type groupSafeSessionIngestor struct{ session.Ingestor }

func (i groupSafeSessionIngestor) IngestSession(ctx context.Context, sess *session.Session, opts ...session.IngestOption) error {
	if isGroupMemorySubject(sess) {
		return nil
	}
	return i.Ingestor.IngestSession(ctx, sess, opts...)
}

func isGroupMemorySubject(sess *session.Session) bool {
	return sess != nil && strings.HasPrefix(strings.TrimSpace(sess.UserID), "group:")
}

func memoryServiceBackend(service memory.Service) MemoryBackend {
	return MemoryBackend{Service: groupSafeMemoryService{Service: service}, Tools: service.Tools()}
}

func memoryIngestorBackend(ingestor session.Ingestor, tools []agenttool.Tool) MemoryBackend {
	return MemoryBackend{Ingestor: groupSafeSessionIngestor{Ingestor: ingestor}, Tools: tools}
}

// Release returns one acquired runtime Memory backend to its provider. The
// provider owns the underlying service and closes it only after the last
// Runner using that exact immutable configuration releases it.
func (b MemoryBackend) Release() error {
	if b.release == nil {
		return nil
	}
	return b.release()
}

type MemoryProvider interface {
	MemoryBackend(context.Context, config.TenantConfig, model.Model) (MemoryBackend, error)
}

type MemoryReaderProvider interface {
	MemoryReader(context.Context, config.TenantConfig) (memory.Reader, error)
}

type MemoryServiceProvider interface {
	MemoryService(context.Context, config.TenantConfig) (memory.Service, error)
}

type MemoryEmbedderProvider interface {
	MemoryEmbedder(context.Context) (embedder.Embedder, error)
}

type MemoryProviderOption func(*ManagedMemoryProvider)

func WithMemoryEmbedderProvider(provider MemoryEmbedderProvider) MemoryProviderOption {
	return func(managed *ManagedMemoryProvider) {
		managed.embedderProvider = provider
	}
}

func WithMemoryBackendHealth(registry *backendhealth.Registry) MemoryProviderOption {
	return func(managed *ManagedMemoryProvider) {
		managed.health = registry
	}
}

// ManagedMemoryProvider selects only framework-native Memory integrations.
// Durable remote backends use separate runtime/reader clients so a read-only
// Gateway cannot create a model-less service later reused for extraction.
// InMemory is the single-process exception: the Console lazily reads the same
// runtime service because two in-memory services would be two databases.
type ManagedMemoryProvider struct {
	defaultPostgresDSN string
	secrets            credential.SecretResolver
	profiles           platformstorage.BackendProfileResolver
	httpClient         *http.Client
	embedderProvider   MemoryEmbedderProvider
	health             *backendhealth.Registry

	mu            sync.Mutex
	runtime       map[string]*managedMemoryRuntime
	runtimeBuilds map[string]*memoryProviderBuild
	readers       map[string]memory.Reader
	readerBuilds  map[string]*memoryProviderBuild
	readerClosers []io.Closer
	closed        bool
}

type managedMemoryRuntime struct {
	backend MemoryBackend
	closer  io.Closer
	refs    int
}

type memoryProviderBuild struct {
	done chan struct{}
	err  error
}

func NewManagedMemoryProvider(defaultPostgresDSN string, secrets credential.SecretResolver, profiles platformstorage.BackendProfileResolver, httpClient *http.Client, options ...MemoryProviderOption) (*ManagedMemoryProvider, error) {
	if strings.TrimSpace(defaultPostgresDSN) == "" {
		return nil, errors.New("default Memory PostgreSQL DSN is required")
	}
	if secrets == nil {
		return nil, errors.New("Memory secret resolver is required")
	}
	if profiles == nil {
		return nil, errors.New("Memory backend profile resolver is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	provider := &ManagedMemoryProvider{
		defaultPostgresDSN: defaultPostgresDSN, secrets: secrets, profiles: profiles, httpClient: httpClient,
		runtime: make(map[string]*managedMemoryRuntime), runtimeBuilds: make(map[string]*memoryProviderBuild),
		readers: make(map[string]memory.Reader), readerBuilds: make(map[string]*memoryProviderBuild),
	}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider, nil
}

func (p *ManagedMemoryProvider) MemoryBackend(ctx context.Context, tenantConfig config.TenantConfig, configuredModel model.Model) (MemoryBackend, error) {
	if configuredModel == nil {
		return MemoryBackend{}, errors.New("Memory extractor model is required")
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return MemoryBackend{}, ErrManagedProviderClosed
	}
	backend, connection, physicalKey, err := p.resolve(ctx, tenantConfig)
	if err != nil {
		return MemoryBackend{}, err
	}
	key := memoryRuntimeKey(physicalKey, tenantConfig.ConfigVersion)
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return MemoryBackend{}, ErrManagedProviderClosed
		}
		if cached := p.runtime[key]; cached != nil {
			cached.refs++
			result := p.leasedMemoryBackendLocked(key, cached.backend)
			p.mu.Unlock()
			return result, nil
		}
		if build := p.runtimeBuilds[key]; build != nil {
			done := build.done
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return MemoryBackend{}, ctx.Err()
			case <-done:
				if build.err != nil {
					return MemoryBackend{}, build.err
				}
				continue
			}
		}
		build := &memoryProviderBuild{done: make(chan struct{})}
		p.runtimeBuilds[key] = build
		p.mu.Unlock()

		result, closer, buildErr := p.buildMemoryBackend(ctx, tenantConfig, configuredModel, backend, connection)
		if buildErr == nil && p.health != nil && backendhealth.ShouldProtect(backend) {
			healthKey := memoryHealthKey(tenantConfig.Storage.Memory.ProfileID, backend)
			var probeReader memory.Reader
			if result.Service != nil {
				probeReader = result.Service
			} else if reader, ok := closer.(memory.Reader); ok {
				probeReader = reader
			}
			if probeReader != nil {
				buildErr = p.health.RegisterProbe(healthKey, func(probeCtx context.Context) error {
					_, probeErr := probeReader.ReadMemories(probeCtx, memory.UserKey{AppName: tenantConfig.AppName(), UserID: "__backend_probe__"}, 1)
					return probeErr
				})
				if buildErr == nil {
					buildErr = p.health.Check(ctx, healthKey)
				}
			}
			if buildErr == nil {
				result = observeMemoryBackend(result, p.health, healthKey)
			}
		}
		p.mu.Lock()
		delete(p.runtimeBuilds, key)
		if buildErr == nil && p.closed {
			buildErr = ErrManagedProviderClosed
		}
		var leased MemoryBackend
		if buildErr == nil {
			p.runtime[key] = &managedMemoryRuntime{backend: result, closer: closer, refs: 1}
			leased = p.leasedMemoryBackendLocked(key, result)
		}
		build.err = buildErr
		close(build.done)
		p.mu.Unlock()
		if buildErr != nil {
			if closer != nil {
				_ = closer.Close()
			}
			return MemoryBackend{}, buildErr
		}
		return leased, nil
	}
}

func (p *ManagedMemoryProvider) buildMemoryBackend(ctx context.Context, tenantConfig config.TenantConfig, configuredModel model.Model, backend, connection string) (MemoryBackend, io.Closer, error) {
	switch backend {
	case MemoryDriverPostgres:
		postgresScope := platformstorage.FrameworkPostgresScope("memory", tenantConfig.TenantID)
		if strings.TrimSpace(connection) == strings.TrimSpace(p.defaultPostgresDSN) {
			postgresScope = platformstorage.FrameworkTenantPostgresScope("memory", tenantConfig.TenantID, tenantConfig.AppName())
		}
		service, err := memorypostgres.NewService(
			memorypostgres.WithPostgresClientDSN(connection),
			memorypostgres.WithExtraOptions(postgresScope),
			memorypostgres.WithSkipDBInit(true), memorypostgres.WithSoftDelete(true),
			memorypostgres.WithExtractor(newTenantMemoryExtractor(configuredModel)),
			memorypostgres.WithDisableAutoMemoryOnExternalContext(true),
		)
		if err != nil {
			return MemoryBackend{}, nil, fmt.Errorf("construct tenant %q PostgreSQL Memory: %w", tenantConfig.AppName(), err)
		}
		return memoryServiceBackend(service), service, nil
	case MemoryDriverInMemory:
		service := memoryinmemory.NewMemoryService(
			memoryinmemory.WithExtractor(newTenantMemoryExtractor(configuredModel)),
			memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
		)
		return memoryServiceBackend(service), service, nil
	case MemoryDriverRedis:
		service, err := memoryredis.NewService(
			memoryredis.WithRedisClientURL(normalizeRedisURL(connection)), memoryredis.WithKeyPrefix("agent-memory:"),
			memoryredis.WithExtractor(newTenantMemoryExtractor(configuredModel)), memoryredis.WithDisableAutoMemoryOnExternalContext(true),
		)
		if err != nil {
			return MemoryBackend{}, nil, fmt.Errorf("construct tenant %q Redis Memory: %w", tenantConfig.AppName(), err)
		}
		return memoryServiceBackend(service), service, nil
	case MemoryDriverMySQL:
		service, err := memorymysql.NewService(
			memorymysql.WithMySQLClientDSN(connection), memorymysql.WithSoftDelete(true),
			memorymysql.WithExtractor(newTenantMemoryExtractor(configuredModel)), memorymysql.WithDisableAutoMemoryOnExternalContext(true),
		)
		if err != nil {
			return MemoryBackend{}, nil, fmt.Errorf("construct tenant %q MySQL Memory: %w", tenantConfig.AppName(), err)
		}
		return memoryServiceBackend(service), service, nil
	case MemoryDriverChromaDB:
		service, err := p.newChromaMemoryService(ctx, tenantConfig, connection, newTenantMemoryExtractor(configuredModel))
		if err != nil {
			return MemoryBackend{}, nil, err
		}
		return memoryServiceBackend(service), service, nil
	case MemoryDriverMem0:
		service, err := mem0.NewService(mem0.WithSelfHostedOSS(), mem0.WithHost(connection), mem0.WithHTTPClient(p.httpClient), mem0.WithLoadToolEnabled(true))
		if err != nil {
			return MemoryBackend{}, nil, fmt.Errorf("construct tenant %q Mem0 Memory: %w", tenantConfig.AppName(), err)
		}
		return memoryIngestorBackend(service, service.Tools()), service, nil
	case MemoryDriverTencentDB:
		service, err := p.newTencentDBMemoryService(connection)
		if err != nil {
			return MemoryBackend{}, nil, fmt.Errorf("construct tenant %q TencentDB Memory: %w", tenantConfig.AppName(), err)
		}
		return memoryIngestorBackend(service, service.Tools()), service, nil
	default:
		return MemoryBackend{}, nil, fmt.Errorf("unsupported Memory backend %q", backend)
	}
}

func memoryRuntimeKey(physicalKey string, configVersion uint64) string {
	return fmt.Sprintf("runtime\x00%s\x00%d", physicalKey, configVersion)
}

func (p *ManagedMemoryProvider) leasedMemoryBackendLocked(key string, backend MemoryBackend) MemoryBackend {
	var once sync.Once
	var releaseErr error
	backend.release = func() error {
		once.Do(func() { releaseErr = p.releaseMemoryBackend(key) })
		return releaseErr
	}
	return backend
}

func (p *ManagedMemoryProvider) releaseMemoryBackend(key string) error {
	p.mu.Lock()
	entry := p.runtime[key]
	if entry == nil {
		p.mu.Unlock()
		return nil
	}
	entry.refs--
	if entry.refs > 0 {
		p.mu.Unlock()
		return nil
	}
	delete(p.runtime, key)
	closer := entry.closer
	p.mu.Unlock()
	if closer != nil {
		return closer.Close()
	}
	return nil
}

func (p *ManagedMemoryProvider) MemoryReader(ctx context.Context, tenantConfig config.TenantConfig) (memory.Reader, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, ErrManagedProviderClosed
	}
	backend, connection, physicalKey, err := p.resolve(ctx, tenantConfig)
	if err != nil {
		return nil, err
	}
	runtimeKey := memoryRuntimeKey(physicalKey, tenantConfig.ConfigVersion)
	key := "reader\x00" + physicalKey
	if backend == MemoryDriverInMemory {
		key = "reader\x00" + runtimeKey
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrManagedProviderClosed
		}
		if cached := p.readers[key]; cached != nil {
			p.mu.Unlock()
			return cached, nil
		}
		if build := p.readerBuilds[key]; build != nil {
			done := build.done
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if build.err != nil {
					return nil, build.err
				}
				continue
			}
		}
		build := &memoryProviderBuild{done: make(chan struct{})}
		p.readerBuilds[key] = build
		p.mu.Unlock()

		reader, closer, buildErr := p.buildMemoryReader(ctx, tenantConfig, backend, connection, runtimeKey)
		if buildErr == nil && p.health != nil && backendhealth.ShouldProtect(backend) {
			healthKey := memoryHealthKey(tenantConfig.Storage.Memory.ProfileID, backend)
			buildErr = p.health.RegisterProbe(healthKey, func(probeCtx context.Context) error {
				_, probeErr := reader.ReadMemories(probeCtx, memory.UserKey{AppName: tenantConfig.AppName(), UserID: "__backend_probe__"}, 1)
				return probeErr
			})
			if buildErr == nil {
				buildErr = p.health.Check(ctx, healthKey)
			}
			if buildErr == nil {
				if service, ok := reader.(memory.Service); ok {
					reader = observeMemoryService(service, p.health, healthKey)
				} else {
					reader = &healthMemoryReader{Reader: reader, registry: p.health, key: healthKey}
				}
			}
		}
		p.mu.Lock()
		delete(p.readerBuilds, key)
		if buildErr == nil && p.closed {
			buildErr = ErrManagedProviderClosed
		}
		if buildErr == nil {
			p.readers[key] = reader
			if closer != nil {
				p.readerClosers = append(p.readerClosers, closer)
			}
		}
		build.err = buildErr
		close(build.done)
		p.mu.Unlock()
		if buildErr != nil {
			if closer != nil {
				_ = closer.Close()
			}
			return nil, buildErr
		}
		return reader, nil
	}
}

func (p *ManagedMemoryProvider) buildMemoryReader(ctx context.Context, tenantConfig config.TenantConfig, backend, connection, runtimeKey string) (memory.Reader, io.Closer, error) {
	switch backend {
	case MemoryDriverPostgres:
		service, err := memorypostgres.NewService(memorypostgres.WithPostgresClientDSN(connection), memorypostgres.WithSkipDBInit(true), memorypostgres.WithSoftDelete(true))
		if err != nil {
			return nil, nil, err
		}
		return service, service, nil
	case MemoryDriverInMemory:
		return &managedInMemoryReader{provider: p, runtimeKey: runtimeKey}, nil, nil
	case MemoryDriverRedis:
		service, err := memoryredis.NewService(memoryredis.WithRedisClientURL(normalizeRedisURL(connection)), memoryredis.WithKeyPrefix("agent-memory:"))
		if err != nil {
			return nil, nil, err
		}
		return service, service, nil
	case MemoryDriverMySQL:
		service, err := memorymysql.NewService(memorymysql.WithMySQLClientDSN(connection), memorymysql.WithSoftDelete(true))
		if err != nil {
			return nil, nil, err
		}
		return service, service, nil
	case MemoryDriverChromaDB:
		service, err := p.newChromaMemoryService(ctx, tenantConfig, connection, nil)
		if err != nil {
			return nil, nil, err
		}
		return service, service, nil
	case MemoryDriverMem0:
		service, err := mem0.NewService(mem0.WithSelfHostedOSS(), mem0.WithHost(connection), mem0.WithHTTPClient(p.httpClient))
		if err != nil {
			return nil, nil, err
		}
		return service, service, nil
	case MemoryDriverTencentDB:
		return nil, nil, fmt.Errorf("%w: TencentDB Memory does not expose the framework memory.Reader interface", platformstorage.ErrMemoryDirectAccessUnsupported)
	default:
		return nil, nil, fmt.Errorf("unsupported Memory backend %q", backend)
	}
}

func (p *ManagedMemoryProvider) MemoryService(ctx context.Context, tenantConfig config.TenantConfig) (memory.Service, error) {
	reader, err := p.MemoryReader(ctx, tenantConfig)
	if err != nil {
		return nil, err
	}
	if service, ok := reader.(memory.Service); ok {
		return service, nil
	}
	managed, ok := reader.(*managedInMemoryReader)
	if !ok {
		return nil, errors.New("configured Memory backend does not support writes")
	}
	service, err := managed.service(ctx)
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, errors.New("in-memory Memory backend is not initialized")
	}
	return service, nil
}

type managedInMemoryReader struct {
	provider   *ManagedMemoryProvider
	runtimeKey string
}

func (r *managedInMemoryReader) service(ctx context.Context) (memory.Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.provider.mu.Lock()
	entry := r.provider.runtime[r.runtimeKey]
	r.provider.mu.Unlock()
	if entry == nil {
		return nil, nil
	}
	return entry.backend.Service, nil
}

func (r *managedInMemoryReader) ReadMemories(ctx context.Context, userKey memory.UserKey, limit int) ([]*memory.Entry, error) {
	service, err := r.service(ctx)
	if err != nil || service == nil {
		return nil, err
	}
	return service.ReadMemories(ctx, userKey, limit)
}

func (r *managedInMemoryReader) SearchMemories(ctx context.Context, userKey memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	service, err := r.service(ctx)
	if err != nil || service == nil {
		return nil, err
	}
	return service.SearchMemories(ctx, userKey, query, opts...)
}

func (p *ManagedMemoryProvider) resolve(ctx context.Context, tenantConfig config.TenantConfig) (driver, connection, key string, err error) {
	physical, err := p.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, platformstorage.BackendDomainMemory, tenantConfig.Storage.Memory.ProfileID)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve tenant %q Memory backend profile: %w", tenantConfig.AppName(), err)
	}
	driver = strings.ToLower(strings.TrimSpace(physical.Driver))
	if driver == "" {
		driver = MemoryDriverPostgres
	}
	reference := strings.TrimSpace(physical.ConnectionRef)
	if reference != "" {
		connection, err = p.secrets.Resolve(ctx, reference)
		if err != nil {
			return "", "", "", fmt.Errorf("resolve tenant %q Memory backend: %w", tenantConfig.AppName(), err)
		}
		connection = strings.TrimSpace(connection)
	}
	switch driver {
	case MemoryDriverPostgres:
		if connection == "" {
			connection = p.defaultPostgresDSN
		}
	case MemoryDriverInMemory:
		if connection != "" {
			return "", "", "", fmt.Errorf("tenant %q in-memory Memory backend does not accept a connection", tenantConfig.AppName())
		}
	case MemoryDriverRedis, MemoryDriverMySQL, MemoryDriverMem0, MemoryDriverChromaDB, MemoryDriverTencentDB:
		if connection == "" {
			return "", "", "", fmt.Errorf("tenant %q %s Memory backend requires a connection", tenantConfig.AppName(), driver)
		}
	default:
		return "", "", "", fmt.Errorf("unsupported Memory backend %q", driver)
	}
	return driver, connection, tenantConfig.AppName() + "\x00" + driver + "\x00" + reference + "\x00" + backendConnectionFingerprint(connection), nil
}

type chromaMemoryConnection struct {
	BaseURL     string `json:"base_url"`
	APIKey      string `json:"api_key,omitempty"`
	BearerToken string `json:"bearer_token,omitempty"`
	Tenant      string `json:"tenant,omitempty"`
	Database    string `json:"database,omitempty"`
}

type tencentDBMemoryConnection struct {
	GatewayURL string `json:"gateway_url"`
	APIKey     string `json:"api_key,omitempty"`
}

func (p *ManagedMemoryProvider) newChromaMemoryService(ctx context.Context, tenantConfig config.TenantConfig, encoded string, extractor extractor.MemoryExtractor) (*memorychromadb.Service, error) {
	if p.embedderProvider == nil {
		return nil, errors.New("ChromaDB Memory requires the platform Memory embedder provider")
	}
	connection, err := parseChromaMemoryConnection(encoded)
	if err != nil {
		return nil, fmt.Errorf("tenant %q: %w", tenantConfig.AppName(), err)
	}
	memoryEmbedder, err := p.embedderProvider.MemoryEmbedder(ctx)
	if err != nil {
		return nil, fmt.Errorf("construct tenant %q ChromaDB Memory embedder: %w", tenantConfig.AppName(), err)
	}
	options := []memorychromadb.ServiceOpt{
		memorychromadb.WithBaseURL(connection.BaseURL),
		memorychromadb.WithEmbedder(memoryEmbedder),
		memorychromadb.WithCollectionName(memoryCollectionName(tenantConfig.TenantID, tenantConfig.AppCode)),
		memorychromadb.WithHTTPClient(p.httpClient),
		memorychromadb.WithSoftDelete(true),
		memorychromadb.WithDisableAutoMemoryOnExternalContext(true),
	}
	if connection.APIKey = strings.TrimSpace(connection.APIKey); connection.APIKey != "" {
		options = append(options, memorychromadb.WithAPIKey(connection.APIKey))
	}
	if connection.BearerToken = strings.TrimSpace(connection.BearerToken); connection.BearerToken != "" {
		options = append(options, memorychromadb.WithBearerToken(connection.BearerToken))
	}
	if connection.Tenant = strings.TrimSpace(connection.Tenant); connection.Tenant != "" {
		options = append(options, memorychromadb.WithTenant(connection.Tenant))
	}
	if connection.Database = strings.TrimSpace(connection.Database); connection.Database != "" {
		options = append(options, memorychromadb.WithDatabase(connection.Database))
	}
	if extractor != nil {
		options = append(options, memorychromadb.WithExtractor(extractor))
	}
	service, err := memorychromadb.NewService(options...)
	if err != nil {
		return nil, fmt.Errorf("construct tenant %q ChromaDB Memory: %w", tenantConfig.AppName(), err)
	}
	return service, nil
}

func (p *ManagedMemoryProvider) newTencentDBMemoryService(encoded string) (*memorytencentdb.Service, error) {
	connection, err := parseTencentDBMemoryConnection(encoded)
	if err != nil {
		return nil, err
	}
	options := []memorytencentdb.Option{
		memorytencentdb.WithGatewayURL(connection.GatewayURL),
		memorytencentdb.WithHTTPClient(p.httpClient),
	}
	if connection.APIKey = strings.TrimSpace(connection.APIKey); connection.APIKey != "" {
		options = append(options, memorytencentdb.WithAPIKey(connection.APIKey))
	}
	return memorytencentdb.NewService(options...)
}

func parseChromaMemoryConnection(encoded string) (chromaMemoryConnection, error) {
	var connection chromaMemoryConnection
	if err := json.Unmarshal([]byte(encoded), &connection); err != nil {
		return chromaMemoryConnection{}, fmt.Errorf("decode ChromaDB Memory connection: %w", err)
	}
	connection.BaseURL = strings.TrimSpace(connection.BaseURL)
	if connection.BaseURL == "" {
		return chromaMemoryConnection{}, errors.New("ChromaDB Memory base_url is required")
	}
	connection.APIKey = strings.TrimSpace(connection.APIKey)
	connection.BearerToken = strings.TrimSpace(connection.BearerToken)
	connection.Tenant = strings.TrimSpace(connection.Tenant)
	connection.Database = strings.TrimSpace(connection.Database)
	return connection, nil
}

func parseTencentDBMemoryConnection(encoded string) (tencentDBMemoryConnection, error) {
	var connection tencentDBMemoryConnection
	if err := json.Unmarshal([]byte(encoded), &connection); err != nil {
		return tencentDBMemoryConnection{}, fmt.Errorf("decode TencentDB Memory connection: %w", err)
	}
	connection.GatewayURL = strings.TrimSpace(connection.GatewayURL)
	if connection.GatewayURL == "" {
		return tencentDBMemoryConnection{}, errors.New("TencentDB Memory gateway_url is required")
	}
	connection.APIKey = strings.TrimSpace(connection.APIKey)
	return connection, nil
}

func memoryCollectionName(tenantID, appCode string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + appCode))
	return "trpc_memory_" + hex.EncodeToString(digest[:12])
}

func (p *ManagedMemoryProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	closers := append([]io.Closer(nil), p.readerClosers...)
	for _, entry := range p.runtime {
		if entry != nil && entry.closer != nil {
			closers = append(closers, entry.closer)
		}
	}
	p.readerClosers = nil
	p.runtime = make(map[string]*managedMemoryRuntime)
	p.readers = make(map[string]memory.Reader)
	p.mu.Unlock()
	var result error
	for index := len(closers) - 1; index >= 0; index-- {
		result = errors.Join(result, closers[index].Close())
	}
	return result
}

var _ MemoryProvider = (*ManagedMemoryProvider)(nil)
var _ MemoryReaderProvider = (*ManagedMemoryProvider)(nil)
