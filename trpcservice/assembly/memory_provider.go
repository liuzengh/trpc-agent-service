package assembly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/extractor"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory/mem0"
	memorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	preloadMemoryBudget = 4
	knowledgeSearchTool = "knowledge_search"
	currentTimeTool     = "environment_context_current_time"

	MemoryDriverPostgres = "postgres"
	MemoryDriverInMemory = "inmemory"
	MemoryDriverMem0     = "mem0"
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

// GovernedToolNames returns the fail-closed tool allow-list: tenant-selectable
// platform tools plus framework memory, artifact, knowledge, and time tools.
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

	mu            sync.Mutex
	runtime       map[string]*managedMemoryRuntime
	readers       map[string]memory.Reader
	readerClosers []io.Closer
}

type managedMemoryRuntime struct {
	backend MemoryBackend
	closer  io.Closer
	refs    int
}

func NewManagedMemoryProvider(defaultPostgresDSN string, secrets credential.SecretResolver, profiles platformstorage.BackendProfileResolver, httpClient *http.Client) (*ManagedMemoryProvider, error) {
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
	return &ManagedMemoryProvider{
		defaultPostgresDSN: defaultPostgresDSN, secrets: secrets, profiles: profiles, httpClient: httpClient,
		runtime: make(map[string]*managedMemoryRuntime), readers: make(map[string]memory.Reader),
	}, nil
}

func (p *ManagedMemoryProvider) MemoryBackend(ctx context.Context, tenantConfig config.TenantConfig, configuredModel model.Model) (MemoryBackend, error) {
	if configuredModel == nil {
		return MemoryBackend{}, errors.New("Memory extractor model is required")
	}
	backend, connection, physicalKey, err := p.resolve(ctx, tenantConfig)
	if err != nil {
		return MemoryBackend{}, err
	}
	key := memoryRuntimeKey(physicalKey, tenantConfig.ConfigVersion)
	p.mu.Lock()
	if cached := p.runtime[key]; cached != nil {
		cached.refs++
		result := p.leasedMemoryBackendLocked(key, cached.backend)
		p.mu.Unlock()
		return result, nil
	}
	p.mu.Unlock()

	var (
		result MemoryBackend
		closer io.Closer
	)
	switch backend {
	case MemoryDriverPostgres:
		service, err := memorypostgres.NewService(
			memorypostgres.WithPostgresClientDSN(connection),
			memorypostgres.WithExtraOptions(platformstorage.FrameworkPostgresScope("memory", tenantConfig.TenantID)),
			memorypostgres.WithSoftDelete(true),
			memorypostgres.WithExtractor(newTenantMemoryExtractor(configuredModel)),
			memorypostgres.WithDisableAutoMemoryOnExternalContext(true),
		)
		if err != nil {
			return MemoryBackend{}, fmt.Errorf("construct tenant %q PostgreSQL Memory: %w", tenantConfig.AppName(), err)
		}
		result, closer = MemoryBackend{Service: service, Tools: service.Tools()}, service
	case MemoryDriverInMemory:
		service := memoryinmemory.NewMemoryService(
			memoryinmemory.WithExtractor(newTenantMemoryExtractor(configuredModel)),
			memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
		)
		result, closer = MemoryBackend{Service: service, Tools: service.Tools()}, service
	case MemoryDriverMem0:
		service, err := mem0.NewService(
			mem0.WithSelfHostedOSS(),
			mem0.WithHost(connection),
			mem0.WithHTTPClient(p.httpClient),
			mem0.WithLoadToolEnabled(true),
		)
		if err != nil {
			return MemoryBackend{}, fmt.Errorf("construct tenant %q Mem0 Memory: %w", tenantConfig.AppName(), err)
		}
		result, closer = MemoryBackend{Ingestor: service, Tools: service.Tools()}, service
	default:
		return MemoryBackend{}, fmt.Errorf("unsupported Memory backend %q", backend)
	}

	p.mu.Lock()
	if cached := p.runtime[key]; cached != nil {
		cached.refs++
		leased := p.leasedMemoryBackendLocked(key, cached.backend)
		p.mu.Unlock()
		if closer != nil {
			_ = closer.Close()
		}
		return leased, nil
	}
	entry := &managedMemoryRuntime{backend: result, closer: closer, refs: 1}
	p.runtime[key] = entry
	leased := p.leasedMemoryBackendLocked(key, result)
	p.mu.Unlock()
	return leased, nil
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
	backend, connection, physicalKey, err := p.resolve(ctx, tenantConfig)
	if err != nil {
		return nil, err
	}
	runtimeKey := memoryRuntimeKey(physicalKey, tenantConfig.ConfigVersion)
	key := "reader\x00" + physicalKey
	if backend == MemoryDriverInMemory {
		key = "reader\x00" + runtimeKey
	}
	p.mu.Lock()
	if cached := p.readers[key]; cached != nil {
		p.mu.Unlock()
		return cached, nil
	}
	p.mu.Unlock()

	var (
		reader memory.Reader
		closer io.Closer
	)
	switch backend {
	case MemoryDriverPostgres:
		service, err := memorypostgres.NewService(
			memorypostgres.WithPostgresClientDSN(connection),
			memorypostgres.WithSoftDelete(true),
		)
		if err != nil {
			return nil, err
		}
		reader, closer = service, service
	case MemoryDriverInMemory:
		// InMemory is intentionally a single-process development backend. A
		// second reader-only framework service would be a different database,
		// so the Console lazily reads the same runtime service instead.
		reader = &managedInMemoryReader{provider: p, runtimeKey: runtimeKey}
	case MemoryDriverMem0:
		service, err := mem0.NewService(mem0.WithSelfHostedOSS(), mem0.WithHost(connection), mem0.WithHTTPClient(p.httpClient))
		if err != nil {
			return nil, err
		}
		reader, closer = service, service
	default:
		return nil, fmt.Errorf("unsupported Memory backend %q", backend)
	}
	p.mu.Lock()
	if cached := p.readers[key]; cached != nil {
		p.mu.Unlock()
		if closer != nil {
			_ = closer.Close()
		}
		return cached, nil
	}
	p.readers[key] = reader
	if closer != nil {
		p.readerClosers = append(p.readerClosers, closer)
	}
	p.mu.Unlock()
	return reader, nil
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
	case MemoryDriverMem0:
		if connection == "" {
			return "", "", "", fmt.Errorf("tenant %q Mem0 Memory backend requires a connection", tenantConfig.AppName())
		}
	default:
		return "", "", "", fmt.Errorf("unsupported Memory backend %q", driver)
	}
	return driver, connection, tenantConfig.AppName() + "\x00" + driver + "\x00" + reference, nil
}

func (p *ManagedMemoryProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
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
