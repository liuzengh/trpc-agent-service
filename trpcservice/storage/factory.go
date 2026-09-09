package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/mem0"
	"trpc.group/trpc-go/trpc-agent-go/memory/pgvector"
	"trpc.group/trpc-go/trpc-agent-go/session"
	mysqlsession "trpc.group/trpc-go/trpc-agent-go/session/mysql"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// MemoryBackend represents the two memory contracts in tRPC-Agent-Go v1.11.2.
// pgvector supplies Service; mem0 supplies Ingestor plus read-only tools.
type MemoryBackend struct {
	Service  memory.Service
	Ingestor session.Ingestor
	Tools    []agenttool.Tool
}

// Factory resolves and caches tenant-selected framework backends.
type Factory interface {
	SessionService(tenant.AgentApp) (session.Service, error)
	MemoryBackend(tenant.AgentApp) (MemoryBackend, error)
	Close() error
}

// SessionRoute controls migration-time read and write backends.
type SessionRoute struct {
	Reader  string
	Writers []string
}

// RouteSource resolves migration-time session routes from durable state so
// every replica converges on the same route without process-local memory.
type RouteSource interface {
	ActiveRoute(ctx context.Context, appID string) (SessionRoute, bool, error)
}

// routeSourceTimeout bounds the durable route lookup on the request path.
const routeSourceTimeout = 5 * time.Second

// FactoryConfig contains node-level backend endpoints.
type FactoryConfig struct {
	RedisURL    string
	MySQLDSN    string
	PGVectorDSN string
	Mem0BaseURL string
}

type backendConstructors struct {
	redisSession func(tenant.AgentApp) (session.Service, error)
	mysqlSession func(tenant.AgentApp) (session.Service, error)
	pgvector     func(tenant.AgentApp) (MemoryBackend, io.Closer, error)
	mem0         func(tenant.AgentApp) (MemoryBackend, io.Closer, error)
}

// BackendFactory is the production tenant-level backend cache.
type BackendFactory struct {
	mu           sync.Mutex
	closed       bool
	constructors backendConstructors
	sessions     map[string]session.Service
	memories     map[string]MemoryBackend
	closers      []io.Closer
	routes       map[string]SessionRoute
	routeSource  RouteSource
}

// NewBackendFactory constructs a backend factory. pgvectorEmbedder may be nil
// only when no application selects pgvector.
func NewBackendFactory(
	config FactoryConfig,
	pgvectorEmbedder embedder.Embedder,
) *BackendFactory {
	return newBackendFactory(defaultBackendConstructors(config, pgvectorEmbedder))
}

func newBackendFactory(constructors backendConstructors) *BackendFactory {
	return &BackendFactory{
		constructors: constructors,
		sessions:     make(map[string]session.Service),
		memories:     make(map[string]MemoryBackend),
		routes:       make(map[string]SessionRoute),
	}
}

// SessionService returns one cached session service per app version/backend.
// Migration routes resolve from the durable RouteSource first so all replicas
// converge; the process-local route remains for source-less deployments.
func (f *BackendFactory) SessionService(app tenant.AgentApp) (session.Service, error) {
	if app.ID == "" || app.TenantID == "" {
		return nil, errors.New("session backend app and tenant IDs are required")
	}
	route, migrating, err := f.resolveRoute(app.ID)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, errors.New("backend factory is closed")
	}
	if !migrating {
		return f.sessionServiceLocked(app, app.Backends.Session)
	}
	reader, err := f.sessionServiceLocked(app, route.Reader)
	if err != nil {
		return nil, err
	}
	writers := make([]session.Service, 0, len(route.Writers))
	for _, backend := range route.Writers {
		writer, err := f.sessionServiceLocked(app, backend)
		if err != nil {
			return nil, err
		}
		writers = append(writers, writer)
	}
	return newRoutedSessionService(reader, writers...), nil
}

// resolveRoute prefers the durable route source (shared across replicas) and
// falls back to the process-local route set by the migrator.
func (f *BackendFactory) resolveRoute(appID string) (SessionRoute, bool, error) {
	f.mu.Lock()
	source := f.routeSource
	f.mu.Unlock()
	if source != nil {
		ctx, cancel := context.WithTimeout(context.Background(), routeSourceTimeout)
		defer cancel()
		route, ok, err := source.ActiveRoute(ctx, appID)
		if err != nil {
			return SessionRoute{}, false, fmt.Errorf("resolve migration route: %w", err)
		}
		if ok {
			return route, true, nil
		}
	}
	f.mu.Lock()
	route, ok := f.routes[appID]
	f.mu.Unlock()
	return route, ok, nil
}

// SetRouteSource installs a durable route resolver shared across replicas.
func (f *BackendFactory) SetRouteSource(source RouteSource) {
	f.mu.Lock()
	f.routeSource = source
	f.mu.Unlock()
}

// SessionServiceFor returns a concrete backend, bypassing migration routing.
func (f *BackendFactory) SessionServiceFor(
	app tenant.AgentApp,
	backend string,
) (session.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, errors.New("backend factory is closed")
	}
	return f.sessionServiceLocked(app, backend)
}

func (f *BackendFactory) sessionServiceLocked(
	app tenant.AgentApp,
	backend string,
) (session.Service, error) {
	key := backendCacheKey(app, backend)
	if service, ok := f.sessions[key]; ok {
		return service, nil
	}
	var (
		service session.Service
		err     error
	)
	switch backend {
	case "redis":
		if f.constructors.redisSession == nil {
			return nil, errors.New("Redis session constructor is unavailable")
		}
		service, err = f.constructors.redisSession(app)
	case "mysql":
		if f.constructors.mysqlSession == nil {
			return nil, errors.New("MySQL session constructor is unavailable")
		}
		service, err = f.constructors.mysqlSession(app)
	default:
		return nil, fmt.Errorf("unsupported session backend %q", backend)
	}
	if err != nil {
		return nil, fmt.Errorf("create %s session backend: %w", backend, err)
	}
	if service == nil {
		return nil, fmt.Errorf("%s session constructor returned nil", backend)
	}
	f.sessions[key] = service
	f.closers = append(f.closers, service)
	return service, nil
}

// SetSessionRoute atomically changes migration-time routing for an app.
func (f *BackendFactory) SetSessionRoute(appID string, route SessionRoute) error {
	if appID == "" || route.Reader == "" || len(route.Writers) == 0 {
		return errors.New("session route app, reader, and writers are required")
	}
	for _, backend := range append([]string{route.Reader}, route.Writers...) {
		if backend != "redis" && backend != "mysql" {
			return fmt.Errorf("unsupported routed session backend %q", backend)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errors.New("backend factory is closed")
	}
	f.routes[appID] = SessionRoute{
		Reader:  route.Reader,
		Writers: append([]string(nil), route.Writers...),
	}
	return nil
}

// ClearSessionRoute restores the app's configured backend.
func (f *BackendFactory) ClearSessionRoute(appID string) {
	f.mu.Lock()
	delete(f.routes, appID)
	f.mu.Unlock()
}

// MemoryBackend returns one cached memory integration per app version/backend.
func (f *BackendFactory) MemoryBackend(app tenant.AgentApp) (MemoryBackend, error) {
	if app.ID == "" || app.TenantID == "" {
		return MemoryBackend{}, errors.New("memory backend app and tenant IDs are required")
	}
	key := backendCacheKey(app, app.Backends.Memory)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return MemoryBackend{}, errors.New("backend factory is closed")
	}
	if backend, ok := f.memories[key]; ok {
		return backend, nil
	}

	var (
		backend MemoryBackend
		closer  io.Closer
		err     error
	)
	switch app.Backends.Memory {
	case "pgvector":
		if f.constructors.pgvector == nil {
			return MemoryBackend{}, errors.New("pgvector constructor is unavailable")
		}
		backend, closer, err = f.constructors.pgvector(app)
	case "mem0":
		if f.constructors.mem0 == nil {
			return MemoryBackend{}, errors.New("mem0 constructor is unavailable")
		}
		backend, closer, err = f.constructors.mem0(app)
	default:
		return MemoryBackend{}, fmt.Errorf("unsupported memory backend %q", app.Backends.Memory)
	}
	if err != nil {
		return MemoryBackend{}, fmt.Errorf("create %s memory backend: %w", app.Backends.Memory, err)
	}
	if backend.Service == nil && backend.Ingestor == nil {
		return MemoryBackend{}, fmt.Errorf("%s memory constructor returned no integration", app.Backends.Memory)
	}
	f.memories[key] = backend
	if closer != nil {
		f.closers = append(f.closers, closer)
	}
	return backend, nil
}

// Close releases every cached backend exactly once.
func (f *BackendFactory) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	closers := append([]io.Closer(nil), f.closers...)
	f.mu.Unlock()

	var result error
	for index := len(closers) - 1; index >= 0; index-- {
		result = errors.Join(result, closers[index].Close())
	}
	return result
}

func defaultBackendConstructors(
	config FactoryConfig,
	pgvectorEmbedder embedder.Embedder,
) backendConstructors {
	redisURL := normalizeRedisURL(config.RedisURL)
	return backendConstructors{
		redisSession: func(app tenant.AgentApp) (session.Service, error) {
			if redisURL == "" {
				return nil, errors.New("Redis URL is empty")
			}
			return redissession.NewService(
				redissession.WithRedisClientURL(redisURL),
				redissession.WithCompatMode(redissession.CompatModeNone),
				redissession.WithKeyPrefix("agent:"+app.TenantID+":"+app.ID+":"),
			)
		},
		mysqlSession: func(tenant.AgentApp) (session.Service, error) {
			if config.MySQLDSN == "" {
				return nil, errors.New("MySQL DSN is empty")
			}
			return mysqlsession.NewService(
				mysqlsession.WithMySQLClientDSN(config.MySQLDSN),
				mysqlsession.WithTablePrefix("agent"),
			)
		},
		pgvector: func(tenant.AgentApp) (MemoryBackend, io.Closer, error) {
			if config.PGVectorDSN == "" {
				return MemoryBackend{}, nil, errors.New("pgvector DSN is empty")
			}
			if pgvectorEmbedder == nil || pgvectorEmbedder.GetDimensions() <= 0 {
				return MemoryBackend{}, nil, errors.New("pgvector embedder with positive dimensions is required")
			}
			service, err := pgvector.NewService(
				pgvector.WithPGVectorClientDSN(config.PGVectorDSN),
				pgvector.WithEmbedder(pgvectorEmbedder),
				pgvector.WithIndexDimension(pgvectorEmbedder.GetDimensions()),
				pgvector.WithSchema("public"),
				pgvector.WithTableName("agent_memory"),
			)
			if err != nil {
				return MemoryBackend{}, nil, err
			}
			return MemoryBackend{Service: service}, service, nil
		},
		mem0: func(tenant.AgentApp) (MemoryBackend, io.Closer, error) {
			if config.Mem0BaseURL == "" {
				return MemoryBackend{}, nil, errors.New("mem0 base URL is empty")
			}
			service, err := mem0.NewService(
				mem0.WithSelfHostedOSS(),
				mem0.WithHost(config.Mem0BaseURL),
			)
			if err != nil {
				return MemoryBackend{}, nil, err
			}
			return MemoryBackend{
				Ingestor: service,
				Tools:    service.Tools(),
			}, service, nil
		},
	}
}

func normalizeRedisURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "://") {
		return value
	}
	return "redis://" + value
}

func backendCacheKey(app tenant.AgentApp, backend string) string {
	return fmt.Sprintf("%s:%d:%s", app.ID, app.Version, backend)
}
