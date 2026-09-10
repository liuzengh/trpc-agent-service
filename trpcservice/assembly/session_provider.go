package assembly

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

const (
	SessionDriverPostgres = "postgres"
	SessionDriverRedis    = "redis"
	SessionDriverInMemory = "inmemory"
)

// SessionProvider resolves the framework Session service selected by one
// immutable tenant configuration.
type SessionProvider interface {
	Session(context.Context, config.TenantConfig) (session.Service, error)
}

// ManagedSessionProvider uses only framework-native Session implementations.
// Services are cached by tenant application and physical backend so config
// version changes do not split one conversation's Session history.
type ManagedSessionProvider struct {
	defaultPostgresDSN string
	secrets            credential.SecretResolver
	profiles           platformstorage.BackendProfileResolver
	summarizer         sessionsummary.SessionSummarizer
	routes             platformstorage.SessionMigrationRouteSource

	mu       sync.Mutex
	services map[string]session.Service
}

func NewManagedSessionProvider(defaultPostgresDSN string, secrets credential.SecretResolver, profiles platformstorage.BackendProfileResolver, summarizer sessionsummary.SessionSummarizer, routes platformstorage.SessionMigrationRouteSource) (*ManagedSessionProvider, error) {
	if strings.TrimSpace(defaultPostgresDSN) == "" {
		return nil, errors.New("default Session PostgreSQL DSN is required")
	}
	if secrets == nil {
		return nil, errors.New("Session secret resolver is required")
	}
	if profiles == nil {
		return nil, errors.New("Session backend profile resolver is required")
	}
	if summarizer == nil {
		return nil, errors.New("Session summarizer is required")
	}
	return &ManagedSessionProvider{
		defaultPostgresDSN: defaultPostgresDSN,
		secrets:            secrets,
		profiles:           profiles,
		summarizer:         summarizer,
		routes:             routes,
		services:           make(map[string]session.Service),
	}, nil
}

func (p *ManagedSessionProvider) Session(ctx context.Context, tenantConfig config.TenantConfig) (session.Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.routes != nil {
		route, ok, err := p.routes.ActiveSessionMigrationRoute(ctx, tenantConfig.TenantID, tenantConfig.AppCode)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant %q Session migration route: %w", tenantConfig.AppName(), err)
		}
		if ok {
			reader, err := p.SessionServiceFor(ctx, tenantConfig, route.Reader)
			if err != nil {
				return nil, err
			}
			writers := make([]session.Service, 0, len(route.Writers))
			for _, backend := range route.Writers {
				writer, err := p.SessionServiceFor(ctx, tenantConfig, backend)
				if err != nil {
					return nil, err
				}
				writers = append(writers, writer)
			}
			return newRoutedSessionService(reader, writers...), nil
		}
	}
	physical, err := p.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, platformstorage.BackendDomainSession, tenantConfig.Storage.Session.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant %q Session backend profile: %w", tenantConfig.AppName(), err)
	}
	backend := platformstorage.SessionBackendRef{Driver: physical.Driver, ConnectionRef: physical.ConnectionRef}.Normalize(SessionDriverPostgres)
	return p.SessionServiceFor(ctx, tenantConfig, backend)
}

// SessionServiceFor resolves one physical backend without applying migration
// routing. SessionMigrator uses this method for source/target backfill.
func (p *ManagedSessionProvider) SessionServiceFor(ctx context.Context, tenantConfig config.TenantConfig, backend platformstorage.SessionBackendRef) (session.Service, error) {
	backend = backend.Normalize(SessionDriverPostgres)
	driver := backend.Driver
	connection := ""
	if reference := backend.ConnectionRef; reference != "" {
		value, err := p.secrets.Resolve(ctx, reference)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant %q Session backend: %w", tenantConfig.AppName(), err)
		}
		connection = strings.TrimSpace(value)
	}
	if driver == SessionDriverPostgres && connection == "" {
		connection = p.defaultPostgresDSN
	}
	if driver == SessionDriverRedis && connection == "" {
		return nil, fmt.Errorf("tenant %q Redis Session backend requires a connection", tenantConfig.AppName())
	}
	if driver == SessionDriverInMemory && connection != "" {
		return nil, fmt.Errorf("tenant %q in-memory Session backend does not accept a connection", tenantConfig.AppName())
	}

	key := tenantConfig.AppName() + "\x00" + driver + "\x00" + backend.ConnectionRef
	p.mu.Lock()
	defer p.mu.Unlock()
	if service := p.services[key]; service != nil {
		return service, nil
	}

	var (
		service session.Service
		err     error
	)
	switch driver {
	case SessionDriverPostgres:
		service, err = sessionpostgres.NewService(
			sessionpostgres.WithPostgresClientDSN(connection),
			sessionpostgres.WithExtraOptions(platformstorage.FrameworkPostgresScope("session", tenantConfig.TenantID)),
			sessionpostgres.WithSkipDBInit(true),
			sessionpostgres.WithSummarizer(p.summarizer),
		)
	case SessionDriverRedis:
		service, err = redissession.NewService(
			redissession.WithRedisClientURL(normalizeRedisURL(connection)),
			redissession.WithCompatMode(redissession.CompatModeNone),
			redissession.WithKeyPrefix("agent:"+tenantConfig.TenantID+":"+tenantConfig.AppCode+":"),
			redissession.WithSummarizer(p.summarizer),
		)
	case SessionDriverInMemory:
		service = sessioninmemory.NewSessionService(sessioninmemory.WithSummarizer(p.summarizer))
	default:
		return nil, fmt.Errorf("unsupported Session backend %q", driver)
	}
	if err != nil {
		return nil, fmt.Errorf("construct tenant %q %s Session backend: %w", tenantConfig.AppName(), driver, err)
	}
	p.services[key] = service
	return service, nil
}

func (p *ManagedSessionProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	services := make([]session.Service, 0, len(p.services))
	for key, service := range p.services {
		services = append(services, service)
		delete(p.services, key)
	}
	p.mu.Unlock()
	var result error
	for _, service := range services {
		result = errors.Join(result, service.Close())
	}
	return result
}

func normalizeRedisURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "://") {
		return value
	}
	return "redis://" + value
}

var _ SessionProvider = (*ManagedSessionProvider)(nil)
