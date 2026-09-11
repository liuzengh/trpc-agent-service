package assembly

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	_ "github.com/mattn/go-sqlite3"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionclickhouse "trpc.group/trpc-go/trpc-agent-go/session/clickhouse"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionmongodb "trpc.group/trpc-go/trpc-agent-go/session/mongodb"
	sessionmysql "trpc.group/trpc-go/trpc-agent-go/session/mysql"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
	sessionsqlite "trpc.group/trpc-go/trpc-agent-go/session/sqlite"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

const (
	SessionDriverPostgres   = "postgres"
	SessionDriverRedis      = "redis"
	SessionDriverInMemory   = "inmemory"
	SessionDriverMySQL      = "mysql"
	SessionDriverSQLite     = "sqlite"
	SessionDriverMongoDB    = "mongodb"
	SessionDriverClickHouse = "clickhouse"
)

var ErrManagedProviderClosed = errors.New("managed framework provider is closed")

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
	repairs            platformstorage.SessionMigrationRepairStore
	health             *backendhealth.Registry

	mu       sync.Mutex
	services map[string]session.Service
	closed   bool
}

type SessionProviderOption func(*ManagedSessionProvider)

func WithSessionBackendHealth(registry *backendhealth.Registry) SessionProviderOption {
	return func(provider *ManagedSessionProvider) { provider.health = registry }
}

func WithSessionMigrationRepairs(store platformstorage.SessionMigrationRepairStore) SessionProviderOption {
	return func(provider *ManagedSessionProvider) { provider.repairs = store }
}

func NewManagedSessionProvider(defaultPostgresDSN string, secrets credential.SecretResolver, profiles platformstorage.BackendProfileResolver, summarizer sessionsummary.SessionSummarizer, routes platformstorage.SessionMigrationRouteSource, options ...SessionProviderOption) (*ManagedSessionProvider, error) {
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
	provider := &ManagedSessionProvider{
		defaultPostgresDSN: defaultPostgresDSN,
		secrets:            secrets,
		profiles:           profiles,
		summarizer:         summarizer,
		routes:             routes,
		services:           make(map[string]session.Service),
	}
	if repairs, ok := routes.(platformstorage.SessionMigrationRepairStore); ok {
		provider.repairs = repairs
	}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider, nil
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
			primary, err := p.SessionServiceFor(ctx, tenantConfig, route.PrimaryWriter)
			if err != nil {
				return nil, err
			}
			replicas := make([]session.Service, 0, len(route.ReplicaWriters))
			for _, backend := range route.ReplicaWriters {
				writer, err := p.SessionServiceFor(ctx, tenantConfig, backend)
				if err != nil {
					return nil, err
				}
				replicas = append(replicas, writer)
			}
			return newRoutedSessionService(reader, primary, replicas, route, p.repairs), nil
		}
	}
	physical, err := p.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, platformstorage.BackendDomainSession, tenantConfig.Storage.Session.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant %q Session backend profile: %w", tenantConfig.AppName(), err)
	}
	backend := platformstorage.SessionBackendRef{ProfileID: tenantConfig.Storage.Session.ProfileID, Driver: physical.Driver, ConnectionRef: physical.ConnectionRef}.Normalize(SessionDriverPostgres)
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
	if driver == SessionDriverInMemory && connection != "" {
		return nil, fmt.Errorf("tenant %q in-memory Session backend does not accept a connection", tenantConfig.AppName())
	}
	if driver != SessionDriverPostgres && driver != SessionDriverInMemory && connection == "" {
		return nil, fmt.Errorf("tenant %q %s Session backend requires a connection", tenantConfig.AppName(), driver)
	}

	profileKey := ""
	if p.health != nil {
		profileKey = backend.ProfileID
	}
	key := tenantConfig.AppName() + "\x00" + profileKey + "\x00" + driver + "\x00" + backend.ConnectionRef + "\x00" + backendConnectionFingerprint(connection)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrManagedProviderClosed
	}
	if service := p.services[key]; service != nil {
		return service, nil
	}

	var (
		service session.Service
		err     error
	)
	switch driver {
	case SessionDriverPostgres:
		postgresScope := platformstorage.FrameworkPostgresScope("session", tenantConfig.TenantID)
		if strings.TrimSpace(connection) == strings.TrimSpace(p.defaultPostgresDSN) {
			postgresScope = platformstorage.FrameworkTenantPostgresScope("session", tenantConfig.TenantID, tenantConfig.AppName())
		}
		service, err = sessionpostgres.NewService(
			sessionpostgres.WithPostgresClientDSN(connection),
			sessionpostgres.WithExtraOptions(postgresScope),
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
	case SessionDriverMySQL:
		service, err = sessionmysql.NewService(
			sessionmysql.WithMySQLClientDSN(connection),
			sessionmysql.WithSummarizer(p.summarizer),
		)
	case SessionDriverSQLite:
		database, openErr := sql.Open("sqlite3", connection)
		if openErr != nil {
			return nil, fmt.Errorf("open tenant %q SQLite Session backend: %w", tenantConfig.AppName(), openErr)
		}
		service, err = sessionsqlite.NewService(database, sessionsqlite.WithSummarizer(p.summarizer))
		if err != nil {
			_ = database.Close()
		}
	case SessionDriverMongoDB:
		service, err = sessionmongodb.NewService(
			sessionmongodb.WithMongoClientURI(connection),
			sessionmongodb.WithSummarizer(p.summarizer),
		)
	case SessionDriverClickHouse:
		service, err = sessionclickhouse.NewService(
			sessionclickhouse.WithClickHouseDSN(connection),
			sessionclickhouse.WithSummarizer(p.summarizer),
		)
	case SessionDriverInMemory:
		service = sessioninmemory.NewSessionService(sessioninmemory.WithSummarizer(p.summarizer))
	default:
		return nil, fmt.Errorf("unsupported Session backend %q", driver)
	}
	if err != nil {
		return nil, fmt.Errorf("construct tenant %q %s Session backend: %w", tenantConfig.AppName(), driver, err)
	}
	if p.health != nil && backend.ProfileID != "" && backendhealth.ShouldProtect(driver) {
		healthKey := sessionHealthKey(backend)
		if err := p.health.RegisterProbe(healthKey, func(probeCtx context.Context) error {
			_, probeErr := service.ListAppStates(probeCtx, tenantConfig.AppName())
			return probeErr
		}); err != nil {
			_ = service.Close()
			return nil, fmt.Errorf("register tenant %q Session backend probe: %w", tenantConfig.AppName(), err)
		}
		if err := p.health.Check(ctx, healthKey); err != nil {
			_ = service.Close()
			return nil, fmt.Errorf("probe tenant %q Session backend: %w", tenantConfig.AppName(), err)
		}
		service = observeSessionService(service, p.health, healthKey)
	}
	p.services[key] = service
	return service, nil
}

func (p *ManagedSessionProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
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
