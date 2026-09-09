package migrationruntime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/dataprojection"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var ErrSessionMigrationUnsupported = errors.New("session migration requires complete session-only state without native summaries or TTL")

type SessionCoordinator interface {
	WithRead(context.Context, string, datamigration.Domain, string, func(context.Context, string) error) error
	WithWrite(context.Context, string, datamigration.Domain, string, []string, func(context.Context, string) error) error
}

type SessionBackendOptions struct {
	MaxEvents   int
	MaxSessions int
	MaxBackends int
}

// SessionBackends owns undecorated official services. Migration reads and
// projections use these services instead of recursively entering the live route.
type SessionBackends struct {
	db        *sql.DB
	profiles  storage.BackendProfileResolver
	options   SessionBackendOptions
	mu        sync.Mutex
	backends  map[string]*sessionBackend
	closed    bool
	changed   *sync.Cond
	closeOnce sync.Once
	closeErr  error
}

func NewSessionBackends(db *sql.DB, profiles storage.BackendProfileResolver, options ...SessionBackendOptions) (*SessionBackends, error) {
	if db == nil || profiles == nil || len(options) > 1 {
		return nil, datamigration.ErrMigrationCapability
	}
	value := SessionBackendOptions{MaxEvents: 100000, MaxSessions: 100000, MaxBackends: 128}
	if len(options) == 1 {
		if options[0].MaxEvents != 0 {
			value.MaxEvents = options[0].MaxEvents
		}
		if options[0].MaxSessions != 0 {
			value.MaxSessions = options[0].MaxSessions
		}
		if options[0].MaxBackends != 0 {
			value.MaxBackends = options[0].MaxBackends
		}
	}
	if value.MaxEvents < 1 || value.MaxEvents > 100000 || value.MaxSessions < 1 || value.MaxSessions > 1000000 || value.MaxBackends < 1 || value.MaxBackends > 4096 {
		return nil, datamigration.ErrMigrationCapability
	}
	return &SessionBackends{db: db, profiles: profiles, options: value, backends: make(map[string]*sessionBackend)}, nil
}

func (r *SessionBackends) Resolve(ctx context.Context, tenantID string, domain datamigration.Domain, profileID string) (datamigration.LiveBackend, func(), error) {
	if r == nil || domain != datamigration.DomainSession || tenant.ValidateTenantID(tenantID) != nil {
		return nil, nil, datamigration.ErrMigrationCapability
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var encoded []byte
	if err := r.db.QueryRowContext(ctx, `SELECT config->'storage' FROM tenants WHERE id=$1 AND status <> 'deleted'`, tenantID).Scan(&encoded); err != nil {
		return nil, nil, sessionBackendError("load tenant storage")
	}
	var config tenant.StorageConfig
	if err := json.Unmarshal(encoded, &config); err != nil {
		return nil, nil, sessionBackendError("decode tenant storage")
	}
	if config.SessionConfig["session_ttl"] != "" {
		return nil, nil, ErrSessionMigrationUnsupported
	}
	backend, err := r.backend(ctx, tenantID, profileID, config)
	if err != nil {
		return nil, nil, err
	}
	return backend, r.releaseBackend(backend), nil
}

func (r *SessionBackends) backend(ctx context.Context, tenantID, profileID string, config tenant.StorageConfig) (*sessionBackend, error) {
	if tenant.ValidateTenantID(tenantID) != nil || profileID == "" {
		return nil, datamigration.ErrMigrationCapability
	}
	var profile storage.BackendProfile
	var err error
	if scoped, ok := r.profiles.(storage.TenantBackendProfileResolver); ok {
		profile, err = scoped.ResolveBackendProfileForTenant(tenantID, profileID)
	} else {
		profile, err = r.profiles.ResolveBackendProfile(profileID)
	}
	if err != nil {
		return nil, sessionBackendError("resolve profile")
	}
	if profile.Backend != "redis" && profile.Backend != "postgres" {
		return nil, datamigration.ErrMigrationCapability
	}
	config.SessionBackend, config.SessionProfile = profile.Backend, profileID
	encoded, err := json.Marshal(config.SessionConfig)
	if err != nil {
		return nil, datamigration.ErrMigrationCapability
	}
	cacheKey := tenantID + "\x00" + profileID + "\x00" + string(encoded)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.changed == nil {
		r.changed = sync.NewCond(&r.mu)
	}
	if r.closed {
		return nil, datamigration.ErrMigrationCapability
	}
	if existing := r.backends[cacheKey]; existing != nil {
		existing.refs++
		existing.lastUsed = time.Now()
		return existing, nil
	}
	if len(r.backends) >= r.options.MaxBackends {
		var idleKey string
		var idle *sessionBackend
		for key, candidate := range r.backends {
			if candidate.refs == 0 && (idle == nil || candidate.lastUsed.Before(idle.lastUsed)) {
				idleKey, idle = key, candidate
			}
		}
		if idle == nil {
			return nil, sessionBackendError("backend capacity reached")
		}
		delete(r.backends, idleKey)
		if err := idle.close(); err != nil {
			return nil, err
		}
	}
	service, err := storage.NewBackendFactoryWithProfiles(r.profiles).CreateSessionServiceForTenant(tenantID, &config)
	if err != nil {
		return nil, sessionBackendError("create official service")
	}
	backend := &sessionBackend{tenantID: tenantID, service: service, options: r.options}
	backend.backendName = profile.Backend
	switch profile.Backend {
	case "redis":
		opts, parseErr := redis.ParseURL(profile.ConnectionString)
		if parseErr != nil {
			_ = service.Close()
			return nil, sessionBackendError("create inventory connection")
		}
		backend.redis = redis.NewClient(opts)
		backend.identity = sessionStorageIdentity(tenantID, "redis", opts.Addr, fmt.Sprint(opts.DB), "default-prefix")
		if err := backend.redis.Ping(ctx).Err(); err != nil {
			_ = backend.close()
			return nil, sessionBackendError("probe inventory connection")
		}
		if info, infoErr := backend.redis.ClientInfo(ctx).Result(); infoErr == nil && info != nil && info.LAddr != "" {
			backend.identity = sessionStorageIdentity(tenantID, "redis", info.LAddr, fmt.Sprint(opts.DB), "default-prefix")
		}
	case "postgres":
		backend.postgres, err = sql.Open("postgres", profile.ConnectionString)
		if err != nil {
			_ = service.Close()
			return nil, sessionBackendError("create inventory connection")
		}
		backend.postgres.SetMaxOpenConns(2)
		if err := backend.postgres.PingContext(ctx); err != nil {
			_ = backend.close()
			return nil, sessionBackendError("probe inventory connection")
		}
		var server, database, schema string
		var port int
		if err := backend.postgres.QueryRowContext(ctx, `SELECT COALESCE(inet_server_addr()::text,'local-socket'),COALESCE(inet_server_port(),0),current_database(),current_schema()`).Scan(&server, &port, &database, &schema); err != nil {
			_ = backend.close()
			return nil, sessionBackendError("identify PostgreSQL storage")
		}
		backend.identity = sessionStorageIdentity(tenantID, "postgres", server, fmt.Sprint(port), database, schema, "session_states")
	}
	r.backends[cacheKey] = backend
	backend.refs, backend.lastUsed = 1, time.Now()
	return backend, nil
}

func (r *SessionBackends) releaseBackend(backend *sessionBackend) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			backend.refs--
			backend.lastUsed = time.Now()
			if r.changed != nil {
				r.changed.Broadcast()
			}
		})
	}
}

func (r *SessionBackends) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		if r.changed == nil {
			r.changed = sync.NewCond(&r.mu)
		}
		for {
			active := false
			for _, backend := range r.backends {
				if backend.refs > 0 {
					active = true
					break
				}
			}
			if !active {
				break
			}
			r.changed.Wait()
		}
		for _, backend := range r.backends {
			r.closeErr = errors.Join(r.closeErr, backend.close())
		}
	})
	return r.closeErr
}

type sessionBackend struct {
	tenantID    string
	backendName string
	identity    string
	service     session.Service
	redis       *redis.Client
	postgres    *sql.DB
	options     SessionBackendOptions
	refs        int
	lastUsed    time.Time
}

func (b *sessionBackend) MigrationInfo() datamigration.LiveBackendInfo {
	return datamigration.LiveBackendInfo{Backend: b.backendName, Identity: b.identity, Compatibility: "session/v1;shared-state=reject;native-summary=reject;ttl=disabled"}
}

func sessionStorageIdentity(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

func (b *sessionBackend) Read(ctx context.Context, recordKey string, version int64) (datamigration.Record, error) {
	key, err := parseSessionRecordKey(b.tenantID, recordKey)
	if err != nil {
		return datamigration.Record{}, err
	}
	value, err := b.service.GetSession(ctx, key, session.WithEventNum(b.options.MaxEvents))
	if err != nil {
		return datamigration.Record{}, sessionBackendError("read session")
	}
	// Capture has no journal version yet; the projection constructor requires one.
	payloadVersion := version
	if payloadVersion == 0 {
		payloadVersion = 1
	}
	var record datamigration.Record
	if value == nil {
		record, err = dataprojection.NewSessionTombstone(key, payloadVersion)
	} else {
		if err := requireSessionOnlyState(value.State); err != nil {
			return datamigration.Record{}, err
		}
		record, err = dataprojection.NewSessionRecord(ctx, b.service, key, payloadVersion, b.options.MaxEvents)
	}
	if err != nil {
		return datamigration.Record{}, err
	}
	record.Version = version
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func (b *sessionBackend) Apply(ctx context.Context, record datamigration.Record) error {
	if _, err := parseSessionRecordKey(b.tenantID, record.Key); err != nil {
		return err
	}
	projector, err := dataprojection.NewSessionProjector(func(_ context.Context, tenantID, appName string) (session.Service, error) {
		if tenantID != b.tenantID || validateSessionApp(b.tenantID, appName) != nil {
			return nil, datamigration.ErrInvalidRecord
		}
		return b.service, nil
	}, b.options.MaxEvents)
	if err != nil {
		return err
	}
	return projector.Apply(ctx, b.tenantID, record)
}

func (b *sessionBackend) close() error {
	var result error
	if b.service != nil {
		result = errors.Join(result, b.service.Close())
	}
	if b.redis != nil {
		result = errors.Join(result, b.redis.Close())
	}
	if b.postgres != nil {
		result = errors.Join(result, b.postgres.Close())
	}
	if result != nil {
		return sessionBackendError("close backend")
	}
	return nil
}

func sessionRecordKey(key session.Key) (string, error) {
	record, err := dataprojection.NewSessionTombstone(key, 1)
	return record.Key, err
}

func parseSessionRecordKey(tenantID, value string) (session.Key, error) {
	const prefix = "session/v1/"
	if !strings.HasPrefix(value, prefix) {
		return session.Key{}, datamigration.ErrInvalidRecord
	}
	encoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil {
		return session.Key{}, datamigration.ErrInvalidRecord
	}
	var identity struct {
		AppName   string `json:"app_name"`
		UserID    string `json:"user_id"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(encoded, &identity); err != nil {
		return session.Key{}, datamigration.ErrInvalidRecord
	}
	key := session.Key{AppName: identity.AppName, UserID: identity.UserID, SessionID: identity.SessionID}
	canonical, err := sessionRecordKey(key)
	if err != nil || canonical != value || validateSessionApp(tenantID, key.AppName) != nil {
		return session.Key{}, datamigration.ErrInvalidRecord
	}
	return key, nil
}

func validateSessionApp(tenantID, appName string) error {
	prefix := sessionAppPrefix(tenantID)
	if prefix == "" || !strings.HasPrefix(appName, prefix) || tenant.ValidateAgentAppName(strings.TrimPrefix(appName, prefix)) != nil {
		return datamigration.ErrInvalidRecord
	}
	return nil
}

func sessionAppPrefix(tenantID string) string {
	value, err := storage.TenantScopedAppName(&tenant.Tenant{ID: tenantID}, "migration")
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(value, "migration")
}

func requireSessionOnlyState(state session.StateMap) error {
	for key := range state {
		if strings.HasPrefix(key, session.StateAppPrefix) || strings.HasPrefix(key, session.StateUserPrefix) {
			return ErrSessionMigrationUnsupported
		}
	}
	return nil
}

func sessionBackendError(operation string) error {
	return fmt.Errorf("%w: session %s", datamigration.ErrMigrationCapability, operation)
}
