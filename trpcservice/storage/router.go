package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgebase"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// SecretResolver resolves a SecretRef without exposing its value to callers.
type SecretResolver func(tenant.SecretRef) (string, error)
type ScopedSecretResolver func(context.Context, string, string, tenant.SecretRef) (string, error)

// PostgresTarget is a resolved route. DSN is intentionally never formatted.
type PostgresTarget struct {
	DSN string
	DB  *sql.DB
}

// ArtifactForRoute constructs a migration-owned artifact service.
func (router *Router) ArtifactForRoute(ctx context.Context, route tenant.BackendConfig) (artifact.Service, error) {
	return router.artifactService(ctx, "", "", route)
}

// ArtifactForScope constructs a migration-owned artifact service while
// enforcing the immutable tenant/application secret namespace.
func (router *Router) ArtifactForScope(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (artifact.Service, error) {
	return router.artifactService(ctx, tenantID, appID, route)
}

// MemoryForRoute constructs a migration-owned tenant/app memory service.
func (router *Router) MemoryForRoute(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (memory.Service, error) {
	return router.memoryService(ctx, tenantID, appID, route)
}

// SessionForRoute constructs a migration-owned tenant/app Session service.
// The route is copied without its mirror so a backfill never recursively
// dual-writes through the runtime migration configuration.
func (router *Router) SessionForRoute(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (session.Service, error) {
	route = route.Clone()
	route.MigrationTarget = nil
	return router.sessionService(ctx, tenantID, appID, route)
}

// MigrationLedgerDB returns the platform database used for idempotency records.
func (router *Router) MigrationLedgerDB() *sql.DB {
	if router == nil {
		return nil
	}
	return router.defaultTarget.DB
}

// Router resolves each storage domain independently and caches external pools.
// A zero credential selects the platform PostgreSQL pool; a credential resolves
// to a DSN for a separately migrated PostgreSQL cluster.
type Router struct {
	defaultTarget PostgresTarget
	resolve       SecretResolver
	resolveScoped ScopedSecretResolver
	mu            sync.Mutex
	targets       map[[32]byte]PostgresTarget
	closed        bool
	observe       OperationObserver
}

// SetScopedResolver installs the production tenant/application authorization
// boundary before any Runtime Bundle or migration worker is started.
func (router *Router) SetScopedResolver(resolve ScopedSecretResolver) {
	if router != nil {
		router.resolveScoped = resolve
	}
}

// SetOperationObserver installs the process telemetry sink before Runtime
// Bundles are created. Observations never contain payloads or external IDs.
func (router *Router) SetOperationObserver(observer OperationObserver) {
	if router == nil {
		return
	}
	router.mu.Lock()
	router.observe = observer
	router.mu.Unlock()
}

// NewRouter creates a production router with one mandatory default target.
func NewRouter(defaultDSN string, defaultDB *sql.DB, resolver SecretResolver) (*Router, error) {
	if defaultDSN == "" || defaultDB == nil || resolver == nil {
		return nil, errors.New("storage: default PostgreSQL target and secret resolver are required")
	}
	return &Router{defaultTarget: PostgresTarget{DSN: defaultDSN, DB: defaultDB}, resolve: resolver, targets: make(map[[32]byte]PostgresTarget)}, nil
}

// Services builds the immutable Runtime Bundle services for one route profile.
func (router *Router) Services(ctx context.Context, profile tenant.StorageProfile) (*Services, error) {
	return router.services(ctx, "", "", profile, tenant.KnowledgePolicy{})
}

// ServicesForApp builds all routed services using trusted tenant/app scope.
func (router *Router) ServicesForApp(ctx context.Context, tenantID string, app tenant.AgentApp) (*Services, error) {
	if tenantID == "" || app.ID == "" {
		return nil, errors.New("storage: tenant and app scope are required")
	}
	return router.services(ctx, tenantID, app.ID, app.Storage, app.Knowledge)
}

// KnowledgeForApp resolves only the scoped Knowledge service for Admin ingest
// and search operations.
func (router *Router) KnowledgeForApp(ctx context.Context, tenantID string, app tenant.AgentApp) (*knowledgebase.Service, error) {
	if tenantID == "" || app.ID == "" || !app.Knowledge.Enabled {
		return nil, errors.New("storage: enabled tenant/app knowledge configuration is required")
	}
	return router.knowledgeForRoutes(ctx, tenantID, app.ID, app.Storage.Knowledge, app.Knowledge)
}

// KnowledgeForRoute constructs a migration-owned target index from an
// immutable config version.
func (router *Router) KnowledgeForRoute(ctx context.Context, tenantID, appID string, route tenant.BackendConfig, policy tenant.KnowledgePolicy) (*knowledgebase.Service, error) {
	return router.knowledgeService(ctx, tenantID, appID, route, policy)
}

func (router *Router) knowledgeForRoutes(ctx context.Context, tenantID, appID string, route tenant.BackendConfig, policy tenant.KnowledgePolicy) (*knowledgebase.Service, error) {
	primaryRoute := route.Clone()
	primaryRoute.MigrationTarget = nil
	primary, err := router.knowledgeService(ctx, tenantID, appID, primaryRoute, policy)
	if err != nil || primary == nil {
		return primary, err
	}
	var mirror *knowledgebase.Service
	if route.MigrationTarget != nil {
		mirror, err = router.knowledgeService(ctx, tenantID, appID, *route.MigrationTarget, policy)
		if err != nil {
			_ = primary.Close()
			return nil, err
		}
	}
	return primary.WithMigration(router.defaultTarget.DB, mirror), nil
}

func (router *Router) services(ctx context.Context, tenantID, appID string, profile tenant.StorageProfile, knowledgePolicy tenant.KnowledgePolicy) (*Services, error) {
	if router == nil || ctx == nil {
		return nil, errors.New("storage: router and context are required")
	}
	if err := ValidateRoutedProfile(profile); err != nil {
		return nil, err
	}
	artifactService, err := router.artifactService(ctx, tenantID, appID, profile.Artifact)
	if err != nil {
		return nil, fmt.Errorf("storage: resolve artifact backend: %w", err)
	}
	sessionService, err := router.sessionService(ctx, tenantID, appID, profile.Session)
	if err != nil {
		if closer, ok := artifactService.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("storage: resolve session backend: %w", err)
	}
	memoryService, err := router.memoryService(ctx, tenantID, appID, profile.Memory)
	if err != nil {
		_ = sessionService.Close()
		if closer, ok := artifactService.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("storage: resolve memory backend: %w", err)
	}
	services := &Services{Session: sessionService, Memory: memoryService, Artifact: artifactService}
	services.Knowledge, err = router.knowledgeForRoutes(ctx, tenantID, appID, profile.Knowledge, knowledgePolicy)
	if err != nil {
		_ = services.Close()
		return nil, err
	}
	if profile.Session.MigrationTarget != nil {
		shadow, resolveErr := router.SessionForRoute(ctx, tenantID, appID, *profile.Session.MigrationTarget)
		if resolveErr != nil {
			_ = services.Close()
			return nil, fmt.Errorf("storage: resolve session migration target: %w", resolveErr)
		}
		services.Session = &MirroredSession{Primary: services.Session, Target: shadow}
	}
	if profile.Memory.MigrationTarget != nil {
		shadow, resolveErr := router.memoryService(ctx, tenantID, appID, *profile.Memory.MigrationTarget)
		if resolveErr != nil {
			_ = services.Close()
			return nil, fmt.Errorf("storage: resolve memory migration target: %w", resolveErr)
		}
		services.Memory = &MirroredMemory{Primary: services.Memory, Target: shadow}
	}
	if profile.Artifact.MigrationTarget != nil {
		target, resolveErr := router.artifactService(ctx, tenantID, appID, *profile.Artifact.MigrationTarget)
		if resolveErr != nil {
			_ = services.Close()
			return nil, fmt.Errorf("storage: resolve artifact migration target: %w", resolveErr)
		}
		services.Artifact = &MirroredArtifact{Primary: services.Artifact, Target: target}
	}
	router.mu.Lock()
	observer := router.observe
	router.mu.Unlock()
	if observer != nil {
		services.Session = &ObservedSession{Delegate: services.Session, TenantID: tenantID, AppID: appID, Backend: string(profile.Session.Type), Observe: observer}
		services.Memory = &ObservedMemory{Delegate: services.Memory, TenantID: tenantID, AppID: appID, Backend: string(profile.Memory.Type), Observe: observer}
	}
	return services, nil
}

func (router *Router) sessionService(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (session.Service, error) {
	switch route.Type {
	case tenant.BackendPostgres:
		target, err := router.ResolveForScope(ctx, tenantID, appID, route)
		if err != nil {
			return nil, err
		}
		return newPostgresSession(target.DSN)
	case tenant.BackendRedis:
		rawURL, keyPrefix, err := router.resolveRedisSessionRoute(ctx, tenantID, appID, route)
		if err != nil {
			return nil, err
		}
		return newRedisSession(rawURL, keyPrefix)
	default:
		return nil, fmt.Errorf("storage: session backend %q is unavailable", route.Type)
	}
}

func (router *Router) resolveRedisSessionRoute(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (string, string, error) {
	if tenantID == "" || appID == "" || strings.TrimSpace(route.Namespace) == "" || route.MigrationTarget != nil || ((route.Endpoint == "") == route.Credential.IsZero()) {
		return "", "", errors.New("storage: Redis session scope, namespace, and direct route are required")
	}
	rawURL := strings.TrimSpace(route.Endpoint)
	if !route.Credential.IsZero() {
		resolved, err := router.resolveRef(ctx, tenantID, appID, route.Credential)
		if err != nil || strings.TrimSpace(resolved) == "" {
			return "", "", errors.New("storage: resolve Redis session credential failed")
		}
		rawURL = strings.TrimSpace(resolved)
	}
	if !validRedisURL(rawURL) {
		return "", "", errors.New("storage: Redis session URL is invalid")
	}
	return rawURL, physicalNamespace(route.Namespace, tenantID, appID), nil
}

func validRedisURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "redis" || parsed.Scheme == "rediss") && parsed.Host != "" && parsed.Fragment == ""
}

func validRedisEndpoint(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && validRedisURL(value) && parsed.User == nil && parsed.RawQuery == ""
}

// Resolve returns the concrete PostgreSQL target for a backend route.
func (router *Router) Resolve(ctx context.Context, route tenant.BackendConfig) (PostgresTarget, error) {
	return router.ResolveForScope(ctx, "", "", route)
}

// ResolveForScope returns a concrete PostgreSQL target after tenant/app secret
// authorization. Credential-free routes still use the platform pool.
func (router *Router) ResolveForScope(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (PostgresTarget, error) {
	if router == nil || ctx == nil || route.Type != tenant.BackendPostgres {
		return PostgresTarget{}, errors.New("storage: only routed PostgreSQL backends are currently available")
	}
	if route.Credential.IsZero() {
		return router.defaultTarget, nil
	}
	dsn, err := router.resolveRef(ctx, tenantID, appID, route.Credential)
	if err != nil {
		return PostgresTarget{}, errors.New("storage: resolve PostgreSQL credential failed")
	}
	// Include only a one-way digest of the resolved DSN in the cache identity.
	// Rotating a mounted/file secret creates a new pool without logging or
	// retaining the value as a map key visible to diagnostics.
	identity := sha256.Sum256([]byte(string(route.Credential.Provider) + "\x00" + route.Credential.Key + "\x00" + dsn))
	router.mu.Lock()
	if router.closed {
		router.mu.Unlock()
		return PostgresTarget{}, errors.New("storage: router is closed")
	}
	if target, ok := router.targets[identity]; ok {
		router.mu.Unlock()
		return target, nil
	}
	router.mu.Unlock()
	db, err := backend.OpenPostgres(ctx, dsn)
	if err != nil {
		return PostgresTarget{}, errors.New("storage: connect routed PostgreSQL backend failed")
	}
	target := PostgresTarget{DSN: dsn, DB: db}
	router.mu.Lock()
	if router.closed {
		router.mu.Unlock()
		_ = db.Close()
		return PostgresTarget{}, errors.New("storage: router is closed")
	}
	if existing, ok := router.targets[identity]; ok {
		router.mu.Unlock()
		_ = db.Close()
		return existing, nil
	}
	router.targets[identity] = target
	router.mu.Unlock()
	return target, nil
}

func (router *Router) resolveRef(ctx context.Context, tenantID, appID string, ref tenant.SecretRef) (string, error) {
	if router.resolveScoped != nil && tenantID != "" && appID != "" {
		return router.resolveScoped(ctx, tenantID, appID, ref)
	}
	return router.resolve(ref)
}

// Close releases only pools opened for external routes; the caller owns defaultDB.
func (router *Router) Close() error {
	if router == nil {
		return nil
	}
	router.mu.Lock()
	if router.closed {
		router.mu.Unlock()
		return nil
	}
	router.closed = true
	targets := router.targets
	router.targets = nil
	router.mu.Unlock()
	var result error
	for _, target := range targets {
		result = errors.Join(result, target.DB.Close())
	}
	return result
}

// ValidateRoutedProfile is the production route gate used by Admin publish.
func ValidateRoutedProfile(profile tenant.StorageProfile) error {
	for name, route := range map[string]tenant.BackendConfig{"session": profile.Session, "summary": profile.Summary} {
		if route.Type != tenant.BackendPostgres && route.Type != tenant.BackendRedis {
			return fmt.Errorf("storage: %s backend must be postgres or redis, got %q", name, route.Type)
		}
		if route.Type == tenant.BackendRedis && (strings.TrimSpace(route.Namespace) == "" || ((route.Endpoint == "") == route.Credential.IsZero()) || (route.Endpoint != "" && !validRedisEndpoint(route.Endpoint))) {
			return fmt.Errorf("storage: %s Redis backend requires namespace and a valid endpoint or credential", name)
		}
		if route.MigrationTarget != nil {
			target := *route.MigrationTarget
			primary := route.Clone()
			primary.MigrationTarget = nil
			if sameRoute(primary, target) {
				return fmt.Errorf("storage: %s migration target must differ from the primary route", name)
			}
			if target.Type != tenant.BackendPostgres && target.Type != tenant.BackendRedis {
				return fmt.Errorf("storage: %s migration target must be postgres or redis, got %q", name, target.Type)
			}
			if target.Type == tenant.BackendRedis && (strings.TrimSpace(target.Namespace) == "" || ((target.Endpoint == "") == target.Credential.IsZero()) || (target.Endpoint != "" && !validRedisEndpoint(target.Endpoint))) {
				return fmt.Errorf("storage: %s Redis migration target requires namespace and a valid endpoint or credential", name)
			}
		}
	}
	if profile.Audit.Type != tenant.BackendPostgres {
		return fmt.Errorf("storage: audit primary backend must be postgres, got %q", profile.Audit.Type)
	}
	if profile.Audit.MigrationTarget != nil {
		target := *profile.Audit.MigrationTarget
		if target.Type != tenant.BackendExternal || !validExternalEndpoint(target.Endpoint) || target.Credential.IsZero() {
			return errors.New("storage: audit archive target must be external with HTTPS endpoint and credential")
		}
	}
	if profile.Memory.Type != tenant.BackendPostgres && profile.Memory.Type != tenant.BackendExternal {
		return fmt.Errorf("storage: memory backend must be postgres or external, got %q", profile.Memory.Type)
	}
	if profile.Memory.MigrationTarget != nil && profile.Memory.MigrationTarget.Type != tenant.BackendPostgres && profile.Memory.MigrationTarget.Type != tenant.BackendExternal {
		return errors.New("storage: memory migration target must be postgres or external")
	}
	if profile.Memory.MigrationTarget != nil && profile.Memory.Type != tenant.BackendPostgres {
		return errors.New("storage: external memory reverse backfill is not supported")
	}
	for _, route := range []tenant.BackendConfig{profile.Memory, func() tenant.BackendConfig {
		if profile.Memory.MigrationTarget != nil {
			return *profile.Memory.MigrationTarget
		}
		return tenant.BackendConfig{}
	}()} {
		if route.Type == tenant.BackendExternal && (!validExternalEndpoint(route.Endpoint) || route.Credential.IsZero()) {
			return errors.New("storage: external memory HTTPS endpoint and credential are required")
		}
	}
	if profile.Artifact.Type != tenant.BackendPostgres && profile.Artifact.Type != tenant.BackendS3 {
		return fmt.Errorf("storage: artifact backend must be postgres or s3, got %q", profile.Artifact.Type)
	}
	if profile.Artifact.MigrationTarget != nil && profile.Artifact.MigrationTarget.Type != tenant.BackendPostgres && profile.Artifact.MigrationTarget.Type != tenant.BackendS3 {
		return fmt.Errorf("storage: artifact migration target must be postgres or s3, got %q", profile.Artifact.MigrationTarget.Type)
	}
	if profile.Knowledge.Type != tenant.BackendPostgres && profile.Knowledge.Type != tenant.BackendQdrant {
		return fmt.Errorf("storage: knowledge backend must be postgres or qdrant, got %q", profile.Knowledge.Type)
	}
	if profile.Knowledge.MigrationTarget != nil && profile.Knowledge.MigrationTarget.Type != tenant.BackendPostgres && profile.Knowledge.MigrationTarget.Type != tenant.BackendQdrant {
		return errors.New("storage: knowledge migration target must be postgres or qdrant")
	}
	if !sameRoute(profile.Session, profile.Summary) {
		return errors.New("storage: session and summary routes must match")
	}
	if !profile.Audit.Credential.IsZero() {
		return errors.New("storage: platform audit primary must not declare an external credential")
	}
	return nil
}

// Preflight resolves every active runtime route and verifies the target schema
// before a configuration may become runnable.
func (router *Router) Preflight(ctx context.Context, profile tenant.StorageProfile) error {
	return router.preflight(ctx, "", "", profile)
}

func (router *Router) preflight(ctx context.Context, tenantID, appID string, profile tenant.StorageProfile) error {
	if err := ValidateRoutedProfile(profile); err != nil {
		return err
	}
	if err := router.preflightSessionRoute(ctx, tenantID, appID, "session", profile.Session); err != nil {
		return err
	}
	if profile.Session.MigrationTarget != nil {
		if err := router.preflightSessionRoute(ctx, tenantID, appID, "session migration target", *profile.Session.MigrationTarget); err != nil {
			return err
		}
	}
	routes := []struct {
		name   string
		route  tenant.BackendConfig
		tables []string
	}{}
	if profile.Memory.Type == tenant.BackendPostgres {
		routes = append(routes, struct {
			name   string
			route  tenant.BackendConfig
			tables []string
		}{"memory", profile.Memory, []string{"runtime_memories"}})
	}
	if profile.Artifact.Type == tenant.BackendPostgres {
		artifactRoute := profile.Artifact.Clone()
		if artifactRoute.MigrationTarget != nil && artifactRoute.MigrationTarget.Type == tenant.BackendS3 {
			artifactRoute.MigrationTarget = nil
		}
		routes = append(routes, struct {
			name   string
			route  tenant.BackendConfig
			tables []string
		}{"artifact", artifactRoute, []string{"runtime_artifacts"}})
	} else if profile.Artifact.MigrationTarget != nil && profile.Artifact.MigrationTarget.Type == tenant.BackendPostgres {
		routes = append(routes, struct {
			name   string
			route  tenant.BackendConfig
			tables []string
		}{"artifact migration target", *profile.Artifact.MigrationTarget, []string{"runtime_artifacts", "storage_migration_items"}})
	}
	for _, entry := range routes {
		target, err := router.ResolveForScope(ctx, tenantID, appID, entry.route)
		if err != nil {
			return fmt.Errorf("storage: %s route preflight failed", entry.name)
		}
		if err := requireTables(ctx, target.DB, entry.tables); err != nil {
			return fmt.Errorf("storage: %s route schema is unavailable", entry.name)
		}
		if entry.route.MigrationTarget != nil {
			shadow, err := router.ResolveForScope(ctx, tenantID, appID, *entry.route.MigrationTarget)
			if err != nil {
				return fmt.Errorf("storage: %s migration target preflight failed", entry.name)
			}
			tables := append(append([]string(nil), entry.tables...), "storage_migration_items")
			if err := requireTables(ctx, shadow.DB, tables); err != nil {
				return fmt.Errorf("storage: %s migration target schema is unavailable", entry.name)
			}
		}
	}
	if err := requireTables(ctx, router.defaultTarget.DB, []string{"runtime_artifact_catalog", "runtime_knowledge_documents", "session_heads", "storage_migration_items"}); err != nil {
		return errors.New("storage: migration catalog schema is unavailable")
	}
	return nil
}

func (router *Router) preflightSessionRoute(ctx context.Context, tenantID, appID, name string, route tenant.BackendConfig) error {
	route = route.Clone()
	route.MigrationTarget = nil
	if route.Type == tenant.BackendPostgres {
		target, err := router.ResolveForScope(ctx, tenantID, appID, route)
		if err != nil || requireTables(ctx, target.DB, []string{"runtime_session_states", "runtime_session_events", "runtime_session_track_events", "runtime_session_summaries", "runtime_app_states", "runtime_user_states"}) != nil {
			return fmt.Errorf("storage: %s route schema is unavailable", name)
		}
		return nil
	}
	if tenantID == "" {
		tenantID = "preflight"
	}
	if appID == "" {
		appID = "preflight"
	}
	service, err := router.sessionService(ctx, tenantID, appID, route)
	if err != nil {
		return fmt.Errorf("storage: %s route preflight failed", name)
	}
	_, probeErr := service.GetSession(ctx, session.Key{AppName: "tenant/preflight/app/preflight", UserID: "preflight", SessionID: "preflight"})
	closeErr := service.Close()
	if probeErr != nil || closeErr != nil {
		return fmt.Errorf("storage: %s route is unreachable", name)
	}
	return nil
}

// PreflightApp verifies external Artifact/Knowledge clients in addition to the
// shared SQL schemas before a published app can receive new work.
func (router *Router) PreflightApp(ctx context.Context, tenantID string, app tenant.AgentApp) error {
	if err := router.preflight(ctx, tenantID, app.ID, app.Storage); err != nil {
		return err
	}
	if app.Storage.Audit.MigrationTarget != nil && app.Storage.Audit.MigrationTarget.Type == tenant.BackendExternal {
		credential, err := router.resolveRef(ctx, tenantID, app.ID, app.Storage.Audit.MigrationTarget.Credential)
		if err != nil || credential == "" {
			return errors.New("storage: audit archive credential preflight failed")
		}
	}
	appName, _ := tenant.CanonicalAppName(tenantID, app.ID)
	for _, route := range []tenant.BackendConfig{app.Storage.Memory, func() tenant.BackendConfig {
		if app.Storage.Memory.MigrationTarget != nil {
			return *app.Storage.Memory.MigrationTarget
		}
		return tenant.BackendConfig{}
	}()} {
		if route.Type != tenant.BackendExternal {
			continue
		}
		service, err := router.memoryService(ctx, tenantID, app.ID, route)
		if err != nil {
			return errors.New("storage: external memory route preflight failed")
		}
		_, err = service.ReadMemories(ctx, memory.UserKey{AppName: appName, UserID: "preflight"}, 1)
		_ = service.Close()
		if err != nil {
			return errors.New("storage: external memory route is unreachable")
		}
	}
	artifactService, err := router.artifactService(ctx, tenantID, app.ID, app.Storage.Artifact)
	if err != nil {
		return errors.New("storage: artifact route preflight failed")
	}
	if app.Storage.Artifact.Type == tenant.BackendS3 {
		if _, err := artifactService.ListArtifactKeys(ctx, artifact.SessionInfo{AppName: appName, UserID: "preflight", SessionID: "preflight"}); err != nil {
			if closer, ok := artifactService.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return errors.New("storage: S3 artifact route is unreachable")
		}
	}
	if closer, ok := artifactService.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	if app.Storage.Artifact.MigrationTarget != nil && app.Storage.Artifact.MigrationTarget.Type == tenant.BackendS3 {
		target, err := router.artifactService(ctx, tenantID, app.ID, *app.Storage.Artifact.MigrationTarget)
		if err != nil {
			return errors.New("storage: artifact migration target preflight failed")
		}
		if _, err := target.ListArtifactKeys(ctx, artifact.SessionInfo{AppName: appName, UserID: "preflight", SessionID: "preflight"}); err != nil {
			if closer, ok := target.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return errors.New("storage: S3 artifact migration target is unreachable")
		}
		if closer, ok := target.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	knowledgeService, err := router.knowledgeService(ctx, tenantID, app.ID, app.Storage.Knowledge, app.Knowledge)
	if err != nil {
		return errors.New("storage: knowledge route preflight failed")
	}
	if knowledgeService != nil {
		_ = knowledgeService.Close()
	}
	if app.Storage.Knowledge.MigrationTarget != nil {
		target, err := router.knowledgeService(ctx, tenantID, app.ID, *app.Storage.Knowledge.MigrationTarget, app.Knowledge)
		if err != nil {
			return errors.New("storage: knowledge migration target preflight failed")
		}
		if target != nil {
			_ = target.Close()
		}
	}
	return nil
}

func requireTables(ctx context.Context, db *sql.DB, tables []string) error {
	for _, table := range tables {
		var name sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)`, table).Scan(&name); err != nil || !name.Valid {
			return errors.New("storage: required table is missing")
		}
	}
	return nil
}

func sameRoute(left, right tenant.BackendConfig) bool {
	if left.Type != right.Type || left.Endpoint != right.Endpoint || left.Credential != right.Credential || left.Namespace != right.Namespace {
		return false
	}
	if left.MigrationTarget == nil || right.MigrationTarget == nil {
		return left.MigrationTarget == nil && right.MigrationTarget == nil
	}
	return sameRoute(*left.MigrationTarget, *right.MigrationTarget)
}
