package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"golang.org/x/sync/singleflight"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	postgressession "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

const sessionResourceType = "session"

type SessionRouter struct {
	repository controlplane.Repository
	secrets    secret.Store
	startup    session.Service
	summarizer summary.SessionSummarizer
	mu         sync.RWMutex
	closed     bool
	services   map[string]session.Service
	group      singleflight.Group
}

type SessionMigrationItem struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
}

type SessionMigrationVerification struct {
	MigrationID    string `json:"migration_id"`
	UserID         string `json:"user_id"`
	SessionID      string `json:"session_id"`
	SourceEvents   int    `json:"source_events"`
	TargetEvents   int    `json:"target_events"`
	StateMatched   bool   `json:"state_matched"`
	SummaryMatched bool   `json:"summary_matched"`
	Passed         bool   `json:"passed"`
}

func NewSessionRouter(
	repository controlplane.Repository,
	secretStore secret.Store,
	startup session.Service,
	summarizer summary.SessionSummarizer,
) (*SessionRouter, error) {
	if repository == nil || secretStore == nil || startup == nil {
		return nil, errors.New("session router dependencies are required")
	}
	return &SessionRouter{
		repository: repository, secrets: secretStore, startup: startup,
		summarizer: summarizer, services: make(map[string]session.Service),
	}, nil
}

func (r *SessionRouter) CreateSession(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
	opts ...session.Option,
) (*session.Session, error) {
	ctx, span := startStorageSpan(ctx, "session.create", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.CreateSession(ctx, key, state, opts...)
}

func (r *SessionRouter) GetSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) (*session.Session, error) {
	ctx, span := startStorageSpan(ctx, "session.get", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.GetSession(ctx, key, opts...)
}

func (r *SessionRouter) ListSessions(
	ctx context.Context,
	key session.UserKey,
	opts ...session.Option,
) ([]*session.Session, error) {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.ListSessions(ctx, key, opts...)
}

func (r *SessionRouter) DeleteSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) error {
	ctx, span := startStorageSpan(ctx, "session.delete", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.DeleteSession(ctx, key, opts...)
}

func (r *SessionRouter) UpdateAppState(
	ctx context.Context,
	appName string,
	state session.StateMap,
) error {
	service, err := r.serviceFor(ctx, appName)
	if err != nil {
		return err
	}
	return service.UpdateAppState(ctx, appName, state)
}

func (r *SessionRouter) DeleteAppState(ctx context.Context, appName string, key string) error {
	service, err := r.serviceFor(ctx, appName)
	if err != nil {
		return err
	}
	return service.DeleteAppState(ctx, appName, key)
}

func (r *SessionRouter) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	service, err := r.serviceFor(ctx, appName)
	if err != nil {
		return nil, err
	}
	return service.ListAppStates(ctx, appName)
}

func (r *SessionRouter) UpdateUserState(
	ctx context.Context,
	key session.UserKey,
	state session.StateMap,
) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.UpdateUserState(ctx, key, state)
}

func (r *SessionRouter) ListUserStates(
	ctx context.Context,
	key session.UserKey,
) (session.StateMap, error) {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.ListUserStates(ctx, key)
}

func (r *SessionRouter) DeleteUserState(
	ctx context.Context,
	key session.UserKey,
	stateKey string,
) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.DeleteUserState(ctx, key, stateKey)
}

func (r *SessionRouter) UpdateSessionState(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
) error {
	ctx, span := startStorageSpan(ctx, "session.state.update", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.UpdateSessionState(ctx, key, state)
}

func (r *SessionRouter) AppendEvent(
	ctx context.Context,
	sess *session.Session,
	item *event.Event,
	opts ...session.Option,
) error {
	if sess == nil {
		return session.ErrNilSession
	}
	ctx, span := startStorageSpan(ctx, "session.event.append", sess.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return service.AppendEvent(ctx, sess, item, opts...)
}

func (r *SessionRouter) CreateSessionSummary(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	if sess == nil {
		return session.ErrNilSession
	}
	ctx, span := startStorageSpan(ctx, "session.summary.create", sess.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return service.CreateSessionSummary(ctx, sess, filterKey, force)
}

func (r *SessionRouter) EnqueueSummaryJob(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	if sess == nil {
		return session.ErrNilSession
	}
	service, err := r.serviceFor(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return service.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

func (r *SessionRouter) GetSessionSummaryText(
	ctx context.Context,
	sess *session.Session,
	opts ...session.SummaryOption,
) (string, bool) {
	if sess == nil {
		return "", false
	}
	service, err := r.serviceFor(ctx, sess.AppName)
	if err != nil {
		return "", false
	}
	return service.GetSessionSummaryText(ctx, sess, opts...)
}

func (r *SessionRouter) Ready(ctx context.Context) error {
	if r == nil || r.repository == nil {
		return errors.New("session router is not initialized")
	}
	if err := r.repository.Ready(ctx); err != nil {
		return err
	}
	_, err := r.startup.ListAppStates(ctx, readinessAppName)
	return err
}

func (r *SessionRouter) BackfillSession(
	ctx context.Context,
	tenantID string,
	migrationID string,
	item SessionMigrationItem,
) (SessionMigrationVerification, error) {
	migration, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	key := session.Key{
		AppName: "t/" + migration.TenantID + "/a/" + migration.AppID,
		UserID:  item.UserID, SessionID: item.SessionID,
	}
	sourceSession, err := source.GetSession(ctx, key)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	_ = target.DeleteSession(ctx, key)
	targetSession, err := target.CreateSession(ctx, key, cloneStateMap(sourceSession.State))
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	for index := range sourceSession.Events {
		if err := target.AppendEvent(ctx, targetSession, sourceSession.Events[index].Clone()); err != nil {
			return SessionMigrationVerification{}, fmt.Errorf("backfill session event %d: %w", index, err)
		}
	}
	appState, err := source.ListAppStates(ctx, key.AppName)
	if err == nil && len(appState) > 0 {
		if err := target.UpdateAppState(ctx, key.AppName, cloneStateMap(appState)); err != nil {
			return SessionMigrationVerification{}, err
		}
	}
	userKey := session.UserKey{AppName: key.AppName, UserID: key.UserID}
	userState, err := source.ListUserStates(ctx, userKey)
	if err == nil && len(userState) > 0 {
		if err := target.UpdateUserState(ctx, userKey, cloneStateMap(userState)); err != nil {
			return SessionMigrationVerification{}, err
		}
	}
	if _, ok := source.GetSessionSummaryText(ctx, sourceSession); ok {
		if err := target.CreateSessionSummary(ctx, targetSession, "", true); err != nil {
			return SessionMigrationVerification{}, err
		}
	}
	return verifySessionServices(ctx, migration.ID, key, source, target)
}

func (r *SessionRouter) VerifySession(
	ctx context.Context,
	tenantID string,
	migrationID string,
	item SessionMigrationItem,
) (SessionMigrationVerification, error) {
	migration, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	key := session.Key{
		AppName: "t/" + migration.TenantID + "/a/" + migration.AppID,
		UserID:  item.UserID, SessionID: item.SessionID,
	}
	return verifySessionServices(ctx, migration.ID, key, source, target)
}

func (r *SessionRouter) migrationServices(
	ctx context.Context,
	tenantID string,
	migrationID string,
) (controlplane.BackendMigration, session.Service, session.Service, error) {
	migration, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	if migration.ResourceType != sessionResourceType {
		return controlplane.BackendMigration{}, nil, nil, errors.New("migration is not for session")
	}
	sourceBinding, err := r.repository.GetBackendBinding(ctx, tenantID, migration.SourceBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	targetBinding, err := r.repository.GetBackendBinding(ctx, tenantID, migration.TargetBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	source, err := r.cachedService(ctx, sourceBinding)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	target, err := r.cachedService(ctx, targetBinding)
	return migration, source, target, err
}

func verifySessionServices(
	ctx context.Context,
	migrationID string,
	key session.Key,
	source session.Service,
	target session.Service,
) (SessionMigrationVerification, error) {
	sourceSession, err := source.GetSession(ctx, key)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	targetSession, err := target.GetSession(ctx, key)
	if err != nil {
		return SessionMigrationVerification{}, err
	}
	result := SessionMigrationVerification{
		MigrationID: migrationID, UserID: key.UserID, SessionID: key.SessionID,
		SourceEvents: len(sourceSession.Events), TargetEvents: len(targetSession.Events),
		StateMatched: stateMapsEqual(sourceSession.State, targetSession.State),
	}
	_, sourceSummary := source.GetSessionSummaryText(ctx, sourceSession)
	_, targetSummary := target.GetSessionSummaryText(ctx, targetSession)
	result.SummaryMatched = sourceSummary == targetSummary
	result.Passed = result.SourceEvents == result.TargetEvents &&
		result.StateMatched && result.SummaryMatched
	return result, nil
}

func stateMapsEqual(first session.StateMap, second session.StateMap) bool {
	if len(first) != len(second) {
		return false
	}
	for key, value := range first {
		if !bytes.Equal(value, second[key]) {
			return false
		}
	}
	return true
}

func (r *SessionRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := make([]session.Service, 0, len(r.services)+1)
	services = append(services, r.startup)
	for _, service := range r.services {
		services = append(services, service)
	}
	r.mu.Unlock()
	var closeErr error
	closed := make(map[session.Service]struct{})
	for _, service := range services {
		if _, exists := closed[service]; exists {
			continue
		}
		closed[service] = struct{}{}
		closeErr = errors.Join(closeErr, service.Close())
	}
	return closeErr
}

type sessionBackendConfig struct {
	URL         string `json:"url"`
	KeyPrefix   string `json:"key_prefix"`
	DSN         string `json:"dsn"`
	TablePrefix string `json:"table_prefix"`
	TTL         string `json:"ttl"`
}

func (r *SessionRouter) serviceFor(ctx context.Context, appName string) (session.Service, error) {
	if appName == readinessAppName {
		return r.startup, nil
	}
	tenantID, appID, err := runtimecontext.ParseStorageScope(appName)
	if err != nil {
		return nil, err
	}
	migration, migrationErr := r.repository.GetActiveBackendMigration(
		ctx, tenantID, appID, sessionResourceType,
	)
	if migrationErr == nil {
		return r.migrationService(ctx, migration)
	}
	if !errors.Is(migrationErr, controlplane.ErrNotFound) {
		return nil, migrationErr
	}
	binding, err := resolveBackendBinding(ctx, r.repository, tenantID, appID, sessionResourceType)
	if err != nil {
		return nil, err
	}
	return r.cachedService(ctx, binding)
}

func (r *SessionRouter) migrationService(
	ctx context.Context,
	migration controlplane.BackendMigration,
) (session.Service, error) {
	sourceBinding, err := r.repository.GetBackendBinding(
		ctx, migration.TenantID, migration.SourceBindingID,
	)
	if err != nil {
		return nil, err
	}
	source, err := r.cachedService(ctx, sourceBinding)
	if err != nil {
		return nil, err
	}
	if migration.State == controlplane.MigrationPlanned ||
		migration.State == controlplane.MigrationRollback {
		return source, nil
	}
	targetBinding, err := r.repository.GetBackendBinding(
		ctx, migration.TenantID, migration.TargetBindingID,
	)
	if err != nil {
		return nil, err
	}
	target, err := r.cachedService(ctx, targetBinding)
	if err != nil {
		return nil, err
	}
	repair := func(ctx context.Context, _ error) {
		_ = r.repository.AdjustBackendMigrationRepair(
			ctx, migration.TenantID, migration.ID, 1,
		)
	}
	switch migration.State {
	case controlplane.MigrationDualWrite, controlplane.MigrationBackfill,
		controlplane.MigrationVerify:
		return &dualSessionService{primary: source, secondary: target, onSecondaryError: repair}, nil
	case controlplane.MigrationCutover:
		return &dualSessionService{primary: target, secondary: source, onSecondaryError: repair}, nil
	default:
		return nil, fmt.Errorf("unsupported active session migration state %q", migration.State)
	}
}

func (r *SessionRouter) cachedService(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (session.Service, error) {
	if binding.BackendType == "startup_config" {
		return r.startup, nil
	}
	cacheKey := binding.ID + "\x00" + fmt.Sprint(binding.Version)
	r.mu.RLock()
	service := r.services[cacheKey]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errors.New("session router is closed")
	}
	if service != nil {
		return service, nil
	}
	value, err, _ := r.group.Do(cacheKey, func() (any, error) {
		r.mu.RLock()
		cached := r.services[cacheKey]
		r.mu.RUnlock()
		if cached != nil {
			return cached, nil
		}
		built, err := r.build(ctx, binding)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = built.Close()
			return nil, errors.New("session router is closed")
		}
		r.services[cacheKey] = built
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(session.Service), nil
}

func (r *SessionRouter) build(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (session.Service, error) {
	var config sessionBackendConfig
	if err := decodeStorageConfig(binding.Config, &config); err != nil {
		return nil, err
	}
	var ttl time.Duration
	var err error
	if config.TTL != "" {
		ttl, err = time.ParseDuration(config.TTL)
		if err != nil || ttl < 0 {
			return nil, errors.New("session binding ttl is invalid")
		}
	}
	switch strings.ToLower(binding.BackendType) {
	case "inmemory":
		options := []inmemory.ServiceOpt{}
		if r.summarizer != nil {
			options = append(options, inmemory.WithSummarizer(r.summarizer))
		}
		return inmemory.NewSessionService(options...), nil
	case "redis":
		endpoint, err := r.endpoint(ctx, config.URL, binding.SecretRef)
		if err != nil {
			return nil, err
		}
		options := []redissession.ServiceOpt{
			redissession.WithRedisClientURL(endpoint),
			redissession.WithKeyPrefix(config.KeyPrefix),
			redissession.WithSessionTTL(ttl),
			redissession.WithEnableAsyncPersist(false),
			redissession.WithEnableUserSessionIndex(true),
			redissession.WithCompatMode(redissession.CompatModeNone),
		}
		if r.summarizer != nil {
			options = append(options, redissession.WithSummarizer(r.summarizer))
		}
		return redissession.NewService(options...)
	case "postgres":
		endpoint, err := r.endpoint(ctx, config.DSN, binding.SecretRef)
		if err != nil {
			return nil, err
		}
		options := []postgressession.ServiceOpt{
			postgressession.WithPostgresClientDSN(endpoint),
			postgressession.WithTablePrefix(config.TablePrefix),
			postgressession.WithSessionTTL(ttl),
			postgressession.WithEnableAsyncPersist(false),
		}
		if r.summarizer != nil {
			options = append(options, postgressession.WithSummarizer(r.summarizer))
		}
		return postgressession.NewService(options...)
	default:
		return nil, fmt.Errorf("unsupported session backend %q", binding.BackendType)
	}
}

func (r *SessionRouter) endpoint(
	ctx context.Context,
	configured string,
	secretRef string,
) (string, error) {
	if secretRef != "" {
		return r.secrets.Resolve(ctx, secretRef)
	}
	if strings.TrimSpace(configured) == "" {
		return "", errors.New("session backend endpoint or secret_ref is required")
	}
	return configured, nil
}

var _ session.Service = (*SessionRouter)(nil)
