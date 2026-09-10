// Package redispostgres implements the supported Redis-to-PostgreSQL Session
// migration adapter.
package redispostgres

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const migrationPrincipal = "data-migration"

// Copier resolves the fixed Redis source and PostgreSQL target for one
// migration record and copies complete Session data between them.
type Copier struct {
	core     migration.RedisPostgresCopier
	importer *sessionpostgres.SummaryImporter
}

// NewCopier resolves the source and target Session services selected by record.
// The caller must Close the returned copier after the migration run ends.
func NewCopier(
	ctx context.Context,
	configs config.Resolver,
	sessions platformsession.Resolver,
	postgresSessions *sessionpostgres.SessionResolver,
	record migration.Record,
) (*Copier, error) {
	if configs == nil || sessions == nil || postgresSessions == nil {
		return nil, migration.NewPermanentError(errors.New("data migration copier dependencies are required"))
	}
	if err := record.Validate(); err != nil {
		return nil, migration.NewPermanentError(err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sourceExec, err := resolveExecution(ctx, configs, record, record.SourceConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve data migration source: %w", err)
	}
	targetExec, err := resolveExecution(ctx, configs, record, record.TargetConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve data migration target: %w", err)
	}
	if sourceExec.Config.BackendConfig.Session.Kind != tenant.BackendRedis ||
		sourceExec.Config.BackendConfig.Session.Provider != "redis" {
		return nil, migration.NewPermanentError(errors.New("data migration source session backend must use redis"))
	}
	if targetExec.Config.BackendConfig.Session.Kind != tenant.BackendSQL ||
		targetExec.Config.BackendConfig.Session.Provider != "postgres" {
		return nil, migration.NewPermanentError(errors.New("data migration target session backend must use postgres"))
	}
	source, err := sessions.ResolveSession(ctx, sourceExec)
	if err != nil {
		return nil, fmt.Errorf("resolve redis source session: %w", err)
	}
	if source == nil {
		return nil, migration.NewPermanentError(errors.New("resolved redis source session is required"))
	}
	target, err := sessions.ResolveSession(ctx, targetExec)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres target session: %w", err)
	}
	if target == nil {
		return nil, migration.NewPermanentError(errors.New("resolved postgres target session is required"))
	}
	importer, err := postgresSessions.NewSummaryImporter(ctx, targetExec)
	if err != nil {
		return nil, err
	}
	return &Copier{
		core: migration.RedisPostgresCopier{
			Source:    verifiedRedisSource{Service: source},
			Target:    target,
			Summaries: importer,
		},
		importer: importer,
	}, nil
}

// CopySession moves one Session selected by the platform-owned catalog.
func (c *Copier) CopySession(ctx context.Context, key session.Key) error {
	if c == nil {
		return errors.New("redis postgres migration copier is required")
	}
	return c.core.CopySession(ctx, key)
}

// VerifySession confirms one copied Session retains its authoritative data.
func (c *Copier) VerifySession(ctx context.Context, key session.Key) error {
	if c == nil {
		return errors.New("redis postgres migration copier is required")
	}
	return c.core.VerifySession(ctx, key)
}

type verifiedRedisSource struct {
	session.Service
}

func (s verifiedRedisSource) GetSession(
	ctx context.Context,
	key session.Key,
	options ...session.Option,
) (*session.Session, error) {
	value, err := s.Service.GetSession(ctx, key, options...)
	if err != nil || value != nil {
		if err != nil || value == nil {
			return value, err
		}
		if err := s.loadTrackEvents(ctx, key, value); err != nil {
			return nil, fmt.Errorf("load redis source tracks: %w", err)
		}
		return value, nil
	}
	values, err := s.ListSessions(ctx, session.UserKey{
		AppName: key.AppName,
		UserID:  key.UserID,
	}, session.WithListSessionOnlyMeta())
	if err != nil {
		return nil, fmt.Errorf("verify absent redis source session: %w", err)
	}
	for _, candidate := range values {
		if candidate != nil && candidate.ID == key.SessionID {
			return nil, errors.New("redis source returned an existing session as absent")
		}
	}
	return nil, nil
}

type redisTrackReader interface {
	GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error)
}

func (s verifiedRedisSource) loadTrackEvents(ctx context.Context, key session.Key, value *session.Session) error {
	tracks, err := session.TracksFromState(value.State)
	if err != nil {
		return err
	}
	if len(tracks) == 0 {
		return nil
	}
	reader, ok := s.Service.(redisTrackReader)
	if !ok {
		return errors.New("redis source track reader is required")
	}
	if value.Tracks == nil {
		value.Tracks = make(map[session.Track]*session.TrackEvents, len(tracks))
	}
	for _, track := range tracks {
		history, err := reader.GetTrackEvents(ctx, key, track, session.WithEventNum(1<<30))
		if err != nil {
			return fmt.Errorf("read track %q: %w", track, err)
		}
		if history == nil {
			history = &session.TrackEvents{Track: track}
		}
		value.Tracks[track] = history
	}
	return nil
}

// GetSessionSummaries fails closed because the pinned Redis provider exposes
// only summary text, not the complete filter-keyed Summary records required
// for lossless migration.
func (s verifiedRedisSource) GetSessionSummaries(
	ctx context.Context,
	key session.Key,
) (map[string]*session.Summary, error) {
	if s.Service == nil {
		return nil, migration.NewPermanentError(errors.New("redis summary source is not initialized"))
	}
	value, err := s.Service.GetSession(ctx, key, session.WithEventNum(math.MaxInt))
	if err != nil {
		return nil, fmt.Errorf("read redis session for summary inventory: %w", err)
	}
	if value == nil {
		return map[string]*session.Summary{}, nil
	}
	if len(value.Summaries) > 0 {
		return nil, migration.ErrSummaryImportRequired
	}
	if len(value.Events) == 0 {
		// A Redis summary is derived from event history. With complete history
		// confirmed empty, there is no summary inventory to import; do not use
		// the provider's boolean text lookup, which also hides backend errors.
		return map[string]*session.Summary{}, nil
	}
	// Redis has no filter-keyed summary enumeration API. For a non-empty
	// session, the provider's boolean summary API conflates "missing" with a
	// Redis read failure, so do not silently treat false as an empty inventory.
	return nil, migration.ErrSummaryImportRequired
}

// Close releases the dedicated PostgreSQL summary import connection pool.
func (c *Copier) Close() {
	if c != nil && c.importer != nil {
		c.importer.Close()
		c.importer = nil
	}
}

func resolveExecution(
	ctx context.Context,
	configs config.Resolver,
	record migration.Record,
	version string,
) (worker.Execution, error) {
	runtime := tenant.RuntimeContext{
		TenantID:           record.TenantID,
		AppID:              record.AppID,
		ConfigVersion:      version,
		SessionPrincipalID: migrationPrincipal,
		SessionID:          record.ID,
		UserID:             migrationPrincipal,
		TraceID:            record.ID,
	}
	cfg, err := configs.ResolveAppConfig(ctx, record.TenantID, record.AppID, version)
	if err != nil {
		return worker.Execution{}, err
	}
	if cfg.TenantID != record.TenantID || cfg.AppID != record.AppID || cfg.Version != version {
		return worker.Execution{}, migration.NewPermanentError(errors.New("resolved app config does not match data migration"))
	}
	return worker.Execution{Tenant: runtime, Config: cfg}, nil
}
