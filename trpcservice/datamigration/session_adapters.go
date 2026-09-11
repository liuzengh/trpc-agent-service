package datamigration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"

	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
)

// RedisSessionSource scans both the current HashIdx catalog and the legacy
// zset catalog. The framework service performs the actual compatibility read,
// so a logical session present in both formats resolves exactly as production
// traffic does. Redis Cluster needs a per-master cursor implementation and is
// rejected by this URL-based adapter instead of silently scanning one shard.
type RedisSessionSource struct {
	client    *goredis.Client
	service   *redissession.Service
	appName   string
	keyPrefix string
	maxEvents int
}

func NewRedisSessionSource(rawURL, keyPrefix, appName string, maxEvents int) (*RedisSessionSource, error) {
	if strings.TrimSpace(rawURL) == "" || strings.TrimSpace(appName) == "" {
		return nil, errors.New("redis session migration URL and app name are required")
	}
	if maxEvents <= 0 || maxEvents > 1_000_000 {
		return nil, errors.New("redis session migration max events must be between 1 and 1000000")
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("invalid redis session migration URL")
	}
	client := goredis.NewClient(options)
	probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(probeCtx).Err(); err != nil {
		_ = client.Close()
		return nil, errors.New("redis session migration source is unavailable")
	}
	if info, err := client.ClusterInfo(probeCtx).Result(); err == nil &&
		(strings.Contains(info, "cluster_enabled:1") || strings.Contains(info, "cluster_state:")) {
		_ = client.Close()
		return nil, errors.New("redis cluster session migration requires a per-master scanner")
	}
	service, err := redissession.NewService(
		redissession.WithRedisClientURL(rawURL),
		redissession.WithKeyPrefix(keyPrefix),
		redissession.WithSessionEventLimit(maxEvents+1),
		redissession.WithCompatMode(redissession.CompatModeLegacy),
	)
	if err != nil {
		_ = client.Close()
		return nil, errors.New("create redis session migration reader")
	}
	return &RedisSessionSource{client: client, service: service, appName: appName, keyPrefix: keyPrefix, maxEvents: maxEvents}, nil
}

func (s *RedisSessionSource) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return errors.New("redis session migration source is closed")
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return errors.New("redis session migration source is unavailable")
	}
	return nil
}

func (s *RedisSessionSource) Close() error {
	if s == nil {
		return nil
	}
	var first error
	if s.service != nil {
		first = s.service.Close()
		s.service = nil
	}
	if s.client != nil {
		if err := s.client.Close(); first == nil {
			first = err
		}
		s.client = nil
	}
	return first
}

func (s *RedisSessionSource) Scan(ctx context.Context, cursor string, batchSize int) ([]SessionObject, string, bool, error) {
	if s == nil || s.client == nil || s.service == nil {
		return nil, cursor, false, errors.New("redis session migration source is closed")
	}
	stage, scanCursor, err := parseSessionScanCursor(cursor)
	if err != nil {
		return nil, cursor, false, err
	}
	var pattern string
	if stage == "h" {
		pattern = escapeRedisGlob(s.prefixed("hashidx:meta:"+s.appName+":")) + "*"
	} else {
		pattern = escapeRedisGlob(s.prefixed("sess:{"+s.appName+"}:")) + "*"
	}
	keys, nextCursor, err := s.client.Scan(ctx, scanCursor, pattern, int64(batchSize)).Result()
	if err != nil {
		return nil, cursor, false, errors.New("scan redis session catalog")
	}

	objects := make([]SessionObject, 0, len(keys))
	seen := make(map[session.Key]struct{})
	for _, redisKey := range keys {
		logicalKeys, err := s.logicalKeys(ctx, stage, redisKey)
		if err != nil {
			return nil, cursor, false, err
		}
		for _, key := range logicalKeys {
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			object, exists, err := s.load(ctx, key)
			if err != nil {
				return nil, cursor, false, err
			}
			if exists {
				objects = append(objects, object)
			}
		}
	}

	if nextCursor != 0 {
		return objects, stage + ":" + strconv.FormatUint(nextCursor, 10), false, nil
	}
	if stage == "h" {
		return objects, "z:0", false, nil
	}
	return objects, "z:0", true, nil
}

func (s *RedisSessionSource) logicalKeys(ctx context.Context, stage, redisKey string) ([]session.Key, error) {
	if stage == "h" {
		prefix := s.prefixed("hashidx:meta:" + s.appName + ":")
		rest := strings.TrimPrefix(redisKey, prefix)
		end := strings.Index(rest, "}:")
		if !strings.HasPrefix(rest, "{") || end < 1 || end+2 >= len(rest) {
			return nil, fmt.Errorf("invalid redis hashidx session key %q", redisKey)
		}
		return []session.Key{{AppName: s.appName, UserID: rest[1:end], SessionID: rest[end+2:]}}, nil
	}
	prefix := s.prefixed("sess:{" + s.appName + "}:")
	userID := strings.TrimPrefix(redisKey, prefix)
	if userID == "" || userID == redisKey {
		return nil, fmt.Errorf("invalid redis legacy session key %q", redisKey)
	}
	sessions, err := s.service.ListSessions(ctx, session.UserKey{AppName: s.appName, UserID: userID}, session.WithListSessionOnlyMeta())
	if err != nil {
		return nil, errors.New("list legacy redis sessions")
	}
	keys := make([]session.Key, 0, len(sessions))
	for _, item := range sessions {
		if item != nil && item.ID != "" {
			keys = append(keys, session.Key{AppName: s.appName, UserID: userID, SessionID: item.ID})
		}
	}
	return keys, nil
}

func (s *RedisSessionSource) load(ctx context.Context, key session.Key) (SessionObject, bool, error) {
	item, err := s.service.GetSession(ctx, key, session.WithEventNum(s.maxEvents+1))
	if err != nil {
		return SessionObject{}, false, errors.New("read redis session")
	}
	if item == nil {
		return SessionObject{}, false, nil
	}
	if len(item.Events) > s.maxEvents {
		return SessionObject{}, false, fmt.Errorf("redis session %q exceeds max event safety limit", key.SessionID)
	}
	appState, err := s.service.ListAppStates(ctx, key.AppName)
	if err != nil {
		return SessionObject{}, false, errors.New("read redis application state")
	}
	userState, err := s.service.ListUserStates(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID})
	if err != nil {
		return SessionObject{}, false, errors.New("read redis user state")
	}
	return SessionObject{
		Key:       key,
		State:     sessionLocalState(item.State),
		Events:    append([]event.Event(nil), item.Events...),
		AppState:  cloneSessionState(appState),
		UserState: cloneSessionState(userState),
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
		SourceVer: item.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}, true, nil
}

func (s *RedisSessionSource) prefixed(value string) string {
	if s.keyPrefix == "" {
		return value
	}
	return s.keyPrefix + ":" + value
}

type PostgresSessionTarget struct {
	store *sessionturn.Postgres
}

func NewPostgresSessionTarget(store *sessionturn.Postgres) (*PostgresSessionTarget, error) {
	if store == nil {
		return nil, errors.New("postgres session migration target is required")
	}
	return &PostgresSessionTarget{store: store}, nil
}

func (t *PostgresSessionTarget) Import(ctx context.Context, object SessionObject) error {
	return t.store.ImportFrozenSnapshot(ctx, sessionturn.MigrationSnapshot{
		Key: object.Key, State: object.State, Events: object.Events,
		AppState: object.AppState, UserState: object.UserState,
		CreatedAt: object.CreatedAt, UpdatedAt: object.UpdatedAt,
	})
}

func (t *PostgresSessionTarget) Hash(ctx context.Context, key session.Key) (string, bool, error) {
	snapshot, err := t.store.Load(ctx, key)
	if err != nil || snapshot == nil {
		return "", false, err
	}
	appState, err := t.store.ListAppStates(ctx, key.AppName)
	if err != nil {
		return "", false, err
	}
	userState, err := t.store.ListUserStates(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID})
	if err != nil {
		return "", false, err
	}
	hash, err := HashSessionObject(SessionObject{
		Key: key, State: snapshot.State, Events: snapshot.Events,
		AppState: appState, UserState: userState,
	})
	return hash, true, err
}

func parseSessionScanCursor(value string) (string, uint64, error) {
	if value == "" {
		return "h", 0, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 || (parts[0] != "h" && parts[0] != "z") {
		return "", 0, errors.New("invalid redis session migration cursor")
	}
	cursor, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, errors.New("invalid redis session migration cursor")
	}
	return parts[0], cursor, nil
}

func escapeRedisGlob(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "*", "\\*", "?", "\\?", "[", "\\[", "]", "\\]")
	return replacer.Replace(value)
}

func sessionLocalState(source session.StateMap) session.StateMap {
	result := make(session.StateMap)
	for key, value := range source {
		if strings.HasPrefix(key, session.StateAppPrefix) || strings.HasPrefix(key, session.StateUserPrefix) || strings.HasPrefix(key, session.StateTempPrefix) {
			continue
		}
		result[key] = append([]byte(nil), value...)
		if value == nil {
			result[key] = nil
		}
	}
	return result
}

func cloneSessionState(source session.StateMap) session.StateMap {
	result := make(session.StateMap, len(source))
	for key, value := range source {
		result[key] = append([]byte(nil), value...)
		if value == nil {
			result[key] = nil
		}
	}
	return result
}
