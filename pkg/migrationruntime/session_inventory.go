package migrationruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Keys enumerates the actual official backend, including sessions that predate
// the platform inventory. Payloads are read through session.Service. This
// metadata reader is tied to the default layouts selected by BackendFactory.
func (b *sessionBackend) Keys(ctx context.Context, cursor string, limit int) ([]string, string, bool, error) {
	if limit < 1 || limit > 10000 || (cursor != "" && !strings.HasPrefix(cursor, "session/v1/")) {
		return nil, "", false, datamigration.ErrInvalidMigration
	}
	if cursor != "" {
		if _, err := parseSessionRecordKey(b.tenantID, cursor); err != nil {
			return nil, "", false, err
		}
	}
	var keys []string
	var err error
	switch {
	case b.redis != nil:
		keys, err = b.redisKeys(ctx)
	case b.postgres != nil:
		keys, err = b.postgresKeys(ctx)
	default:
		return nil, "", false, datamigration.ErrMigrationCapability
	}
	if err != nil {
		return nil, "", false, err
	}
	sort.Strings(keys)
	start := sort.SearchStrings(keys, cursor)
	for start < len(keys) && keys[start] <= cursor {
		start++
	}
	end := start + limit
	if end >= len(keys) {
		return keys[start:], "", true, nil
	}
	return keys[start:end], keys[end-1], false, nil
}

func (b *sessionBackend) postgresKeys(ctx context.Context) ([]string, error) {
	prefix := sessionAppPrefix(b.tenantID)
	var unsupported bool
	if err := b.postgres.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM app_states WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at > clock_timestamp())
		UNION ALL SELECT 1 FROM user_states WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at > clock_timestamp())
		UNION ALL SELECT 1 FROM session_summaries WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at > clock_timestamp())
		UNION ALL SELECT 1 FROM session_states WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND expires_at > clock_timestamp()
		UNION ALL SELECT 1 FROM session_events WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND expires_at > clock_timestamp()
		UNION ALL SELECT 1 FROM session_track_events WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND expires_at > clock_timestamp()
	)`, prefix).Scan(&unsupported); err != nil {
		return nil, sessionBackendError("inspect PostgreSQL capabilities")
	}
	if unsupported {
		return nil, ErrSessionMigrationUnsupported
	}
	rows, err := b.postgres.QueryContext(ctx, `SELECT app_name,user_id,session_id FROM session_states
		WHERE left(app_name,length($1))=$1 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at > clock_timestamp())
		ORDER BY app_name,user_id,session_id LIMIT $2`, prefix, b.options.MaxSessions+1)
	if err != nil {
		return nil, sessionBackendError("enumerate PostgreSQL sessions")
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var key session.Key
		if err := rows.Scan(&key.AppName, &key.UserID, &key.SessionID); err != nil {
			return nil, sessionBackendError("decode PostgreSQL inventory")
		}
		if validateSessionApp(b.tenantID, key.AppName) != nil {
			return nil, datamigration.ErrInvalidRecord
		}
		encoded, err := sessionRecordKey(key)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded)
		if len(result) > b.options.MaxSessions {
			return nil, sessionBackendError("inventory bound reached")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, sessionBackendError("read PostgreSQL inventory")
	}
	return result, nil
}

func (b *sessionBackend) redisKeys(ctx context.Context) ([]string, error) {
	prefix := sessionAppPrefix(b.tenantID)
	unsupportedPatterns := []string{"appstate:{" + prefix + "*}", "userstate:{" + prefix + "*}:*", "hashidx:userstate:" + prefix + "*", "sesssum:{" + prefix + "*}:*", "hashidx:sesssum:" + prefix + "*"}
	for _, pattern := range unsupportedPatterns {
		if err := b.scanRedis(ctx, pattern, func(key string) error {
			count, err := b.redis.HLen(ctx, key).Result()
			if err != nil {
				return sessionBackendError("inspect Redis shared state")
			}
			if count > 0 {
				return ErrSessionMigrationUnsupported
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	expiringPatterns := []string{"event:{" + prefix + "*}:*", "track:{" + prefix + "*}:*", "trackidx:{" + prefix + "*}:*", "hashidx:evtdata:" + prefix + "*", "hashidx:evtidx:time:" + prefix + "*", "hashidx:trkdata:" + prefix + "*", "hashidx:trkidx:time:" + prefix + "*", "hashidx:trkidx:names:" + prefix + "*"}
	for _, pattern := range expiringPatterns {
		if err := b.scanRedis(ctx, pattern, func(key string) error {
			ttl, err := b.redis.PTTL(ctx, key).Result()
			if err != nil {
				return sessionBackendError("inspect Redis data expiry")
			}
			if ttl > 0 {
				return ErrSessionMigrationUnsupported
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	seen := make(map[string]struct{})
	appendKey := func(key session.Key) error {
		if validateSessionApp(b.tenantID, key.AppName) != nil {
			return datamigration.ErrInvalidRecord
		}
		encoded, err := sessionRecordKey(key)
		if err != nil {
			return err
		}
		seen[encoded] = struct{}{}
		if len(seen) > b.options.MaxSessions {
			return sessionBackendError("inventory bound reached")
		}
		return nil
	}
	if err := b.scanRedis(ctx, "sess:{"+prefix+"*}:*", func(nativeKey string) error {
		ttl, err := b.redis.PTTL(ctx, nativeKey).Result()
		if err != nil {
			return sessionBackendError("inspect Redis expiry")
		}
		if ttl > 0 {
			return ErrSessionMigrationUnsupported
		}
		tail := strings.TrimPrefix(nativeKey, "sess:{")
		appName, userID, found := strings.Cut(tail, "}:")
		if !found || validateSessionApp(b.tenantID, appName) != nil {
			return datamigration.ErrInvalidRecord
		}
		var cursor uint64
		for {
			items, next, err := b.redis.HScan(ctx, nativeKey, cursor, "*", 128).Result()
			if err != nil {
				return sessionBackendError("enumerate Redis session hash")
			}
			for index := 0; index+1 < len(items); index += 2 {
				var meta struct {
					ID string `json:"id"`
				}
				if len(items[index+1]) > 16<<20 || json.Unmarshal([]byte(items[index+1]), &meta) != nil || meta.ID != items[index] {
					return datamigration.ErrInvalidRecord
				}
				if err := appendKey(session.Key{AppName: appName, UserID: userID, SessionID: meta.ID}); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}); err != nil {
		return nil, err
	}
	if err := b.scanRedis(ctx, "hashidx:meta:"+prefix+"*", func(nativeKey string) error {
		ttl, err := b.redis.PTTL(ctx, nativeKey).Result()
		if err != nil {
			return sessionBackendError("inspect Redis expiry")
		}
		if ttl > 0 {
			return ErrSessionMigrationUnsupported
		}
		value, err := b.redis.Get(ctx, nativeKey).Bytes()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return sessionBackendError("read Redis session metadata")
		}
		var meta struct {
			ID      string `json:"id"`
			AppName string `json:"appName"`
			UserID  string `json:"userID"`
		}
		if len(value) > 16<<20 || json.Unmarshal(value, &meta) != nil {
			return datamigration.ErrInvalidRecord
		}
		if nativeKey != fmt.Sprintf("hashidx:meta:%s:{%s}:%s", meta.AppName, meta.UserID, meta.ID) {
			return datamigration.ErrInvalidRecord
		}
		return appendKey(session.Key{AppName: meta.AppName, UserID: meta.UserID, SessionID: meta.ID})
	}); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(seen))
	for key := range seen {
		result = append(result, key)
	}
	return result, nil
}

func (b *sessionBackend) scanRedis(ctx context.Context, pattern string, visit func(string) error) error {
	var cursor uint64
	count := 0
	for {
		keys, next, err := b.redis.Scan(ctx, cursor, pattern, 128).Result()
		if err != nil {
			return sessionBackendError("scan Redis inventory")
		}
		for _, key := range keys {
			count++
			if count > b.options.MaxSessions*4+128 {
				return sessionBackendError("inventory scan bound reached")
			}
			if err := visit(key); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
