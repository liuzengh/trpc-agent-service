package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
	prefix string
}

func NewRedisStore(address string) *RedisStore {
	options := &redis.Options{Addr: address}
	if address == "" {
		options.Addr = "127.0.0.1:6379"
	}
	if parsed, err := url.Parse(address); err == nil && (parsed.Scheme == "redis" || parsed.Scheme == "rediss") {
		if configured, err := redis.ParseURL(address); err == nil {
			options = configured
		}
	}
	return &RedisStore{client: redis.NewClient(options), prefix: "trpc-agent:"}
}
func (s *RedisStore) stream(tenant, session string) string {
	return s.prefix + "events:" + tenant + ":" + session
}
func (s *RedisStore) idem(tenant, session string) string {
	return s.prefix + "idem:" + tenant + ":" + session
}
func (s *RedisStore) memory(tenant, session string) string {
	return s.prefix + "memory:" + tenant + ":" + session
}
func (s *RedisStore) GetSession(ctx context.Context, tenant, session string) (Session, error) {
	state, err := s.GetSessionState(ctx, tenant, session)
	return state.Session, err
}
func (s *RedisStore) GetSessionState(ctx context.Context, tenant, session string) (SessionState, error) {
	events, err := s.ListSessionEvents(ctx, tenant, session, 0)
	if err != nil {
		return SessionState{}, err
	}
	return materializeSession(tenant, session, events)
}
func (s *RedisStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if event.TenantID == "" || event.SessionID == "" || event.IdempotencyKey == "" {
		return errors.New("platform: invalid session event")
	}
	stream := s.stream(event.TenantID, event.SessionID)
	idem := s.idem(event.TenantID, event.SessionID)
	signature := event.Type + "\x00" + string(event.Payload)
	for attempts := 0; attempts < 32; attempts++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			if err := s.checkMigration(ctx, tx, event.TenantID); err != nil {
				return err
			}
			current, err := tx.Get(ctx, s.fence(event.TenantID, event.SessionID)).Uint64()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if event.FencingToken > 0 && event.FencingToken < current && !importingMigration(ctx) {
				return ErrStaleFencingToken
			}
			prior, err := tx.HGet(ctx, idem, event.IdempotencyKey).Result()
			if err == nil {
				if prior == signature {
					return nil
				}
				return ErrDuplicateEvent
			}
			if !errors.Is(err, redis.Nil) {
				return err
			}
			count, err := tx.ZCard(ctx, stream).Result()
			if err != nil {
				return err
			}
			event.Sequence = uint64(count + 1)
			if event.ID == "" {
				event.ID = event.TenantID + ":" + event.SessionID + ":" + itoa(event.Sequence)
			}
			if event.OccurredAt.IsZero() {
				event.OccurredAt = time.Now().UTC()
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if event.FencingToken > current {
					pipe.Set(ctx, s.fence(event.TenantID, event.SessionID), strconv.FormatUint(event.FencingToken, 10), 0)
				}
				pipe.ZAdd(ctx, stream, redis.Z{Score: float64(event.Sequence), Member: encoded})
				pipe.HSet(ctx, idem, event.IdempotencyKey, signature)
				return nil
			})
			return err
		}, stream, idem, s.fence(event.TenantID, event.SessionID), s.migrationKey(event.TenantID))
		if err == nil || errors.Is(err, ErrDuplicateEvent) {
			return err
		}
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
	}
	return redis.TxFailedErr
}
func (s *RedisStore) ListSessionEvents(ctx context.Context, tenant, session string, after uint64) ([]SessionEvent, error) {
	values, err := s.client.ZRangeByScore(ctx, s.stream(tenant, session), &redis.ZRangeBy{Min: strconv.FormatUint(after+1, 10), Max: "+inf"}).Result()
	if err != nil {
		return nil, err
	}
	items := make([]SessionEvent, 0, len(values))
	for _, value := range values {
		var e SessionEvent
		if err := json.Unmarshal([]byte(value), &e); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	return items, nil
}
func (s *RedisStore) ListMemory(ctx context.Context, tenant, session string) ([]MemoryRecord, error) {
	values, err := s.client.HGetAll(ctx, s.memory(tenant, session)).Result()
	if err != nil {
		return nil, err
	}
	items := make([]MemoryRecord, 0, len(values))
	for _, value := range values {
		var m MemoryRecord
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}
func (s *RedisStore) PutMemory(ctx context.Context, m MemoryRecord) error {
	if m.TenantID == "" || m.SessionID == "" || m.Key == "" {
		return errors.New("platform: invalid memory record")
	}
	if m.ID == "" {
		m.ID = m.TenantID + ":" + m.SessionID + ":" + m.Key
	}
	if m.UpdatedAt.IsZero() {
		m.UpdatedAt = time.Now().UTC()
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.fencedWrite(ctx, m.TenantID, m.SessionID, m.FencingToken, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, s.memory(m.TenantID, m.SessionID), m.Key, encoded)
		return nil
	})
}
func (s *RedisStore) Health(ctx context.Context) BackendHealth {
	status := "healthy"
	if err := s.client.Ping(ctx).Err(); err != nil {
		status = "unavailable"
	}
	return BackendHealth{Backend: "redis", Status: status, Checked: time.Now().UTC()}
}
func (s *RedisStore) Close() error { return s.client.Close() }
