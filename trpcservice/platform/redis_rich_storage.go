package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func (s *RedisStore) fence(tenant, session string) string {
	return s.prefix + "fence:" + tenant + ":" + session
}

// fencedWrite checks the session generation in the same transaction as the write.
func (s *RedisStore) fencedWrite(ctx context.Context, tenant, session string, token uint64, write func(redis.Pipeliner) error) error {
	key := s.fence(tenant, session)
	for attempt := 0; attempt < 32; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			if err := s.checkMigration(ctx, tx, tenant); err != nil {
				return err
			}
			current, err := tx.Get(ctx, key).Uint64()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if token > 0 && token < current && !importingMigration(ctx) {
				return ErrStaleFencingToken
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if token > current {
					pipe.Set(ctx, key, strconv.FormatUint(token, 10), 0)
				}
				return write(pipe)
			})
			return err
		}, key, s.migrationKey(tenant))
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
	}
	return redis.TxFailedErr
}

func (s *RedisStore) PutArtifact(ctx context.Context, item Artifact) (Artifact, error) {
	if item.TenantID == "" || item.SessionID == "" || item.Name == "" || item.ContentRef == "" {
		return Artifact{}, errors.New("invalid Artifact")
	}
	if item.ID == "" {
		item.ID = "artifact-" + stableID(item.TenantID+"\x00"+item.SessionID+"\x00"+item.Name+"\x00"+item.ContentRef)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.Status == "" {
		item.Status = "published"
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return Artifact{}, err
	}
	err = s.fencedWrite(ctx, item.TenantID, item.SessionID, item.FencingToken, func(pipe redis.Pipeliner) error {
		pipe.HSetNX(ctx, s.prefix+"artifacts:"+item.TenantID, item.ID, encoded)
		return nil
	})
	return item, err
}
func (s *RedisStore) ListArtifacts(ctx context.Context, tenant, session string) ([]Artifact, error) {
	values, err := s.client.HGetAll(ctx, s.prefix+"artifacts:"+tenant).Result()
	if err != nil {
		return nil, err
	}
	items := []Artifact{}
	for _, value := range values {
		var item Artifact
		if err := json.Unmarshal([]byte(value), &item); err != nil {
			return nil, err
		}
		if item.TenantID == tenant && (session == "" || item.SessionID == session) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}
func (s *RedisStore) PutKnowledge(ctx context.Context, item KnowledgeRecord) (KnowledgeRecord, error) {
	if item.TenantID == "" || item.AgentAppID == "" || item.Source == "" || item.Content == "" {
		return KnowledgeRecord{}, errors.New("invalid Knowledge")
	}
	if item.ID == "" {
		item.ID = "knowledge-" + stableID(item.TenantID+"\x00"+item.AgentAppID+"\x00"+item.Source)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	if item.IndexStatus == "" {
		item.IndexStatus = "authoritative"
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return KnowledgeRecord{}, err
	}
	err = s.fencedWrite(ctx, item.TenantID, "", 0, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, s.prefix+"knowledge:"+item.TenantID, item.ID, encoded)
		return nil
	})
	return item, err
}
func (s *RedisStore) ListKnowledge(ctx context.Context, tenant, app string) ([]KnowledgeRecord, error) {
	values, err := s.client.HGetAll(ctx, s.prefix+"knowledge:"+tenant).Result()
	if err != nil {
		return nil, err
	}
	items := []KnowledgeRecord{}
	for _, value := range values {
		var item KnowledgeRecord
		if err := json.Unmarshal([]byte(value), &item); err != nil {
			return nil, err
		}
		if item.TenantID == tenant && (app == "" || item.AgentAppID == app) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}
