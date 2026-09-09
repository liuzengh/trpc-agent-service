package datamigration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// RedisSource reads an ordered, versioned record index.  Writers must assign a
// strictly increasing integer version per tenant/domain; this makes the
// watermark a stable keyset cursor rather than a fragile rank/offset cursor.
// The source is deliberately read-only: dual-write is enabled by the control
// plane before CatchUp starts.
type RedisSource struct {
	Client *redis.Client
	Prefix string
}

const maxMigrationBatchPayloadBytes = 64 << 20

var boundedRedisReadScript = redis.NewScript(`
local values = {}
local total = 0
local max_record = tonumber(ARGV[1])
local max_batch = tonumber(ARGV[2])
for i, key in ipairs(KEYS) do
  local value = redis.call('GET', key)
  if not value then
    return redis.error_reply('MIGRATION_RECORD_MISSING')
  end
  local deleted = redis.call('GET', key .. ':deleted')
  local size = string.len(value)
  if size > max_record then
    return redis.error_reply('MIGRATION_RECORD_TOO_LARGE')
  end
  if deleted ~= '1' then total = total + size end
  if total > max_batch then
    return redis.error_reply('MIGRATION_BATCH_TOO_LARGE')
  end
  values[#values + 1] = deleted == '1' and '1' or '0'
  values[#values + 1] = deleted == '1' and '' or value
end
return values
`)

type redisSnapshotCursor struct {
	Watermark int64 `json:"watermark"`
	Last      int64 `json:"last"`
}

func (s *RedisSource) Snapshot(ctx context.Context, tenantID string, domain Domain, cursor string, limit int) (Batch, error) {
	if err := s.validate(tenantID, domain, limit); err != nil {
		return Batch{}, err
	}
	state, err := decodeRedisSnapshotCursor(cursor)
	if err != nil {
		return Batch{}, err
	}
	if cursor == "" {
		latest, err := s.Client.ZRevRangeWithScores(ctx, s.indexKey(tenantID, domain), 0, 0).Result()
		if err != nil {
			return Batch{}, fmt.Errorf("redis snapshot watermark: %w", err)
		}
		if len(latest) == 0 {
			return Batch{Done: true}, nil
		}
		state.Watermark, err = redisVersion(latest[0].Score)
		if err != nil {
			return Batch{}, err
		}
		state.Last = -1
	}
	if state.Last > state.Watermark {
		return Batch{}, fmt.Errorf("%w: redis snapshot cursor is beyond watermark", ErrInvalidRecord)
	}
	entries, err := s.Client.ZRangeByScoreWithScores(ctx, s.indexKey(tenantID, domain), &redis.ZRangeBy{
		Min: "(" + strconv.FormatInt(state.Last, 10), Max: strconv.FormatInt(state.Watermark, 10), Offset: 0, Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return Batch{}, fmt.Errorf("redis snapshot read: %w", err)
	}
	done, entries, err := boundedRedisEntries(entries, limit)
	if err != nil {
		return Batch{}, err
	}
	batch, next, err := s.records(ctx, tenantID, domain, entries)
	if err != nil {
		return Batch{}, err
	}
	batch.Watermark = state.Watermark
	if done {
		batch.Done = true
		batch.NextCursor = ""
		return batch, nil
	}
	state.Last = next
	batch.NextCursor, err = encodeRedisSnapshotCursor(state)
	if err != nil {
		return Batch{}, err
	}
	return batch, nil
}

func (s *RedisSource) Changes(ctx context.Context, tenantID string, domain Domain, watermark int64, limit int) (Batch, error) {
	if err := s.validate(tenantID, domain, limit); err != nil {
		return Batch{}, err
	}
	if watermark < 0 {
		return Batch{}, fmt.Errorf("%w: negative change watermark", ErrInvalidRecord)
	}
	entries, err := s.Client.ZRangeByScoreWithScores(ctx, s.indexKey(tenantID, domain), &redis.ZRangeBy{
		// Read one sentinel row. Changes has no cursor argument, so returning
		// Done=true here is necessary when the page exactly fills the caller's
		// batch; otherwise the next call would repeat an unchanged watermark.
		Min: "(" + strconv.FormatInt(watermark, 10), Max: "+inf", Offset: 0, Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return Batch{}, fmt.Errorf("redis change read: %w", err)
	}
	done, entries, err := boundedRedisEntries(entries, limit)
	if err != nil {
		return Batch{}, err
	}
	batch, _, err := s.records(ctx, tenantID, domain, entries)
	if err != nil {
		return Batch{}, err
	}
	batch.Done = done
	if len(entries) == 0 {
		batch.Watermark = watermark
	}
	if len(entries) > 0 {
		batch.Watermark, err = redisVersion(entries[len(entries)-1].Score)
		if err != nil {
			return Batch{}, err
		}
	}
	if !done {
		// Executor persists this token to prove each bounded CatchUp batch made
		// progress. Changes itself resumes from the durable watermark.
		batch.NextCursor = strconv.FormatInt(batch.Watermark, 10)
	}
	return batch, nil
}

func (s *RedisSource) records(ctx context.Context, tenantID string, domain Domain, entries []redis.Z) (Batch, int64, error) {
	batch := Batch{Records: make([]Record, 0, len(entries))}
	if len(entries) == 0 {
		return batch, 0, nil
	}
	keys := make([]string, len(entries))
	versions := make([]int64, len(entries))
	for i, entry := range entries {
		version, err := redisVersion(entry.Score)
		if err != nil {
			return Batch{}, 0, err
		}
		if i > 0 && version <= versions[i-1] {
			return Batch{}, 0, fmt.Errorf("%w: redis versions must be strictly increasing", ErrInvalidRecord)
		}
		versions[i] = version
		member, ok := entry.Member.(string)
		if !ok || member == "" {
			return Batch{}, 0, fmt.Errorf("%w: redis index member is not a string", ErrInvalidRecord)
		}
		keys[i] = s.VersionedValueKey(tenantID, domain, member, version)
	}
	result, err := boundedRedisReadScript.Run(ctx, s.Client, keys, maxRecordPayloadBytes, maxMigrationBatchPayloadBytes).Result()
	if err != nil {
		if strings.Contains(err.Error(), "MIGRATION_RECORD_MISSING") ||
			strings.Contains(err.Error(), "MIGRATION_RECORD_TOO_LARGE") ||
			strings.Contains(err.Error(), "MIGRATION_BATCH_TOO_LARGE") {
			return Batch{}, 0, fmt.Errorf("%w: redis source payload violates migration bounds", ErrInvalidRecord)
		}
		return Batch{}, 0, fmt.Errorf("redis record read: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != len(keys)*2 {
		return Batch{}, 0, fmt.Errorf("%w: redis source returned an invalid batch", ErrInvalidRecord)
	}
	totalBytes := 0
	for i := range keys {
		deleted, ok := values[i*2].(string)
		if !ok || (deleted != "0" && deleted != "1") {
			return Batch{}, 0, fmt.Errorf("%w: redis source returned invalid deletion marker", ErrInvalidRecord)
		}
		value := values[i*2+1]
		var payload []byte
		switch typed := value.(type) {
		case string:
			payload = []byte(typed)
		case []byte:
			payload = typed
		default:
			return Batch{}, 0, fmt.Errorf("%w: redis record %q has unsupported value", ErrInvalidRecord, entries[i].Member)
		}
		totalBytes += len(payload)
		if len(payload) > maxRecordPayloadBytes || totalBytes > maxMigrationBatchPayloadBytes {
			return Batch{}, 0, fmt.Errorf("%w: redis source payload violates migration bounds", ErrInvalidRecord)
		}
		isDeleted := deleted == "1"
		if isDeleted {
			payload = []byte{}
		}
		record := Record{Key: entries[i].Member.(string), Version: versions[i], Payload: payload, Deleted: isDeleted}
		digest := sha256.Sum256(payload)
		record.Hash = hex.EncodeToString(digest[:])
		batch.Records = append(batch.Records, record)
	}
	return batch, versions[len(versions)-1], nil
}

func (s *RedisSource) validate(tenantID string, domain Domain, limit int) error {
	if s == nil || s.Client == nil || strings.TrimSpace(s.Prefix) == "" {
		return fmt.Errorf("%w: redis source is unavailable", ErrMigrationCapability)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil || !validDomain(domain) {
		return fmt.Errorf("%w: invalid tenant or domain", ErrInvalidMigration)
	}
	if limit <= 0 || limit > 10000 {
		return fmt.Errorf("%w: batch size must be 1..10000", ErrInvalidMigration)
	}
	return nil
}

func (s *RedisSource) indexKey(tenantID string, domain Domain) string {
	return s.IndexKey(tenantID, domain)
}

// IndexKey returns the ordered-set key that dual-write producers must update.
// It is exported so a writer and a migration operator cannot drift in key
// naming while the source remains otherwise read-only.
func (s *RedisSource) IndexKey(tenantID string, domain Domain) string {
	return s.Prefix + ":index:" + tenantID + ":" + string(domain)
}

// VersionedValueKey is the immutable payload key paired with an IndexKey
// member. Producers must write this key before publishing the ZSET member and
// must never mutate it; updates and deletes publish a new version instead.
func (s *RedisSource) VersionedValueKey(tenantID string, domain Domain, member string, version int64) string {
	return s.Prefix + ":value:" + tenantID + ":" + string(domain) + ":" + member + ":" + strconv.FormatInt(version, 10)
}

// VersionedDeletedKey is the explicit deletion marker paired with a
// VersionedValueKey. Producers must retain both keys until the watermark passes.
func (s *RedisSource) VersionedDeletedKey(tenantID string, domain Domain, member string, version int64) string {
	return s.VersionedValueKey(tenantID, domain, member, version) + ":deleted"
}

func redisVersion(score float64) (int64, error) {
	// Redis sorted-set scores are IEEE-754 doubles. Above 2^53-1 distinct
	// integer versions collapse to the same score and a keyset cursor can skip
	// data, so reject them instead of pretending int64 precision is available.
	const maxExactRedisVersion = float64(1<<53 - 1)
	version := int64(score)
	if score <= 0 || score > maxExactRedisVersion || float64(version) != score {
		return 0, fmt.Errorf("%w: invalid redis version %v", ErrInvalidRecord, score)
	}
	return version, nil
}

func boundedRedisEntries(entries []redis.Z, limit int) (bool, []redis.Z, error) {
	if len(entries) <= limit {
		return true, entries, nil
	}
	last, err := redisVersion(entries[limit-1].Score)
	if err != nil {
		return false, nil, err
	}
	sentinel, err := redisVersion(entries[limit].Score)
	if err != nil {
		return false, nil, err
	}
	if sentinel <= last {
		return false, nil, fmt.Errorf("%w: redis versions must be globally unique and increasing", ErrInvalidRecord)
	}
	return false, entries[:limit], nil
}

func encodeRedisSnapshotCursor(cursor redisSnapshotCursor) (string, error) {
	b, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode redis snapshot cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodeRedisSnapshotCursor(value string) (redisSnapshotCursor, error) {
	if value == "" {
		return redisSnapshotCursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return redisSnapshotCursor{}, fmt.Errorf("%w: invalid redis snapshot cursor", ErrInvalidRecord)
	}
	var cursor redisSnapshotCursor
	if err := json.Unmarshal(b, &cursor); err != nil || cursor.Watermark < 0 || cursor.Last < -1 || cursor.Last > cursor.Watermark {
		return redisSnapshotCursor{}, fmt.Errorf("%w: invalid redis snapshot cursor", ErrInvalidRecord)
	}
	return cursor, nil
}
