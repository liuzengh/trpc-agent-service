// Package memorydriver contains the data-plane primitives used by the Memory
// migration authority. It deliberately knows the two storage encodings, while
// Worker and Agent code continues to depend only on memory.Service.
package memorydriver

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/redis/go-redis/v9"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

// Image is the exact framework entry stored in both official backends.
// Coordinates are deliberately kept in the Entry, rather than inferred from
// a Redis key, so malformed or legacy keys cannot widen tenant scope.
type Image struct {
	Entry agentmemory.Entry
}

// RedisSource exports one tenant's official memory/redis hash records. It is
// used only after the migration authority has enabled dual-write; SCAN alone
// is not a consistent snapshot and must never be used as a cutover signal.
type RedisSource struct {
	Client    redis.UniversalClient
	KeyPrefix string
}

// RedisTarget is the reverse-direction counterpart to PostgresTarget.  It
// writes the exact upstream redis-memory hash encoding, so a cutover rollback
// can keep dual-write intact without making Worker depend on Redis internals.
// Every user's records live in one Redis hash, making the replacement atomic
// at EXEC time (and, with the upstream hash tag, in one cluster slot).
type RedisTarget struct {
	Client    redis.UniversalClient
	KeyPrefix string
}

func (s RedisSource) ExportTenant(ctx context.Context, tenantID string) ([]Image, string, error) {
	if s.Client == nil || tenantID == "" || strings.ContainsAny(tenantID, "\x00\r\n") {
		return nil, "", runtime.ErrInvariantViolation
	}
	pattern := "mem:*"
	if s.KeyPrefix != "" {
		pattern = s.KeyPrefix + ":" + pattern
	}
	var cursor uint64
	seen := make(map[string]Image)
	for {
		keys, next, err := s.Client.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return nil, "", fmt.Errorf("scan redis memory: %w", err)
		}
		commands := make([]*redis.MapStringStringCmd, 0, len(keys))
		_, readErr := s.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range keys {
				commands = append(commands, pipe.HGetAll(ctx, key))
			}
			return nil
		})
		if readErr != nil {
			return nil, "", fmt.Errorf("read redis memory hashes: %w", readErr)
		}
		for _, command := range commands {
			values, commandErr := command.Result()
			if commandErr != nil {
				return nil, "", fmt.Errorf("read redis memory hash: %w", commandErr)
			}
			for _, encoded := range values {
				var entry agentmemory.Entry
				if err := json.Unmarshal([]byte(encoded), &entry); err != nil {
					return nil, "", fmt.Errorf("decode redis memory: %w", err)
				}
				// A deployment Redis namespace is shared by tenants. SCAN is
				// deliberately broad because the upstream key format is not a
				// stable tenant partition; entries outside this tenant are normal
				// and must not turn a tenant export into a cross-tenant failure.
				if !strings.HasPrefix(entry.AppName, tenantID+"/") {
					continue
				}
				if err := validateEntry(tenantID, entry); err != nil {
					return nil, "", err
				}
				coordinate := entry.AppName + "\x00" + entry.UserID + "\x00" + entry.ID
				if prior, exists := seen[coordinate]; exists && !sameEntry(prior.Entry, entry) {
					return nil, "", runtime.ErrIdempotencyCollision
				}
				seen[coordinate] = Image{Entry: entry}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	images := make([]Image, 0, len(seen))
	for _, image := range seen {
		images = append(images, image)
	}
	sort.Slice(images, func(i, j int) bool { return coordinate(images[i]) < coordinate(images[j]) })
	digest, err := Digest(images)
	return images, digest, err
}

// ApplyUser replaces precisely one user's Redis hash with the source image.
// It validates all coordinates before issuing MULTI/EXEC; no value from a
// different tenant/app/user can be written into the shared Redis namespace.
func (t RedisTarget) ApplyUser(ctx context.Context, key UserKey, images []Image) (string, error) {
	if t.Client == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if key.TenantID == "" || key.AppName == "" || key.UserID == "" || !strings.HasPrefix(key.AppName, key.TenantID+"/") {
		return "", runtime.ErrTenantScope
	}
	values := make(map[string]string, len(images))
	for _, image := range images {
		entry := image.Entry
		if entry.AppName != key.AppName || entry.UserID != key.UserID || validateEntry(key.TenantID, entry) != nil {
			return "", runtime.ErrTenantScope
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return "", err
		}
		if prior, exists := values[entry.ID]; exists && prior != string(encoded) {
			return "", runtime.ErrIdempotencyCollision
		}
		values[entry.ID] = string(encoded)
	}
	redisKey := redisMemoryKey(t.KeyPrefix, key.AppName, key.UserID)
	_, err := t.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, redisKey)
		if len(values) != 0 {
			pipe.HSet(ctx, redisKey, values)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("replace redis memory user image: %w", err)
	}
	return Digest(images)
}

func redisMemoryKey(prefix, appName, userID string) string {
	base := fmt.Sprintf("mem:{%s:%s}", appName, userID)
	if prefix == "" {
		return base
	}
	if strings.HasSuffix(prefix, ":") {
		return prefix + base
	}
	return prefix + ":" + base
}

// LoadUser is the repair source for a single tenant/app/user coordinate.
// It shares RedisSource's defensive decode and tenant checks, then narrows the
// complete tenant image; this is intentionally conservative over clever key
// parsing because upstream Redis key layout is not a service contract.
func (s RedisSource) LoadUser(ctx context.Context, key UserKey) ([]Image, string, error) {
	if key.TenantID == "" || key.AppName == "" || key.UserID == "" || !strings.HasPrefix(key.AppName, key.TenantID+"/") {
		return nil, "", runtime.ErrTenantScope
	}
	all, _, err := s.ExportTenant(ctx, key.TenantID)
	if err != nil {
		return nil, "", err
	}
	result := make([]Image, 0)
	for _, image := range all {
		if image.Entry.AppName == key.AppName && image.Entry.UserID == key.UserID {
			result = append(result, image)
		}
	}
	digest, err := Digest(result)
	return result, digest, err
}

// PostgresTarget applies an export to the schema owned by this service's
// migration baseline. The upsert is idempotent and rejects a globally reused
// memory ID that belongs to another app/user instead of moving tenant data.
type PostgresTarget struct{ DB *sql.DB }

func (t PostgresTarget) ExportTenant(ctx context.Context, tenantID string) ([]Image, string, error) {
	if t.DB == nil || tenantID == "" {
		return nil, "", runtime.ErrBackendUnavailable
	}
	rows, err := t.DB.QueryContext(ctx, `SELECT memory_data FROM public.memories WHERE app_name LIKE $1 AND deleted_at IS NULL`, tenantID+"/%")
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	images := make([]Image, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, "", err
		}
		var entry agentmemory.Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, "", fmt.Errorf("decode postgres memory: %w", err)
		}
		if err := validateEntry(tenantID, entry); err != nil {
			return nil, "", err
		}
		images = append(images, Image{Entry: entry})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	sort.Slice(images, func(i, j int) bool { return coordinate(images[i]) < coordinate(images[j]) })
	digest, err := Digest(images)
	return images, digest, err
}

func (t PostgresTarget) Apply(ctx context.Context, images []Image) (string, error) {
	if t.DB == nil {
		return "", runtime.ErrBackendUnavailable
	}
	tx, err := t.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	for _, image := range images {
		entry := image.Entry
		if err := validateEntry(tenantFromApp(entry.AppName), entry); err != nil {
			return "", err
		}
		var appName, userID string
		err := tx.QueryRowContext(ctx, `SELECT app_name, user_id FROM public.memories WHERE memory_id = $1 FOR UPDATE`, entry.ID).Scan(&appName, &userID)
		if err != nil && err != sql.ErrNoRows {
			return "", err
		}
		if err == nil && (appName != entry.AppName || userID != entry.UserID) {
			return "", runtime.ErrIdempotencyCollision
		}
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			return "", marshalErr
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO public.memories (memory_id, app_name, user_id, memory_data, created_at, updated_at, deleted_at)
			VALUES ($1,$2,$3,$4,$5,$6,NULL)
			ON CONFLICT (memory_id) DO UPDATE SET memory_data=EXCLUDED.memory_data, updated_at=EXCLUDED.updated_at, deleted_at=NULL`,
			entry.ID, entry.AppName, entry.UserID, data, entry.CreatedAt, entry.UpdatedAt)
		if err != nil {
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return Digest(images)
}

// ApplyUser makes the target exactly match the source user image.  Deletion
// is part of the same transaction as upserts, which is essential for Clear
// and Delete during online dual-write.
func (t PostgresTarget) ApplyUser(ctx context.Context, key UserKey, images []Image) (string, error) {
	if t.DB == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if key.TenantID == "" || key.UserID == "" || !strings.HasPrefix(key.AppName, key.TenantID+"/") {
		return "", runtime.ErrTenantScope
	}
	for _, image := range images {
		if image.Entry.AppName != key.AppName || image.Entry.UserID != key.UserID || validateEntry(key.TenantID, image.Entry) != nil {
			return "", runtime.ErrTenantScope
		}
	}
	tx, err := t.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	ids := make([]string, 0, len(images))
	for _, image := range images {
		entry := image.Entry
		var appName, userID string
		err := tx.QueryRowContext(ctx, `SELECT app_name,user_id FROM public.memories WHERE memory_id=$1 FOR UPDATE`, entry.ID).Scan(&appName, &userID)
		if err != nil && err != sql.ErrNoRows {
			return "", err
		}
		if err == nil && (appName != entry.AppName || userID != entry.UserID) {
			return "", runtime.ErrIdempotencyCollision
		}
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			return "", marshalErr
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO public.memories(memory_id,app_name,user_id,memory_data,created_at,updated_at,deleted_at)
VALUES($1,$2,$3,$4,$5,$6,NULL) ON CONFLICT(memory_id) DO UPDATE SET memory_data=EXCLUDED.memory_data,updated_at=EXCLUDED.updated_at,deleted_at=NULL`,
			entry.ID, entry.AppName, entry.UserID, data, entry.CreatedAt, entry.UpdatedAt); err != nil {
			return "", err
		}
		ids = append(ids, entry.ID)
	}
	if len(ids) == 0 {
		_, err = tx.ExecContext(ctx, `UPDATE public.memories SET deleted_at=clock_timestamp() WHERE app_name=$1 AND user_id=$2 AND deleted_at IS NULL`, key.AppName, key.UserID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE public.memories SET deleted_at=clock_timestamp() WHERE app_name=$1 AND user_id=$2 AND deleted_at IS NULL AND NOT (memory_id=ANY($3))`, key.AppName, key.UserID, ids)
	}
	if err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return Digest(images)
}

func (t PostgresTarget) LoadUser(ctx context.Context, key UserKey) ([]Image, string, error) {
	if t.DB == nil {
		return nil, "", runtime.ErrBackendUnavailable
	}
	if key.TenantID == "" || key.UserID == "" || !strings.HasPrefix(key.AppName, key.TenantID+"/") {
		return nil, "", runtime.ErrTenantScope
	}
	rows, err := t.DB.QueryContext(ctx, `SELECT memory_data FROM public.memories WHERE app_name=$1 AND user_id=$2 AND deleted_at IS NULL`, key.AppName, key.UserID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	images := make([]Image, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, "", err
		}
		var entry agentmemory.Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, "", fmt.Errorf("decode postgres memory: %w", err)
		}
		if entry.AppName != key.AppName || entry.UserID != key.UserID || validateEntry(key.TenantID, entry) != nil {
			return nil, "", runtime.ErrTenantScope
		}
		images = append(images, Image{Entry: entry})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	digest, err := Digest(images)
	return images, digest, err
}

func Digest(images []Image) (string, error) {
	values := make([]string, 0, len(images))
	for _, image := range images {
		if err := validateEntry(tenantFromApp(image.Entry.AppName), image.Entry); err != nil {
			return "", err
		}
		encoded, err := json.Marshal(image.Entry)
		if err != nil {
			return "", err
		}
		values = append(values, coordinate(image)+"\x00"+string(encoded))
	}
	sort.Strings(values)
	sum := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

func coordinate(image Image) string {
	return image.Entry.AppName + "\x00" + image.Entry.UserID + "\x00" + image.Entry.ID
}

func validateEntry(tenantID string, entry agentmemory.Entry) error {
	if tenantID == "" || entry.ID == "" || entry.UserID == "" || entry.Memory == nil || entry.Memory.Memory == "" ||
		entry.AppName == "" || !strings.HasPrefix(entry.AppName, tenantID+"/") || entry.CreatedAt.IsZero() || entry.UpdatedAt.IsZero() {
		return runtime.ErrTenantScope
	}
	return nil
}

func tenantFromApp(appName string) string {
	tenantID, _, _ := strings.Cut(appName, "/")
	return tenantID
}

func sameEntry(left, right agentmemory.Entry) bool {
	leftData, leftErr := json.Marshal(left)
	rightData, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftData) == string(rightData)
}
