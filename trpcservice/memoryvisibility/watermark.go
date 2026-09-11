// Package memoryvisibility records the durable write watermark used to make
// read-after-write behaviour observable across runtime nodes.
package memoryvisibility

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	ErrVisibilityTimeout    = errors.New("memory visibility watermark timeout")
	ErrBackendUnavailable   = errors.New("memory visibility backend unavailable")
	ErrCrossNodeUnsupported = errors.New("memory visibility is node-local")
)

// Scope is the minimum isolation boundary for a memory watermark.
type Scope struct {
	TenantID    string
	AppName     string
	PrincipalID string
}

func (s Scope) validate() error {
	if s.TenantID == "" || s.AppName == "" || s.PrincipalID == "" {
		return errors.New("memory visibility scope requires tenant, app, and principal")
	}
	return nil
}

// Watermark is a backend-observed value, not a local guess. Epoch changes
// identify a restarted backend writer and LastError explains a stalled value.
type Watermark struct {
	Scope     Scope
	Epoch     string
	Value     int64
	UpdatedAt time.Time
	LastError string
	CrossNode bool
}

// ReadResult deliberately returns a degraded result on a timeout. Callers may
// decide whether eventual reads are acceptable without mistaking them for a
// strong read-after-write success.
type ReadResult struct {
	Watermark Watermark
	Visible   bool
	Degraded  bool
}

type minimumContextKey struct{}

// WithMinimum attaches a required watermark to a memory read context. A
// wrapped runtime backend waits on the shared store before serving the read.
func WithMinimum(ctx context.Context, minimum int64) context.Context {
	return context.WithValue(ctxOrBackground(ctx), minimumContextKey{}, minimum)
}

func Minimum(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	value, ok := ctx.Value(minimumContextKey{}).(int64)
	return value, ok
}

type Store interface {
	WriteWatermark(context.Context, Scope) (Watermark, error)
	ReadAtLeast(context.Context, Scope, int64) (ReadResult, error)
	WaitUntilVisible(context.Context, Scope, int64) (ReadResult, error)
	CrossNode() bool
	Close() error
}

type Postgres struct {
	pool  *pgxpool.Pool
	epoch string
	poll  time.Duration
}

func NewPostgres(pool *pgxpool.Pool, epoch string) (*Postgres, error) {
	if pool == nil {
		return nil, errors.New("memory visibility postgres pool is nil")
	}
	if epoch == "" {
		return nil, errors.New("memory visibility postgres epoch is required")
	}
	return &Postgres{pool: pool, epoch: epoch, poll: 25 * time.Millisecond}, nil
}

func (p *Postgres) Close() error    { return nil }
func (p *Postgres) CrossNode() bool { return true }

func (p *Postgres) WriteWatermark(ctx context.Context, scope Scope) (result Watermark, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.write", "postgres", scope.TenantID, "")
	defer func() { finish(err) }()
	if err := scope.validate(); err != nil {
		return Watermark{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err = p.pool.QueryRow(ctx, `
		INSERT INTO memory_visibility_watermarks
			(tenant_id, app_name, principal_id, backend_epoch, watermark, last_error)
		VALUES ($1, $2, $3, $4, 1, '')
		ON CONFLICT (tenant_id, app_name, principal_id) DO UPDATE
		SET backend_epoch = EXCLUDED.backend_epoch,
		    watermark = memory_visibility_watermarks.watermark + 1,
		    updated_at = clock_timestamp(),
		    last_error = ''
		RETURNING backend_epoch, watermark, updated_at, last_error`,
		scope.TenantID, scope.AppName, scope.PrincipalID, p.epoch,
	).Scan(&result.Epoch, &result.Value, &result.UpdatedAt, &result.LastError)
	if err != nil {
		return Watermark{}, fmt.Errorf("%w: write watermark: %v", ErrBackendUnavailable, err)
	}
	result.Scope, result.CrossNode = scope, true
	return result, nil
}

func (p *Postgres) read(ctx context.Context, scope Scope) (ReadResult, error) {
	if err := scope.validate(); err != nil {
		return ReadResult{}, err
	}
	var result ReadResult
	var watermark Watermark
	err := p.pool.QueryRow(ctx, `
		SELECT backend_epoch, watermark, updated_at, last_error
		FROM memory_visibility_watermarks
		WHERE tenant_id = $1 AND app_name = $2 AND principal_id = $3`,
		scope.TenantID, scope.AppName, scope.PrincipalID,
	).Scan(&watermark.Epoch, &watermark.Value, &watermark.UpdatedAt, &watermark.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		watermark = Watermark{Scope: scope, CrossNode: true}
	} else if err != nil {
		return ReadResult{}, fmt.Errorf("%w: read watermark: %v", ErrBackendUnavailable, err)
	}
	watermark.Scope, watermark.CrossNode = scope, true
	result.Watermark = watermark
	return result, nil
}

func (p *Postgres) ReadAtLeast(ctx context.Context, scope Scope, minimum int64) (result ReadResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.read", "postgres", scope.TenantID, "")
	defer func() { finish(err) }()
	if minimum < 0 {
		return ReadResult{}, errors.New("memory visibility minimum must be non-negative")
	}
	result, err = p.read(ctxOrBackground(ctx), scope)
	if err != nil {
		return ReadResult{}, err
	}
	result.Visible = result.Watermark.Value >= minimum
	return result, nil
}

func (p *Postgres) WaitUntilVisible(ctx context.Context, scope Scope, minimum int64) (result ReadResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.wait", "postgres", scope.TenantID, "")
	defer func() { finish(err) }()
	if minimum < 0 {
		return ReadResult{}, errors.New("memory visibility minimum must be non-negative")
	}
	ctx = ctxOrBackground(ctx)
	for {
		result, err = p.ReadAtLeast(ctx, scope, minimum)
		if err != nil {
			return ReadResult{}, err
		}
		if result.Visible {
			return result, nil
		}
		select {
		case <-ctx.Done():
			result.Degraded = true
			return result, fmt.Errorf("%w: %v", ErrVisibilityTimeout, ctx.Err())
		case <-time.After(p.poll):
		}
	}
}

type Redis struct {
	client *redis.Client
	prefix string
	epoch  string
	poll   time.Duration
}

func NewRedis(client *redis.Client, prefix, epoch string) (*Redis, error) {
	if client == nil {
		return nil, errors.New("memory visibility redis client is nil")
	}
	if prefix == "" {
		prefix = "trpc:memory-visibility"
	}
	if epoch == "" {
		return nil, errors.New("memory visibility redis epoch is required")
	}
	return &Redis{client: client, prefix: prefix, epoch: epoch, poll: 25 * time.Millisecond}, nil
}

func (r *Redis) Close() error    { return nil }
func (r *Redis) CrossNode() bool { return true }

func (r *Redis) key(scope Scope) (string, error) {
	if err := scope.validate(); err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(scope.TenantID + "\x1f" + scope.AppName + "\x1f" + scope.PrincipalID))
	return r.prefix + ":" + hex.EncodeToString(hash[:]), nil
}

var redisWriteWatermark = redis.NewScript(`
	local value = redis.call('HINCRBY', KEYS[1], 'watermark', 1)
	redis.call('HSET', KEYS[1], 'epoch', ARGV[1], 'updated_ns', ARGV[2], 'last_error', '')
	return {value, ARGV[1], ARGV[2], ''}
`)

func (r *Redis) WriteWatermark(ctx context.Context, scope Scope) (result Watermark, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.write", "redis", scope.TenantID, "")
	defer func() { finish(err) }()
	key, err := r.key(scope)
	if err != nil {
		return Watermark{}, err
	}
	values, err := redisWriteWatermark.Run(ctxOrBackground(ctx), r.client, []string{key}, r.epoch, time.Now().UnixNano()).Result()
	if err != nil {
		return Watermark{}, fmt.Errorf("%w: write watermark: %v", ErrBackendUnavailable, err)
	}
	items, ok := values.([]interface{})
	if !ok || len(items) < 4 {
		return Watermark{}, fmt.Errorf("%w: malformed redis watermark response", ErrBackendUnavailable)
	}
	value, err := redisValueInt64(items[0])
	if err != nil {
		return Watermark{}, fmt.Errorf("%w: malformed redis watermark value: %v", ErrBackendUnavailable, err)
	}
	return Watermark{Scope: scope, Epoch: fmt.Sprint(items[1]), Value: value, UpdatedAt: time.Now(), CrossNode: true}, nil
}

func (r *Redis) read(ctx context.Context, scope Scope) (ReadResult, error) {
	key, err := r.key(scope)
	if err != nil {
		return ReadResult{}, err
	}
	values, err := r.client.HMGet(ctxOrBackground(ctx), key, "watermark", "epoch", "updated_ns", "last_error").Result()
	if err != nil {
		return ReadResult{}, fmt.Errorf("%w: read watermark: %v", ErrBackendUnavailable, err)
	}
	result := ReadResult{Watermark: Watermark{Scope: scope, CrossNode: true}}
	if len(values) == 0 || values[0] == nil {
		return result, nil
	}
	value, err := redisValueInt64(values[0])
	if err != nil {
		return ReadResult{}, fmt.Errorf("%w: malformed redis watermark: %v", ErrBackendUnavailable, err)
	}
	result.Watermark.Value = value
	result.Watermark.Epoch = fmt.Sprint(values[1])
	if ns, err := redisValueInt64(values[2]); err == nil {
		result.Watermark.UpdatedAt = time.Unix(0, ns)
	}
	if values[3] != nil {
		result.Watermark.LastError = fmt.Sprint(values[3])
	}
	return result, nil
}

func (r *Redis) ReadAtLeast(ctx context.Context, scope Scope, minimum int64) (result ReadResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.read", "redis", scope.TenantID, "")
	defer func() { finish(err) }()
	if minimum < 0 {
		return ReadResult{}, errors.New("memory visibility minimum must be non-negative")
	}
	result, err = r.read(ctxOrBackground(ctx), scope)
	if err != nil {
		return ReadResult{}, err
	}
	result.Visible = result.Watermark.Value >= minimum
	return result, nil
}

func (r *Redis) WaitUntilVisible(ctx context.Context, scope Scope, minimum int64) (result ReadResult, err error) {
	ctx, finish := observability.StartStorage(ctx, "memory.watermark.wait", "redis", scope.TenantID, "")
	defer func() { finish(err) }()
	ctx = ctxOrBackground(ctx)
	for {
		result, err = r.ReadAtLeast(ctx, scope, minimum)
		if err != nil {
			return ReadResult{}, err
		}
		if result.Visible {
			return result, nil
		}
		select {
		case <-ctx.Done():
			result.Degraded = true
			return result, fmt.Errorf("%w: %v", ErrVisibilityTimeout, ctx.Err())
		case <-time.After(r.poll):
		}
	}
}

func redisValueInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		var parsed int64
		_, err := fmt.Sscan(v, &parsed)
		return parsed, err
	case []byte:
		var parsed int64
		_, err := fmt.Sscan(string(v), &parsed)
		return parsed, err
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

// Memory is intentionally node-local. It is useful for tests and demo mode,
// but CrossNode reports false so callers cannot claim a distributed guarantee.
type Memory struct {
	mu     sync.Mutex
	nodeID string
	epoch  string
	values map[Scope]Watermark
}

func NewMemory(nodeID string) *Memory {
	if nodeID == "" {
		nodeID = "memory"
	}
	return &Memory{nodeID: nodeID, epoch: fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()), values: make(map[Scope]Watermark)}
}

func (m *Memory) Close() error    { return nil }
func (m *Memory) CrossNode() bool { return false }

func (m *Memory) WriteWatermark(_ context.Context, scope Scope) (Watermark, error) {
	if err := scope.validate(); err != nil {
		return Watermark{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value := m.values[scope]
	value.Scope, value.Epoch, value.CrossNode = scope, m.epoch, false
	value.Value++
	value.UpdatedAt = time.Now()
	value.LastError = ""
	m.values[scope] = value
	return value, nil
}

func (m *Memory) ReadAtLeast(_ context.Context, scope Scope, minimum int64) (ReadResult, error) {
	if minimum < 0 {
		return ReadResult{}, errors.New("memory visibility minimum must be non-negative")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value := m.values[scope]
	value.Scope, value.Epoch, value.CrossNode = scope, m.epoch, false
	return ReadResult{Watermark: value, Visible: value.Value >= minimum}, nil
}

func (m *Memory) WaitUntilVisible(ctx context.Context, scope Scope, minimum int64) (ReadResult, error) {
	result, err := m.ReadAtLeast(ctx, scope, minimum)
	if err != nil || result.Visible {
		return result, err
	}
	result.Degraded = true
	return result, fmt.Errorf("%w: in-memory visibility cannot wait across nodes", ErrCrossNodeUnsupported)
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
