// Package storage contains the runtime-owned Redis backend.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redistopology"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/redis/go-redis/v9"
	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

const officialRedisNamespace = "official-v1"

// RedisBackend owns the official Redis session and memory services used by a
// Runtime. Constructing a backend only parses the URL and creates an
// independent health client; the official services are created lazily by
// Ready after the health check succeeds.
//
// The official services each create and own their own Redis client. Keeping
// those clients separate from healthClient prevents one service's Close from
// invalidating another service or the health probe.
type RedisBackend struct {
	healthClient redis.UniversalClient
	prefix       string
	newSession   func() (session.Service, error)
	newMemory    func() (frameworkmemory.Service, error)

	mu           sync.Mutex
	closed       bool
	closeDone    chan struct{}
	session      session.Service
	memory       frameworkmemory.Service
	closeErr     error
	strong       bool
	redisURL     string
	messagingURL string
	fingerprint  persistence.BackendFingerprint
}

func (b *RedisBackend) Fingerprint() persistence.BackendFingerprint { return b.fingerprint }

// NewRedisBackend validates the Redis URL and namespace without contacting
// Redis. The normalized prefix is always isolated below the official-v1
// namespace so Phase 1 snapshot keys are never read or migrated.
func NewRedisBackend(redisURL, prefix string) (*RedisBackend, error) {
	return newRedisBackend(redisURL, prefix, false, prefix, nil, "")
}

// NewFencedRedisBackend creates a Redis backend whose Session service writes
// to the platform-owned fenced-v1 namespace.
func NewFencedRedisBackend(redisURL, prefix string, messagingPrefix ...string) (*RedisBackend, error) {
	return NewFencedRedisBackendWithConfig(redisURL, prefix, sessionfence.Limits{MaxTurnEvents: 512, MaxTurnBytes: 2 << 20}, firstString(messagingPrefix), "")
}

func NewFencedRedisBackendWithLimits(redisURL, prefix string, limits sessionfence.Limits, messagingPrefix ...string) (*RedisBackend, error) {
	return NewFencedRedisBackendWithConfig(redisURL, prefix, limits, firstString(messagingPrefix), "")
}

func NewFencedRedisBackendWithConfig(redisURL, prefix string, limits sessionfence.Limits, messagingPrefix, messagingURL string) (*RedisBackend, error) {
	coordPrefix := prefix
	if strings.TrimSpace(messagingPrefix) != "" {
		coordPrefix = messagingPrefix
	}
	return newRedisBackend(redisURL, prefix, true, coordPrefix, []sessionfence.Limits{limits}, messagingURL)
}

func newRedisBackend(redisURL, prefix string, fenced bool, coordPrefix string, limits []sessionfence.Limits, messagingURL string) (*RedisBackend, error) {
	redisURL = strings.TrimSpace(redisURL)
	if redisURL == "" {
		return nil, errors.New("redis url is empty")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	base := strings.TrimRight(strings.TrimSpace(prefix), ":")
	if base == "" {
		return nil, errors.New("redis key prefix is empty")
	}
	officialPrefix := base + ":" + officialRedisNamespace
	backend := &RedisBackend{
		healthClient: redis.NewClient(options),
		prefix:       officialPrefix,
		closeDone:    make(chan struct{}),
		strong:       fenced,
		redisURL:     redisURL,
		messagingURL: messagingURL,
	}
	backend.newSession = func() (session.Service, error) {
		if fenced {
			// Strong mode stores fenced Session data below the same global
			// prefix as messaging so CompleteTurn can commit both domains in
			// one Redis Lua invocation. tenant/session coordinates remain part
			// of the opaque keys and preserve isolation.
			svc, err := sessionfence.New(redisURL, coordPrefix, coordPrefix)
			if err != nil {
				return nil, err
			}
			if len(limits) > 0 {
				svc.SetLimits(limits[0])
			}
			return svc, nil
		}
		return sessionredis.NewService(
			sessionredis.WithRedisClientURL(redisURL),
			sessionredis.WithKeyPrefix(officialPrefix),
			sessionredis.WithCompatMode(sessionredis.CompatModeNone),
			sessionredis.WithEnableAsyncPersist(false),
			sessionredis.WithSessionTTL(0),
			sessionredis.WithTrackEventTTL(0),
			sessionredis.WithAppStateTTL(0),
			sessionredis.WithUserStateTTL(0),
			sessionredis.WithSessionEventLimit(1000),
			sessionredis.WithEnableUserSessionIndex(true),
			sessionredis.WithEnableTracing(false),
		)
	}
	backend.newMemory = func() (frameworkmemory.Service, error) {
		return memoryredis.NewService(
			memoryredis.WithRedisClientURL(redisURL),
			memoryredis.WithKeyPrefix(officialPrefix),
			memoryredis.WithMemoryLimit(0),
			memoryredis.WithExtractor(nil),
			memoryredis.WithToolEnabled(frameworkmemory.AddToolName, false),
			memoryredis.WithToolEnabled(frameworkmemory.UpdateToolName, false),
			memoryredis.WithToolEnabled(frameworkmemory.SearchToolName, false),
			memoryredis.WithToolEnabled(frameworkmemory.LoadToolName, false),
			memoryredis.WithToolEnabled(frameworkmemory.DeleteToolName, false),
			memoryredis.WithToolEnabled(frameworkmemory.ClearToolName, false),
		)
	}
	return backend, nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// Ready verifies Redis and initializes the official services on the first
// successful probe. If Redis is unavailable, no service is retained and a
// later Ready call retries initialization after recovery.
func (b *RedisBackend) Ready(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("redis backend is closed")
	}
	if err := b.healthClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	if b.strong {
		if err := verifyStrongRedisTopology(ctx, b.healthClient.(*redis.Client), b.redisURL, b.messagingURL); err != nil {
			return err
		}
	}
	if b.session != nil && b.memory != nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Constructors are deliberately after the health probe. memory/redis
	// performs its own Ping and both official services create their own client.
	sessions, err := b.newSession()
	if err != nil {
		return errors.Join(fmt.Errorf("create redis session service: %w", err), closeSession(sessions))
	}
	if sessions == nil {
		return errors.New("create redis session service: factory returned nil service")
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, closeSession(sessions))
	}

	memories, err := b.newMemory()
	if err != nil {
		// A partially constructed service may be returned alongside an error.
		// Release both values before a later Ready call retries construction.
		return errors.Join(
			fmt.Errorf("create redis memory service: %w", err),
			closeMemory(memories),
			closeSession(sessions),
		)
	}
	if memories == nil {
		return errors.Join(
			errors.New("create redis memory service: factory returned nil service"),
			closeSession(sessions),
		)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, closeMemory(memories), closeSession(sessions))
	}

	b.session = sessions
	b.memory = memories
	return nil
}

func verifyStrongRedisTopology(ctx context.Context, client *redis.Client, redisURL, messagingURL string) error {
	runID, err := redistopology.VerifyPrimaryStandalone(ctx, client)
	if err != nil {
		return err
	}
	if strings.TrimSpace(messagingURL) == "" {
		return nil
	}
	messageOpts, err := redis.ParseURL(messagingURL)
	if err != nil {
		return fmt.Errorf("parse messaging Redis URL: %w", err)
	}
	messageClient := redis.NewClient(messageOpts)
	defer messageClient.Close()
	if err := messageClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("messaging Redis ping: %w", err)
	}
	messagingRunID, err := redistopology.VerifyPrimaryStandalone(ctx, messageClient)
	if err != nil {
		return err
	}
	if runID != messagingRunID {
		return errors.New("strong session fencing requires messaging and session Redis to share run_id")
	}
	return nil
}

// Check performs a health probe without constructing or replacing services.
// Runtime uses it to distinguish a transient storage outage from a model or
// agent failure that happens while an already-ready request is running.
func (b *RedisBackend) Check(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("redis backend is closed")
	}
	health := b.healthClient
	b.mu.Unlock()
	return health.Ping(ctx).Err()
}

// Session returns the initialized official session service, or nil before a
// successful Ready call.
func (b *RedisBackend) Session() session.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.session
}

// Memory returns the initialized official memory service, or nil before a
// successful Ready call.
func (b *RedisBackend) Memory() frameworkmemory.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.memory
}

// Prefix returns the isolated key namespace used by the official services.
func (b *RedisBackend) Prefix() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prefix
}

// Close closes official services and then the independent health client. It
// is safe to call concurrently and repeatedly.
func (b *RedisBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		done := b.closeDone
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.closeErr
	}
	b.closed = true
	sessions, memories, health := b.session, b.memory, b.healthClient
	b.session, b.memory = nil, nil
	b.mu.Unlock()

	err := errors.Join(closeSession(sessions), closeMemory(memories), health.Close())
	b.mu.Lock()
	b.closeErr = err
	close(b.closeDone)
	b.mu.Unlock()
	return err
}

func closeSession(service session.Service) error {
	if service == nil {
		return nil
	}
	return service.Close()
}

func closeMemory(service frameworkmemory.Service) error {
	if service == nil {
		return nil
	}
	return service.Close()
}

var _ session.Service = (*sessionredis.Service)(nil)
var _ frameworkmemory.Service = (*memoryredis.Service)(nil)
