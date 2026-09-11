package idempotency

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// New creates and probes the configured idempotency store.
func New(ctx context.Context, cfg config.IdempotencyConfig) (Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var store Store
	switch cfg.Backend {
	case config.IdempotencyBackendLocal:
		store = NewLocalStoreWithCompletedTTL(cfg.CompletedTTL)
	case config.IdempotencyBackendRedis:
		redisStore, err := NewRedisStore(RedisOptions{
			URL:           cfg.RedisURL,
			KeyPrefix:     cfg.RedisPrefix,
			ProcessingTTL: cfg.ProcessingTTL,
			CompletedTTL:  cfg.CompletedTTL,
			RenewInterval: cfg.RenewInterval,
			PollInterval:  cfg.PollInterval,
		})
		if err != nil {
			return nil, err
		}
		store = redisStore
	default:
		return nil, fmt.Errorf("unsupported idempotency backend %q", cfg.Backend)
	}
	if err := store.Ready(ctx); err != nil {
		closeErr := store.Close()
		if closeErr != nil {
			return nil, fmt.Errorf(
				"probe idempotency store: %v; close store: %w",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("probe idempotency store: %w", err)
	}
	return store, nil
}
