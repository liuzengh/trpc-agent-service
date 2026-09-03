package workqueue

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func New(ctx context.Context, cfg config.QueueConfig) (Queue, error) {
	switch cfg.Backend {
	case config.QueueBackendMemory:
		return NewMemoryQueue(128), nil
	case config.QueueBackendRedis:
		return NewRedisQueue(ctx, RedisOptions{
			URL:          cfg.RedisURL,
			KeyPrefix:    cfg.RedisPrefix,
			Stream:       cfg.Stream,
			Group:        cfg.Group,
			Consumer:     cfg.Consumer,
			BlockTimeout: cfg.BlockTimeout,
			ClaimMinIdle: cfg.ClaimMinIdle,
			MaxLen:       cfg.MaxLen,
		})
	default:
		return nil, fmt.Errorf("unsupported queue backend %q", cfg.Backend)
	}
}
