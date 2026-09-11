package workqueue

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func New(ctx context.Context, cfg config.QueueConfig) (Queue, error) {
	switch cfg.Backend {
	case config.QueueBackendMemory:
		return &FairQueue{recent: NewMemoryQueue(128), backlog: NewMemoryQueue(128)}, nil
	case config.QueueBackendRedis:
		opts := RedisOptions{
			URL:          cfg.RedisURL,
			KeyPrefix:    cfg.RedisPrefix,
			Stream:       cfg.Stream,
			Group:        cfg.Group,
			Consumer:     cfg.Consumer,
			BlockTimeout: cfg.BlockTimeout,
			ClaimMinIdle: cfg.ClaimMinIdle,
			MaxLen:       cfg.MaxLen,
		}
		recent, err := NewRedisQueue(ctx, opts)
		if err != nil {
			return nil, err
		}
		opts.Stream += "-backlog"
		backlog, err := NewRedisQueue(ctx, opts)
		if err != nil {
			_ = recent.Close()
			return nil, err
		}
		return &FairQueue{recent: recent, backlog: backlog}, nil
	default:
		return nil, fmt.Errorf("unsupported queue backend %q", cfg.Backend)
	}
}
