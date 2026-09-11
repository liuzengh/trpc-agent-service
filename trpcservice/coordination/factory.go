package coordination

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// New creates and probes the configured Session coordinator.
func New(ctx context.Context, cfg config.CoordinatorConfig) (Coordinator, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var coordinator Coordinator
	switch cfg.Backend {
	case config.CoordinatorBackendLocal:
		coordinator = NewLocalCoordinator()
	case config.CoordinatorBackendRedis:
		redisCoordinator, err := NewRedisCoordinator(RedisOptions{
			URL:           cfg.RedisURL,
			KeyPrefix:     cfg.RedisPrefix,
			LeaseTTL:      cfg.LeaseTTL,
			RenewInterval: cfg.RenewInterval,
			RetryInterval: cfg.RetryInterval,
		})
		if err != nil {
			return nil, err
		}
		coordinator = redisCoordinator
	default:
		return nil, fmt.Errorf("unsupported coordinator backend %q", cfg.Backend)
	}
	if err := coordinator.Ready(ctx); err != nil {
		closeErr := coordinator.Close()
		if closeErr != nil {
			return nil, fmt.Errorf(
				"probe session coordinator: %v; close coordinator: %w",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("probe session coordinator: %w", err)
	}
	return coordinator, nil
}
