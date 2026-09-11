package main

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
)

// Approval storage follows coordination configuration. A Redis deployment
// must never silently fall back to process-local authorizations on failure.
func buildApprovalBackend(ctx context.Context, cfg config.CoordinationConfig) (governance.ApprovalBackend, func(), error) {
	noop := func() {}
	switch cfg.Backend {
	case "inmemory":
		return governance.NewApprovalStore(), noop, nil
	case "redis":
		rawURL, err := config.Secret(cfg.RedisURLEnv)
		if err != nil {
			return nil, noop, errors.New("approval Redis secret is unavailable")
		}
		opts, err := redis.ParseURL(rawURL)
		if err != nil {
			return nil, noop, errors.New("configure approval Redis")
		}
		// Consume may have succeeded before a connection breaks. Do not retry
		// the command or grant authorization after an uncertain response.
		opts.MaxRetries = -1
		opts.ContextTimeoutEnabled = true
		opts.DialTimeout = 3 * time.Second
		opts.ReadTimeout = 3 * time.Second
		opts.WriteTimeout = 3 * time.Second
		client := redis.NewClient(opts)
		closeClient := func() { _ = client.Close() }
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := client.Ping(probeCtx).Err(); err != nil {
			closeClient()
			return nil, noop, errors.New("connect approval Redis")
		}
		return governance.NewRedisApprovalStore(client, cfg.KeyPrefix), closeClient, nil
	default:
		return nil, noop, errors.New("unsupported approval coordination backend")
	}
}
