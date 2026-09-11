// Package storage creates and probes the data services used by the runtime.
package storage

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	postgressession "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

const readinessAppName = "trpc-agent-service-readiness"

type sessionServiceOptions struct {
	summarizer summary.SessionSummarizer
}

type SessionServiceOption func(*sessionServiceOptions)

func WithSessionSummarizer(summarizer summary.SessionSummarizer) SessionServiceOption {
	return func(options *sessionServiceOptions) { options.summarizer = summarizer }
}

// NewSessionService creates the configured tRPC-Agent-Go Session service and
// verifies that its backing store is reachable before returning it.
func NewSessionService(
	ctx context.Context,
	cfg config.SessionConfig,
	optionFunctions ...SessionServiceOption,
) (agentsession.Service, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		service agentsession.Service
		err     error
	)
	options := sessionServiceOptions{}
	for _, option := range optionFunctions {
		if option != nil {
			option(&options)
		}
	}
	switch cfg.Backend {
	case config.SessionBackendInMemory:
		inMemoryOptions := []inmemory.ServiceOpt{}
		if options.summarizer != nil {
			inMemoryOptions = append(inMemoryOptions, inmemory.WithSummarizer(options.summarizer))
		}
		service = inmemory.NewSessionService(inMemoryOptions...)
	case config.SessionBackendRedis:
		redisOptions := []redissession.ServiceOpt{
			redissession.WithRedisClientURL(cfg.RedisURL),
			redissession.WithKeyPrefix(cfg.RedisKeyPrefix),
			redissession.WithSessionTTL(cfg.TTL),
			redissession.WithEnableAsyncPersist(false),
			redissession.WithEnableUserSessionIndex(true),
			redissession.WithCompatMode(redissession.CompatModeNone),
		}
		if options.summarizer != nil {
			redisOptions = append(redisOptions, redissession.WithSummarizer(options.summarizer))
		}
		service, err = redissession.NewService(redisOptions...)
		if err != nil {
			return nil, fmt.Errorf("create Redis session service: %w", err)
		}
	case config.SessionBackendPostgres:
		postgresOptions := []postgressession.ServiceOpt{
			postgressession.WithPostgresClientDSN(cfg.PostgresURL),
			postgressession.WithTablePrefix(cfg.PostgresPrefix),
			postgressession.WithSessionTTL(cfg.TTL),
			postgressession.WithEnableAsyncPersist(false),
		}
		if options.summarizer != nil {
			postgresOptions = append(postgresOptions, postgressession.WithSummarizer(options.summarizer))
		}
		service, err = postgressession.NewService(postgresOptions...)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL session service: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported session backend %q", cfg.Backend)
	}

	if err := ProbeSessionService(ctx, service); err != nil {
		closeErr := service.Close()
		if closeErr != nil {
			return nil, fmt.Errorf(
				"probe session service: %v; close service: %w",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("probe session service: %w", err)
	}
	return service, nil
}

// ProbeSessionService performs a read-only operation against the configured
// backend. For Redis this forces a network round trip and catches bad URLs or
// unavailable servers during startup and readiness checks.
func ProbeSessionService(ctx context.Context, service agentsession.Service) error {
	if service == nil {
		return fmt.Errorf("session service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := service.ListAppStates(ctx, readinessAppName); err != nil {
		return err
	}
	return nil
}
