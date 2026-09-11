package main

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

// Dependency warm-up. A node that starts before MySQL or Redis is listening
// (container start order, a database failover, a rolling restart) used to stay
// permanently degraded: the in-memory fallback was chosen at startup and
// nothing ever retried. Waiting briefly turns that boot-order race into a short
// delay, and only a real outage still ends in degraded mode.
const (
	dependencyWaitTimeout  = 60 * time.Second
	dependencyPollInterval = 2 * time.Second
)

// retryUntil calls attempt until it succeeds, the timeout elapses, or ctx is
// done, and returns the last error. Kept separate from the dependencies so the
// retry policy is testable without containers.
func retryUntil(ctx context.Context, timeout, interval time.Duration, attempt func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := attempt()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(interval):
		}
	}
}

// openMySQLWhenReady waits for MySQL to accept connections before the stores
// are built. It returns nil when the wait runs out, which leaves the caller in
// its documented degraded mode (no persistence, audit or worker).
func openMySQLWhenReady(ctx context.Context, dsn string, logger *slog.Logger) *sql.DB {
	var db *sql.DB
	err := retryUntil(ctx, dependencyWaitTimeout, dependencyPollInterval, func() error {
		handle, err := storage.OpenMySQL(dsn)
		if err != nil {
			logger.Warn("waiting for mysql", "err", err)
			return err
		}
		db = handle
		return nil
	})
	if db == nil {
		logger.Error("mysql unavailable after waiting, running degraded (no persistence/audit/worker)",
			"waited", dependencyWaitTimeout, "err", err)
		return nil
	}
	logger.Info("mysql ready")
	return db
}

// redisWhenReady waits for Redis to answer PING. It returns false when the wait
// runs out; the data plane still starts (individual commands then fail and the
// stream redelivers), so a late Redis needs no node restart.
func redisWhenReady(ctx context.Context, url string, logger *slog.Logger) bool {
	err := retryUntil(ctx, dependencyWaitTimeout, dependencyPollInterval, func() error {
		rb, err := bus.NewRedisFromURL(url)
		if err != nil {
			return err
		}
		defer func() { _ = rb.Client().Close() }()
		return rb.Client().Ping(ctx).Err()
	})
	if err != nil {
		logger.Warn("redis not reachable yet, starting anyway (commands will retry)", "waited", dependencyWaitTimeout, "err", err)
		return false
	}
	logger.Info("redis ready")
	return true
}
