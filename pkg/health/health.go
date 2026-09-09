//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

// Package health provides dependency health checks for enterprise services.
package health

import (
	"context"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
)

// StatusHealthy is reported when a dependency responds successfully.
const StatusHealthy = "healthy"

// checkTimeout bounds each dependency probe so a hung dependency cannot
// block the health endpoint indefinitely.
const checkTimeout = 2 * time.Second

// Pinger is implemented by dependencies that can be probed with a ping,
// such as *sql.DB and the tenant SQL repository.
type Pinger interface {
	PingContext(ctx context.Context) error
}

// HealthChecker probes service dependencies and reports their status.
// Nil dependencies are skipped, so each service only reports what it uses.
type HealthChecker struct {
	redis   *redis.Client
	storage storage.StorageAdapter
	db      Pinger

	// drain reports whether the process is shutting down. Readiness must fail
	// during the drain so load balancers stop routing new traffic here even
	// though every dependency is still reachable.
	drain interface{ Draining() bool }
}

// Option configures a HealthChecker.
type Option func(*HealthChecker)

// WithRedis registers a Redis client to probe.
func WithRedis(client *redis.Client) Option {
	return func(h *HealthChecker) { h.redis = client }
}

// WithStorage registers a storage adapter to probe.
func WithStorage(adapter storage.StorageAdapter) Option {
	return func(h *HealthChecker) { h.storage = adapter }
}

// WithDatabase registers a database to probe.
func WithDatabase(db Pinger) Option {
	return func(h *HealthChecker) { h.db = db }
}

// WithDrainState makes readiness fail while the given coordinator is draining.
func WithDrainState(d interface{ Draining() bool }) Option {
	return func(h *HealthChecker) { h.drain = d }
}

// New creates a HealthChecker for the given dependencies.
func New(opts ...Option) *HealthChecker {
	h := &HealthChecker{}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Check probes every registered dependency and returns a per-dependency
// status. A dependency is "healthy" only when its probe succeeds; otherwise
// the value carries the failure reason.
func (h *HealthChecker) Check(ctx context.Context) map[string]string {
	if ctx == nil {
		ctx = context.Background()
	}
	status := make(map[string]string)
	if h == nil {
		status["health_checker"] = "unhealthy"
		return status
	}

	if h.redis != nil {
		probeCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		if err := h.redis.Ping(probeCtx).Err(); err != nil {
			// Readiness is frequently exposed through an ingress or cluster
			// control plane. Driver errors can contain hostnames, usernames or
			// connection strings, so the response reports only health state.
			status["redis"] = "unhealthy"
		} else {
			status["redis"] = StatusHealthy
		}
		cancel()
	}

	if h.storage != nil {
		probeCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		if err := h.storage.HealthCheck(probeCtx); err != nil {
			status["storage"] = "unhealthy"
		} else {
			status["storage"] = StatusHealthy
		}
		cancel()
	}

	if h.db != nil {
		probeCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		if err := h.db.PingContext(probeCtx); err != nil {
			status["database"] = "unhealthy"
		} else {
			status["database"] = StatusHealthy
		}
		cancel()
	}

	return status
}

// IsHealthy reports whether every registered dependency is healthy.
func (h *HealthChecker) IsHealthy(ctx context.Context) bool {
	return healthy(h.Check(ctx))
}

// Report returns the health check response body: per-dependency status plus
// an overall status and timestamp, and the HTTP status code to return with it.
func (h *HealthChecker) Report(ctx context.Context) (map[string]interface{}, int) {
	// Once drain has started, readiness is unconditionally false. Avoid probing
	// remote dependencies on this path: a hung database or Redis client must not
	// delay the 503 that tells the load balancer to stop routing here.
	if h != nil && h.drain != nil && h.drain.Draining() {
		return map[string]interface{}{
			"status": "draining",
			"time":   time.Now().Format(time.RFC3339),
			"checks": map[string]string{"shutdown": "draining"},
		}, http.StatusServiceUnavailable
	}
	checks := h.Check(ctx)

	overall := StatusHealthy
	code := http.StatusOK
	if !healthy(checks) {
		overall = "unhealthy"
		code = http.StatusServiceUnavailable
	}

	// A draining process is deliberately not ready, regardless of dependency
	// health, so that in-flight work can finish without new arrivals.
	return map[string]interface{}{
		"status": overall,
		"time":   time.Now().Format(time.RFC3339),
		"checks": checks,
	}, code
}

// healthy reports whether all dependency statuses are healthy.
func healthy(checks map[string]string) bool {
	for _, v := range checks {
		if v != StatusHealthy {
			return false
		}
	}
	return true
}
