// Package storage builds the platform's shared session backend
// (proposal doc 3.3): in-memory for development, Redis for restart-safe and
// future multi-node sessions. It is the only place importing the framework's
// redis session module, keeping the dependency surface in one file.
package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	redisession "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// SessionConfig is the platform-level session backend selection. Its fields
// mirror config.SessionStorage; main converts between them so this package
// stays free of a config import.
type SessionConfig struct {
	Backend    string        // "" or "memory" | "redis"
	RedisURL   string        // redis://host:port, required for "redis"
	KeyPrefix  string        // namespaces every key; "" uses the backend default
	SessionTTL time.Duration // 0 means no expiration
}

// probeTimeout bounds the startup smoke test so an unreachable Redis fails
// the boot quickly instead of hanging it.
const probeTimeout = 5 * time.Second

// pingTimeout bounds one readiness probe. It is shorter than probeTimeout
// because readiness fires every few seconds and must not stack up when the
// backend is slow: a probe that outlives the interval turns a degraded Redis
// into a probe backlog.
const pingTimeout = 2 * time.Second

// pingAppName is the fixed target of the readiness probe. Nothing ever writes
// app state under it, so the lookup exercises the read path against a key that
// is expected to be absent.
const pingAppName = "readyz"

// NewSessionService builds the configured session backend and probes it once
// (create → append → read back → delete) through the public session.Service
// interface, so a misconfigured or unreachable Redis fails startup instead
// of the first user message.
func NewSessionService(sc SessionConfig) (session.Service, error) {
	var svc session.Service
	switch strings.ToLower(sc.Backend) {
	case "", "memory":
		svc = inmemory.NewSessionService()
	case "redis":
		s, err := redisession.NewService(
			redisession.WithRedisClientURL(sc.RedisURL),
			redisession.WithKeyPrefix(sc.KeyPrefix),
			redisession.WithSessionTTL(sc.SessionTTL),
		)
		if err != nil {
			return nil, fmt.Errorf("storage: build redis session service (%s): %w", sc.RedisURL, err)
		}
		svc = s
	default:
		return nil, fmt.Errorf("storage: unknown session backend %q (want memory or redis)", sc.Backend)
	}
	if err := probe(svc); err != nil {
		_ = svc.Close()
		return nil, fmt.Errorf("storage: session backend %q probe: %w", sc.Backend, err)
	}
	return svc, nil
}

// Ping reports whether the session backend can serve reads right now. It is
// the readiness probe behind /readyz (proposal doc 3.6): unlike the startup
// probe it is strictly read-only, because readiness fires every few seconds
// and must not leave a pair of garbage keys in a shared backend on each beat.
// An absent app state counts as healthy — the point is that the backend
// answered, not that the key exists. A nil svc is unhealthy: it means the
// platform was assembled without a session backend at all.
//
// It reads app state instead of calling GetSession on a probe key on purpose.
// The framework's redis GetSession only logs a warning when its
// checkSessionExists round trip cannot reach Redis, then reports the session as
// absent and returns (nil, nil) — so a GetSession-based probe answers
// "healthy" while the backend is down, which is the exact opposite of what a
// readiness gate is for. ListAppStates maps straight onto HGETALL, returns the
// connection error verbatim, and still writes nothing.
func Ping(ctx context.Context, svc session.Service) error {
	if svc == nil {
		return fmt.Errorf("storage: no session service")
	}
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if _, err := svc.ListAppStates(ctx, pingAppName); err != nil {
		return fmt.Errorf("storage: session backend read: %w", err)
	}
	return nil
}

// probe exercises the same write/read path the runner uses (AppendEvent is
// the hot call in production) and cleans up after itself.
func probe(svc session.Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	key := session.Key{AppName: "probe", UserID: "probe", SessionID: "probe"}
	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		return fmt.Errorf("create probe session: %w", err)
	}
	ev := &event.Event{
		ID:           "probe-event",
		Timestamp:    time.Now(),
		Author:       "probe",
		InvocationID: "probe",
		Response: &model.Response{
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "probe"}}},
		},
	}
	if err := svc.AppendEvent(ctx, sess, ev); err != nil {
		return fmt.Errorf("append probe event: %w", err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("read back probe session: %w", err)
	}
	if got == nil || len(got.Events) == 0 {
		return fmt.Errorf("probe event not visible on read-back")
	}
	if err := svc.DeleteSession(ctx, key); err != nil {
		return fmt.Errorf("delete probe session: %w", err)
	}
	return nil
}
