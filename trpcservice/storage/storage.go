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
