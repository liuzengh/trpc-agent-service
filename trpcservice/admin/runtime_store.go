package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
)

const runtimeConfigKey = "runtime-config"

// ErrSnapshotUnavailable marks a Load that could not reach the snapshot
// store at all, as opposed to a snapshot that exists but cannot be parsed.
// The distinction matters at boot: an unreachable store is the very failure
// the session probe reports a few lines later with a message that names the
// dependency, so the caller may fall back to the file config; a broken
// snapshot is data corruption and must not be silently ignored.
var ErrSnapshotUnavailable = errors.New("runtime config: snapshot store unavailable")

// RuntimeStore preserves live admin changes independently of a Pod's writable
// filesystem. Redis is used only when the platform already selected Redis as
// its shared backend.
type RuntimeStore interface {
	Load(context.Context) (*config.Config, error)
	Save(context.Context, *config.Config) error
}

type redisRuntimeStore struct{ state coordination.StateStore }

// NewRedisRuntimeStore returns a config snapshot store over the shared
// coordinator namespace.
func NewRedisRuntimeStore(state coordination.StateStore) RuntimeStore {
	return &redisRuntimeStore{state: state}
}

func (s *redisRuntimeStore) Load(ctx context.Context) (*config.Config, error) {
	raw, err := s.state.Get(ctx, runtimeConfigKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotUnavailable, err)
	}
	if raw == "" {
		return nil, nil
	}
	cfg, err := config.LoadBytes([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("parse persisted runtime config: %w", err)
	}
	return cfg, nil
}

func (s *redisRuntimeStore) Save(ctx context.Context, cfg *config.Config) error {
	raw, err := config.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.state.Set(ctx, runtimeConfigKey, string(raw))
}
