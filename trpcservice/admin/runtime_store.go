package admin

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
)

const runtimeConfigKey = "runtime-config"

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
	if err != nil || raw == "" {
		return nil, err
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
