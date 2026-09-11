package storage

import (
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorymysql "trpc.group/trpc-go/trpc-agent-go/memory/mysql"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionmysql "trpc.group/trpc-go/trpc-agent-go/session/mysql"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// backends is the single table that maps a backend id to the constructors the
// session and memory domains use. Both domains share a backend vocabulary (the
// tenant picks one id per domain), so keeping them in one row per backend means
// adding a backend is one table entry instead of two parallel switches that can
// drift apart — the drift is exactly how a domain ends up accepting a backend
// the other one rejects.
type backends struct {
	session func(SessionConfig) (session.Service, error)
	memory  func(MemoryConfig) (memory.Service, error)
}

// backendTable lists every backend the platform can build for these domains.
var backendTable = map[Backend]backends{
	BackendInMemory: {
		session: func(SessionConfig) (session.Service, error) {
			return sessioninmemory.NewSessionService(), nil
		},
		memory: func(MemoryConfig) (memory.Service, error) {
			return memoryinmemory.NewMemoryService(), nil
		},
	},
	BackendRedis: {
		session: func(cfg SessionConfig) (session.Service, error) {
			svc, err := sessionredis.NewService(sessionredis.WithRedisClientURL(cfg.RedisURL))
			if err != nil {
				return nil, fmt.Errorf("storage: redis session: %w", err)
			}
			return svc, nil
		},
		memory: func(cfg MemoryConfig) (memory.Service, error) {
			svc, err := memoryredis.NewService(memoryredis.WithRedisClientURL(cfg.RedisURL))
			if err != nil {
				return nil, fmt.Errorf("storage: redis memory: %w", err)
			}
			return svc, nil
		},
	},
	BackendMySQL: {
		session: func(cfg SessionConfig) (session.Service, error) {
			svc, err := sessionmysql.NewService(sessionmysql.WithMySQLClientDSN(cfg.MySQLDSN))
			if err != nil {
				return nil, fmt.Errorf("storage: mysql session: %w", err)
			}
			return svc, nil
		},
		memory: func(cfg MemoryConfig) (memory.Service, error) {
			svc, err := memorymysql.NewService(memorymysql.WithMySQLClientDSN(cfg.MySQLDSN))
			if err != nil {
				return nil, fmt.Errorf("storage: mysql memory: %w", err)
			}
			return svc, nil
		},
	},
}

// SupportedBackends lists the backend ids the session/memory domains accept.
func SupportedBackends() []Backend {
	out := make([]Backend, 0, len(backendTable))
	for id := range backendTable {
		out = append(out, id)
	}
	return out
}
