// Package storage constructs tenant-scoped tRPC-Agent-Go storage services.
package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgebase"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// Services groups storage instances owned by one Runtime Bundle.
type Services struct {
	Session   session.Service
	Memory    memory.Service
	Artifact  artifact.Service
	Knowledge *knowledgebase.Service
	once      sync.Once
	closeErr  error
}

// NewPostgres constructs the Runner services backed by PostgreSQL. Platform
// coordination still uses Redis and the fenced SQL write store.
func NewPostgres(profile tenant.StorageProfile, dsn string, db *sql.DB) (*Services, error) {
	if err := ValidatePostgresProfile(profile); err != nil {
		return nil, err
	}
	if dsn == "" || db == nil {
		return nil, errors.New("storage: PostgreSQL DSN and database are required")
	}
	return newPostgresServices(dsn, dsn, db)
}

func newPostgresServices(sessionDSN, memoryDSN string, artifactDB *sql.DB) (*Services, error) {
	if sessionDSN == "" || memoryDSN == "" || artifactDB == nil {
		return nil, errors.New("storage: routed PostgreSQL services are incomplete")
	}
	sessions, err := newPostgresSession(sessionDSN)
	if err != nil {
		return nil, err
	}
	memories, err := newPostgresMemory(memoryDSN)
	if err != nil {
		_ = sessions.Close()
		return nil, err
	}
	return &Services{Session: sessions, Memory: memories, Artifact: &PostgresArtifactService{DB: artifactDB}}, nil
}

func newPostgresSession(dsn string) (session.Service, error) {
	service, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(dsn),
		sessionpostgres.WithTablePrefix("runtime_"),
		sessionpostgres.WithSkipDBInit(true),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: create PostgreSQL session service: %w", err)
	}
	return service, nil
}

func newRedisSession(rawURL, keyPrefix string) (session.Service, error) {
	if rawURL == "" || keyPrefix == "" {
		return nil, errors.New("storage: Redis session URL and tenant key prefix are required")
	}
	service, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL(rawURL),
		sessionredis.WithKeyPrefix(keyPrefix),
		sessionredis.WithEnableUserSessionIndex(true),
	)
	if err != nil {
		// The upstream URL parser includes the full URL in parse errors. Redis
		// credentials may arrive through SecretRef, so never expose that error.
		return nil, errors.New("storage: create Redis session service failed")
	}
	return service, nil
}

func newPostgresMemory(dsn string) (memory.Service, error) {
	service, err := memorypostgres.NewService(
		memorypostgres.WithPostgresClientDSN(dsn),
		memorypostgres.WithTableName("runtime_memories"),
		memorypostgres.WithSkipDBInit(true),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: create PostgreSQL memory service: %w", err)
	}
	return service, nil
}

// ValidatePostgresProfile checks every declared data domain before startup.
func ValidatePostgresProfile(profile tenant.StorageProfile) error {
	for name, backend := range map[string]tenant.BackendConfig{
		"session": profile.Session, "memory": profile.Memory,
		"summary": profile.Summary, "artifact": profile.Artifact,
		"knowledge": profile.Knowledge, "audit": profile.Audit,
	} {
		if backend.Type != tenant.BackendPostgres {
			return fmt.Errorf("storage: %s backend must be postgres, got %q", name, backend.Type)
		}
	}
	return nil
}

// NewTestServices constructs isolated services for deterministic tests.
func NewTestServices(profile tenant.StorageProfile) (*Services, error) {
	for name, backend := range map[string]tenant.BackendConfig{"session": profile.Session, "memory": profile.Memory, "summary": profile.Summary, "artifact": profile.Artifact, "knowledge": profile.Knowledge, "audit": profile.Audit} {
		if backend.Type != tenant.BackendInMemory {
			return nil, errors.New("storage: " + name + " backend is not available in the offline runtime")
		}
	}
	return &Services{Session: sessioninmemory.NewSessionService(), Memory: memoryinmemory.NewMemoryService(), Artifact: artifactmemory.NewService()}, nil
}

// Close releases services injected into Runner; Runner treats them as borrowed.
func (services *Services) Close() error {
	if services == nil {
		return nil
	}
	services.once.Do(func() {
		var memoryErr, sessionErr, artifactErr, knowledgeErr error
		if services.Memory != nil {
			memoryErr = services.Memory.Close()
		}
		if services.Session != nil {
			sessionErr = services.Session.Close()
		}
		if closer, ok := services.Artifact.(interface{ Close() error }); ok {
			artifactErr = closer.Close()
		}
		if services.Knowledge != nil {
			knowledgeErr = services.Knowledge.Close()
		}
		services.closeErr = errors.Join(memoryErr, sessionErr, artifactErr, knowledgeErr)
	})
	return services.closeErr
}
