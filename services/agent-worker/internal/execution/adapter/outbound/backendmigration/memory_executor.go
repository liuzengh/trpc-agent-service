package backendmigration

import (
	"context"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
)

var ErrBusy = executionv1.ErrBackendMigrationBusy

type MemoryScopeSource interface {
	WithMemoryMigrationScopes(context.Context, string, string, string, func([]memorystore.Scope) error) error
}

type memoryStore interface {
	MemoryTarget
	Close()
}

type MemoryExecutor struct {
	Scopes MemoryScopeSource
	Open   func(context.Context, datav1.Snapshot, string) (memoryStore, error)
}

func (e MemoryExecutor) Execute(ctx context.Context, request executionv1.BackendMigrationRequest) (executionv1.BackendMigrationResponse, error) {
	if e.Scopes == nil || request.TenantID == "" || request.SourceDeploymentRevision == "" || request.AgentID == "" || request.Source.Backend.ValidateForRole("memory") != nil || request.Target.Backend.ValidateForRole("memory") != nil || request.Source.Backend.TenantID != request.TenantID || request.Target.Backend.TenantID != request.TenantID || request.Source.Backend.Matches(request.Target.Backend) {
		return executionv1.BackendMigrationResponse{}, ErrInvalidPlan
	}
	open := e.Open
	if open == nil {
		open = openMemory
	}
	var response executionv1.BackendMigrationResponse
	err := e.Scopes.WithMemoryMigrationScopes(ctx, request.TenantID, request.SourceDeploymentRevision, request.AgentID, func(scopes []memorystore.Scope) error {
		if len(scopes) == 0 {
			return nil
		}
		source, err := open(ctx, request.Source.Backend, request.Source.Password)
		request.Source.Password = ""
		if err != nil {
			return err
		}
		defer source.Close()
		target, err := open(ctx, request.Target.Backend, request.Target.Password)
		request.Target.Password = ""
		if err != nil {
			return err
		}
		defer target.Close()
		result, err := Migrate(ctx, Plan{Memories: scopes}, nil, nil, source, target)
		response.MemoryScopesCopied = result.MemoriesCopied
		return err
	})
	return response, err
}

func openMemory(ctx context.Context, backend datav1.Snapshot, password string) (memoryStore, error) {
	if backend.Limits.MaxBytes <= 0 || uint64(backend.Limits.MaxBytes) > uint64(^uint(0)>>1) {
		return nil, ErrInvalidPlan
	}
	capacity := int(backend.Limits.MaxBytes)
	switch backend.Kind {
	case datav1.PostgreSQL:
		if backend.PostgreSQL == nil {
			return nil, ErrInvalidPlan
		}
		t := backend.PostgreSQL
		target := memorystore.Target{Host: t.Host, Port: uint16(t.Port), Database: t.Database, Username: t.Username, SSLMode: t.SSLMode, MaxConcurrency: int32(backend.Limits.MaxConcurrency)}
		dsn, err := memorystore.CredentialDSN(target, password)
		if err != nil {
			return nil, err
		}
		return memorystore.Open(ctx, dsn, target, capacity)
	case datav1.Redis:
		if backend.Redis == nil {
			return nil, ErrInvalidPlan
		}
		t := backend.Redis
		return memorystore.OpenRedis(ctx, memorystore.RedisTarget{Host: t.Host, Port: uint16(t.Port), Database: int(t.Database), Username: t.Username, TLS: t.TLS, MaxConcurrency: int(backend.Limits.MaxConcurrency)}, password, capacity)
	default:
		return nil, ErrInvalidPlan
	}
}
