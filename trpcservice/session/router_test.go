package session

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/noop"
)

func TestValidateBackend(t *testing.T) {
	tests := []struct {
		name    string
		ref     tenant.BackendRef
		wantErr string
	}{
		{
			name: "explicit postgres",
			ref:  tenant.BackendRef{Kind: tenant.BackendSQL, Provider: postgresProvider, Name: "sessions"},
		},
		{
			name:    "missing provider",
			ref:     tenant.BackendRef{Kind: tenant.BackendSQL, Name: "sessions"},
			wantErr: "backend provider is required",
		},
		{
			name: "explicit redis",
			ref:  tenant.BackendRef{Kind: tenant.BackendRedis, Provider: redisProvider, Name: "sessions"},
		},
		{
			name:    "unsupported provider",
			ref:     tenant.BackendRef{Kind: tenant.BackendSQL, Provider: "mysql", Name: "sessions"},
			wantErr: `session provider "mysql" is not supported`,
		},
		{
			name:    "provider kind mismatch",
			ref:     tenant.BackendRef{Kind: tenant.BackendRedis, Provider: postgresProvider, Name: "sessions"},
			wantErr: "does not match",
		},
		{
			name: "inmemory provider",
			ref:  tenant.BackendRef{Kind: tenant.BackendInMemory, Provider: "inmemory", Name: "sessions"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateBackend(test.ref)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateBackend() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateBackend() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestNewRouterAllowsInMemoryOnlyLocalRuntime(t *testing.T) {
	provider := testResolver{}
	router, err := NewRouter(nil, nil, provider)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	defer func() { _ = router.Close() }()
	exec := worker.Execution{Config: tenant.AppConfig{BackendConfig: tenant.BackendConfig{
		Session: tenant.BackendRef{Kind: tenant.BackendInMemory, Provider: inmemoryProvider, Name: "local"},
	}}}
	if _, err := router.ResolveSession(context.Background(), exec); err != nil {
		t.Fatalf("ResolveSession() error = %v", err)
	}
}

type testResolver struct{}

func (testResolver) ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error) {
	return noop.NewService(), nil
}

func (testResolver) Close() error { return nil }
