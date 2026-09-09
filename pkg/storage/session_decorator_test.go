package storage

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type decoratedSessionProbe struct {
	session.Service
	calls int
}

func (s *decoratedSessionProbe) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	s.calls++
	return s.Service.CreateSession(ctx, key, state, opts...)
}

func TestSessionDecoratorIsProtectedByStrictExecutionFence(t *testing.T) {
	var probe *decoratedSessionProbe
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		WriteFence: &countingFenceAuthorizer{},
		SessionDecorator: func(_ *tenant.Tenant, inner session.Service) (session.Service, error) {
			probe = &decoratedSessionProbe{Service: inner}
			return probe, nil
		},
	})
	defer adapter.Close()
	value := &tenant.Tenant{ID: "tenant-a", Storage: tenant.StorageConfig{SessionBackend: "inmemory", MemoryBackend: "inmemory"}}
	service, _, release, err := adapter.AcquireServices(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	key := session.Key{AppName: "tsa1:8:tenant-b:support", UserID: "user-a", SessionID: "session-a"}
	if _, err := service.CreateSession(strictFenceContext(nil), key, nil); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("foreign scope accepted: %v", err)
	}
	if probe.calls != 0 {
		t.Fatal("rejected scope reached migration decorator")
	}
	key.AppName = "tsa1:8:tenant-a:support"
	if _, err := service.CreateSession(strictFenceContext(nil), key, nil); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 1 {
		t.Fatalf("valid create did not pass decorator: %d", probe.calls)
	}
}
