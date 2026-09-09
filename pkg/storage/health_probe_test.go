package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type healthProbeProfileResolver struct {
	mu       sync.Mutex
	probes   []string
	profiles map[string]BackendProfile
	err      error
}

type panicHealthProbeService struct{}

func (panicHealthProbeService) HealthCheck(context.Context) error { panic("provider secret") }

func TestStorageHealthProbeContainsProviderPanic(t *testing.T) {
	supported, err := probeBackendHealth(context.Background(), panicHealthProbeService{})
	if !supported || !errors.Is(err, ErrBackendHealthProbePanic) {
		t.Fatalf("supported=%v err=%v, want panic error", supported, err)
	}
}

type panicHealthProbeResolver struct{ healthProbeProfileResolver }

func (*panicHealthProbeResolver) HealthCheckBackendProfile(context.Context, string, string) error {
	panic("profile credential")
}

func TestStorageProfileHealthProbeContainsResolverPanic(t *testing.T) {
	checker := &panicHealthProbeResolver{}
	err := checkBackendProfileHealthSafely(checker, context.Background(), "tenant-a", "profile-a")
	if !errors.Is(err, ErrBackendHealthProbePanic) {
		t.Fatalf("profile health error=%v, want ErrBackendHealthProbePanic", err)
	}
}

func (r *healthProbeProfileResolver) ResolveBackendProfile(profileID string) (BackendProfile, error) {
	profile, ok := r.profiles[profileID]
	if !ok {
		return BackendProfile{}, ErrBackendProfileNotFound
	}
	return profile, nil
}

func (r *healthProbeProfileResolver) HealthCheckBackendProfile(_ context.Context, tenantID, profileID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes = append(r.probes, tenantID+":"+profileID)
	return r.err
}

func TestStorageHealthCheckFailsClosedForUnprobedRemoteProfile(t *testing.T) {
	configured := testStorageTenant("tenant-remote", "redis")
	configured.Status = tenant.TenantStatusActive
	configured.Storage.SessionProfile = "shared-redis"
	configured.Storage.MemoryProfile = "shared-redis"
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		RequireBackendHealthProbe: true,
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			return []*tenant.Tenant{configured}, nil
		},
	})
	defer adapter.Close()
	adapter.buildBackend = func(t *tenant.Tenant, _ time.Time) (*backendInstance, error) {
		return &backendInstance{
			sessionService: sessioninmem.NewSessionService(),
			memoryService:  inmemory.NewMemoryService(),
			tenantID:       t.ID,
			sessionProfile: t.Storage.SessionProfile,
			memoryProfile:  t.Storage.MemoryProfile,
		}, nil
	}

	err := adapter.HealthCheck(context.Background())
	if !errors.Is(err, ErrBackendHealthProbeRequired) {
		t.Fatalf("HealthCheck error=%v, want ErrBackendHealthProbeRequired", err)
	}
}

func TestStorageHealthCheckUsesRemoteProfileProbe(t *testing.T) {
	configured := testStorageTenant("tenant-remote", "redis")
	configured.Status = tenant.TenantStatusActive
	configured.Storage.SessionProfile = "shared-redis"
	configured.Storage.MemoryProfile = "shared-redis"
	resolver := &healthProbeProfileResolver{profiles: map[string]BackendProfile{
		"shared-redis": {Backend: "redis", ConnectionString: "rediss://example.invalid"},
	}}
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		RequireBackendHealthProbe: true,
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			return []*tenant.Tenant{configured}, nil
		},
		BackendProfiles: resolver,
	})
	defer adapter.Close()
	adapter.buildBackend = func(t *tenant.Tenant, _ time.Time) (*backendInstance, error) {
		return &backendInstance{
			sessionService: sessioninmem.NewSessionService(),
			memoryService:  inmemory.NewMemoryService(),
			tenantID:       t.ID,
			sessionProfile: t.Storage.SessionProfile,
			memoryProfile:  t.Storage.MemoryProfile,
		}, nil
	}

	if err := adapter.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck error=%v", err)
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.probes) != 2 || resolver.probes[0] != "tenant-remote:shared-redis" || resolver.probes[1] != "tenant-remote:shared-redis" {
		t.Fatalf("profile probes=%v, want one probe per service", resolver.probes)
	}
}
