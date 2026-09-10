// Package artifact resolves tenant-scoped framework Artifact services. Storage
// implementations remain below this package and never depend on Profile.
package artifact

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	storageartifact "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	sdkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

const domain = "artifact"

// BackendProfileReader is intentionally read-only: turns can use only their
// already published, immutable Backend Profile.
type BackendProfileReader interface {
	GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error)
}

// PostgresStoreBuilder opens a Store from a deployment-owned DSN. Its close
// callback is owned by the Runtime Bundle that requested the Store.
type PostgresStoreBuilder func(context.Context, string, string) (storageartifact.Store, func() error, error)

// Resolver selects an Artifact Store from the immutable artifact binding.
// It adapts existing platform stores to the official framework contract; it
// does not introduce a parallel artifact framework.
type Resolver struct {
	Profiles            BackendProfileReader
	PostgresConnections map[string]string
	Stores              map[string]storageartifact.Store
	BuildPostgres       PostgresStoreBuilder
}

// Resolve returns an exact-AppName SDK service and a Bundle-owned closer.
// The default Store is retained only as a compatibility bridge for snapshots
// published before the artifact binding was introduced.
func (r Resolver) Resolve(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (sdkartifact.Service, func(context.Context) error, error) {
	if ctx == nil || r.Profiles == nil || snapshot.Key.TenantID == "" || snapshot.Key.AgentAppID == "" ||
		snapshot.Key.ConfigVersion < 1 || snapshot.AppName != snapshot.Key.TenantID+"/"+snapshot.Key.AgentAppID {
		return nil, nil, runtime.ErrInvariantViolation
	}
	binding, bound := snapshot.BackendBindingFor(domain)
	if !bound {
		store := r.Stores["default"]
		if store == nil {
			return nil, nil, runtime.ErrCapabilityUnsupported
		}
		return scopedSDKService{app: snapshot.AppName, inner: sdkService(store)}, nil, nil
	}
	if !requires(binding.Required, "strong_ryw") {
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	backend, err := r.Profiles.GetBackend(ctx, snapshot.Key.TenantID, binding.BackendProfileID, binding.BackendVersion)
	if err != nil {
		return nil, nil, err
	}
	if backend.TenantID != snapshot.Key.TenantID || backend.ProfileID != binding.BackendProfileID || backend.Version != binding.BackendVersion {
		return nil, nil, runtime.ErrTenantScope
	}
	if backend.Status != "active" || backend.Provider != "postgres" || (backend.SchemaVersion != 1 && backend.SchemaVersion != 2) ||
		backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 || !backend.Capabilities["strong_ryw"] {
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	connectionID := "default"
	if backend.SchemaVersion == 2 {
		connectionID = backend.Configuration["connection_id"]
	}
	if !validConnectionID(connectionID) {
		return nil, nil, runtime.ErrInvariantViolation
	}
	if store := r.Stores[connectionID]; store != nil {
		return scopedSDKService{app: snapshot.AppName, inner: sdkService(store)}, nil, nil
	}
	dsn, ok := r.PostgresConnections[connectionID]
	if !ok || !validDSN(dsn) || r.BuildPostgres == nil {
		return nil, nil, fmt.Errorf("%w: artifact postgres connection %q is not deployed", runtime.ErrCapabilityUnsupported, connectionID)
	}
	store, closeFn, err := r.BuildPostgres(ctx, connectionID, dsn)
	if err != nil {
		return nil, nil, err
	}
	if store == nil {
		if closeFn != nil {
			_ = closeFn()
		}
		return nil, nil, runtime.ErrInvariantViolation
	}
	return scopedSDKService{app: snapshot.AppName, inner: sdkService(store)}, closer(closeFn), nil
}

func sdkService(store storageartifact.Store) *storageartifact.SDKService {
	return &storageartifact.SDKService{Store: store, MapTenant: func(appName string) string {
		tenantID, err := storageartifact.TenantFromAppName(appName)
		if err != nil {
			return ""
		}
		return tenantID
	}}
}

func closer(fn func() error) func(context.Context) error {
	if fn == nil {
		return nil
	}
	return func(context.Context) error { return fn() }
}

func requires(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func validConnectionID(value string) bool {
	if len(value) == 0 || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validDSN(value string) bool {
	return strings.TrimSpace(value) != "" && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

// scopedSDKService refuses a different AppName even if it belongs to the same
// tenant, closing the scope gap in the upstream SessionInfo contract.
type scopedSDKService struct {
	app   string
	inner sdkartifact.Service
}

func (s scopedSDKService) SaveArtifact(ctx context.Context, info sdkartifact.SessionInfo, filename string, value *sdkartifact.Artifact) (int, error) {
	if !s.allowed(info) {
		return 0, runtime.ErrTenantScope
	}
	return s.inner.SaveArtifact(ctx, info, filename, value)
}
func (s scopedSDKService) LoadArtifact(ctx context.Context, info sdkartifact.SessionInfo, filename string, version *int) (*sdkartifact.Artifact, error) {
	if !s.allowed(info) {
		return nil, runtime.ErrTenantScope
	}
	return s.inner.LoadArtifact(ctx, info, filename, version)
}
func (s scopedSDKService) ListArtifactKeys(ctx context.Context, info sdkartifact.SessionInfo) ([]string, error) {
	if !s.allowed(info) {
		return nil, runtime.ErrTenantScope
	}
	return s.inner.ListArtifactKeys(ctx, info)
}
func (s scopedSDKService) DeleteArtifact(ctx context.Context, info sdkartifact.SessionInfo, filename string) error {
	if !s.allowed(info) {
		return runtime.ErrTenantScope
	}
	return s.inner.DeleteArtifact(ctx, info, filename)
}
func (s scopedSDKService) ListVersions(ctx context.Context, info sdkartifact.SessionInfo, filename string) ([]int, error) {
	if !s.allowed(info) {
		return nil, runtime.ErrTenantScope
	}
	return s.inner.ListVersions(ctx, info, filename)
}
func (s scopedSDKService) allowed(info sdkartifact.SessionInfo) bool {
	return s.inner != nil && info.AppName == s.app
}

var _ sdkartifact.Service = scopedSDKService{}
