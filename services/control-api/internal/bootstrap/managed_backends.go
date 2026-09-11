package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deployment "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	backend "github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
)

// Catalog configuration binds metadata, not runtime implementation availability.
// Never populate adapter maps or runtime_data_capabilities from available entries.
func bindManagedCatalogDigest(c Config, p *deployment.PlatformExecutionContract) error {
	if c.PlatformBackendCatalogSHA256 == "" && c.PlatformBackendTargetsSHA256 == "" {
		return nil
	}
	for _, digest := range []string{c.PlatformBackendCatalogSHA256, c.PlatformBackendTargetsSHA256} {
		b, e := hex.DecodeString(digest)
		if e != nil || len(b) != 32 || hex.EncodeToString(b) != digest {
			return errors.New("backend catalog and targets require pinned SHA256 digests")
		}
	}
	sum := sha256.Sum256([]byte("managed-directory-v1\n" + c.PlatformBackendCatalogSHA256 + "\n" + c.PlatformBackendTargetsSHA256))
	p.ManagedCatalogDigest = "sha256:" + hex.EncodeToString(sum[:])
	return nil
}

type backendTenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
}
type deploymentBackendAccess struct {
	targets *backend.RuntimeCatalog
	tenants backendTenantAccess
}

func (a deploymentBackendAccess) ResolveDeploymentBackend(ctx context.Context, tenant, actor string, r deployment.BackendRequest) (datav1.Snapshot, error) {
	if a.targets == nil || a.tenants == nil {
		return datav1.Snapshot{}, backend.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return datav1.Snapshot{}, err
	}
	ok, err := a.tenants.IsActiveMember(ctx, tenant, actor)
	if err != nil {
		return datav1.Snapshot{}, err
	}
	if !ok {
		return datav1.Snapshot{}, backend.ErrNotAvailable
	}
	return a.targets.ResolveSnapshot(tenant, backend.Selection{BackendID: r.BackendID, Revision: r.Revision, Role: backend.Role(r.Role)})
}

type profileBackendAccess struct{ catalog *backend.Catalog }

func (a profileBackendAccess) CheckBackend(ctx context.Context, tenant, id string, revision uint64, role string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.catalog == nil {
		return backend.ErrUnavailable
	}
	_, err := a.catalog.Resolve(tenant, backend.Selection{BackendID: id, Revision: revision, Role: backend.Role(role)})
	return err
}

// Only role-scoped Memory (PostgreSQL/Redis) or Session (Redis) identities receive passwords.
// Catalog availability alone never grants a runtime principal access.
func (a deploymentBackendAccess) ResolveManagedCredentialAudience(ctx context.Context, tenant, id string, revision uint64, role string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if a.targets == nil {
		return "", backend.ErrUnavailable
	}
	snapshot, err := a.targets.ResolveSnapshot(tenant, backend.Selection{BackendID: id, Revision: revision, Role: backend.Role(role)})
	if err != nil {
		return "", err
	}
	principalOK := role == "memory" && ((snapshot.Kind == datav1.PostgreSQL && snapshot.PostgreSQL != nil && snapshot.PostgreSQL.Username == "memory_runtime") || (snapshot.Kind == datav1.Redis && snapshot.Redis != nil && snapshot.Redis.Username == "memory_runtime"))
	principalOK = principalOK || (role == "session" && snapshot.Kind == datav1.Redis && snapshot.Redis != nil && snapshot.Redis.Username == "session_runtime")
	principalOK = principalOK || (role == "artifact" && snapshot.Kind == datav1.S3 && snapshot.S3 != nil)
	principalOK = principalOK || (role == "knowledge" && snapshot.Kind == datav1.Qdrant && snapshot.Qdrant != nil)
	if !principalOK || snapshot.ValidateForRole(role) != nil {
		return "", backend.ErrCapability
	}
	return snapshot.Digest()
}
