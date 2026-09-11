// router_domains.go extends the per-tenant backend routing beyond session and
// memory to the remaining data domains: vector (knowledge), artifact, and
// audit. Summary is intentionally absent (it follows the session backend).
//
// Each domain is exposed as a thin dispatcher that resolves the tenant's
// data_backend selection per call and delegates to the concrete backend.
// The production backend (Milvus / MinIO / MySQL) may be nil when unconfigured;
// every domain then falls back to its in-memory implementation.
package storage

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

// NewRouterVectorFactory returns a VectorStoreFactory that dispatches each
// knowledge base's vector store to the tenant-selected backend (Milvus by
// default, in-memory opt-in). milvus may be nil (Milvus unconfigured); every
// tenant then falls back to the in-memory store.
func NewRouterVectorFactory(r *Router, milvus, inmem knowledge.VectorStoreFactory) knowledge.VectorStoreFactory {
	if inmem == nil {
		inmem = knowledge.InMemoryVectorStoreFactory()
	}
	return func(ctx context.Context, kb *knowledge.KnowledgeBase) (vectorstore.VectorStore, error) {
		backend, err := r.backendFor(ctx, kb.TenantID, tenant.DomainVector, string(BackendMilvus))
		if err != nil {
			return nil, err
		}
		if backend == string(BackendInMemory) || milvus == nil {
			return inmem(ctx, kb)
		}
		return milvus(ctx, kb)
	}
}

// routerArtifactService dispatches artifact operations to the tenant-selected
// backend (MinIO by default, in-memory opt-in), keyed on sessionInfo.AppName.
type routerArtifactService struct {
	r     *Router
	minio artifact.Service // nil when unconfigured
	inmem artifact.Service
}

// NewRouterArtifactService wraps the per-tenant artifact backend selection.
func NewRouterArtifactService(r *Router, minio, inmem artifact.Service) artifact.Service {
	if inmem == nil {
		inmem = artifactinmemory.NewService()
	}
	return &routerArtifactService{r: r, minio: minio, inmem: inmem}
}

func (s *routerArtifactService) resolve(ctx context.Context, tenantID string) artifact.Service {
	backend, err := s.r.backendFor(ctx, tenantID, tenant.DomainArtifact, string(BackendMinIO))
	if err == nil && backend == string(BackendInMemory) {
		return s.inmem
	}
	if s.minio == nil {
		return s.inmem
	}
	return s.minio
}

func (s *routerArtifactService) SaveArtifact(ctx context.Context, si artifact.SessionInfo, filename string, art *artifact.Artifact) (int, error) {
	return s.resolve(ctx, si.AppName).SaveArtifact(ctx, si, filename, art)
}

func (s *routerArtifactService) LoadArtifact(ctx context.Context, si artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	return s.resolve(ctx, si.AppName).LoadArtifact(ctx, si, filename, version)
}

func (s *routerArtifactService) ListArtifactKeys(ctx context.Context, si artifact.SessionInfo) ([]string, error) {
	return s.resolve(ctx, si.AppName).ListArtifactKeys(ctx, si)
}

func (s *routerArtifactService) DeleteArtifact(ctx context.Context, si artifact.SessionInfo, filename string) error {
	return s.resolve(ctx, si.AppName).DeleteArtifact(ctx, si, filename)
}

func (s *routerArtifactService) ListVersions(ctx context.Context, si artifact.SessionInfo, filename string) ([]int, error) {
	return s.resolve(ctx, si.AppName).ListVersions(ctx, si, filename)
}

// routerAuditRecorder dispatches audit entries (governance decisions) to the
// tenant-selected backend (MySQL by default, in-memory opt-in). Usage metering
// (RecordUsage) always lands in MySQL when available so the budget meter and
// usage reports can query usage_records globally regardless of the tenant's
// audit backend.
type routerAuditRecorder struct {
	r     *Router
	mysql audit.Recorder // nil when unconfigured
	inmem audit.Recorder
}

// NewRouterAuditRecorder wraps the per-tenant audit backend selection.
func NewRouterAuditRecorder(r *Router, mysql, inmem audit.Recorder) audit.Recorder {
	if inmem == nil {
		inmem = audit.NewMemRecorder()
	}
	return &routerAuditRecorder{r: r, mysql: mysql, inmem: inmem}
}

func (a *routerAuditRecorder) resolve(tenantID string) audit.Recorder {
	// Audit is asynchronous and best-effort; the tenant lookup runs on a
	// background context because Record carries no context.
	backend, err := a.r.backendFor(context.Background(), tenantID, tenant.DomainAudit, string(BackendMySQL))
	if err == nil && backend == string(BackendInMemory) {
		return a.inmem
	}
	if a.mysql == nil {
		return a.inmem
	}
	return a.mysql
}

func (a *routerAuditRecorder) Record(e audit.Entry) {
	a.resolve(e.TenantID).Record(e)
}

func (a *routerAuditRecorder) RecordUsage(ctx context.Context, e audit.UsageEntry) error {
	if a.mysql != nil {
		return a.mysql.RecordUsage(ctx, e)
	}
	return a.inmem.RecordUsage(ctx, e)
}

func (a *routerAuditRecorder) Close() error {
	var errs []error
	if a.mysql != nil {
		errs = append(errs, a.mysql.Close())
	}
	errs = append(errs, a.inmem.Close())
	return errors.Join(errs...)
}
