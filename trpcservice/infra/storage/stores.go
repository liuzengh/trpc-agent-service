// Unified data-access aggregate.
//
// The platform's storage is organized into data domains (see docs/多后端适配
// 方案.md and CONTEXT.md 数据访问与存储域):
//
//	session / memory / vector / artifact / audit -> per-tenant backend
//	    selection via storage.Router (Tenant.DataBackend); each domain has a
//	    production backend and an in-memory opt-in, dispatched per call.
//	knowledge (metadata) -> knowledge.Manager (vector stores are routed per
//	    tenant by the Router-wrapped factory).
//	summary -> no standalone domain: summaries live in the session backend
//	    (framework session tables).
//
// DataStores is the assembly point for these domains: main builds it once and
// hands the domain implementations on to consumers (currently the worker).
// It also makes the "how each domain is stored" decision explicit and testable
// without scattering construction across call sites.
//
// Routing rule (stage 18 + G1 decision): a domain joins the Router's per-tenant
// selection when a second backend implementation exists. All five domains now
// qualify: session/memory (in-memory/Redis/MySQL), vector (Milvus/in-memory),
// artifact (MinIO/in-memory), audit (MySQL/in-memory).
package storage

import (
	"trpc.group/trpc-go/trpc-agent-go/artifact"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
)

// DataStores aggregates the platform's data-domain backends.
type DataStores struct {
	// Router resolves per-tenant session/memory backends (in-memory / MySQL /
	// Redis) from Tenant.DataBackend; the only domains with multiple backends.
	Router *Router
	// Knowledge is the knowledge-base domain: vector store (Milvus or the
	// in-memory fallback) plus MySQL metadata, exposed as search tools.
	Knowledge *knowledge.Manager
	// Artifacts is the artifact domain: named, versioned files in MinIO (S3),
	// stored through the framework artifact.Service contract.
	Artifacts artifact.Service
	// Auditor is the audit domain: governance decisions + usage accounting
	// written to MySQL asynchronously. nil disables auditing.
	Auditor audit.Recorder
}

// NewDataStores assembles the data-domain backends. Router is required;
// everything else may be nil when the corresponding domain is disabled.
func NewDataStores(router *Router, kb *knowledge.Manager, arts artifact.Service, auditor audit.Recorder) *DataStores {
	return &DataStores{
		Router:    router,
		Knowledge: kb,
		Artifacts: arts,
		Auditor:   auditor,
	}
}
