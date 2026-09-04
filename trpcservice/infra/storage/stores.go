// Unified data-access aggregate.
//
// The platform's storage is organized into data domains (see docs/存储与数据
// 访问设计.md and CONTEXT.md 数据访问与存储域):
//
//	session / memory  -> storage.Router  (per-tenant backend selection)
//	knowledge (vector) -> knowledge.Manager
//	artifact           -> artifact.Service (MinIO, S3)
//	audit              -> audit.Recorder   (MySQL, async batch)
//	summary            -> no standalone domain: summaries live in the session
//	                     backend (framework session/mysql tables)
//
// DataStores is the assembly point for these domains: main builds it once and
// hands the domain implementations on to consumers (currently the worker).
// It also makes the "how each domain is stored" decision explicit and testable
// without scattering construction across call sites.
//
// Expansion rule (stage 18 decision): a domain joins storage.Router's
// per-tenant backend selection only when a second backend implementation
// actually exists. Until then a domain keeps its single production backend
// referenced here, and Tenant.DataBackend remains the reserved extension
// point (domain -> backend value).
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
