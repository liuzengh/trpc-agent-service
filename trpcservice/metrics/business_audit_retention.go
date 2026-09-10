package metrics

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
)

// BusinessAuditPurgeRegistry exposes only low-cardinality business-audit
// retention progress. It deliberately does not reuse the compliance-purge
// metrics: the latter carry approval and legal-hold semantics that do not
// apply to source-library cleanup.
type BusinessAuditPurgeRegistry struct {
	mu                                  sync.RWMutex
	planned, completed, skipped, failed uint64
	quarantined                         uint64
	deleted                             uint64
}

func (r *BusinessAuditPurgeRegistry) Observe(stats purgebusiness.Stats) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.planned += uint64(stats.Planned)
	r.completed += uint64(stats.Completed)
	r.skipped += uint64(stats.Skipped)
	r.failed += uint64(stats.Failed)
	r.quarantined += uint64(stats.Quarantined)
	r.deleted += uint64(stats.Deleted)
	r.mu.Unlock()
}

func (r *BusinessAuditPurgeRegistry) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	planned, completed, skipped, failed, quarantined, deleted := r.planned, r.completed, r.skipped, r.failed, r.quarantined, r.deleted
	r.mu.RUnlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(writer, "# HELP trpc_business_audit_purge_batch_total Source audit purge batches by reconciliation action.\n# TYPE trpc_business_audit_purge_batch_total counter\n")
	fmt.Fprintf(writer, "trpc_business_audit_purge_batch_total{action=\"planned\"} %d\ntrpc_business_audit_purge_batch_total{action=\"completed\"} %d\ntrpc_business_audit_purge_batch_total{action=\"skipped\"} %d\ntrpc_business_audit_purge_batch_total{action=\"failed\"} %d\ntrpc_business_audit_purge_batch_total{action=\"quarantined\"} %d\n", planned, completed, skipped, failed, quarantined)
	fmt.Fprint(writer, "# HELP trpc_business_audit_purge_deleted_facts_total Source audit events and published Outbox rows destroyed.\n# TYPE trpc_business_audit_purge_deleted_facts_total counter\n")
	fmt.Fprintf(writer, "trpc_business_audit_purge_deleted_facts_total %d\n", deleted)
}

var _ http.Handler = (*BusinessAuditPurgeRegistry)(nil)
