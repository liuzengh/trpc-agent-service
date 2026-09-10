package metrics

import (
	"fmt"
	"net/http"
	"sync"
)

// AuditQueryRegistry exposes low-cardinality audit query counters.
type AuditQueryRegistry struct {
	mu          sync.RWMutex
	allowed     uint64
	denied      uint64
	crossTenant uint64
}

func (r *AuditQueryRegistry) ObserveQuery(decision string, crossTenant bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if decision == "allowed" {
		r.allowed++
	} else {
		r.denied++
	}
	if crossTenant {
		r.crossTenant++
	}
}

func (r *AuditQueryRegistry) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	allowed, denied, crossTenant := r.allowed, r.denied, r.crossTenant
	r.mu.RUnlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(writer, "# HELP trpc_audit_query_total Audit queries by stable decision.\n# TYPE trpc_audit_query_total counter\n")
	fmt.Fprintf(writer, "trpc_audit_query_total{decision=\"allowed\"} %d\ntrpc_audit_query_total{decision=\"denied\"} %d\n", allowed, denied)
	fmt.Fprint(writer, "# HELP trpc_audit_query_cross_tenant_total Cross-tenant audit queries.\n# TYPE trpc_audit_query_cross_tenant_total counter\n")
	fmt.Fprintf(writer, "trpc_audit_query_cross_tenant_total %d\n", crossTenant)
}

var _ http.Handler = (*AuditQueryRegistry)(nil)

// AuditPurgeRegistry exposes low-cardinality purge counters and gauges.
type AuditPurgeRegistry struct {
	mu             sync.RWMutex
	planned        uint64
	approved       uint64
	executed       uint64
	quarantined    uint64
	deleted        uint64
	overdueTenants int
	legalHolds     int
}

func (r *AuditPurgeRegistry) Observe(planned, approved, executed, quarantined int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.planned += uint64(planned)
	r.approved += uint64(approved)
	r.executed += uint64(executed)
	r.quarantined += uint64(quarantined)
	r.mu.Unlock()
}

func (r *AuditPurgeRegistry) ObserveDeleted(count int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.deleted += uint64(count)
	r.mu.Unlock()
}

func (r *AuditPurgeRegistry) ObserveGauge(overdueTenants, legalHolds int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.overdueTenants = overdueTenants
	r.legalHolds = legalHolds
	r.mu.Unlock()
}

func (r *AuditPurgeRegistry) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	planned, approved, executed, quarantined, deleted := r.planned, r.approved, r.executed, r.quarantined, r.deleted
	overdue, holds := r.overdueTenants, r.legalHolds
	r.mu.RUnlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(writer, "# HELP trpc_audit_purge_batch_total Purge batches by reconciliation action.\n# TYPE trpc_audit_purge_batch_total counter\n")
	fmt.Fprintf(writer, "trpc_audit_purge_batch_total{action=\"planned\"} %d\ntrpc_audit_purge_batch_total{action=\"approved\"} %d\ntrpc_audit_purge_batch_total{action=\"executed\"} %d\ntrpc_audit_purge_batch_total{action=\"quarantined\"} %d\n",
		planned, approved, executed, quarantined)
	fmt.Fprint(writer, "# HELP trpc_audit_purge_deleted_events_total Compliance audit events destroyed.\n# TYPE trpc_audit_purge_deleted_events_total counter\n")
	fmt.Fprintf(writer, "trpc_audit_purge_deleted_events_total %d\n", deleted)
	fmt.Fprint(writer, "# HELP trpc_audit_purge_overdue_tenants Tenants holding events past their effective retention.\n# TYPE trpc_audit_purge_overdue_tenants gauge\n")
	fmt.Fprintf(writer, "trpc_audit_purge_overdue_tenants %d\n", overdue)
	fmt.Fprint(writer, "# HELP trpc_audit_legal_hold_active Active legal holds blocking purge.\n# TYPE trpc_audit_legal_hold_active gauge\n")
	fmt.Fprintf(writer, "trpc_audit_legal_hold_active %d\n", holds)
}

var _ http.Handler = (*AuditPurgeRegistry)(nil)
