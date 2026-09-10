// Package metrics exposes low-cardinality service metrics. Tenant and request
// identifiers deliberately do not enter this package's label surface.
package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

type AuditRegistry struct {
	mu sync.RWMutex

	backlog       audit.Backlog
	backlogKnown  bool
	alerting      bool
	exportSuccess uint64
	exportError   uint64
	exportNanos   uint64
	quarantine    uint64
	SnapshotTTL   time.Duration
}

func (r *AuditRegistry) ObserveQuarantineAlert() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.quarantine++
	r.mu.Unlock()
}

func (r *AuditRegistry) ObserveAuditExport(success bool, duration time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if success {
		r.exportSuccess++
	} else {
		r.exportError++
	}
	if duration > 0 {
		r.exportNanos += uint64(duration)
	}
}

func (r *AuditRegistry) ObserveAuditBacklog(value audit.Backlog, alerting bool) {
	if r == nil || audit.ValidateBacklog(value) != nil {
		return
	}
	r.mu.Lock()
	r.backlog, r.backlogKnown, r.alerting = value, true, alerting
	r.mu.Unlock()
}

func (r *AuditRegistry) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	value, known, alerting := r.backlog, r.backlogKnown, r.alerting
	success, failed, nanos, quarantine := r.exportSuccess, r.exportError, r.exportNanos, r.quarantine
	r.mu.RUnlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(writer, "# HELP trpc_audit_export_total Audit outbox export attempts by stable outcome.\n# TYPE trpc_audit_export_total counter\ntrpc_audit_export_total{outcome=\"success\"} %d\ntrpc_audit_export_total{outcome=\"error\"} %d\n", success, failed)
	fmt.Fprintf(writer, "# HELP trpc_audit_export_duration_seconds_sum Cumulative audit export processing duration.\n# TYPE trpc_audit_export_duration_seconds_sum counter\ntrpc_audit_export_duration_seconds_sum %g\n", float64(nanos)/float64(time.Second))
	fmt.Fprintf(writer, "# HELP trpc_quarantine_alert_export_total Durable quarantine alerts exported to the compliance sink.\n# TYPE trpc_quarantine_alert_export_total counter\ntrpc_quarantine_alert_export_total %d\n", quarantine)
	fmt.Fprint(writer, "# HELP trpc_audit_backlog_snapshot_ready Whether this process has a valid audit backlog snapshot.\n# TYPE trpc_audit_backlog_snapshot_ready gauge\n")
	if !known {
		fmt.Fprint(writer, "trpc_audit_backlog_snapshot_ready 0\n")
		return
	}
	fmt.Fprint(writer, "trpc_audit_backlog_snapshot_ready 1\n")
	fmt.Fprint(writer, "# HELP trpc_audit_backlog_snapshot_timestamp_seconds Unix timestamp of the last valid audit backlog snapshot.\n# TYPE trpc_audit_backlog_snapshot_timestamp_seconds gauge\n")
	fmt.Fprintf(writer, "trpc_audit_backlog_snapshot_timestamp_seconds %d\n", value.ObservedAt.Unix())
	ttl := r.SnapshotTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now()
	fresh := !value.ObservedAt.After(now) && now.Sub(value.ObservedAt) <= ttl
	fmt.Fprint(writer, "# HELP trpc_audit_autoscaling_snapshot_ready Whether audit autoscaling gauges are fresh.\n# TYPE trpc_audit_autoscaling_snapshot_ready gauge\n")
	if fresh {
		fmt.Fprint(writer, "trpc_audit_autoscaling_snapshot_ready 1\n")
	} else {
		fmt.Fprint(writer, "trpc_audit_autoscaling_snapshot_ready 0\n")
	}
	fmt.Fprint(writer, "# HELP trpc_audit_outbox_backlog Audit outbox rows by fixed state.\n# TYPE trpc_audit_outbox_backlog gauge\n")
	fmt.Fprintf(writer, "trpc_audit_outbox_backlog{state=\"pending\"} %d\ntrpc_audit_outbox_backlog{state=\"claimed\"} %d\ntrpc_audit_outbox_backlog{state=\"retry_wait\"} %d\ntrpc_audit_outbox_backlog{state=\"dead_letter\"} %d\n",
		value.Pending, value.Claimed, value.RetryWait, value.DeadLetter)
	fmt.Fprint(writer, "# HELP trpc_audit_outbox_lag_seconds Age of the oldest active audit outbox row.\n# TYPE trpc_audit_outbox_lag_seconds gauge\n")
	fmt.Fprintf(writer, "trpc_audit_outbox_lag_seconds{destination=\"compliance_postgres\"} %g\n", value.OldestAge.Seconds())
	if fresh {
		fmt.Fprint(writer, "# HELP trpc_audit_outbox_active_backlog Active audit outbox rows for autoscaling.\n# TYPE trpc_audit_outbox_active_backlog gauge\n")
		fmt.Fprintf(writer, "trpc_audit_outbox_active_backlog %d\n", value.Active())
		fmt.Fprint(writer, "# HELP trpc_audit_outbox_lag_max_seconds Audit outbox age for autoscaling.\n# TYPE trpc_audit_outbox_lag_max_seconds gauge\n")
		fmt.Fprintf(writer, "trpc_audit_outbox_lag_max_seconds %g\n", value.OldestAge.Seconds())
	}
	fmt.Fprint(writer, "# HELP trpc_audit_alerting Whether configured audit backlog thresholds are exceeded.\n# TYPE trpc_audit_alerting gauge\n")
	if alerting {
		fmt.Fprint(writer, "trpc_audit_alerting 1\n")
	} else {
		fmt.Fprint(writer, "trpc_audit_alerting 0\n")
	}
}

var _ audit.ExportObserver = (*AuditRegistry)(nil)
var _ audit.QuarantineObserver = (*AuditRegistry)(nil)
var _ audit.BacklogObserver = (*AuditRegistry)(nil)
var _ http.Handler = (*AuditRegistry)(nil)
