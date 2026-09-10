package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

func TestAuditRegistryExportsFixedLowCardinalityMetrics(t *testing.T) {
	registry := &metrics.AuditRegistry{}
	registry.ObserveAuditExport(true, 250*time.Millisecond)
	registry.ObserveAuditExport(false, 750*time.Millisecond)
	registry.ObserveQuarantineAlert()
	registry.ObserveAuditBacklog(audit.Backlog{Pending: 2, Claimed: 3, RetryWait: 4, DeadLetter: 1,
		OldestAge: 5 * time.Second, ObservedAt: time.Now().UTC()}, true)
	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{
		`trpc_audit_export_total{outcome="success"} 1`,
		`trpc_audit_export_total{outcome="error"} 1`,
		`trpc_audit_export_duration_seconds_sum 1`,
		`trpc_quarantine_alert_export_total 1`,
		`trpc_audit_outbox_backlog{state="dead_letter"} 1`,
		`trpc_audit_outbox_lag_seconds{destination="compliance_postgres"} 5`,
		`trpc_audit_outbox_active_backlog 9`,
		`trpc_audit_outbox_lag_max_seconds 5`,
		`trpc_audit_alerting 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q in:\n%s", expected, body)
		}
	}
	for _, forbidden := range []string{"tenant_id", "user_id", "session_id", "request_id"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("high-cardinality label %q exposed", forbidden)
		}
	}
}

func TestAuditRegistryRejectsMutationMethods(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&metrics.AuditRegistry{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("status=%d allow=%q", recorder.Code, recorder.Header().Get("Allow"))
	}
}

func TestAuditRegistryOmitsAutoscalingGaugesWhenSnapshotIsStale(t *testing.T) {
	registry := &metrics.AuditRegistry{SnapshotTTL: time.Second}
	registry.ObserveAuditBacklog(audit.Backlog{Pending: 3, OldestAge: time.Minute, ObservedAt: time.Now().Add(-2 * time.Second)}, true)
	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, "trpc_audit_autoscaling_snapshot_ready 0") || strings.Contains(body, "trpc_audit_outbox_active_backlog") || strings.Contains(body, "trpc_audit_outbox_lag_max_seconds") {
		t.Fatalf("body=%s", body)
	}
}
