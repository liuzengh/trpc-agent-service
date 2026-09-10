package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuditQueryRegistryExposition(t *testing.T) {
	registry := &AuditQueryRegistry{}
	registry.ObserveQuery("allowed", false)
	registry.ObserveQuery("allowed", true)
	registry.ObserveQuery("denied", false)
	rec := httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{"trpc_audit_query_total{decision=\"allowed\"} 2", "trpc_audit_query_total{decision=\"denied\"} 1", "trpc_audit_query_cross_tenant_total 1"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

func TestAuditPurgeRegistryExposition(t *testing.T) {
	registry := &AuditPurgeRegistry{}
	registry.Observe(2, 1, 1, 0)
	registry.ObserveDeleted(5)
	rec := httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{"trpc_audit_purge_batch_total{action=\"planned\"} 2", "trpc_audit_purge_batch_total{action=\"executed\"} 1", "trpc_audit_purge_deleted_events_total 5"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
