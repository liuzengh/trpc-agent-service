package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuditPolicyDefaultsAndValidation(t *testing.T) {
	defaults := normalizedAuditPolicy(AuditPolicy{})
	if defaults != DefaultAuditPolicy() {
		t.Fatalf("defaults = %#v", defaults)
	}
	for _, policy := range []AuditPolicy{
		{RetentionDays: 3651, ContentMode: AuditMetadataOnly, HighRiskFailureMode: "fail_closed"},
		{RetentionDays: 90, ContentMode: "raw", HighRiskFailureMode: "fail_closed"},
		{RetentionDays: 90, ContentMode: AuditMetadataOnly, HighRiskFailureMode: "fail_open"},
	} {
		if _, err := policy.Normalize(); err == nil {
			t.Fatalf("invalid policy accepted: %#v", policy)
		}
	}
}

func TestAuditPolicyControlsRetentionAndContent(t *testing.T) {
	center := NewGovernanceCenter()
	clock := time.Now().UTC()
	center.now = func() time.Time { return clock }
	if err := center.SetAuditPolicy("tenant-a", AuditPolicy{RetentionDays: 1, ContentMode: AuditRedactedSummary, HighRiskFailureMode: "fail_closed"}); err != nil {
		t.Fatal(err)
	}
	if err := center.Record(context.Background(), AuditEvent{TenantID: "tenant-a", Decision: "http.allowed", Content: "token=canary password=hunter2", OccurredAt: clock.Add(-2 * 24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := center.Record(context.Background(), AuditEvent{TenantID: "tenant-a", Decision: "http.allowed", Content: "token=canary password=hunter2", OccurredAt: clock}); err != nil {
		t.Fatal(err)
	}
	items := center.AuditEvents(AuditQuery{TenantID: "tenant-a", Limit: 10})
	if len(items) != 1 || strings.Contains(items[0].Content, "canary") || strings.Contains(items[0].Content, "hunter2") {
		t.Fatalf("retained audit content = %#v", items)
	}
	if err := center.SetAuditPolicy("tenant-b", AuditPolicy{RetentionDays: 90, ContentMode: AuditMetadataOnly, HighRiskFailureMode: "fail_closed"}); err != nil {
		t.Fatal(err)
	}
	if err := center.Record(context.Background(), AuditEvent{TenantID: "tenant-b", Decision: "http.allowed", Content: "token=canary", OccurredAt: clock}); err != nil {
		t.Fatal(err)
	}
	items = center.AuditEvents(AuditQuery{TenantID: "tenant-b", Limit: 10})
	if len(items) != 1 || items[0].Content != "" {
		t.Fatalf("metadata-only content = %#v", items)
	}
}

func TestTenantAuditPolicyHTTPIsTenantScopedAndDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-plane.db")
	if err := MigrateSQLiteControlPlane(path); err != nil {
		t.Fatal(err)
	}
	platform, err := NewSQLiteControlPlane(path)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{
		{TenantID: "tenant-a", TenantName: "A", Role: RoleTenantAdmin},
		{TenantID: "tenant-b", TenantName: "B", Role: RoleViewer},
	}})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer handler.Close()
	client := server.Client()
	request := func(method, url, body string) *http.Response {
		req, _ := http.NewRequest(method, server.URL+url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response := request(http.MethodPatch, "/api/v1/admin/tenants/tenant-a", `{"audit_policy":{"retention_days":7,"content_mode":"redacted_summary","high_risk_failure_mode":"fail_closed"}}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("policy update status = %d", response.StatusCode)
	}
	response.Body.Close()
	response = request(http.MethodPatch, "/api/v1/admin/tenants/tenant-b", `{"audit_policy":{"retention_days":7,"content_mode":"metadata_only","high_risk_failure_mode":"fail_closed"}}`)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant update status = %d", response.StatusCode)
	}
	response.Body.Close()
	items, err := platform.listTenants(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policies := make(map[string]AuditPolicy, len(items))
	for _, item := range items {
		policies[item.ID] = item.AuditPolicy
	}
	if policies["tenant-a"].RetentionDays != 7 {
		t.Fatalf("stored policy = %#v", policies["tenant-a"])
	}
}
