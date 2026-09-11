package workerhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	"testing"
)

func TestPagedRequestsKeepPathAndQuerySeparate(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		if r.Method != http.MethodGet || r.URL.Query().Get("offset") != "7" || r.URL.Query().Get("limit") != "19" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		seen[key] = true
		w.Header().Set("Content-Type", "application/json")
		switch key {
		case "/internal/v1/management/tenants/tenant-a/runs":
			_, _ = fmt.Fprint(w, `{"runs":[],"offset":7,"limit":19,"total":0}`)
		case "/internal/v1/management/tenants/tenant-a/audit-events":
			_, _ = fmt.Fprint(w, `{"events":[],"offset":7,"limit":19,"total":0}`)
		default:
			t.Errorf("unexpected escaped path %q; raw query %q", key, r.URL.RawQuery)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := New(server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.ListRuns(context.Background(), "tenant-a", 7, 19); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if _, err = client.ListAudit(context.Background(), "tenant-a", 7, 19); err != nil {
		t.Fatalf("list audit: %v", err)
	}
	for _, path := range []string{
		"/internal/v1/management/tenants/tenant-a/runs",
		"/internal/v1/management/tenants/tenant-a/audit-events",
	} {
		if !seen[path] {
			t.Errorf("route not reached: %s", path)
		}
	}
}

func TestApprovalDecisionCarriesAuthenticatedActorAndDigest(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.URL.Path != "/internal/v1/approvals/tenants/tenant-a/operations" {
				t.Errorf("path %s", r.URL.Path)
			}
			_, _ = fmt.Fprint(w, `{"operations":[],"offset":0,"limit":25,"total":0}`)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/approvals/tenants/tenant-a/operations/tap_1/decision" {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		var request approvalv1.WorkerDecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.ActorID != "owner" || request.Action != "approve" || request.ExpectedArgumentsDigest != "sha256:"+strings.Repeat("a", 64) {
			t.Errorf("body %#v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"operation":{"operation_id":"tap_1","tenant_id":"tenant-a","run_id":"run","attempt_id":"attempt","node_id":"assistant","tool_name":"mcp_ticket","tool_resource":"ticket","capability":"test.ticket.status.update","target":"TEST-42","parameter_summary":"close","arguments_digest":"sha256:`+strings.Repeat("a", 64)+`","status":"APPROVED","requested_at":"2026-09-10T00:00:00Z","expires_at":"2026-09-10T00:05:00Z"},"outcome":"DECIDED"}`)
	}))
	defer server.Close()
	client, err := New(server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.ListApprovals(context.Background(), "tenant-a", 0, 25); err != nil {
		t.Fatal(err)
	}
	request := approvalv1.DecisionRequest{Action: "approve", ExpectedArgumentsDigest: "sha256:" + strings.Repeat("a", 64)}
	response, err := client.DecideApproval(context.Background(), "tenant-a", "tap_1", "owner", request)
	if err != nil || response.Outcome != "DECIDED" {
		t.Fatal(response, err)
	}
}
