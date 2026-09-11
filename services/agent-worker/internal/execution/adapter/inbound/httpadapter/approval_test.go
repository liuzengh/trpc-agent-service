package httpadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
)

type approvalManagerStub struct {
	decisions     int
	actor, digest string
}

func (s *approvalManagerStub) List(context.Context, string, int, int) (approvalv1.Page, error) {
	return approvalv1.Page{Operations: []approvalv1.Operation{}, Offset: 0, Limit: 25}, nil
}
func (s *approvalManagerStub) Decide(_ context.Context, tenant, id, actor, action, reason, digest string) (approvalv1.DecisionResponse, error) {
	s.decisions++
	s.actor = actor
	s.digest = digest
	return approvalv1.DecisionResponse{Outcome: "DECIDED", Operation: approvalv1.Operation{TenantID: tenant, OperationID: id, Status: approvalv1.StatusApproved}}, nil
}
func approvalRequest(method, path, identity string, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	u, _ := url.Parse(identity)
	leaf := &x509.Certificate{URIs: []*url.URL{u}}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	return r
}
func TestApprovalEndpointsRequireControlAndForwardDecision(t *testing.T) {
	manager := &approvalManagerStub{}
	h, err := New(&attemptStub{}, &finalStub{}, Options{Approvals: manager, ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, approvalRequest(http.MethodGet, "/internal/v1/approvals/tenants/tenant-a/operations?offset=0&limit=25", gatewayID, ""))
	if w.Code != http.StatusForbidden {
		t.Fatal(w.Code)
	}
	body, _ := json.Marshal(approvalv1.WorkerDecisionRequest{ActorID: "owner", Action: "approve", ExpectedArgumentsDigest: "sha256:" + strings.Repeat("a", 64)})
	w = httptest.NewRecorder()
	h.ServeHTTP(w, approvalRequest(http.MethodPost, "/internal/v1/approvals/tenants/tenant-a/operations/tap_1/decision", controlID, string(body)))
	if w.Code != http.StatusOK || manager.decisions != 1 || manager.actor != "owner" || manager.digest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatal(w.Code, w.Body.String(), manager)
	}
}
