package httpadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
)

type managementFake struct{ calls int }

func (m *managementFake) List(context.Context, string, int, int) (managementv1.RunPage, error) {
	m.calls++
	return managementv1.RunPage{Runs: []managementv1.RunSummary{{RunID: "run"}}, Total: 1}, nil
}
func (m *managementFake) Get(context.Context, string, string) (managementv1.RunDetail, error) {
	m.calls++
	return managementv1.RunDetail{RunSummary: managementv1.RunSummary{RunID: "run"}, AttemptsLog: []managementv1.Attempt{}, Timeline: []managementv1.TimelineEvent{}}, nil
}
func (m *managementFake) Audit(context.Context, string, int, int) (managementv1.AuditPage, error) {
	m.calls++
	return managementv1.AuditPage{Events: []managementv1.AuditEvent{}, Total: 0}, nil
}

func managementRequest(path string, identity string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	if identity != "" {
		u, _ := url.Parse(identity)
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{u}}}}}
	}
	return r
}

func TestManagementRoutesRequireControlIdentity(t *testing.T) {
	reader := &managementFake{}
	h, err := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, Management: reader})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		identity string
		want     int
	}{{"", 401}, {gatewayID, 403}, {controlID, 200}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, managementRequest("/internal/v1/management/tenants/tenant/runs?offset=0&limit=25", tc.identity))
		if w.Code != tc.want {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
	if reader.calls != 1 {
		t.Fatal(reader.calls)
	}
}

func TestManagementRoutesRejectUnknownQueryAndExposeNoPayload(t *testing.T) {
	reader := &managementFake{}
	h, _ := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, Management: reader})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, managementRequest("/internal/v1/management/tenants/tenant/runs?secret=value", controlID))
	if w.Code != 400 || reader.calls != 0 {
		t.Fatal(w.Code, reader.calls)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, managementRequest("/internal/v1/management/tenants/tenant/runs/run", controlID))
	if w.Code != 200 || reader.calls != 1 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, reader.calls)
	}
}
