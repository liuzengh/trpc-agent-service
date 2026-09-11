package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestRuntimeWorkerAuthenticationUsesVerifiedURIMapping(t *testing.T) {
	uri, _ := url.Parse("spiffe://platform/worker/one")
	unknown, _ := url.Parse("spiffe://platform/worker/unknown")
	c := &RuntimeConfig{Workers: []RuntimeWorkerIdentity{{PrincipalURI: uri.String(), WorkerID: "worker-one"}}}
	for name, state := range map[string]*tls.ConnectionState{
		"plaintext": nil, "unverified": {PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{uri}}}}, "unknown": {VerifiedChains: [][]*x509.Certificate{{{}}}, PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{unknown}}}}, "multiple": {VerifiedChains: [][]*x509.Certificate{{{}}}, PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{uri, unknown}}}}, "valid": {VerifiedChains: [][]*x509.Certificate{{{}}}, PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{uri}}}},
	} {
		t.Run(name, func(t *testing.T) {
			r := gin.New()
			r.GET("/", c.authenticateWorker(), func(g *gin.Context) { g.Status(204) })
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.TLS = state
			req.Header.Set("X-Worker-ID", "worker-one")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if (w.Code == 204) != (name == "valid") {
				t.Fatalf("status %d", w.Code)
			}
		})
	}
}
func TestAppRunStopsRuntimeListener(t *testing.T) {
	public, runtime := newLifecycleServerStub(), newLifecycleServerStub()
	app := &App{server: public, runtimeServer: runtime, shutdownTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if public.shutdownCalls != 1 || runtime.shutdownCalls != 1 {
		t.Fatal("listener lifecycle mismatch")
	}
}
