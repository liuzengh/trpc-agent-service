package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type readinessStub bool

func (s readinessStub) Ready() bool { return bool(s) }

func TestHandlerSeparatesLivenessAndReadiness(t *testing.T) {
	h := Handler{Checker: readinessStub(false)}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("livez=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz=%d", rec.Code)
	}
	h.Checker = readinessStub(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz=%d", rec.Code)
	}
}
