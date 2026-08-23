package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

type testReadiness struct{ err error }

func (g testReadiness) Ready(context.Context) error { return g.err }

func TestHealthReflectsReadinessGate(t *testing.T) {
	server := NewServer(platform.NewMemoryStore(), platform.Runner{})
	server.Readiness = testReadiness{err: errors.New("migration blocked")}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("blocked readiness status=%d", response.Code)
	}

	server.Readiness = testReadiness{}
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ready status=%d", response.Code)
	}
}
