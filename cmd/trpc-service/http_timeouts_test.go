package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestHTTPTimeoutDefaultsAndOverrides(t *testing.T) {
	service := config.ServiceConfig{}
	if got := effectiveHTTPReadHeaderTimeout(service); got != 15*time.Second {
		t.Fatalf("default read header timeout = %s", got)
	}
	if got := effectiveHTTPReadTimeout(service); got != 5*time.Minute {
		t.Fatalf("default read timeout = %s", got)
	}
	if got := effectiveHTTPIdleTimeout(service); got != 2*time.Minute {
		t.Fatalf("default idle timeout = %s", got)
	}
	service.HTTPReadHeaderTimeout.Duration = 10 * time.Second
	service.HTTPReadTimeout.Duration = 3 * time.Minute
	service.HTTPIdleTimeout.Duration = 90 * time.Second
	if got := effectiveHTTPReadHeaderTimeout(service); got != 10*time.Second {
		t.Fatalf("configured read header timeout = %s", got)
	}
	if got := effectiveHTTPReadTimeout(service); got != 3*time.Minute {
		t.Fatalf("configured read timeout = %s", got)
	}
	if got := effectiveHTTPIdleTimeout(service); got != 90*time.Second {
		t.Fatalf("configured idle timeout = %s", got)
	}
}

func TestHTTPServerTimeoutContractKeepsStreamingWriteUnlimited(t *testing.T) {
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusTeapot) })
	server := newHTTPServer(handler, 15*time.Second, 5*time.Minute, 2*time.Minute)
	recorder := httptest.NewRecorder()
	server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("handler status = %d, want %d", recorder.Code, http.StatusTeapot)
	}
	if server.ReadHeaderTimeout != 15*time.Second || server.ReadTimeout != 5*time.Minute || server.IdleTimeout != 2*time.Minute {
		t.Fatalf("server timeouts = header %s read %s idle %s", server.ReadHeaderTimeout, server.ReadTimeout, server.IdleTimeout)
	}
	if server.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %s, want 0 to preserve SSE streaming", server.WriteTimeout)
	}
}
