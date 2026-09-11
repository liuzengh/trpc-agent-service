package safego

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunRecoversPanicAndReturnsError(t *testing.T) {
	err := Run("test worker", func() { panic("boom") })
	var panicErr *PanicError
	if !errors.As(err, &panicErr) || panicErr.Component != "test worker" || panicErr.Value != "boom" {
		t.Fatalf("Run() error = %#v", err)
	}
}

func TestGoRecoversPanicAndProcessContinues(t *testing.T) {
	done := make(chan struct{})
	Go("test goroutine", func() {
		defer close(done)
		panic("boom")
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("guarded goroutine did not unwind")
	}
}

func TestRecoverHTTPReturns500AndKeepsServing(t *testing.T) {
	requestCount := 0
	handler := RecoverHTTP(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requestCount++
		if requestCount == 1 {
			panic("bad request state")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first status = %d, want 500", first.Code)
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/healthy", nil))
	if second.Code != http.StatusNoContent {
		t.Fatalf("second status = %d, want 204", second.Code)
	}
}

func TestHTTPErrorLogAcceptsServerDiagnostics(t *testing.T) {
	logger := HTTPErrorLog()
	if logger == nil {
		t.Fatal("HTTPErrorLog() = nil")
	}
	if _, err := logger.Writer().Write([]byte("connection reset\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
}
