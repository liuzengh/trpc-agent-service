package web

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

// NewHandler returns the baseline HTTP handler. The endpoints intentionally
// have no integration dependencies so they can be used for process and probe
// checks before the platform components are connected.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/version", versionHandler)
	return mux
}

// NewHandlerWithRunner adds the stage-0 platform loop and injects the trusted
// tenant context at the server boundary.
func NewHandlerWithRunner(runner platform.RunnerAdapter, tenant platform.TenantContext) http.Handler {
	return NewHandlerWithRunnerAndLifecycle(runner, tenant, nil)
}

// NewHandlerWithRunnerAndLifecycle applies admission control and propagates
// service shutdown to active runner contexts.
func NewHandlerWithRunnerAndLifecycle(runner platform.RunnerAdapter, tenant platform.TenantContext, life *lifecycle.Service) http.Handler {
	return newHandler(runner, tenant, life, nil, false)
}

// NewStage1Handler serves the Stage 1 APIs and packaged Management Console
// while retaining all Stage 0 compatibility endpoints.
func NewStage1Handler(runner platform.RunnerAdapter, tenant platform.TenantContext, life *lifecycle.Service, admin http.Handler) http.Handler {
	return newHandler(runner, tenant, life, admin, true)
}

func newHandler(runner platform.RunnerAdapter, tenant platform.TenantContext, life *lifecycle.Service, admin http.Handler, serveFrontend bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if life != nil && life.IsClosing() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "closing"})
			return
		}
		healthHandler(w, r)
	})
	mux.HandleFunc("/version", versionHandler)
	run := platform.RunHandler{Runner: runner}
	mux.Handle("/v1/run", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if life == nil {
			run.ServeHTTP(w, r.WithContext(platform.WithTenantContext(r.Context(), tenant)))
			return
		}
		release, ok := life.Acquire()
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, map[string]map[string]string{"error": {"code": "service_closing", "message": "service is shutting down"}})
			return
		}
		defer release()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		go func() {
			select {
			case <-life.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
		run.ServeHTTP(w, r.WithContext(platform.WithTenantContext(ctx, tenant)))
	}))
	if admin != nil {
		mux.Handle("/api/", admin)
		mux.Handle("/internal/governance/", admin)
		mux.Handle("/internal/metrics", admin)
	}
	if serveFrontend {
		mux.Handle("/", frontendHandler())
	}
	return mux
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func versionHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": trpcservice.Version})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// These responses are fixed and encoding cannot fail in practice. Keep the
	// handler independent of a logger while still returning valid JSON.
	_ = json.NewEncoder(w).Encode(value)
}
