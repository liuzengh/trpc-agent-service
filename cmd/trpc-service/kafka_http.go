package main

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// KafkaHTTPDependencies are the Gateway HTTP dependencies. External IM ingress
// is owned by long-lived channel receivers; HTTP only serves the authenticated
// console plus health/readiness endpoints.
type KafkaHTTPDependencies struct {
	Observer  metrics.HTTPObserver
	Console   web.ConsoleDependencies
	ConsoleFS fs.FS
	// AuthHandler owns /api/v1/auth/* and applies its own protection to private routes.
	AuthHandler http.Handler
	// Audits receives login-session lifecycle events from the middleware.
	Audits identity.AuditRecorder
}

// NewKafkaHTTPHandler builds routes that do not execute model work inline.
func NewKafkaHTTPHandler(dependencies KafkaHTTPDependencies) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	if dependencies.Console.Configurations == nil {
		return nil, fmt.Errorf("tenant configuration repository is required")
	}
	if dependencies.Console.Producer == nil {
		return nil, fmt.Errorf("Kafka producer is required")
	}
	if dependencies.ConsoleFS != nil {
		if dependencies.AuthHandler == nil {
			return nil, fmt.Errorf("login auth handler is required for the console")
		}
		if dependencies.Console.LoginSessions == nil {
			return nil, fmt.Errorf("login session store is required for the console")
		}
		console, err := web.NewConsoleHandler(dependencies.Console)
		if err != nil {
			return nil, fmt.Errorf("construct platform console: %w", err)
		}
		mux.Handle("/api/v1/", identity.CSRFMiddleware(identity.SessionMiddleware(dependencies.Console.LoginSessions, dependencies.Console.Identities, dependencies.Audits, identity.PasswordChangeMiddleware(console))))
		mux.Handle("/api/v1/auth/", dependencies.AuthHandler)
		spa, err := web.NewConsoleSPAHandler(dependencies.ConsoleFS)
		if err != nil {
			return nil, fmt.Errorf("construct console assets: %w", err)
		}
		mux.Handle("/console/", spa)
	}
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		for name, probe := range dependencies.Console.Probes {
			probeContext, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			err := probe(probeContext)
			cancel()
			if err != nil {
				http.Error(writer, "dependency not ready: "+name, http.StatusServiceUnavailable)
				return
			}
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	if dependencies.Observer != nil {
		return dependencies.Observer.WrapHTTP(mux), nil
	}
	return mux, nil
}

// NewWorkerHTTPHandler exposes only process health for a Worker role. Worker
// pods never mount the Console, authentication routes, webhook ingress, or any
// other Gateway surface.
func NewWorkerHTTPHandler(observer metrics.HTTPObserver, probes map[string]web.DependencyProbe) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		for name, probe := range probes {
			probeContext, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			err := probe(probeContext)
			cancel()
			if err != nil {
				http.Error(writer, "dependency not ready: "+name, http.StatusServiceUnavailable)
				return
			}
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	if observer != nil {
		return observer.WrapHTTP(mux)
	}
	return mux
}
