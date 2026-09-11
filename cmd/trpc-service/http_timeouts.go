package main

import (
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

const (
	defaultHTTPReadHeaderTimeout = 15 * time.Second
	defaultHTTPReadTimeout       = 5 * time.Minute
	defaultHTTPIdleTimeout       = 2 * time.Minute
)

func effectiveHTTPReadHeaderTimeout(service config.ServiceConfig) time.Duration {
	if service.HTTPReadHeaderTimeout.Duration > 0 {
		return service.HTTPReadHeaderTimeout.Duration
	}
	return defaultHTTPReadHeaderTimeout
}

func effectiveHTTPReadTimeout(service config.ServiceConfig) time.Duration {
	if service.HTTPReadTimeout.Duration > 0 {
		return service.HTTPReadTimeout.Duration
	}
	return defaultHTTPReadTimeout
}

func effectiveHTTPIdleTimeout(service config.ServiceConfig) time.Duration {
	if service.HTTPIdleTimeout.Duration > 0 {
		return service.HTTPIdleTimeout.Duration
	}
	return defaultHTTPIdleTimeout
}

func newHTTPServer(handler http.Handler, readHeaderTimeout, readTimeout, idleTimeout time.Duration) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		// Intentionally disabled: SSE responses can legitimately remain open
		// beyond conventional request deadlines. Stream liveness is enforced
		// by application-level heartbeat and idle-timeout handling instead.
		WriteTimeout: 0,
	}
}
