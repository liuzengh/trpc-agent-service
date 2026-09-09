//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"crypto/subtle"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsHandler returns a Prometheus handler that fails closed unless an
// operator supplies a bearer token. The unauthenticated mode is intended only
// for an explicitly isolated local development network; it must never be used
// as a production access-control mechanism.
//
// A configured token always takes precedence over allowUnauthenticated. A
// request with more than one Authorization header is rejected to avoid parser
// differentials between proxies and the Go server.
func MetricsHandler(token string, allowUnauthenticated bool) http.Handler {
	token = strings.TrimSpace(token)
	handler := promhttp.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			if !allowUnauthenticated {
				http.NotFound(w, r)
				return
			}
			handler.ServeHTTP(w, r)
			return
		}

		values := r.Header.Values("Authorization")
		const prefix = "Bearer "
		if len(values) != 1 || !strings.HasPrefix(values[0], prefix) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="metrics"`)
			http.Error(w, "metrics authorization required", http.StatusUnauthorized)
			return
		}
		presented := strings.TrimSpace(strings.TrimPrefix(values[0], prefix))
		if presented == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="metrics"`)
			http.Error(w, "metrics authorization required", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// MetricsHandlerFromEnv builds the process metrics endpoint from the
// composition-root environment. Production should set METRICS_AUTH_TOKEN;
// METRICS_ALLOW_UNAUTHENTICATED is reserved for an explicitly isolated local
// development network and is false for every unset or malformed value.
func MetricsHandlerFromEnv() http.Handler {
	allow, _ := strconv.ParseBool(os.Getenv("METRICS_ALLOW_UNAUTHENTICATED"))
	return MetricsHandler(os.Getenv("METRICS_AUTH_TOKEN"), allow)
}
