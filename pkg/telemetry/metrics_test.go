//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandlerFailsClosedWithoutConfiguration(t *testing.T) {
	handler := MetricsHandler("", false)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestMetricsHandlerAllowsExplicitLocalDevelopmentMode(t *testing.T) {
	handler := MetricsHandler("", true)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), "go_goroutines") {
		t.Fatal("metrics response does not contain a standard Prometheus metric")
	}
}

func TestMetricsHandlerRequiresSingleBearerToken(t *testing.T) {
	handler := MetricsHandler("metrics-secret-for-test", true)
	tests := []struct {
		name   string
		header []string
		want   int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "wrong scheme", header: []string{"Basic metrics-secret-for-test"}, want: http.StatusUnauthorized},
		{name: "wrong token", header: []string{"Bearer wrong"}, want: http.StatusUnauthorized},
		{name: "duplicate", header: []string{"Bearer metrics-secret-for-test", "Bearer metrics-secret-for-test"}, want: http.StatusUnauthorized},
		{name: "valid", header: []string{"Bearer metrics-secret-for-test"}, want: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			for _, value := range test.header {
				request.Header.Add("Authorization", value)
			}
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
		})
	}
}
