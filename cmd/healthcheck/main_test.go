package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunAcceptsReadyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Fatalf("path = %q, want /readyz", request.URL.Path)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := run([]string{"-address", server.URL}, server.Client()); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

func TestRunRejectsUnreadyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if err := run([]string{"-address", server.URL}, server.Client()); err == nil {
		t.Fatal("run() error = nil, want readiness failure")
	}
}

func TestCheckURLRejectsUnsafeConfiguration(t *testing.T) {
	for _, value := range []config{
		{address: "https://127.0.0.1:8080", path: "/readyz", timeout: 1},
		{address: "http://127.0.0.1:8080?x=1", path: "/readyz", timeout: 1},
		{address: "http://127.0.0.1:8080", path: "readyz", timeout: 1},
		{address: "http://127.0.0.1:8080", path: "/readyz?x=1", timeout: 1},
		{address: "http://127.0.0.1:8080", path: "/readyz", timeout: 0},
	} {
		if _, err := checkURL(value); err == nil {
			t.Fatalf("checkURL(%+v) error = nil", value)
		}
	}
}
