package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPProbeDoesNotLeakBodyURLOrFollowRedirect(t *testing.T) {
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-canary" {
			t.Error("missing explicit probe credential")
		}
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
		_, _ = io.WriteString(w, "secret-canary")
	}))
	defer server.Close()
	result := httpProbe(context.Background(), server.URL+"/private-path-canary", map[string]string{"Authorization": "Bearer secret-canary"})
	if result.State != "down" || strings.Contains(result.Detail, "canary") || reached.Load() != 0 {
		t.Fatal("probe leaked or redirected")
	}
	for _, endpoint := range []string{"https://user:secret@host/path", "https://host/path?key=secret", "file:///tmp/secret"} {
		if result := httpProbe(context.Background(), endpoint, nil); result.Detail != "invalid endpoint configuration" {
			t.Fatal("unsafe endpoint accepted")
		}
	}
}
func TestLocalAddressNormalization(t *testing.T) {
	for raw, want := range map[string]string{":8080": "http://127.0.0.1:8080", "0.0.0.0:8080": "http://127.0.0.1:8080", "[::]:8080": "http://[::1]:8080", "localhost:8080": "http://localhost:8080"} {
		got, err := localBase(raw)
		if err != nil || got != want {
			t.Fatal("address mismatch", err)
		}
	}
	if _, err := localBase("https://host/private-key"); err == nil {
		t.Fatal("URL used as bind address")
	}
}
func TestInvalidLocalFlagsDoNotEchoValues(t *testing.T) {
	for _, args := range [][]string{{}, {"status", "-secret=canary"}, {"stop", "-timeout=canary"}, {"unknown"}} {
		_, err := run(args, io.Discard)
		if err == nil || strings.Contains(err.Error(), "canary") {
			t.Fatal("bad flag accepted or exposed")
		}
	}
}

func TestComposeArrayAndNDJSON(t *testing.T) {
	for _, raw := range []string{`[{"Service":"redis","State":"running","Health":"healthy"}]`, "{\"Service\":\"redis\",\"State\":\"running\",\"Health\":\"healthy\"}\n"} {
		services := parseComposeServices([]byte(raw))
		if len(services) != 1 || services[0].Service != "redis" {
			t.Fatal("Compose format not recognized")
		}
	}
}

func TestModelProbeOnlyMissingListingIsUnknown(t *testing.T) {
	for _, code := range []int{200, 401, 404, 405, 429, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/models" {
				t.Error("unexpected model probe path")
			}
			w.WriteHeader(code)
		}))
		result := modelProbe(context.Background(), server.URL+"/v1", "secret-canary")
		server.Close()
		want := "down"
		if code == 200 {
			want = "ok"
		} else if code == 404 || code == 405 {
			want = "unknown"
		}
		if result.State != want {
			t.Fatalf("status %d: got %s, want %s", code, result.State, want)
		}
		if result := modelProbe(context.Background(), server.URL, "secret-canary"); result.State != "down" {
			t.Fatal("unreachable model incorrectly treated as missing listing")
		}
	}
}
