package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func TestProbeHost(t *testing.T) {
	cases := map[string]string{
		":8080":          "127.0.0.1:8080", // the default listen address
		"0.0.0.0:8080":   "127.0.0.1:8080",
		"[::]:8080":      "127.0.0.1:8080",
		"127.0.0.1:9090": "127.0.0.1:9090", // already dialable: left alone
		"10.0.0.5:8080":  "10.0.0.5:8080",
	}
	for addr, want := range cases {
		if got := probeHost(addr); got != want {
			t.Fatalf("probeHost(%q) = %q, want %q", addr, got, want)
		}
	}
}

// addrOf strips the scheme so an httptest URL can be fed to runHealthcheck the
// way the -addr flag would.
func addrOf(t *testing.T, url string) string {
	t.Helper()
	return strings.TrimPrefix(url, "http://")
}

// TestRunHealthcheckProbesTheReadyPath pins the coupling between the self-probe
// and the server route: the manifests (Compose healthcheck, Kubernetes exec
// probe) call `-healthcheck`, so if the two paths ever drifted the deployment
// would report a healthy process as down.
func TestRunHealthcheckProbesTheReadyPath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if code := runHealthcheck(addrOf(t, srv.URL)); code != 0 {
		t.Fatalf("runHealthcheck = %d, want 0", code)
	}
	if gotPath != web.ReadyPath {
		t.Fatalf("self-probe hit %q, want %q", gotPath, web.ReadyPath)
	}
}

func TestRunHealthcheckExitCodes(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		}))
		defer srv.Close()
		if code := runHealthcheck(addrOf(t, srv.URL)); code != 0 {
			t.Fatalf("ready = %d, want 0", code)
		}
	})

	t.Run("not ready", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable","reasons":["redis down"]}`))
		}))
		defer srv.Close()
		if code := runHealthcheck(addrOf(t, srv.URL)); code != 1 {
			t.Fatalf("unavailable = %d, want 1", code)
		}
	})

	t.Run("nothing listening", func(t *testing.T) {
		// A server that is closed keeps its address, which now refuses
		// connections: the shape of every "app died" or "still booting" probe.
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		addr := addrOf(t, srv.URL)
		srv.Close()
		if code := runHealthcheck(addr); code != 1 {
			t.Fatalf("unreachable = %d, want 1", code)
		}
	})
}
