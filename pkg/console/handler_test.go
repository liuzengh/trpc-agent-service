package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAssetsAndSecurityHeaders(t *testing.T) {
	handler := NewHandler()
	for _, asset := range []struct{ path, contentType, marker string }{
		{"/console/", "text/html; charset=utf-8", "<html lang=\"zh-CN\">"},
		{"/console/styles.css", "text/css; charset=utf-8", ":root"},
		{"/console/app.js", "text/javascript; charset=utf-8", "use strict"},
		{"/console/favicon.svg", "image/svg+xml", "<svg"},
	} {
		t.Run(asset.path, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(method, asset.path, nil))
				if response.Code != http.StatusOK || response.Header().Get("Content-Type") != asset.contentType {
					t.Fatalf("%s %s: status %d, type %q", method, asset.path, response.Code, response.Header().Get("Content-Type"))
				}
				if method == http.MethodHead && response.Body.Len() != 0 {
					t.Fatal("HEAD returned an asset body")
				}
				if method == http.MethodGet && !strings.Contains(response.Body.String(), asset.marker) {
					t.Fatal("GET did not return the embedded asset")
				}
				for name, want := range map[string]string{
					"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
					"Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY",
				} {
					if got := response.Header().Get(name); got != want {
						t.Errorf("%s = %q, want %q", name, got, want)
					}
				}
				csp := response.Header().Get("Content-Security-Policy")
				for _, directive := range []string{"default-src 'none'", "script-src 'self'", "style-src 'self'", "connect-src 'self'", "frame-ancestors 'none'", "base-uri 'none'", "object-src 'none'"} {
					if !strings.Contains(csp, directive) {
						t.Errorf("CSP lacks %q", directive)
					}
				}
				for _, unsafe := range []string{"unsafe-inline", "unsafe-eval", "https:", "http:", "data:"} {
					if strings.Contains(csp, unsafe) {
						t.Errorf("CSP permits %q", unsafe)
					}
				}
			}
		})
	}
}

func TestAssetRoutesDoNotExposeOtherPaths(t *testing.T) {
	handler := NewHandler()
	for _, path := range []string{
		"/", "/console/index.html", "/console/assets/app.js", "/console/handler.go",
		"/console/unknown", "/console/../handler.go", "/console/%2e%2e/handler.go",
		"/console//app.js", "/console/app.js/extra", "/api/v1/tenants",
	} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("status %d, want 404", response.Code)
			}
			if strings.Contains(response.Body.String(), "<html") || strings.Contains(response.Body.String(), "use strict") {
				t.Fatal("unknown route exposed an application asset")
			}
		})
	}
}

func TestAssetsRejectUnsupportedMethods(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodTrace, "BREW"} {
		response := httptest.NewRecorder()
		NewHandler().ServeHTTP(response, httptest.NewRequest(method, "/console/app.js", nil))
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("%s: status %d, Allow %q", method, response.Code, response.Header().Get("Allow"))
		}
		if strings.Contains(response.Body.String(), "use strict") {
			t.Errorf("%s exposed script body", method)
		}
	}
}

func TestCanonicalConsoleRedirect(t *testing.T) {
	response := httptest.NewRecorder()
	NewHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/console", nil))
	if response.Code != http.StatusPermanentRedirect || response.Header().Get("Location") != "/console/" {
		t.Fatalf("status %d, Location %q", response.Code, response.Header().Get("Location"))
	}
}
