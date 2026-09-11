package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalFormBoundary(t *testing.T) {
	l := &lab{bot: "test-bot", nonce: "test-nonce", origin: "http://127.0.0.1:12345", ctx: context.Background(), status: snapshot{Marker: "test-marker", State: "IDLE"}}
	for _, tc := range []struct {
		name, method, path, host, origin, media, body string
		want                                          int
	}{
		{"page", "GET", "/test-nonce", "127.0.0.1:12345", "", "", "", 200},
		{"status", "POST", "/test-nonce", "127.0.0.1:12345", l.origin, "application/json", `{"action":"status"}`, 200},
		{"dns rebinding", "GET", "/test-nonce", "evil.test", "", "", "", 404},
		{"wrong nonce", "GET", "/guess", "127.0.0.1:12345", "", "", "", 404},
		{"foreign origin", "POST", "/test-nonce", "127.0.0.1:12345", "https://evil.test", "application/json", `{"action":"status"}`, 403},
		{"no confirmation", "POST", "/test-nonce", "127.0.0.1:12345", l.origin, "application/json", `{"action":"connect","secret":"synthetic"}`, 400},
		{"unknown field", "POST", "/test-nonce", "127.0.0.1:12345", l.origin, "application/json", `{"action":"status","url":"https://evil.test"}`, 400},
		{"duplicate action", "POST", "/test-nonce", "127.0.0.1:12345", l.origin, "application/json", `{"action":"status","action":"connect"}`, 400},
		{"second object", "POST", "/test-nonce", "127.0.0.1:12345", l.origin, "application/json", `{"action":"status"}{}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, l.origin+tc.path, strings.NewReader(tc.body))
			r.Host = tc.host
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Content-Type", tc.media)
			w := httptest.NewRecorder()
			l.handler(w, r)
			if w.Code != tc.want {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "synthetic") {
				t.Fatal("secret echoed")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cacheable")
			}
		})
	}
	if l.busy {
		t.Fatal("rejected request started a connection")
	}
}
