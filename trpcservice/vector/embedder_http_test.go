package vector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func embedderTestConfig(endpoint string) HTTPEmbedderConfig {
	return HTTPEmbedderConfig{
		Endpoint: endpoint, Model: "suite-model", ModelVersion: "v1",
		Dimension: 4, MaxInputBytes: 1024, Timeout: 5 * time.Second,
		APIKey: "suite-key",
	}
}

func writeEmbeddingResponse(w http.ResponseWriter, status int, model string, values []float64, raw string) {
	w.WriteHeader(status)
	if raw != "" {
		_, _ = w.Write([]byte(raw))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data":  []map[string]any{{"embedding": values}},
		"model": model,
	})
}

func TestHTTPEmbedderConfigValidation(t *testing.T) {
	base := embedderTestConfig("https://api.example.com/v1/embeddings")
	if _, err := NewHTTPEmbedder(base); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*HTTPEmbedderConfig){
		"empty endpoint":      func(c *HTTPEmbedderConfig) { c.Endpoint = "" },
		"non-http endpoint":   func(c *HTTPEmbedderConfig) { c.Endpoint = "ftp://x" },
		"empty model":         func(c *HTTPEmbedderConfig) { c.Model = " " },
		"zero dimension":      func(c *HTTPEmbedderConfig) { c.Dimension = 0 },
		"zero input bound":    func(c *HTTPEmbedderConfig) { c.MaxInputBytes = 0 },
		"short timeout":       func(c *HTTPEmbedderConfig) { c.Timeout = time.Millisecond },
		"empty api key":       func(c *HTTPEmbedderConfig) { c.APIKey = "" },
		"tiny response bound": func(c *HTTPEmbedderConfig) { c.MaxRespBytes = 10 },
		"huge response bound": func(c *HTTPEmbedderConfig) { c.MaxRespBytes = 1 << 30 },
	}
	for name, mutate := range cases {
		c := base
		mutate(&c)
		if _, err := NewHTTPEmbedder(c); !errors.Is(err, ErrEmbedderConfig) {
			t.Fatalf("%s: want ErrEmbedderConfig got %v", name, err)
		}
	}
}

func TestHTTPEmbedderPlaintextNonLoopbackRejected(t *testing.T) {
	c := embedderTestConfig("http://api.example.com/v1/embeddings")
	if _, err := NewHTTPEmbedder(c); !errors.Is(err, ErrEmbedderConfig) {
		t.Fatalf("plaintext non-loopback: want ErrEmbedderConfig got %v", err)
	}
	for _, host := range []string{"http://127.0.0.1:1/x", "http://localhost:1/x", "http://[::1]:1/x"} {
		if _, err := NewHTTPEmbedder(embedderTestConfig(host)); err != nil {
			t.Fatalf("loopback %s rejected: %v", host, err)
		}
	}
}

func TestHTTPEmbedderAllowPlainHTTPFlag(t *testing.T) {
	// Flag off (default): LAN plain-text endpoint stays rejected.
	c := embedderTestConfig("http://192.168.1.6:7997/v1/embeddings")
	if _, err := NewHTTPEmbedder(c); !errors.Is(err, ErrEmbedderConfig) {
		t.Fatalf("flag-off private http: want ErrEmbedderConfig got %v", err)
	}
	// Flag on: accepted, and the API key becomes optional.
	c.AllowPlainHTTP = true
	c.APIKey = ""
	if _, err := NewHTTPEmbedder(c); err != nil {
		t.Fatalf("flag-on private http without key: %v", err)
	}
	// The unauthenticated path sends no Authorization header.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected Authorization header for keyless sidecar")
		}
		writeEmbeddingResponse(w, 200, "suite-model", []float64{1, 2, 3, 4}, "")
	}))
	defer srv.Close()
	cfg := embedderTestConfig(srv.URL)
	cfg.AllowPlainHTTP = true
	cfg.APIKey = ""
	e2, err := NewHTTPEmbedder(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Embed(context.Background(), "hello"); err != nil {
		t.Fatalf("keyless loopback embed: %v", err)
	}
	// HTTPS endpoints still require a key even with the flag.
	hc := embedderTestConfig("https://api.example.com/v1/embeddings")
	hc.AllowPlainHTTP = true
	hc.APIKey = ""
	if _, err := NewHTTPEmbedder(hc); !errors.Is(err, ErrEmbedderConfig) {
		t.Fatalf("https without key (flag on): want ErrEmbedderConfig got %v", err)
	}
}

func TestHTTPEmbedderSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer suite-key" {
			t.Errorf("missing bearer token")
		}
		writeEmbeddingResponse(w, 200, "suite-model", []float64{1, 2, 3, 4}, "")
	}))
	defer srv.Close()
	e, err := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	emb, err := e.Embed(context.Background(), "hello")
	if err != nil {
		t.Fatalf("success path: %v", err)
	}
	if len(emb.Values) != 4 || emb.Model != "suite-model" {
		t.Fatalf("unexpected embedding %+v", emb)
	}
}

func TestHTTPEmbedderDimensionMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 200, "suite-model", []float64{1, 2}, "")
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderDimension) {
		t.Fatalf("want ErrEmbedderDimension got %v", err)
	}
}

func TestHTTPEmbedderModelMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 200, "other-model", []float64{1, 2, 3, 4}, "")
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderModel) {
		t.Fatalf("want ErrEmbedderModel got %v", err)
	}
}

func TestHTTPEmbedderAllFinite(t *testing.T) {
	if !allFinite([]float64{1, 2, 3}) {
		t.Fatal("finite vector reported non-finite")
	}
	if allFinite([]float64{1, math.NaN(), 3}) {
		t.Fatal("NaN passed as finite")
	}
	if allFinite([]float64{1, math.Inf(-1), 3}) {
		t.Fatal("Inf passed as finite")
	}
	// Extreme JSON numbers overflow before the finite guard; they must
	// surface as a parse/classification error, never silent success.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 200, "suite-model", nil, `{"data":[{"embedding":[1,1e999,3,4]}],"model":"suite-model"}`)
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); err == nil {
		t.Fatal("overflowing vector accepted")
	}
}

func TestHTTPEmbedderBodyTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
	}))
	defer srv.Close()
	cfg := embedderTestConfig(srv.URL)
	cfg.MaxRespBytes = 1024
	e, _ := NewHTTPEmbedder(cfg)
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderBounded) {
		t.Fatalf("want ErrEmbedderBounded got %v", err)
	}
}

func TestHTTPEmbedderRedirectBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/moved")
		w.WriteHeader(302)
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	_, err := e.Embed(context.Background(), "x")
	if err == nil || !errors.Is(err, ErrEmbedderCall) || !strings.Contains(err.Error(), "redirect blocked") {
		t.Fatalf("want redirect-blocked ErrEmbedderCall got %v", err)
	}
}

func TestHTTPEmbedder429Classification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 429, "", nil, "")
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderRateLimit) {
		t.Fatalf("want ErrEmbedderRateLimit got %v", err)
	}
}

func TestHTTPEmbedder5xxAnd4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 500, "", nil, "")
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderCall) {
		t.Fatalf("5xx: want ErrEmbedderCall got %v", err)
	}
}

func TestHTTPEmbedderInputBound(t *testing.T) {
	e, _ := NewHTTPEmbedder(embedderTestConfig("http://127.0.0.1:1/x"))
	if _, err := e.Embed(context.Background(), strings.Repeat("x", 2048)); !errors.Is(err, ErrEmbedderBounded) {
		t.Fatalf("want ErrEmbedderBounded got %v", err)
	}
}

func TestHTTPEmbedderInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEmbeddingResponse(w, 200, "", nil, "not-json")
	}))
	defer srv.Close()
	e, _ := NewHTTPEmbedder(embedderTestConfig(srv.URL))
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, ErrEmbedderCall) {
		t.Fatalf("want ErrEmbedderCall got %v", err)
	}
}
