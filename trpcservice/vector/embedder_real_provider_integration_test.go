//go:build integration

package vector

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestHTTPEmbedderRealProvider verifies the production HTTPEmbedder code
// path against a REAL embedding endpoint. It is env-gated (skips unless
// configured) and each run costs exactly one inference request:
//
//	REAL_EMBEDDER_ENDPOINT  full URL, e.g. http://host:7997/embeddings
//	REAL_EMBEDDER_MODEL     served model name
//	REAL_EMBEDDER_DIMENSION expected vector dimension
//	REAL_EMBEDDER_API_KEY   optional (empty for internal sidecars)
//	REAL_EMBEDDER_ALLOW_PLAIN_HTTP=1  required for private-network http://
//
// Semantic quality remains a separate evaluation (NOT PROVEN here).
func TestHTTPEmbedderRealProvider(t *testing.T) {
	endpoint := os.Getenv("REAL_EMBEDDER_ENDPOINT")
	if endpoint == "" {
		t.Skip("REAL_EMBEDDER_ENDPOINT is not set; real provider verification skipped")
	}
	model := os.Getenv("REAL_EMBEDDER_MODEL")
	if model == "" {
		t.Skip("REAL_EMBEDDER_MODEL is not set")
	}
	dim := 0
	if d := os.Getenv("REAL_EMBEDDER_DIMENSION"); d != "" {
		if _, err := fmtSscan(d, &dim); err != nil {
			t.Fatalf("bad REAL_EMBEDDER_DIMENSION: %v", err)
		}
	}
	if dim <= 0 {
		t.Skip("REAL_EMBEDDER_DIMENSION must be a positive integer")
	}
	cfg := HTTPEmbedderConfig{
		Endpoint: endpoint, Model: model, ModelVersion: "deployed",
		Dimension: dim, MaxInputBytes: 8192, Timeout: 30 * time.Second,
		APIKey:         os.Getenv("REAL_EMBEDDER_API_KEY"),
		AllowPlainHTTP: os.Getenv("REAL_EMBEDDER_ALLOW_PLAIN_HTTP") == "1",
	}
	e, err := NewHTTPEmbedder(cfg)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	emb, err := e.Embed(ctx, "WS-7 真实 provider 验证文本：容量与公平性联合发布")
	if err != nil {
		t.Fatalf("real embed: %v", err)
	}
	if len(emb.Values) != dim {
		t.Fatalf("dimension: got %d want %d", len(emb.Values), dim)
	}
	for _, v := range emb.Values {
		if v != v || v > 1e30 || v < -1e30 {
			t.Fatalf("non-finite value in real embedding")
		}
	}
	if emb.Model != model {
		t.Fatalf("model echo: got %q want %q", emb.Model, model)
	}
	t.Logf("REAL-PROVIDER-OK: endpoint=%s model=%s dimension=%d values[0..3]=%.4f,%.4f,%.4f,%.4f",
		endpoint, model, len(emb.Values), emb.Values[0], emb.Values[1], emb.Values[2], emb.Values[3])
	// Error-classification sanity: a context cancelled before the call is a
	// timeout-class error, not a silent success.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if _, err := e.Embed(cctx, "x"); err == nil || !errors.Is(err, ErrEmbedderTimeout) {
		t.Fatalf("cancelled context classification: want ErrEmbedderTimeout got %v", err)
	}
}

func fmtSscan(s string, out *int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	*out = n
	return 1, nil
}
