package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPEmbedder is a production embedding adapter that calls an
// OpenAI-compatible /v1/embeddings endpoint over HTTPS. It implements the
// EmbeddingProvider interface. The endpoint, model, dimension and API key
// are all server-owned configuration; user input cannot override any of
// them.
//
// Bounded contract:
//   - context deadline honoured (no unbounded wait)
//   - request body bounded (input text ≤ maxInputBytes)
//   - response body bounded (maxResponseBytes)
//   - fixed timeout (no retry-until-success)
//   - 429/5xx/timeout/invalid response classified, not silently retried
//   - dimension validated against the configured dimension
//   - model/version validated against the configured model
//   - no prompt/content/token logged
//   - secret never in argv/env/trace/metric
//   - unknown outcome preserved for reconciliation (never silently success)
type HTTPEmbedder struct {
	endpoint      string
	model         string
	modelVersion  string
	dimension     int
	maxInputBytes int
	maxRespBytes  int64
	timeout       time.Duration
	apiKey        string
	client        *http.Client
}

// HTTPEmbedderConfig is the server-owned configuration for one embedding
// endpoint. All fields are required and validated by NewHTTPEmbedder.
type HTTPEmbedderConfig struct {
	Endpoint      string        // canonical, e.g. "https://api.provider.com/v1/embeddings"
	Model         string        // embedding model identifier
	ModelVersion  string        // version fingerprint
	Dimension     int           // expected embedding dimension
	MaxInputBytes int           // max input text bytes per request
	MaxRespBytes  int64         // max response body bytes (bounded read; 0 => 1 MiB)
	Timeout       time.Duration // per-request deadline
	APIKey        string        // bearer token (from approved Secret path); optional for plain-text internal sidecars
	// AllowPlainHTTP permits plain-text HTTP endpoints outside loopback,
	// for private-network embedding sidecars (docker-internal infinity/TEI
	// deployments). It is a server-owned trust decision: fail-closed by
	// default, never derivable from user input. With this flag the API key
	// becomes optional (internal sidecars typically run unauthenticated);
	// HTTPS endpoints still require a key.
	AllowPlainHTTP bool
}

// ErrEmbedderConfig / ErrEmbedderCall / ErrEmbedderDimension are stable
// sentinel errors for classification.
var (
	ErrEmbedderConfig    = fmt.Errorf("embedder: invalid configuration")
	ErrEmbedderCall      = fmt.Errorf("embedder: request failed")
	ErrEmbedderDimension = fmt.Errorf("embedder: dimension mismatch")
	ErrEmbedderModel     = fmt.Errorf("embedder: model mismatch")
	ErrEmbedderNonFinite = fmt.Errorf("embedder: non-finite embedding value")
	ErrEmbedderRateLimit = fmt.Errorf("embedder: rate limited")
	ErrEmbedderTimeout   = fmt.Errorf("embedder: timeout")
	ErrEmbedderBounded   = fmt.Errorf("embedder: input exceeds bound")
)

func NewHTTPEmbedder(cfg HTTPEmbedderConfig) (*HTTPEmbedder, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" || (!strings.HasPrefix(cfg.Endpoint, "https://") && !strings.HasPrefix(cfg.Endpoint, "http://")) {
		return nil, fmt.Errorf("%w: endpoint must be a valid HTTP(S) URL", ErrEmbedderConfig)
	}
	// Production endpoints must be HTTPS; plain HTTP only for loopback or
	// for an explicitly flagged private-network sidecar.
	if strings.HasPrefix(cfg.Endpoint, "http://") && !isLoopbackHost(cfg.Endpoint) && !cfg.AllowPlainHTTP {
		return nil, fmt.Errorf("%w: plain-text HTTP allowed only for loopback endpoints or with AllowPlainHTTP", ErrEmbedderConfig)
	}
	if strings.TrimSpace(cfg.Model) == "" || strings.TrimSpace(cfg.ModelVersion) == "" {
		return nil, fmt.Errorf("%w: model and version required", ErrEmbedderConfig)
	}
	if cfg.Dimension < 1 || cfg.Dimension > MaxVectorDimension {
		return nil, fmt.Errorf("%w: dimension out of range", ErrEmbedderConfig)
	}
	if cfg.MaxInputBytes < 1 || cfg.MaxInputBytes > MaxContentBytes {
		return nil, fmt.Errorf("%w: max input bytes out of range", ErrEmbedderConfig)
	}
	if cfg.Timeout < time.Second || cfg.Timeout > 5*time.Minute {
		return nil, fmt.Errorf("%w: timeout out of range", ErrEmbedderConfig)
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		// HTTPS endpoints authenticate; plain-text internal sidecars may
		// run unauthenticated when the explicit flag opts in.
		if !strings.HasPrefix(cfg.Endpoint, "https://") && (isLoopbackHost(cfg.Endpoint) || cfg.AllowPlainHTTP) {
			// unauthenticated internal sidecar: acceptable
		} else {
			return nil, fmt.Errorf("%w: API key required for HTTPS endpoints", ErrEmbedderConfig)
		}
	}
	maxResp := cfg.MaxRespBytes
	if maxResp == 0 {
		maxResp = 1 << 20
	}
	if maxResp < 1024 || maxResp > 64<<20 {
		return nil, fmt.Errorf("%w: max response bytes out of range", ErrEmbedderConfig)
	}
	return &HTTPEmbedder{
		endpoint:      cfg.Endpoint,
		model:         cfg.Model,
		modelVersion:  cfg.ModelVersion,
		dimension:     cfg.Dimension,
		maxInputBytes: cfg.MaxInputBytes,
		maxRespBytes:  maxResp,
		timeout:       cfg.Timeout,
		apiKey:        cfg.APIKey,
		// Redirects are disabled: any 3xx surfaces as a call failure so
		// credentials never follow off-host.
		client: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}, nil
}

// allFinite reports whether every vector value is a finite float.
// Defense-in-depth: strict JSON parsers reject NaN/Inf literals, but a
// lenient provider path or future decoder change must never store
// non-finite values downstream.
func allFinite(values []float64) bool {
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}

// isLoopbackHost reports whether the endpoint host is explicit loopback.
// Plain-text HTTP is accepted only for loopback so local httptest suites
// keep working while production endpoints are forced onto HTTPS.
func isLoopbackHost(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	h := u.Hostname()
	return h == "127.0.0.1" || h == "localhost" || h == "::1"
}

// Embed implements EmbeddingProvider. The input text is bounded, the request
// uses a fixed deadline, and the response is validated for dimension and
// model compatibility. Errors are classified; unknown outcomes are never
// silently converted to success.
func (e *HTTPEmbedder) Embed(ctx context.Context, text string) (Embedding, error) {
	if e == nil {
		return Embedding{}, ErrEmbedderConfig
	}
	if err := ctx.Err(); err != nil {
		return Embedding{}, fmt.Errorf("%w: %v", ErrEmbedderTimeout, err)
	}
	if len(text) > e.maxInputBytes {
		return Embedding{}, fmt.Errorf("%w: input %d > %d bytes", ErrEmbedderBounded, len(text), e.maxInputBytes)
	}
	reqBody, err := json.Marshal(map[string]any{
		"model": e.model,
		"input": text,
	})
	if err != nil {
		return Embedding{}, fmt.Errorf("%w: %v", ErrEmbedderCall, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return Embedding{}, fmt.Errorf("%w: %v", ErrEmbedderCall, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	}
	httpResp, err := e.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return Embedding{}, fmt.Errorf("%w: context cancelled", ErrEmbedderTimeout)
		}
		return Embedding{}, fmt.Errorf("%w: %v", ErrEmbedderCall, err)
	}
	defer httpResp.Body.Close()
	switch {
	case httpResp.StatusCode >= 300 && httpResp.StatusCode < 400:
		return Embedding{}, fmt.Errorf("%w: redirect blocked (HTTP %d)", ErrEmbedderCall, httpResp.StatusCode)
	case httpResp.StatusCode == http.StatusTooManyRequests:
		return Embedding{}, fmt.Errorf("%w: HTTP 429", ErrEmbedderRateLimit)
	case httpResp.StatusCode >= 500:
		return Embedding{}, fmt.Errorf("%w: HTTP %d", ErrEmbedderCall, httpResp.StatusCode)
	case httpResp.StatusCode != http.StatusOK:
		return Embedding{}, fmt.Errorf("%w: HTTP %d", ErrEmbedderCall, httpResp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(httpResp.Body, e.maxRespBytes+1))
	if err != nil {
		return Embedding{}, fmt.Errorf("%w: %v", ErrEmbedderCall, err)
	}
	if int64(len(body)) > e.maxRespBytes {
		return Embedding{}, fmt.Errorf("%w: response body %d > %d bytes", ErrEmbedderBounded, len(body), e.maxRespBytes)
	}
	var response struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return Embedding{}, fmt.Errorf("%w: invalid response body", ErrEmbedderCall)
	}
	if len(response.Data) == 0 {
		return Embedding{}, fmt.Errorf("%w: empty embedding response", ErrEmbedderCall)
	}
	values := response.Data[0].Embedding
	if len(values) != e.dimension {
		return Embedding{}, fmt.Errorf("%w: got %d want %d", ErrEmbedderDimension, len(values), e.dimension)
	}
	if response.Model != "" && response.Model != e.model {
		return Embedding{}, fmt.Errorf("%w: got %q want %q", ErrEmbedderModel, response.Model, e.model)
	}
	if !allFinite(values) {
		return Embedding{}, fmt.Errorf("%w: NaN/Inf in embedding vector", ErrEmbedderNonFinite)
	}
	return Embedding{Values: values, Model: e.model, ModelVersion: e.modelVersion, Dimension: e.dimension}, nil
}
