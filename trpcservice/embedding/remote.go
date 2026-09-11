// Package embedding wraps the framework Embedder with a bounded, private HTTP
// boundary used by the Knowledge runtime.
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/openai/openai-go/option"
	"go.opentelemetry.io/otel/propagation"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	openaiembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
)

const MaxResponseBytes = 4 << 20
const DefaultBaseURL = "https://api.openai.com/v1"

// The SDK only sees this inert address. The private Doer routes a clone to the
// deployment-configured endpoint, preventing SDK error logs from exposing it.
const sdkBaseURL = "https://embedding.invalid/v1"

type Config struct {
	Model      string
	BaseURL    string
	APIKey     string `json:"-"`
	Dimensions int
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Model) == "" || len(c.Model) > 256 || strings.IndexFunc(c.Model, unicode.IsControl) >= 0 {
		return errors.New("embedding model ID is invalid")
	}
	if strings.TrimSpace(c.APIKey) == "" || len(c.APIKey) > 8192 || strings.IndexFunc(c.APIKey, unicode.IsControl) >= 0 {
		return errors.New("embedding requires an explicit valid API key")
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return errors.New("embedding API root must be HTTP(S) without credentials, query or fragment")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("embedding API port is invalid")
		}
	}
	if c.Dimensions < 1 || c.Dimensions > 65536 {
		return errors.New("embedding dimensions must be between 1 and 65536")
	}
	return nil
}

// ProviderError carries only a fixed category and numeric status. Original
// transport errors, provider descriptions, headers and raw bodies are discarded.
type ProviderError struct {
	Kind       string
	HTTPStatus int
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("embedding provider failed: kind=%s http_status=%d", e.Kind, e.HTTPStatus)
}
func (e *ProviderError) Unwrap() error {
	if e.Kind == "timeout" {
		return context.DeadlineExceeded
	}
	if e.Kind == "canceled" {
		return context.Canceled
	}
	return nil
}

type Remote struct {
	base      embedder.Embedder
	transport *http.Transport
	closed    atomic.Bool
}

func NewRemote(c Config) (*Remote, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	doer := &privateDoer{client: client, config: c}
	base := openaiembedder.New(openaiembedder.WithModel(c.Model), openaiembedder.WithDimensions(c.Dimensions),
		openaiembedder.WithAPIKey(c.APIKey), openaiembedder.WithBaseURL(sdkBaseURL), openaiembedder.WithMaxRetries(0),
		openaiembedder.WithRequestOptions(option.WithHTTPClient(doer), option.WithMaxRetries(0), option.WithHeaderDel("OpenAI-Organization"), option.WithHeaderDel("OpenAI-Project")))
	return &Remote{base: base, transport: transport}, nil
}
func (r *Remote) GetDimensions() int { return r.base.GetDimensions() }
func (r *Remote) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	v, _, err := r.GetEmbeddingWithUsage(ctx, text)
	return v, err
}
func (r *Remote) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	if r.closed.Load() {
		return nil, nil, errors.New("embedding client closed")
	}
	if strings.TrimSpace(text) == "" || len(text) > 131072 {
		return nil, nil, errors.New("embedding input is empty or exceeds 128 KiB")
	}
	return r.base.GetEmbeddingWithUsage(ctx, text)
}
func (r *Remote) Close() error { r.closed.Store(true); r.transport.CloseIdleConnections(); return nil }

type privateDoer struct {
	client *http.Client
	config Config
}

func (d *privateDoer) Do(sdkRequest *http.Request) (*http.Response, error) {
	if sdkRequest.Method != http.MethodPost || sdkRequest.URL.String() != sdkBaseURL+"/embeddings" {
		return nil, &ProviderError{Kind: "request"}
	}
	// Never mutate the SDK request or expose its authorization to log output.
	request := sdkRequest.Clone(sdkRequest.Context())
	request.URL, _ = url.Parse(strings.TrimRight(d.config.BaseURL, "/") + "/embeddings")
	request.Host = request.URL.Host
	request.Header = make(http.Header)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+d.config.APIKey)
	propagation.TraceContext{}.Inject(request.Context(), propagation.HeaderCarrier(request.Header))
	response, err := d.client.Do(request)
	if err != nil {
		kind := "transport"
		var network net.Error
		if errors.Is(err, context.Canceled) {
			kind = "canceled"
		} else if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &network) && network.Timeout()) {
			kind = "timeout"
		}
		return nil, &ProviderError{Kind: kind}
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &ProviderError{Kind: "http", HTTPStatus: response.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		kind := "response_read"
		var network net.Error
		if errors.Is(err, context.Canceled) {
			kind = "canceled"
		} else if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &network) && network.Timeout()) {
			kind = "timeout"
		}
		return nil, &ProviderError{Kind: kind}
	}
	if len(body) > MaxResponseBytes {
		return nil, &ProviderError{Kind: "response_limit"}
	}
	var decoded struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &decoded) != nil || len(decoded.Data) != 1 || decoded.Data[0].Index != 0 {
		return nil, &ProviderError{Kind: "response_shape"}
	}
	if len(decoded.Data[0].Embedding) != d.config.Dimensions {
		return nil, &ProviderError{Kind: "dimensions"}
	}
	if ValidateVector(decoded.Data[0].Embedding, d.config.Dimensions) != nil {
		return nil, &ProviderError{Kind: "vector"}
	}
	if decoded.Usage.PromptTokens < 0 || decoded.Usage.PromptTokens > 1e9 || decoded.Usage.TotalTokens < 0 || decoded.Usage.TotalTokens > 1e9 {
		return nil, &ProviderError{Kind: "usage"}
	}
	// Only numeric vectors/usage and deployment-owned model metadata cross back
	// into the SDK. No upstream extensions, headers or descriptions are retained.
	clean, _ := json.Marshal(map[string]any{"object": "list", "model": d.config.Model, "data": decoded.Data, "usage": decoded.Usage})
	return &http.Response{StatusCode: response.StatusCode, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(clean)), ContentLength: int64(len(clean)), Request: sdkRequest}, nil
}

func ValidateVector(vector []float64, dimensions int) error {
	if len(vector) != dimensions {
		return errors.New("embedding dimension mismatch")
	}
	var norm float64
	for _, v := range vector {
		// Qdrant stores float32 vectors. Reject overflow and a vector that
		// would become all-zero after conversion, not just invalid float64.
		stored := float64(float32(v))
		if math.IsNaN(v) || math.IsInf(v, 0) || math.IsInf(stored, 0) {
			return errors.New("embedding has non-finite values")
		}
		norm = math.Hypot(norm, stored)
	}
	if norm == 0 || math.IsInf(norm, 0) {
		return errors.New("embedding norm is zero or non-finite")
	}
	return nil
}

var _ embedder.Embedder = (*Remote)(nil)
