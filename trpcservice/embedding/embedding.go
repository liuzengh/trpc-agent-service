// Package embedding is the one place this platform turns text into vectors.
//
// First phase pins one model and one dimension per deployment (the approved
// plan's "首期一个固定 Embedding 模型/维度"); the dimension is stored next to
// every row that was embedded with it, so a future re-embed is a migration,
// not a silent mix of vector spaces.
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client embeds batches of texts. Implementations must be deterministic for
// a given (model, text): both the index job and a search embed through this
// interface, and a non-deterministic embedder would make every search a
// different question.
type Client interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Model() string
	Dim() int
}

// OpenAIClient speaks the /v1/embeddings shape every OpenAI-compatible
// gateway (including the fake model used by drills) implements.
type OpenAIClient struct {
	baseURL string
	apiKey  string
	model   string
	dim     int
	http    *http.Client
}

// NewOpenAI builds a client. dim is checked against every response: an
// endpoint that answers with a different width has been reconfigured
// underneath the deployment, and accepting the first call would corrupt the
// index one upsert at a time.
func NewOpenAI(baseURL, apiKey, model string, dim int) (*OpenAIClient, error) {
	if baseURL == "" || model == "" {
		return nil, fmt.Errorf("embedding: base url and model are required")
	}
	if dim <= 0 {
		return nil, fmt.Errorf("embedding: dimension must be positive, got %d", dim)
	}
	return &OpenAIClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		dim:     dim,
		http:    &http.Client{Timeout: 60 * time.Second},
	}, nil
}

// Model implements Client.
func (c *OpenAIClient) Model() string { return c.model }

// Dim implements Client.
func (c *OpenAIClient) Dim() int { return c.dim }

// Embed implements Client.
func (c *OpenAIClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{
		"model":      c.model,
		"input":      texts,
		"dimensions": c.dim,
	})
	if err != nil {
		return nil, fmt.Errorf("embedding: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embedding: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("embedding: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding: upstream status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw[:min(len(raw), 200)])))
	}
	var decoded struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("embedding: decode response: %w", err)
	}
	if len(decoded.Data) != len(texts) {
		return nil, fmt.Errorf("embedding: asked for %d embeddings, got %d", len(texts), len(decoded.Data))
	}
	// The response's own index is the contract for ordering; sort by it
	// instead of trusting arrival order.
	out := make([][]float32, len(texts))
	for _, d := range decoded.Data {
		if d.Index < 0 || d.Index >= len(texts) {
			return nil, fmt.Errorf("embedding: response index %d out of range", d.Index)
		}
		if len(d.Embedding) != c.dim {
			return nil, fmt.Errorf("embedding: model %s answered with dimension %d, configuration pins %d",
				c.model, len(d.Embedding), c.dim)
		}
		out[d.Index] = d.Embedding
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("embedding: response is missing index %d", i)
		}
	}
	return out, nil
}
