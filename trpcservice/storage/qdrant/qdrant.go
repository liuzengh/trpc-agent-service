// Package qdrant is the vector-index adapter: a thin REST client for the
// operations this platform actually performs — ensure a collection, upsert
// deterministic points, search under a mandatory filter, delete by filter,
// count.
//
// It is written as a direct HTTP client rather than an SDK because the
// surface is small and the semantics matter more than the coverage: every
// search call *requires* a tenant filter (the type has no way to express
// "search everything"), so a caller cannot forget isolation — that is the
// approved plan's "Qdrant payload 不能单独决定授权" enforced one layer
// earlier, at the only place a query exists.
package qdrant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to one Qdrant collection.
type Client struct {
	baseURL    *url.URL
	collection string
	http       *http.Client
}

// New builds a client. baseURL is the service root (http://qdrant:6333).
func New(baseURL, collection string) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("qdrant: base url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("qdrant: base url %q needs scheme and host", baseURL)
	}
	if collection == "" {
		return nil, fmt.Errorf("qdrant: collection name is required")
	}
	return &Client{
		baseURL:    u,
		collection: collection,
		http:       &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// Point is one vector plus the payload retrieval filters and cross-checks on.
type Point struct {
	ID      string
	Vector  []float32
	Payload map[string]any
}

// Hit is one search result.
type Hit struct {
	ID      string
	Score   float32
	Payload map[string]any
}

// Match is a payload condition: either one exact value, or any of a small
// set. "Any" exists because cleanup names the generations it retires, and
// Qdrant's match has no comparison operators.
type Match struct {
	Key   string
	Value any
	Any   []any
}

// Filter is a conjunction of exact matches. It is deliberately not a general
// query language: everything this platform filters on is an exact id.
type Filter struct {
	Must []Match
}

// statusError carries a non-2xx Qdrant answer. It exists so callers can
// branch on a status without parsing an error string — EnsureCollection's
// "create if the collection is missing" is exactly such a branch, and
// matching on message text is how that branch silently stops working when a
// library changes a word.
type statusError struct {
	Code   int
	Detail string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("qdrant: status %d: %s", e.Code, e.Detail)
}

// EnsureCollection creates the collection if absent and refuses a dimension
// mismatch loudly: silently searching a collection built for a different
// embedding would return plausible-looking garbage, which is worse than a
// failed job.
func (c *Client) EnsureCollection(ctx context.Context, dim int) error {
	if dim <= 0 {
		return fmt.Errorf("qdrant: embedding dimension must be positive, got %d", dim)
	}
	var existing struct {
		Result struct {
			Config struct {
				Params struct {
					Vectors struct {
						Size int `json:"size"`
					} `json:"vectors"`
				} `json:"params"`
			} `json:"config"`
		} `json:"result"`
	}
	status, err := c.do(ctx, http.MethodGet, c.path("/collections/"+c.collection), nil, &existing)
	var se *statusError
	switch {
	case err == nil && status == http.StatusOK:
		if got := existing.Result.Config.Params.Vectors.Size; got != dim {
			return fmt.Errorf("qdrant: collection %q holds %d-dimensional vectors, this build needs %d; re-index under a new collection",
				c.collection, got, dim)
		}
		return nil
	case errors.As(err, &se) && se.Code == http.StatusNotFound:
		body := map[string]any{"vectors": map[string]any{"size": dim, "distance": "Cosine"}}
		if _, err := c.do(ctx, http.MethodPut, c.path("/collections/"+c.collection), body, nil); err != nil {
			return fmt.Errorf("qdrant: create collection: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("qdrant: inspect collection: %w", err)
	default:
		return fmt.Errorf("qdrant: inspect collection: unexpected status %d", status)
	}
}

// Upsert writes points with wait=true: the caller (an index job) needs the
// write to be durable before it records progress, not merely queued.
func (c *Client) Upsert(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	type pointJSON struct {
		ID      string         `json:"id"`
		Vector  []float32      `json:"vector"`
		Payload map[string]any `json:"payload"`
	}
	out := make([]pointJSON, 0, len(points))
	for _, p := range points {
		out = append(out, pointJSON{ID: p.ID, Vector: p.Vector, Payload: p.Payload})
	}
	_, err := c.do(ctx, http.MethodPut, c.path("/collections/"+c.collection+"/points")+"?wait=true",
		map[string]any{"points": out}, nil)
	if err != nil {
		return fmt.Errorf("qdrant: upsert %d points: %w", len(points), err)
	}
	return nil
}

// Search returns the nearest hits under a mandatory filter. An empty filter
// is refused by construction (see Filter docs): callers pass the tenant
// matches, and this method rejects a missing tenant key.
func (c *Client) Search(ctx context.Context, vector []float32, filter Filter, limit int) ([]Hit, error) {
	if err := filter.validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 8
	}
	body := map[string]any{
		"vector":       vector,
		"limit":        limit,
		"with_payload": true,
		"filter":       filter.json(),
	}
	var resp struct {
		Result []struct {
			ID      any            `json:"id"`
			Score   float32        `json:"score"`
			Payload map[string]any `json:"payload"`
		} `json:"result"`
	}
	if _, err := c.do(ctx, http.MethodPost, c.path("/collections/"+c.collection+"/points/search"), body, &resp); err != nil {
		return nil, fmt.Errorf("qdrant: search: %w", err)
	}
	out := make([]Hit, 0, len(resp.Result))
	for _, h := range resp.Result {
		out = append(out, Hit{ID: fmt.Sprint(h.ID), Score: h.Score, Payload: h.Payload})
	}
	return out, nil
}

// DeleteByFilter removes points. An empty filter is refused: "delete
// everything" is never a thing a job in this platform means to say.
func (c *Client) DeleteByFilter(ctx context.Context, filter Filter) error {
	if err := filter.validate(); err != nil {
		return err
	}
	_, err := c.do(ctx, http.MethodPost, c.path("/collections/"+c.collection+"/points/delete")+"?wait=true",
		map[string]any{"filter": filter.json()}, nil)
	if err != nil {
		return fmt.Errorf("qdrant: delete by filter: %w", err)
	}
	return nil
}

// Count reports how many points match.
func (c *Client) Count(ctx context.Context, filter Filter) (int, error) {
	if err := filter.validate(); err != nil {
		return 0, err
	}
	var resp struct {
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}
	if _, err := c.do(ctx, http.MethodPost, c.path("/collections/"+c.collection+"/points/count"),
		map[string]any{"filter": filter.json(), "exact": true}, &resp); err != nil {
		return 0, fmt.Errorf("qdrant: count: %w", err)
	}
	return resp.Result.Count, nil
}

func (f Filter) validate() error {
	for _, m := range f.Must {
		if m.Key == "tenant_id" {
			return nil
		}
	}
	return fmt.Errorf("qdrant: a filter without a tenant_id match is refused: the vector store must never answer across tenants")
}

func (f Filter) json() map[string]any {
	must := make([]map[string]any, 0, len(f.Must))
	for _, m := range f.Must {
		var match map[string]any
		if len(m.Any) > 0 {
			match = map[string]any{"any": m.Any}
		} else {
			match = map[string]any{"value": m.Value}
		}
		must = append(must, map[string]any{"key": m.Key, "match": match})
	}
	return map[string]any{"must": must}
}

func (c *Client) path(p string) string {
	return c.baseURL.String() + p
}

// do performs one JSON request. Qdrant answers errors as
// {"status":{"error":"..."}} with a non-2xx code; that message is surfaced so
// a job's failure says what the vector store actually objected to.
func (c *Client) do(ctx context.Context, method, url string, body any, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		var failure struct {
			Status struct {
				Error string `json:"error"`
			} `json:"status"`
		}
		_ = json.Unmarshal(raw, &failure)
		detail := failure.Status.Error
		if detail == "" {
			detail = string(raw[:min(len(raw), 200)])
		}
		return resp.StatusCode, &statusError{Code: resp.StatusCode, Detail: detail}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
}
