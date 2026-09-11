// Package knowledgestore assembles SDK text ingestion and retrieval around a
// fixed, scoped Qdrant REST vector adapter. It does not implement a RAG engine.
package knowledgestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/text"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	sdkembed "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

var (
	ErrIdentity    = errors.New("knowledge binding invalid")
	ErrUnavailable = errors.New("knowledge backend unavailable")
	ErrCorrupt     = errors.New("knowledge response invalid")
	ErrUnsupported = errors.New("knowledge operation unsupported")
	ErrCapacity    = errors.New("knowledge input exceeds backend capacity")
	ErrNoResults   = errors.New("knowledge has no matching documents")
	ErrClosed      = errors.New("knowledge store closed")
)

type Scope struct{ TenantID, ProfileID, ResourceID string }
type EmbedderConfig struct {
	Model, BaseURL, APIKey string
	Dimensions             int
}
type ImportResult struct{ Documents int }
type Store struct {
	vectors   *restVectors
	embedder  embedder.Embedder
	knowledge *knowledge.BuiltinKnowledge
	client    *http.Client
	transport *http.Transport
	timeout   time.Duration
	maxBytes  int64
	gate      chan struct{}
	closed    atomic.Bool
}

func valid(s string) bool {
	return strings.TrimSpace(s) != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func digest(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func failure(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrUnavailable
}
func Open(ctx context.Context, b datav1.Snapshot, e EmbedderConfig, key string, scope Scope) (*Store, error) {
	if b.ValidateForRole("knowledge") != nil || b.Kind != datav1.Qdrant || b.TenantID != scope.TenantID || !valid(scope.ProfileID) || !valid(scope.ResourceID) || !valid(e.Model) || !valid(e.APIKey) || e.Dimensions != int(b.Qdrant.Dimensions) {
		return nil, ErrIdentity
	}
	u, err := url.Parse(e.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, ErrIdentity
	}
	d, _ := b.Digest()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	tr.MaxConnsPerHost = int(b.Limits.MaxConcurrency)
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s := &Store{client: client, transport: tr, timeout: time.Duration(b.Limits.TimeoutMS) * time.Millisecond, maxBytes: b.Limits.MaxBytes, gate: make(chan struct{}, int(b.Limits.MaxConcurrency))}
	s.vectors = &restVectors{client: client, endpoint: b.Qdrant.Endpoint, collection: b.Qdrant.Collection, vectorName: b.Qdrant.VectorName, dimension: e.Dimensions, key: key, scope: digest(scope.TenantID, scope.ProfileID, scope.ResourceID, d, e.Model, e.BaseURL, fmt.Sprint(e.Dimensions))}
	embeddingClient := *client
	embeddingClient.Transport = redactedTransport{base: tr}
	raw := sdkembed.New(sdkembed.WithModel(e.Model), sdkembed.WithBaseURL(e.BaseURL), sdkembed.WithAPIKey(e.APIKey), sdkembed.WithDimensions(e.Dimensions), sdkembed.WithMaxRetries(0), sdkembed.WithRequestOptions(option.WithHTTPClient(&embeddingClient), option.WithMaxRetries(0)))
	s.embedder = &checkedEmbedder{raw: raw, dimension: e.Dimensions}
	s.knowledge = knowledge.New(knowledge.WithVectorStore(s.vectors), knowledge.WithEmbedder(s.embedder), knowledge.WithEnableSourceSync(false))
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var info struct {
		Result struct {
			Config struct {
				Params struct {
					Vectors map[string]struct {
						Size     int
						Distance string
					}
				}
			}
		}
	}
	if err = s.vectors.request(op, "GET", "", nil, &info); err != nil {
		s.Close()
		return nil, err
	}
	v, ok := info.Result.Config.Params.Vectors[b.Qdrant.VectorName]
	if !ok || v.Size != e.Dimensions || !strings.EqualFold(v.Distance, b.Qdrant.Distance) {
		s.Close()
		return nil, ErrIdentity
	}
	return s, nil
}
func (s *Store) Close() { s.closed.Store(true); s.transport.CloseIdleConnections() }
func (s *Store) begin(ctx context.Context) (context.Context, func(), error) {
	if s.closed.Load() {
		return nil, nil, ErrClosed
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	select {
	case s.gate <- struct{}{}:
	case <-op.Done():
		cancel()
		return nil, nil, op.Err()
	}
	if err := op.Err(); err != nil {
		<-s.gate
		cancel()
		return nil, nil, err
	}
	if s.closed.Load() {
		<-s.gate
		cancel()
		return nil, nil, ErrClosed
	}
	return op, func() { <-s.gate; cancel() }, nil
}
func (s *Store) ImportText(ctx context.Context, name, body string) (ImportResult, error) {
	op, done, err := s.begin(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	defer done()
	if !valid(name) || !valid(body) {
		return ImportResult{}, ErrIdentity
	}
	if int64(len(body)) > s.maxBytes {
		return ImportResult{}, ErrCapacity
	}
	// SDK reader owns its default fixed-size chunking. The source name is opaque
	// so SDK progress logs never contain the caller's document title.
	docs, err := text.New().ReadFromReader(name, strings.NewReader(body))
	if err != nil || len(docs) == 0 {
		return ImportResult{}, ErrCorrupt
	}
	for i, d := range docs {
		d.ID = digest(s.vectors.scope, name, body, fmt.Sprint(i))
	}
	source := &textSource{name: digest(s.vectors.scope, name, body), docs: docs}
	// A fresh importer avoids source-registration state and does not expose
	// SourceSync/recreate/remove operations on a shared collection.
	importer := knowledge.New(knowledge.WithVectorStore(s.vectors), knowledge.WithEmbedder(s.embedder), knowledge.WithEnableSourceSync(false))
	var reported atomic.Bool
	err = importer.AddSource(op, source, knowledge.WithSourceConcurrency(1), knowledge.WithDocConcurrency(1), knowledge.WithShowProgress(false), knowledge.WithShowStats(false), knowledge.WithLoadProgressCallback(func(_ context.Context, event knowledge.LoadProgressEvent) {
		if event.Err != nil {
			reported.Store(true)
		}
	}))
	if err != nil || reported.Load() {
		return ImportResult{}, failure(op)
	}
	return ImportResult{Documents: len(docs)}, nil
}
func (s *Store) Search(ctx context.Context, req *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	op, done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if req == nil || !valid(req.Query) {
		return nil, ErrIdentity
	}
	if int64(len(req.Query)) > s.maxBytes {
		return nil, ErrCapacity
	}
	// This V1 adapter is dense-only. Zero is the SDK tool's default mode.
	if req.SearchMode != 0 && req.SearchMode != vectorstore.SearchModeVector {
		return nil, ErrUnsupported
	}
	if f := req.SearchFilter; f != nil && (len(f.DocumentIDs) > 0 || len(f.Metadata) > 0 || f.FilterCondition != nil) {
		return nil, ErrUnsupported
	}
	// SDK search tools always allocate SearchFilter, even with no conditions.
	// Normalize that semantic empty value without mutating the caller request.
	copy := *req
	copy.SearchFilter = nil
	copy.SearchMode = vectorstore.SearchModeVector
	out, err := s.knowledge.Search(op, &copy)
	if errors.Is(err, ErrNoResults) {
		return nil, ErrNoResults
	}
	if err != nil {
		return nil, failure(op)
	}
	return out, nil
}

type textSource struct {
	name string
	docs []*document.Document
}

func (s *textSource) Name() string                { return s.name }
func (s *textSource) Type() string                { return "text" }
func (s *textSource) GetMetadata() map[string]any { return nil }
func (s *textSource) ReadDocuments(ctx context.Context) ([]*document.Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.docs, nil
}

type checkedEmbedder struct {
	raw       embedder.Embedder
	dimension int
}

func (e *checkedEmbedder) GetDimensions() int { return e.dimension }
func (e *checkedEmbedder) GetEmbedding(ctx context.Context, t string) ([]float64, error) {
	v, _, err := e.GetEmbeddingWithUsage(ctx, t)
	return v, err
}
func (e *checkedEmbedder) GetEmbeddingWithUsage(ctx context.Context, t string) ([]float64, map[string]any, error) {
	v, u, err := e.raw.GetEmbeddingWithUsage(ctx, t)
	if err != nil {
		return nil, nil, failure(ctx)
	}
	if !validVector(v, e.dimension) {
		return nil, nil, ErrCorrupt
	}
	return v, u, nil
}
func validVector(v []float64, n int) bool {
	if len(v) != n {
		return false
	}
	for _, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > math.MaxFloat32 {
			return false
		}
	}
	return true
}

type restVectors struct {
	client                                       *http.Client
	endpoint, collection, vectorName, key, scope string
	dimension                                    int
}

func (v *restVectors) request(ctx context.Context, method, suffix string, body any, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return ErrCorrupt
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, v.endpoint+"/collections/"+url.PathEscape(v.collection)+suffix, bytes.NewReader(data))
	if err != nil {
		return ErrIdentity
	}
	req.Header.Set("Content-Type", "application/json")
	if v.key != "" {
		req.Header.Set("api-key", v.key)
	}
	res, err := v.client.Do(req)
	if err != nil {
		return failure(ctx)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return failure(ctx)
	}
	if out != nil {
		if err = json.NewDecoder(res.Body).Decode(out); err != nil {
			return ErrCorrupt
		}
	} else {
		_, err = io.Copy(io.Discard, res.Body)
		if err != nil {
			return failure(ctx)
		}
	}
	return nil
}
func (v *restVectors) Add(ctx context.Context, d *document.Document, embedding []float64) error {
	if d == nil || !valid(d.ID) || !validVector(embedding, v.dimension) {
		return ErrCorrupt
	}
	// UUID point IDs are deterministic in this fixed scope; no filename can choose
	// another tenant's point. The original SDK document is payload, not a grant.
	h := digest(v.scope, d.ID)
	id := h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
	body := map[string]any{"points": []any{map[string]any{"id": id, "vector": map[string]any{v.vectorName: embedding}, "payload": map[string]any{"worker_scope": v.scope, "document": d}}}}
	var reply struct{ Result struct{ Status string } }
	if err := v.request(ctx, "PUT", "/points?wait=true", body, &reply); err != nil {
		return err
	}
	if reply.Result.Status != "completed" {
		return ErrUnavailable
	}
	return nil
}
func (v *restVectors) Search(ctx context.Context, q *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	if q == nil || !validVector(q.Vector, v.dimension) || q.Filter != nil || q.SearchMode != vectorstore.SearchModeVector {
		return nil, ErrUnsupported
	}
	body := map[string]any{"vector": map[string]any{"name": v.vectorName, "vector": q.Vector}, "with_payload": true, "filter": map[string]any{"must": []any{map[string]any{"key": "worker_scope", "match": map[string]any{"value": v.scope}}}}}
	// Match the official SDK Qdrant adapter default (10), not a cap:
	// explicit positive limits are forwarded unchanged. REST requires limit.
	body["limit"] = 10
	if q.Limit > 0 {
		body["limit"] = q.Limit
	}
	if q.MinScore != 0 {
		body["score_threshold"] = q.MinScore
	}
	var reply struct {
		Result []struct {
			Score   float64
			Payload struct {
				Scope    string `json:"worker_scope"`
				Document *document.Document
			}
		}
	}
	if err := v.request(ctx, "POST", "/points/search", body, &reply); err != nil {
		return nil, err
	}
	if len(reply.Result) == 0 {
		return nil, ErrNoResults
	}
	out := &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{}}
	for _, r := range reply.Result {
		if r.Payload.Scope != v.scope || r.Payload.Document == nil || math.IsNaN(r.Score) || math.IsInf(r.Score, 0) {
			return nil, ErrCorrupt
		}
		out.Results = append(out.Results, &vectorstore.ScoredDocument{Document: r.Payload.Document, Score: r.Score})
	}
	return out, nil
}

// The scoped SDK importer/retriever only needs Add/Search. Wider mutation and
// collection-management methods fail explicitly, never operate without scope.
func (v *restVectors) Get(context.Context, string) (*document.Document, []float64, error) {
	return nil, nil, ErrUnsupported
}
func (v *restVectors) Update(context.Context, *document.Document, []float64) error {
	return ErrUnsupported
}
func (v *restVectors) Delete(context.Context, string) error { return ErrUnsupported }
func (v *restVectors) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return ErrUnsupported
}
func (v *restVectors) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, ErrUnsupported
}
func (v *restVectors) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return 0, ErrUnsupported
}
func (v *restVectors) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return nil, ErrUnsupported
}
func (v *restVectors) Close() error { return nil }

var _ knowledge.Knowledge = (*Store)(nil)
var _ vectorstore.VectorStore = (*restVectors)(nil)

// SDK embedding errors may be logged internally. Replace non-success response
// diagnostics before they reach the SDK; status and successful vectors remain
// unchanged. Transport failures also never include request URLs or credentials.
type redactedTransport struct{ base http.RoundTripper }

func (t redactedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, failure(req.Context())
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		res.Body.Close()
		body := `{"error":{"message":"embedding backend unavailable","type":"provider_error"}}`
		res.Body = io.NopCloser(strings.NewReader(body))
		res.ContentLength = int64(len(body))
		res.Header.Set("Content-Type", "application/json")
		res.Header.Del("Content-Encoding")
	}
	return res, nil
}
