package retrieval

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// Fact is the authoritative PostgreSQL memory fact behind one candidate. It is
// read-only, tenant-scoped and bounded; embeddings, vector_ref, sessions and
// other unrelated columns are never read. Freshness is proven by recomputing
// the server-owned content hash over the authoritative content.
type Fact struct {
	MemoryID       string
	Scope          string
	Content        string
	SourceVersion  int64
	SourceSequence int64
	Deleted        bool
}

// Hydrator reads authoritative facts for server-owned source identifiers. It
// is tenant-scoped, batched and bounded; it never executes caller-shaped SQL.
type Hydrator interface {
	Hydrate(ctx context.Context, tc tenant.TenantContext, sourceIDs []string) (map[string]Fact, error)
}

// HydratorFunc adapts a function to the Hydrator seam.
type HydratorFunc func(ctx context.Context, tc tenant.TenantContext, sourceIDs []string) (map[string]Fact, error)

// Hydrate implements Hydrator.
func (f HydratorFunc) Hydrate(ctx context.Context, tc tenant.TenantContext, sourceIDs []string) (map[string]Fact, error) {
	if f == nil {
		return nil, ErrUnavailable
	}
	return f(ctx, tc, sourceIDs)
}

// Request is the caller-shaped input. Tenant, collection, model, schema,
// dimension and backend are server-owned and are not request fields.
type Request struct {
	Query string
	// TopK must be inside the server-owned MaxTopK bound.
	TopK int
	// MinScore must be finite inside [0,1].
	MinScore float64
	// SourceTypes optionally narrows the server-owned allowlist.
	SourceTypes []string
}

func (r Request) validate(cfg Config) error {
	if r.Query == "" || len(r.Query) > cfg.MaxQueryBytes || !utf8.ValidString(r.Query) || strings.TrimSpace(r.Query) == "" {
		return ErrInvalidRequest
	}
	if r.TopK < 1 || r.TopK > cfg.MaxTopK {
		return ErrInvalidRequest
	}
	if math.IsNaN(r.MinScore) || math.IsInf(r.MinScore, 0) || r.MinScore < 0 || r.MinScore > 1 {
		return ErrInvalidRequest
	}
	if len(r.SourceTypes) > len(cfg.SourceTypes) {
		return ErrInvalidRequest
	}
	for _, requested := range r.SourceTypes {
		if !cfg.allowsSourceType(requested) {
			return ErrInvalidRequest
		}
	}
	return nil
}

// Result is the hydrated, ordered retrieval output. It contains no Milvus or
// SDK types, no vectors, no raw backend errors and no lease/fence fields.
type Result struct {
	DocumentID     string
	Score          float64
	MemoryID       string
	Scope          string
	Content        string
	SourceVersion  int64
	SourceSequence int64
	ContentHash    string
}

// Service composes the retrieval pipeline. Dependencies are injected and
// nil dependencies fail closed; a zero-value Service can never reach a
// backend.
type Service struct {
	store    vector.VectorStore
	embedder vector.EmbeddingProvider
	hydrator Hydrator
	cfg      Config
}

// Dependencies carries the injected pipeline dependencies.
type Dependencies struct {
	Store    vector.VectorStore
	Embedder vector.EmbeddingProvider
	Hydrator Hydrator
}

// NewService fails closed unless the configuration is complete and every
// dependency is present. There is no fake or in-memory fallback.
func NewService(cfg Config, deps Dependencies) (*Service, error) {
	cfg, err := cfg.WithDefaults()
	if err != nil {
		return nil, err
	}
	if deps.Store == nil || deps.Embedder == nil || deps.Hydrator == nil {
		return nil, ErrInvalidConfig
	}
	return &Service{store: deps.Store, embedder: deps.Embedder, hydrator: deps.Hydrator, cfg: cfg}, nil
}

type candidate struct {
	documentID string
	score      float64
	ref        vector.VectorDocumentRef
}

// Retrieve runs the bounded pipeline: validate, embed, search, validate
// candidates, hydrate from PostgreSQL, order deterministically. Embedding and
// search run outside any PostgreSQL transaction; hydration never calls the
// vector backend. Cancellation and deadlines keep their context identity.
func (s *Service) Retrieve(ctx context.Context, tc tenant.TenantContext, req Request) ([]Result, error) {
	if ctx == nil {
		return nil, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tc.Validate(); err != nil {
		return nil, vector.ErrInvalidTenant
	}
	allowlist := s.cfg.SourceTypes
	if len(req.SourceTypes) > 0 {
		allowlist = req.SourceTypes
	}
	if err := req.validate(s.cfg); err != nil {
		return nil, err
	}
	embedding, err := s.embed(ctx, tc, req.Query)
	if err != nil {
		return nil, err
	}
	candidates, err := s.search(ctx, tc, req, embedding, allowlist)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return []Result{}, nil
	}
	results, err := s.hydrate(ctx, tc, req, candidates)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Service) embed(ctx context.Context, tc tenant.TenantContext, query string) (vector.Embedding, error) {
	embedCtx, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
	defer cancel()
	embedding, err := s.embedder.Embed(embedCtx, query)
	if err != nil {
		if embedCtx.Err() != nil {
			return vector.Embedding{}, embedCtx.Err()
		}
		return vector.Embedding{}, mapPipelineError(err)
	}
	if embedding.Model != s.cfg.Model || embedding.ModelVersion != s.cfg.ModelVersion {
		return vector.Embedding{}, vector.ErrInvalidModel
	}
	if embedding.Dimension != s.cfg.Dimension || len(embedding.Values) != s.cfg.Dimension {
		return vector.Embedding{}, vector.ErrInvalidDimension
	}
	return embedding, nil
}

func (s *Service) search(ctx context.Context, tc tenant.TenantContext, req Request, embedding vector.Embedding, allowlist []string) ([]candidate, error) {
	var candidates []candidate
	byID := make(map[string]int)
	for round := 0; round < s.cfg.MaxRounds; round++ {
		limit := req.TopK * s.cfg.CandidateMultiplier * (round + 1)
		if limit > s.cfg.MaxTotalCandidates {
			limit = s.cfg.MaxTotalCandidates
		}
		roundCtx, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
		request := vector.SearchRequest{Query: embedding.Values, TopK: limit, MinScore: req.MinScore}
		hits, err := s.store.Search(tenant.WithContext(roundCtx, tc), request)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, mapPipelineError(err)
		}
		for _, hit := range s.validCandidates(hits, req, allowlist, tc.TenantID) {
			if index, duplicate := byID[hit.documentID]; duplicate {
				if hit.score > candidates[index].score {
					candidates[index] = hit
				}
				continue
			}
			if len(candidates) >= s.cfg.MaxTotalCandidates {
				break
			}
			byID[hit.documentID] = len(candidates)
			candidates = append(candidates, hit)
		}
		if len(candidates) >= req.TopK || len(hits) < limit {
			break
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].documentID < candidates[j].documentID
	})
	return candidates, nil
}

// validCandidates drops malformed, out-of-scope, mismatched, duplicate and
// unbounded hits. Filtered hits are never returned, never hydrated and never
// repaired.
func (s *Service) validCandidates(hits []vector.SearchResult, req Request, allowlist []string, tenantID string) []candidate {
	candidates := make([]candidate, 0, len(hits))
	for _, hit := range hits {
		score := hit.Score
		if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 || score > 1 || score < req.MinScore {
			continue
		}
		ref := hit.Ref
		if ref.TenantID != tenantID || !validDocumentID(ref.DocumentID) {
			continue
		}
		if !allowedSourceType(allowlist, ref.SourceType) {
			continue
		}
		if ref.Model != s.cfg.Model || ref.ModelVersion != s.cfg.ModelVersion {
			continue
		}
		if ref.Dimension != s.cfg.Dimension || ref.SchemaVersion != s.cfg.SchemaVersion {
			continue
		}
		if ref.Operation != vector.OperationUpsert || ref.Deleted {
			continue
		}
		if ref.Validate() != nil {
			continue
		}
		candidates = append(candidates, candidate{documentID: ref.DocumentID, score: score, ref: ref})
		if len(candidates) >= s.cfg.MaxTotalCandidates {
			break
		}
	}
	return candidates
}

func allowedSourceType(allowlist []string, sourceType string) bool {
	for _, allowed := range allowlist {
		if allowed == sourceType {
			return true
		}
	}
	return false
}

func validDocumentID(value string) bool {
	const prefix = "vd-v1-"
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) {
		return false
	}
	for _, r := range value[len(prefix):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// hydrate proves candidates against authoritative PostgreSQL facts and drops
// missing, deleted, tombstoned, stale or projection-mismatched candidates.
// Hydration batch is bounded; PostgreSQL unavailability is a safe failure and
// never degrades into returning raw vector hits.
func (s *Service) hydrate(ctx context.Context, tc tenant.TenantContext, req Request, candidates []candidate) ([]Result, error) {
	results := make([]Result, 0, req.TopK)
	batchLimit := s.cfg.HydrationBatchLimit
	for start := 0; start < len(candidates) && len(results) < req.TopK; start += batchLimit {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		end := start + batchLimit
		if end > len(candidates) {
			end = len(candidates)
		}
		batch := candidates[start:end]
		sourceIDs := make([]string, 0, len(batch))
		for _, item := range batch {
			sourceIDs = append(sourceIDs, item.ref.SourceID)
		}
		hydrateCtx, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
		facts, err := s.hydrator.Hydrate(hydrateCtx, tc, sourceIDs)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPartial) {
				return nil, err
			}
			return nil, ErrUnavailable
		}
		for _, item := range batch {
			if len(results) >= req.TopK {
				break
			}
			fact, ok := facts[item.ref.SourceID]
			if !ok || fact.Deleted {
				continue
			}
			if fact.SourceVersion != item.ref.SourceVersion || fact.SourceSequence != item.ref.SourceSequence {
				continue
			}
			if vector.ContentHash(fact.Content) != item.ref.ContentHash {
				continue
			}
			if "memory:"+fact.Scope != item.ref.ProjectionScope {
				continue
			}
			if fact.MemoryID != item.ref.SourceID {
				continue
			}
			results = append(results, Result{
				DocumentID:     item.documentID,
				Score:          item.score,
				MemoryID:       fact.MemoryID,
				Scope:          fact.Scope,
				Content:        fact.Content,
				SourceVersion:  fact.SourceVersion,
				SourceSequence: fact.SourceSequence,
				ContentHash:    item.ref.ContentHash,
			})
		}
	}
	if len(results) == 0 {
		return []Result{}, nil
	}
	return results, nil
}

// mapPipelineError keeps category-only failures and context identity. Raw
// backend errors are never returned.
func mapPipelineError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, vector.ErrCancelled):
		return context.Canceled
	case errors.Is(err, vector.ErrTimeout):
		return context.DeadlineExceeded
	case errors.Is(err, vector.ErrUnavailable), errors.Is(err, vector.ErrUnknown),
		errors.Is(err, vector.ErrInvalidTenant), errors.Is(err, vector.ErrInvalidSchema),
		errors.Is(err, vector.ErrInvalidDimension), errors.Is(err, vector.ErrInvalidModel),
		errors.Is(err, vector.ErrInvalidFilter), errors.Is(err, vector.ErrInvalidContext):
		return err
	default:
		return ErrUnavailable
	}
}
