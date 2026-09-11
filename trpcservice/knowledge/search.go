package knowledge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
)

// Citation is what reaches the model (and, through it, the user): a locator
// plus the excerpt text. The text was read from SQL after the candidate
// passed every check — never from the vector payload.
type Citation struct {
	DocPublicID string
	Title       string
	DocID       int64
	ChunkOrd    int
	Page        int
	Score       float32
	Text        string
}

// SearchResult carries the citations and a truthful note. An empty result is
// not an error: "the knowledge bases have nothing on this" is an answer the
// model must be able to give, and the plan requires it to be explicit
// ("无匹配或检索故障明确说明").
type SearchResult struct {
	Citations []Citation
	Note      string
}

const (
	// searchFanout asks the vector store for more candidates than will be
	// cited: some will fail the SQL re-verification (deleted, not ready,
	// unbound kb), and the fanout absorbs that without a second round trip.
	searchFanout = 3
	// maxCitationText bounds one excerpt; a citation is a quote, not the
	// document.
	maxCitationText = 600
	// minScore is the relevance floor. Without one, a nearest-neighbour
	// search always returns *something* — measured: an unrelated query came
	// back with cosine 0 and was cited as a match. "无匹配明确说明" means a
	// low-relevance hit is no match.
	minScore = 0.2
)

// Search embeds the query, asks the vector store for candidates under a
// tenant+app+kb filter, then verifies every candidate in SQL before reading
// its text.
func (s *Service) Search(ctx context.Context, tenantID string, appID int64, kbIDs []int64, query string, k int) (SearchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return SearchResult{Note: "the query was empty"}, nil
	}
	if len(kbIDs) == 0 {
		return SearchResult{Note: "this revision binds no knowledge base"}, nil
	}
	if k <= 0 {
		k = 4
	}
	vecs, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		// A retrieval failure is reported as itself — the plan forbids
		// disguising it as "no matches".
		return SearchResult{}, fmt.Errorf("knowledge: embed query: %w", err)
	}
	anyKBs := make([]any, 0, len(kbIDs))
	for _, id := range kbIDs {
		anyKBs = append(anyKBs, id)
	}
	candidates, err := s.vectors.Search(ctx, vecs[0], qdrant.Filter{Must: []qdrant.Match{
		{Key: "tenant_id", Value: tenantID},
		{Key: "app_id", Value: appID},
		{Key: "kb_id", Any: anyKBs},
	}}, k*searchFanout)
	if err != nil {
		return SearchResult{}, err
	}
	if len(candidates) == 0 {
		return SearchResult{Note: "no stored passage matches this question"}, nil
	}

	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return SearchResult{}, err
	}
	out := make([]Citation, 0, k)
	for _, hit := range candidates {
		if len(out) >= k {
			break
		}
		if hit.Score < minScore {
			// Below the relevance floor: treated as no match, not as the
			// least-bad match.
			continue
		}
		docID, ok := payloadInt(hit.Payload, "doc_id")
		if !ok {
			continue
		}
		generation, ok := payloadInt(hit.Payload, "generation")
		if !ok {
			continue
		}
		ord, ok := payloadInt(hit.Payload, "chunk_ord")
		if !ok {
			continue
		}
		c, ok, err := s.verifyAndRead(ctx, scope, tenantID, appID, docID, int(generation), int(ord))
		if err != nil {
			return SearchResult{}, err
		}
		if !ok {
			// The candidate failed SQL verification (deleted, re-ingested,
			// not ready, or not bound to this app); it is skipped, not
			// cited. This is the "未 ready 不召回" rule, enforced per hit.
			continue
		}
		c.Score = hit.Score
		out = append(out, c)
	}
	if len(out) == 0 {
		return SearchResult{Note: "no stored passage is relevant enough to cite"}, nil
	}
	return SearchResult{Citations: out}, nil
}

// verifyAndRead is the SQL half of retrieval: one query that joins the
// document row and reads the chunk, with every authorization fact in the
// WHERE clause. If a fact moves (status, generation), the row disappears —
// there is no window in which a candidate is "checked" and then read.
func (s *Service) verifyAndRead(ctx context.Context, scope controlplane.Scope, tenantID string, appID int64, docID int64, generation, ord int) (Citation, bool, error) {
	var (
		c    Citation
		page int
		text string
	)
	row, err := scope.QueryRow(ctx, `
		SELECT d.public_id, d.title, dc.page, dc.text
		FROM documents d
		JOIN document_chunks dc
		  ON dc.tenant_id = d.tenant_id AND dc.doc_id = d.doc_id AND dc.generation = d.generation
		WHERE d.tenant_id = ? AND d.app_id = ? AND d.doc_id = ?
		  AND d.status = 'ready' AND d.generation = ? AND dc.chunk_ord = ?`,
		tenantID, appID, docID, generation, ord)
	if err != nil {
		return Citation{}, false, err
	}
	switch err := row.Scan(&c.DocPublicID, &c.Title, &page, &text); {
	case errors.Is(err, sql.ErrNoRows):
		return Citation{}, false, nil
	case err != nil:
		return Citation{}, false, fmt.Errorf("knowledge: verify candidate: %w", err)
	}
	c.DocID = docID
	c.ChunkOrd = ord
	c.Page = page
	c.Text = truncateRunes(text, maxCitationText)
	return c, true, nil
}

func payloadInt(payload map[string]any, key string) (int64, bool) {
	v, ok := payload[key]
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	default:
		return 0, false
	}
}
