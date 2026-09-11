package knowledge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/jobs"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
)

// hashedUUID renders a digest as the UUID form Qdrant accepts everywhere.
func hashedUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	h := hex.EncodeToString(sum[:16])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// indexPayload is the doc_index job's body.
type indexPayload struct {
	DocID      int64 `json:"doc_id"`
	Generation int   `json:"generation"`
}

// cleanupPayload is the doc_cleanup job's body.
type cleanupPayload struct {
	DocID            int64  `json:"doc_id"`
	DeleteGeneration int    `json:"delete_generation"`
	ObjectKey        string `json:"object_key"`
}

// IndexDocument is the parse → chunk → embed → upsert pipeline. Every write
// it performs is repeatable, and its final ready-flip is a CAS: if the
// document was tombstoned (or re-ingested) while this ran, the flip writes
// zero rows and the job compensates instead of resurrecting the document.
func (s *Service) IndexDocument(ctx context.Context, tenantID string, docID int64, generation int) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	var (
		status     string
		currentGen int
		objectKey  string
		kbID       int64
		appID      int64
		embedModel string
		embedDim   int
		embVersion int
	)
	row, err := scope.QueryRow(ctx, `
		SELECT status, generation, object_key, kb_id, app_id, embedding_model, embedding_dim, chunker_version
		FROM documents WHERE tenant_id = ? AND doc_id = ?`, tenantID, docID)
	if err != nil {
		return err
	}
	switch err := row.Scan(&status, &currentGen, &objectKey, &kbID, &appID, &embedModel, &embedDim, &embVersion); {
	case errors.Is(err, sql.ErrNoRows):
		return nil // the document is gone; nothing to index
	case err != nil:
		return fmt.Errorf("knowledge: load document: %w", err)
	}
	if currentGen != generation {
		// A newer generation exists (delete or re-ingest landed first). The
		// caller for the newer generation is already queued; this run ends.
		return nil
	}
	switch status {
	case "ready":
		return nil
	case "deleting", "deleted":
		return nil
	case "uploaded":
		if _, err := scope.Exec(ctx, `
			UPDATE documents SET status = 'indexing' WHERE tenant_id = ? AND doc_id = ? AND status = 'uploaded'`,
			tenantID, docID); err != nil {
			return fmt.Errorf("knowledge: open indexing: %w", err)
		}
	case "indexing":
		// A retry of this same job, or reconciliation re-running it.
	default:
		return nil
	}

	if embedModel != s.embedder.Model() || embedDim != s.embedder.Dim() {
		return fmt.Errorf("knowledge: document %d was ingested with embedding %s/%d, this deployment pins %s/%d; re-ingest or re-index is an operator action",
			docID, embedModel, embedDim, s.embedder.Model(), s.embedder.Dim())
	}

	raw, err := s.objects.Get(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("knowledge: fetch object %s: %w", objectKey, err)
	}
	chunks, err := ChunkText(string(raw), WindowRunes, OverlapRunes)
	if err != nil {
		return s.failDocument(ctx, tenantID, docID, generation, err)
	}

	vectors, err := s.embedInBatches(ctx, chunkTexts(chunks))
	if err != nil {
		return err
	}

	// SQL chunks first: a citation reads text from here, so the row must
	// exist before the point that may select it. Replacing the generation's
	// chunks makes a retry idempotent.
	if err := s.replaceChunks(ctx, tenantID, docID, generation, embVersion, chunks); err != nil {
		return err
	}

	points := make([]qdrant.Point, 0, len(chunks))
	for i, c := range chunks {
		points = append(points, qdrant.Point{
			ID:     PointID(tenantID, docID, generation, c.Ord, embVersion),
			Vector: vectors[i],
			Payload: map[string]any{
				"tenant_id":         tenantID,
				"app_id":            appID,
				"kb_id":             kbID,
				"doc_id":            docID,
				"generation":        generation,
				"chunk_ord":         c.Ord,
				"embedding_version": embVersion,
			},
		})
	}
	if err := s.vectors.Upsert(ctx, points); err != nil {
		return err
	}

	// The ready flip: only if this generation is still the row's, and the
	// row is still indexing. Zero rows means the world moved (tombstone or
	// re-ingest) while this job ran — the compensation is to schedule the
	// cleanup of the points just written, because a delete that already ran
	// its cleanup will not see them.
	res, err := scope.Exec(ctx, `
		UPDATE documents
		SET status = 'ready', chunk_count = ?, indexed_count = ?, error = ''
		WHERE tenant_id = ? AND doc_id = ? AND generation = ? AND status IN ('uploaded', 'indexing')`,
		len(chunks), len(chunks), tenantID, docID, generation)
	if err != nil {
		return fmt.Errorf("knowledge: ready flip: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		s.log.Warn("knowledge: index finished after the document moved; compensating",
			"tenant", tenantID, "doc_id", docID, "generation", generation)
		return jobs.Enqueue(ctx, scope, nil, tenantID, KindDocCleanup,
			fmt.Sprintf("doc:%d:g%d:cleanup:late-index:%d", docID, generation+1, time.Now().UnixNano()),
			cleanupPayload{DocID: docID, DeleteGeneration: generation + 1, ObjectKey: ""})
	}
	return nil
}

// failDocument records a terminal parse failure. It is a CAS too: a document
// that vanished mid-parse must not be resurrected as 'failed' either.
func (s *Service) failDocument(ctx context.Context, tenantID string, docID int64, generation int, cause error) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	if _, err := scope.Exec(ctx, `
		UPDATE documents SET status = 'failed', error = ?
		WHERE tenant_id = ? AND doc_id = ? AND generation = ? AND status = 'indexing'`,
		truncateRunes(cause.Error(), 500), tenantID, docID, generation); err != nil {
		return fmt.Errorf("knowledge: mark failed: %w", err)
	}
	return nil
}

// embedInBatches keeps one request bounded: an embedding endpoint that
// chokes on 200 inputs turns one bad document into one bad request, not a
// permanently failing job.
func (s *Service) embedInBatches(ctx context.Context, texts []string) ([][]float32, error) {
	const batch = 16
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += batch {
		end := start + batch
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := s.embedder.Embed(ctx, texts[start:end])
		if err != nil {
			return nil, fmt.Errorf("knowledge: embed chunks %d..%d: %w", start, end, err)
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func chunkTexts(chunks []Chunk) []string {
	out := make([]string, len(chunks))
	for i, c := range chunks {
		out[i] = c.Text
	}
	return out
}

// replaceChunks swaps the generation's chunk rows in one transaction:
// delete-then-insert makes a retry land on the same state, never on a
// half-old half-new mixture.
func (s *Service) replaceChunks(ctx context.Context, tenantID string, docID int64, generation, embVersion int, chunks []Chunk) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		if _, err := tx.Exec(ctx,
			"DELETE FROM document_chunks WHERE tenant_id = ? AND doc_id = ? AND generation = ?",
			tenantID, docID, generation); err != nil {
			return fmt.Errorf("knowledge: clear old chunks: %w", err)
		}
		for _, c := range chunks {
			if _, err := tx.Exec(ctx, `
				INSERT INTO document_chunks (tenant_id, doc_id, generation, chunk_ord, page, text, embedding_version)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				tenantID, docID, generation, c.Ord, c.Page, c.Text, embVersion); err != nil {
				return fmt.Errorf("knowledge: insert chunk %d: %w", c.Ord, err)
			}
		}
		return nil
	})
}

// HandleJob dispatches the knowledge job kinds. roles.go registers it once
// for both kinds.
func (s *Service) HandleJob(ctx context.Context, tenantID, kind string, payload []byte) error {
	switch kind {
	case KindDocIndex:
		var p indexPayload
		if err := jsonPayload(payload, &p); err != nil {
			return err
		}
		return s.IndexDocument(ctx, tenantID, p.DocID, p.Generation)
	case KindDocCleanup:
		var p cleanupPayload
		if err := jsonPayload(payload, &p); err != nil {
			return err
		}
		return s.CleanupDocument(ctx, tenantID, p.DocID, p.DeleteGeneration, p.ObjectKey)
	default:
		return fmt.Errorf("knowledge: unknown job kind %q", kind)
	}
}

// Reconcile re-enqueues work that a crash left behind: documents stuck in
// indexing (their worker died before the ready flip) and deletions stuck in
// deleting (the cleanup job was lost). Re-enqueueing is safe because both
// handlers are idempotent; the time-bucketed key lets a new attempt exist
// beside the old row instead of being deduplicated away.
func (s *Service) Reconcile(ctx context.Context, tenantID string) (int, error) {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return 0, err
	}
	rows, err := scope.Query(ctx, `
		SELECT doc_id, generation, status
		FROM documents
		WHERE tenant_id = ? AND status IN ('indexing', 'deleting')
		  AND updated_at < UTC_TIMESTAMP(6) - INTERVAL ? SECOND`, tenantID, int(IndexLeaseGrace.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("knowledge: reconcile scan: %w", err)
	}
	defer rows.Close()
	type stuck struct {
		docID      int64
		generation int
		status     string
	}
	var found []stuck
	for rows.Next() {
		var st stuck
		if err := rows.Scan(&st.docID, &st.generation, &st.status); err != nil {
			return 0, fmt.Errorf("knowledge: reconcile scan row: %w", err)
		}
		found = append(found, st)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	bucket := time.Now().Unix() / int64(IndexLeaseGrace.Seconds())
	for _, st := range found {
		switch st.status {
		case "indexing":
			if err := jobs.Enqueue(ctx, scope, nil, tenantID, KindDocIndex,
				fmt.Sprintf("doc:%d:g%d:index:reconcile:%d", st.docID, st.generation, bucket),
				indexPayload{DocID: st.docID, Generation: st.generation}); err != nil {
				return 0, err
			}
		case "deleting":
			if err := jobs.Enqueue(ctx, scope, nil, tenantID, KindDocCleanup,
				fmt.Sprintf("doc:%d:g%d:cleanup:reconcile:%d", st.docID, st.generation, bucket),
				cleanupPayload{DocID: st.docID, DeleteGeneration: st.generation}); err != nil {
				return 0, err
			}
		}
	}
	return len(found), nil
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
