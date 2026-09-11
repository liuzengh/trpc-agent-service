// Package knowledge is the document pipeline: upload → parse → chunk →
// embed → index, then retrieve with server-side citations.
//
// The invariants follow the approved plan and are stated here once:
//
//   - SQL is the authority. Qdrant holds vectors to *find* candidates; every
//     candidate is re-verified against `documents` (tenant, app, kb binding,
//     status=ready, generation) before its text is read from
//     `document_chunks`. A vector payload can therefore be stale, tampered
//     with, or from another tenant without ever reaching an answer.
//   - ready is a CAS, not a plan. The index job writes chunks and points,
//     then flips the row to ready only if the generation it read is still
//     the generation on the row. A delete that lands mid-flight bumps the
//     generation first, so the late job fails its CAS and compensates
//     (re-enqueues cleanup) instead of resurrecting a deleted document.
//   - The generation is the epoch of everything. Points carry it, chunk rows
//     carry it, cleanup filters on it (`generation < new_generation` catches
//     late writes), and reconciliation uses it to notice stuck work.
package knowledge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/embedding"
	"github.com/liuzengh/trpc-agent-service/trpcservice/jobs"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/minio"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
)

// KindDocIndex is the job that parses, chunks, embeds and indexes one
// document generation. Cleanup is a separate kind because it must be able to
// run when the object is already gone.
const (
	KindDocIndex   = "doc_index"
	KindDocCleanup = "doc_cleanup"

	// DefaultMaxDocumentBytes bounds what the upload path accepts. MinIO's
	// own Get is bounded by the same number (storage/minio.maxObjectBytes).
	DefaultMaxDocumentBytes = 8 << 20

	// IndexLeaseGrace: work older than this is assumed stuck and is
	// re-enqueued by Reconcile.
	IndexLeaseGrace = 10 * time.Minute
)

// ErrConflict is returned for state collisions (duplicate public id whose
// delete is in flight, say). It maps to 409-style semantics at the caller.
var ErrConflict = errors.New("knowledge: conflicting document state")

// Document is the SQL row as callers see it.
type Document struct {
	DocID        int64
	PublicID     string
	KBID         int64
	Generation   int
	Title        string
	MIME         string
	SizeBytes    int64
	Status       string
	ChunkCount   int
	IndexedCount int
	ObjectKey    string
}

// Service wires the pipeline to its stores. All three of objects, embedder
// and vectors are required: a knowledge service with a missing store would
// accept uploads it cannot fulfil.
type Service struct {
	db             *controlplane.DB
	objects        *minio.Client
	embedder       embedding.Client
	vectors        *qdrant.Client
	maxDocument    int64
	chunkerVersion int
	log            *slog.Logger
}

// Options configures New.
type Options struct {
	DB             *controlplane.DB
	Objects        *minio.Client
	Embedder       embedding.Client
	Vectors        *qdrant.Client
	ChunkerVersion int
	Log            *slog.Logger
}

// New builds the service.
func New(opts Options) (*Service, error) {
	if opts.DB == nil || opts.Objects == nil || opts.Embedder == nil || opts.Vectors == nil {
		return nil, errors.New("knowledge: db, object store, embedder and vector store are all required")
	}
	if opts.ChunkerVersion <= 0 {
		opts.ChunkerVersion = 1
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Service{
		db:             opts.DB,
		objects:        opts.Objects,
		embedder:       opts.Embedder,
		vectors:        opts.Vectors,
		maxDocument:    DefaultMaxDocumentBytes,
		chunkerVersion: opts.ChunkerVersion,
		log:            opts.Log,
	}, nil
}

// IngestDoc puts the bytes and rows one document in 'uploaded' state, then
// enqueues the index job in the same transaction as the row — the only way
// "the bytes are here" and "somebody will index them" cannot disagree.
func (s *Service) IngestDoc(ctx context.Context, tenantID string, appID int64, kbPublicID, publicID, title, mime string, data []byte) (Document, error) {
	if publicID == "" {
		return Document{}, fmt.Errorf("knowledge: a document needs a public id")
	}
	if len(data) == 0 {
		return Document{}, fmt.Errorf("knowledge: refusing an empty document")
	}
	if int64(len(data)) > s.maxDocument {
		return Document{}, fmt.Errorf("knowledge: document is %d bytes, limit is %d", len(data), s.maxDocument)
	}
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return Document{}, err
	}

	var doc Document
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var kbID int64
		row := tx.QueryRow(ctx,
			"SELECT kb_id FROM knowledge_bases WHERE tenant_id = ? AND app_id = ? AND public_id = ? AND status = 'active'",
			tenantID, appID, kbPublicID)
		switch err := row.Scan(&kbID); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: no active knowledge base %q in this app", controlplane.ErrNotFound, kbPublicID)
		case err != nil:
			return fmt.Errorf("knowledge: resolve kb: %w", err)
		}

		// The row decides the generation *before* any bytes are written, so
		// the object key is a function of a fact that already exists.
		var (
			existingStatus string
			generation     int
			currentKey     string
			existingDocID  int64
		)
		err := tx.QueryRow(ctx, `
			SELECT status, generation, object_key, doc_id FROM documents
			WHERE tenant_id = ? AND app_id = ? AND kb_id = ? AND public_id = ?
			FOR UPDATE`, tenantID, appID, kbID, publicID).
			Scan(&existingStatus, &generation, &currentKey, &existingDocID)
		reIngest := false
		switch {
		case errors.Is(err, sql.ErrNoRows):
			generation = 1
		case err != nil:
			return fmt.Errorf("knowledge: inspect existing document: %w", err)
		case existingStatus == "deleting":
			return fmt.Errorf("%w: document %q is being deleted; wait for cleanup to finish", ErrConflict, publicID)
		case existingStatus == "deleted":
			generation++
			reIngest = true
		default:
			return fmt.Errorf("%w: document %q already exists (%s)", ErrConflict, publicID, existingStatus)
		}

		objectKey := objectKeyFor(tenantID, appID, publicID, generation)
		if err := s.objects.Put(ctx, objectKey, data, normalizeMIME(mime)); err != nil {
			return err
		}
		sum := sha256.Sum256(data)

		if reIngest {
			// The id came from the row this transaction just locked; using a
			// zero value here was measured to write nothing and leave the row
			// 'deleted' forever (TestDeleteRaceDoesNotResurrect caught it).
			doc.DocID = existingDocID
			res, err := tx.Exec(ctx, `
				UPDATE documents
				SET generation = ?, status = 'uploaded', title = ?, mime = ?, size_bytes = ?,
				    content_sha256 = ?, object_key = ?, chunk_count = 0, indexed_count = 0,
				    embedding_model = ?, embedding_dim = ?, chunker_version = ?, error = ''
				WHERE tenant_id = ? AND doc_id = ? AND status = 'deleted'`,
				generation, title, normalizeMIME(mime), len(data), hex.EncodeToString(sum[:]), objectKey,
				s.embedder.Model(), s.embedder.Dim(), s.chunkerVersion, tenantID, doc.DocID)
			if err != nil {
				return fmt.Errorf("knowledge: reopen document: %w", err)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return fmt.Errorf("%w: document %q changed state while reopening", ErrConflict, publicID)
			}
		} else {
			res, err := tx.Exec(ctx, `
				INSERT INTO documents
					(tenant_id, app_id, kb_id, public_id, generation, title, mime, size_bytes,
					 content_sha256, object_key, status, embedding_model, embedding_dim, chunker_version)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded', ?, ?, ?)`,
				tenantID, appID, kbID, publicID, generation, title, normalizeMIME(mime), len(data),
				hex.EncodeToString(sum[:]), objectKey, s.embedder.Model(), s.embedder.Dim(), s.chunkerVersion)
			if err != nil {
				return fmt.Errorf("knowledge: insert document: %w", err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				return fmt.Errorf("knowledge: document id: %w", err)
			}
			doc.DocID = id
		}

		doc.PublicID, doc.KBID, doc.Generation = publicID, kbID, generation
		doc.Title, doc.MIME, doc.SizeBytes, doc.Status = title, normalizeMIME(mime), int64(len(data)), "uploaded"
		doc.ObjectKey = objectKey

		if err := jobs.Enqueue(ctx, scope, tx, tenantID, KindDocIndex,
			fmt.Sprintf("doc:%d:g%d:index", doc.DocID, generation),
			map[string]any{"doc_id": doc.DocID, "generation": generation}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Document{}, err
	}
	return doc, nil
}

// DeleteDocument tombstones first and cleans later: the generation bump is
// what makes retrieval and download refuse *immediately*, while the actual
// point/object removal is a job that can retry.
func (s *Service) DeleteDocument(ctx context.Context, tenantID string, appID int64, kbPublicID, publicID string) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var (
			docID      int64
			generation int
			objectKey  string
			status     string
		)
		err := tx.QueryRow(ctx, `
			SELECT d.doc_id, d.generation, d.object_key, d.status
			FROM documents d
			JOIN knowledge_bases kb ON kb.kb_id = d.kb_id AND kb.tenant_id = d.tenant_id
			WHERE d.tenant_id = ? AND d.app_id = ? AND kb.public_id = ? AND d.public_id = ?
			FOR UPDATE`, tenantID, appID, kbPublicID, publicID).
			Scan(&docID, &generation, &objectKey, &status)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return controlplane.ErrNotFound
		case err != nil:
			return fmt.Errorf("knowledge: lock document: %w", err)
		case status == "deleted", status == "deleting":
			return nil // already on its way out; delete is idempotent
		}
		newGeneration := generation + 1
		if _, err := tx.Exec(ctx, `
			UPDATE documents SET status = 'deleting', generation = ?, error = ''
			WHERE tenant_id = ? AND doc_id = ?`, newGeneration, tenantID, docID); err != nil {
			return fmt.Errorf("knowledge: tombstone document: %w", err)
		}
		return jobs.Enqueue(ctx, scope, tx, tenantID, KindDocCleanup,
			fmt.Sprintf("doc:%d:g%d:cleanup", docID, newGeneration),
			map[string]any{
				"doc_id": docID, "delete_generation": newGeneration, "object_key": objectKey,
			})
	})
}

// CleanupDocument is the cleanup job body: remove every point of an older
// generation and the object bytes, then mark the row deleted — but only if
// the row is still *this* deletion (a re-ingest that raced ahead must win).
func (s *Service) CleanupDocument(ctx context.Context, tenantID string, docID int64, deleteGeneration int, objectKey string) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	var currentGeneration int
	row, err := scope.QueryRow(ctx,
		"SELECT generation FROM documents WHERE tenant_id = ? AND doc_id = ?", tenantID, docID)
	if err != nil {
		return err
	}
	switch err := row.Scan(&currentGeneration); {
	case errors.Is(err, sql.ErrNoRows):
		// The row is gone entirely; the points still need removing.
	case err != nil:
		return fmt.Errorf("knowledge: load document for cleanup: %w", err)
	}

	// Delete every point strictly older than the row's current generation:
	// that covers the pre-delete generations *and* an index job that was
	// still in flight when the delete landed (its points carry the old
	// generation and would otherwise be invisible leaks). The retired
	// generations are named explicitly because Qdrant's match has no
	// comparison operators, and the list is bounded by re-ingest count.
	retired := make([]any, 0, currentGeneration)
	for g := 1; g < currentGeneration; g++ {
		retired = append(retired, g)
	}
	if len(retired) > 0 {
		if err := s.vectors.DeleteByFilter(ctx, qdrant.Filter{Must: []qdrant.Match{
			{Key: "tenant_id", Value: tenantID},
			{Key: "doc_id", Value: docID},
			{Key: "generation", Any: retired},
		}}); err != nil {
			return err
		}
	}

	if objectKey != "" {
		if err := s.objects.Delete(ctx, objectKey); err != nil {
			return err
		}
	}
	if _, err := scope.Exec(ctx, `
		UPDATE documents SET status = 'deleted', indexed_count = 0
		WHERE tenant_id = ? AND doc_id = ? AND status = 'deleting'`, tenantID, docID); err != nil {
		return fmt.Errorf("knowledge: finalize deletion: %w", err)
	}
	if _, err := scope.Exec(ctx, `
		DELETE FROM document_chunks WHERE tenant_id = ? AND doc_id = ? AND generation < ?`,
		tenantID, docID, currentGeneration); err != nil {
		return fmt.Errorf("knowledge: drop retired chunks: %w", err)
	}
	return nil
}

// objectKeyFor is the deterministic key. Deterministic because cleanup must
// be able to find an object whose SQL row was already overwritten by a
// re-ingest; the generation makes each ingest's bytes a distinct object.
func objectKeyFor(tenantID string, appID int64, publicID string, generation int) string {
	return fmt.Sprintf("%s/apps/%d/docs/%s/g%d/raw", sanitizeSegment(tenantID), appID, publicID, generation)
}

// sanitizeSegment maps anything outside the object-key charset to '-'. The
// tenant id is already constrained by earlier layers; this is the belt to
// that suspenders, in the one place a key is formed.
func sanitizeSegment(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// normalizeMIME keeps the accepted set small and explicit; the parser is a
// text reader in this phase, and pretending to accept a PDF upload would
// turn into a failed index job later.
func normalizeMIME(mime string) string {
	switch mime {
	case "text/plain", "text/markdown", "text/csv":
		return mime
	default:
		return "text/plain"
	}
}

// jsonPayload is the small helper handlers use to decode their payload.
func jsonPayload(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return fmt.Errorf("knowledge: job payload is empty")
	}
	return json.Unmarshal(raw, out)
}
